package trino

import (
	"context"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

func providerEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "trino",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
	}
	return e
}

// TestReconcileInstanceProvisionsCoordinatorAndWorkers proves the wiring
// reaches the runtime port for both the coordinator (a single instance) and
// the worker set (ContainerSpec.Replicas + StableIdentity: false) —
// independent of readiness, which the fake runtime cannot serve real HTTP
// for (waitCoordinatorReady's retry loop otherwise burns its full
// hardcoded timeout; a short context deadline makes ctx.Done() return
// almost immediately instead, the same pattern prometheus_test.go and
// redpanda_test.go document).
func TestReconcileInstanceProvisionsCoordinatorAndWorkers(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("lake-trino", map[string]any{"workers": 3})
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	p := New()
	if _, err := p.Reconcile(ctx, req); err == nil {
		t.Fatal("want an error: the fake runtime cannot serve /v1/info's real HTTP request")
	}

	coordState, found, err := rt.Inspect(context.Background(), "lake-trino-coordinator")
	if err != nil || !found {
		t.Fatalf("coordinator Inspect: found=%v err=%v", found, err)
	}
	if len(coordState.Env) == 0 && coordState.ID == "" {
		t.Fatal("coordinator was not created")
	}

	workerState, found, err := rt.Inspect(context.Background(), "lake-trino-worker")
	if err != nil || !found {
		t.Fatalf("worker set Inspect: found=%v err=%v", found, err)
	}
	if workerState.ReadyReplicas != 3 {
		t.Errorf("ReadyReplicas = %d, want 3", workerState.ReadyReplicas)
	}
}

// TestReconcileWithoutCatalogRefWritesNoCatalogFile: catalogRef is optional
// — a trino Provider with none declared still reconciles (up to the
// fake-runtime HTTP limit above) with no etc/catalog/lakehouse.properties
// file at all.
func TestReconcileWithoutCatalogRefWritesNoCatalogFile(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("lake-trino-nocat", nil)
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _ = New().Reconcile(ctx, req)

	if _, err := rt.ReadFile(context.Background(), "lake-trino-nocat-coordinator", lakehouseCatalogPath); err == nil {
		t.Fatal("want an error reading a catalog file that was never written")
	}
}

// TestReconcileWithCatalogRefRequiresSecretRefListed proves catalogFile's
// error path: CatalogFacts naming a credential SecretReference that this
// Provider's own spec.secretRefs does not list fails clearly, rather than
// silently writing an unauthenticated (or empty-credential) catalog config.
func TestReconcileWithCatalogRefRequiresSecretRefListed(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("lake-trino-badsecret", map[string]any{"catalogRef": map[string]any{"name": "lakehouse-catalog"}})
	req := reconciler.Request{
		Resource: env,
		Provider: env,
		Runtime:  rt,
		CatalogFacts: &reconciler.CatalogFacts{
			RestInternal: "http://catalog-svc:19120/iceberg",
			S3Internal:   "lake-minio:9000",
			S3SecretRef:  "minio-creds",
		},
		// Secrets deliberately does not include "minio-creds": this
		// Provider's own spec.secretRefs never listed it.
	}
	_, err := New().Reconcile(context.Background(), req)
	if err == nil {
		t.Fatal("want an error: minio-creds is not resolvable from this Provider's own secretRefs")
	}
	if !strings.Contains(err.Error(), "minio-creds") || !strings.Contains(err.Error(), "secretRefs") {
		t.Errorf("error does not name the fix: %v", err)
	}
}

// TestReconcileWithCatalogRefWritesCatalogFile proves the happy path: with
// CatalogFacts resolved and the credential present in req.Secrets, the
// rendered etc/catalog/lakehouse.properties lands on the coordinator
// (and, per the doc comment on reconcileInstance, every worker) with the
// exact facts/credentials embedded — never a guessed value (ADR 015).
func TestReconcileWithCatalogRefWritesCatalogFile(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("lake-trino-cat", map[string]any{"catalogRef": map[string]any{"name": "lakehouse-catalog"}})
	req := reconciler.Request{
		Resource: env,
		Provider: env,
		Runtime:  rt,
		Secrets:  map[string]map[string]string{"minio-creds": {"username": "minioadmin", "password": "minioadminpw"}},
		CatalogFacts: &reconciler.CatalogFacts{
			RestInternal: "http://catalog-svc:19120/iceberg",
			S3Internal:   "lake-minio:9000",
			S3SecretRef:  "minio-creds",
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _ = New().Reconcile(ctx, req)

	live, err := rt.ReadFile(context.Background(), "lake-trino-cat-coordinator", lakehouseCatalogPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	kv := parseProperties(live)
	want := map[string]string{
		"connector.name":           "iceberg",
		"iceberg.catalog.type":     "rest",
		"iceberg.rest-catalog.uri": "http://catalog-svc:19120/iceberg",
		"s3.endpoint":              "http://lake-minio:9000",
		"s3.aws-access-key":        "minioadmin",
		"s3.aws-secret-key":        "minioadminpw",
	}
	for k, v := range want {
		if kv[k] != v {
			t.Errorf("catalog config %s = %q, want %q", k, kv[k], v)
		}
	}

	// Same content on the worker set. workers defaults to 1, and Replicas
	// <= 1 without StableIdentity stays the single-container shape
	// byte-for-byte (no ordinal suffix) — see runtime.ContainerSpec's doc
	// comment.
	workerLive, err := rt.ReadFile(context.Background(), "lake-trino-cat-worker", lakehouseCatalogPath)
	if err != nil {
		t.Fatalf("worker ReadFile: %v", err)
	}
	if string(workerLive) != string(live) {
		t.Error("worker catalog config differs from coordinator's — every node must see identical config")
	}
}

func TestProbeReportsNotFoundWhenNeverReconciled(t *testing.T) {
	rt := fakeruntime.New()
	env := providerEnvelope("never-applied", nil)
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	st, err := New().Probe(context.Background(), req)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if st.IsReady() {
		t.Error("Ready should not be true for a Provider that was never reconciled")
	}
}

func TestValidateSpecRejectsInvalidWorkers(t *testing.T) {
	p := New()
	cases := []struct {
		name string
		cfg  map[string]any
		ok   bool
	}{
		{"unset defaults to 1", nil, true},
		{"valid 3", map[string]any{"workers": 3}, true},
		{"zero rejected", map[string]any{"workers": 0}, false},
		{"negative rejected", map[string]any{"workers": -1}, false},
		{"non-integer rejected", map[string]any{"workers": "three"}, false},
		{"empty image rejected", map[string]any{"image": ""}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := provider.Provider{Configuration: tc.cfg}
			err := p.ValidateSpec(cfg)
			if tc.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.ok && err == nil {
				t.Error("want an error")
			}
		})
	}
}
