package nessie

import (
	"context"
	"strings"
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

func providerEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "nessie",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
	}
	return e
}

// TestWarehouseFactsEnvDerivesLocationAndS3 covers docs/planning/08 D8's
// automatic-derivation path: Catalog.spec.warehouseRef's resolved facts
// produce the identical Quarkus env-var shape defaultWarehouseEnv's
// explicit-config path already established (D10), just sourced from
// WarehouseFacts instead of static Provider configuration.
func TestWarehouseFactsEnvDerivesLocationAndS3(t *testing.T) {
	t.Parallel()
	facts := &reconciler.WarehouseFacts{
		Bucket: "lake", Prefix: "iceberg-warehouse/",
		S3Internal: "lake-minio:9000", S3SecretRef: "minio-root",
	}
	secrets := map[string]map[string]string{
		"minio-root": {"username": "minioadmin", "password": "minioadmin-pw"},
	}
	env, err := warehouseFactsEnv(facts, secrets)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"NESSIE_CATALOG_DEFAULT_WAREHOUSE":                            "warehouse",
		"NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION":                "s3://lake/iceberg-warehouse/",
		"NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT":          "http://lake-minio:9000",
		"NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_PATH_STYLE_ACCESS": "true",
		"NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_REGION":            "us-east-1",
		"NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_AUTH_TYPE":         "STATIC",
		"NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY_NAME":   "warehouse-creds",
		"NESSIE_CATALOG_SECRETS_WAREHOUSE_CREDS_NAME":                 "minioadmin",
		"NESSIE_CATALOG_SECRETS_WAREHOUSE_CREDS_SECRET":               "minioadmin-pw",
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("env[%q] = %q, want %q", k, env[k], v)
		}
	}
	if len(env) != len(want) {
		t.Errorf("env has %d keys, want %d: %v", len(env), len(want), env)
	}
}

// TestWarehouseFactsEnvErrorsWithoutSecretRefListed mirrors
// defaultWarehouseEnv's own "must also be listed in spec.secretRefs" error
// convention: a facts.S3SecretRef the engine resolved as a graph fact but
// that the nessie Provider itself never listed in spec.secretRefs cannot
// have its credential values resolved.
func TestWarehouseFactsEnvErrorsWithoutSecretRefListed(t *testing.T) {
	t.Parallel()
	facts := &reconciler.WarehouseFacts{
		Bucket: "lake", Prefix: "iceberg/", S3Internal: "lake-minio:9000", S3SecretRef: "minio-root",
	}
	_, err := warehouseFactsEnv(facts, map[string]map[string]string{})
	if err == nil || !strings.Contains(err.Error(), "spec.secretRefs") {
		t.Fatalf("err = %v, want a spec.secretRefs error", err)
	}
}

// TestEnsureDerivedWarehouseConfigSkippedWithoutFacts covers the "not
// applicable" no-op: no warehouseRef resolved (req.WarehouseFacts nil)
// leaves the container alone.
func TestEnsureDerivedWarehouseConfigSkippedWithoutFacts(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := providerEnvelope("catalog-svc", map[string]any{})
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	if err := ensureDerivedWarehouseConfig(context.Background(), req, "catalog-svc"); err != nil {
		t.Fatal(err)
	}
	if rt.MutationCount != 0 {
		t.Errorf("MutationCount = %d, want 0 (no facts, nothing to do)", rt.MutationCount)
	}
}

// TestEnsureDerivedWarehouseConfigSkippedWhenExplicitOverrideSet covers
// additive coexistence (docs/planning/08 D8's explicit instruction): an
// explicit configuration.defaultWarehouseLocation always wins outright over
// warehouseRef-derived facts — no removal of the pre-D8 explicit path.
func TestEnsureDerivedWarehouseConfigSkippedWhenExplicitOverrideSet(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := providerEnvelope("catalog-svc", map[string]any{"defaultWarehouseLocation": "s3://explicit/loc/"})
	req := reconciler.Request{
		Resource: env, Provider: env, Runtime: rt,
		WarehouseFacts: &reconciler.WarehouseFacts{Bucket: "lake", Prefix: "iceberg/", S3Internal: "lake-minio:9000", S3SecretRef: "minio-root"},
	}
	if err := ensureDerivedWarehouseConfig(context.Background(), req, "catalog-svc"); err != nil {
		t.Fatal(err)
	}
	if rt.MutationCount != 0 {
		t.Errorf("MutationCount = %d, want 0 (explicit override must win, no derived recreate)", rt.MutationCount)
	}
}

// TestEnsureDerivedWarehouseConfigRecreatesOnceThenIdempotent is the core
// D8 idempotency proof: no new drift-fingerprint bookkeeping is needed
// because EnsureContainer's own spec-hash comparison already makes a
// repeated call with unchanged facts a no-op, and a changed/first-time set
// of facts a clean recreate — the exact bar CLAUDE.md's "every Ensure*
// runtime method must be idempotent" conformance rule sets.
func TestEnsureDerivedWarehouseConfigRecreatesOnceThenIdempotent(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := providerEnvelope("catalog-svc", map[string]any{})
	facts := &reconciler.WarehouseFacts{Bucket: "lake", Prefix: "iceberg/", S3Internal: "lake-minio:9000", S3SecretRef: "minio-root"}
	req := reconciler.Request{
		Resource: env, Provider: env, Runtime: rt,
		Secrets:        map[string]map[string]string{"minio-root": {"username": "u", "password": "p"}},
		WarehouseFacts: facts,
	}
	if err := ensureDerivedWarehouseConfig(context.Background(), req, "catalog-svc"); err != nil {
		t.Fatal(err)
	}
	after1 := rt.MutationCount
	if after1 == 0 {
		t.Fatal("MutationCount = 0 after first call, want at least one create")
	}
	ctr, found, err := rt.Inspect(context.Background(), "catalog-svc")
	if err != nil || !found {
		t.Fatalf("Inspect after first call: found=%v err=%v", found, err)
	}
	if got := ctr.Env["NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION"]; got != "s3://lake/iceberg/" {
		t.Errorf("container env location = %q, want %q", got, "s3://lake/iceberg/")
	}

	// Second call, identical facts: zero additional mutating calls.
	if err := ensureDerivedWarehouseConfig(context.Background(), req, "catalog-svc"); err != nil {
		t.Fatal(err)
	}
	if rt.MutationCount != after1 {
		t.Errorf("MutationCount after second identical call = %d, want unchanged %d", rt.MutationCount, after1)
	}
}
