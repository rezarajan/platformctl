package engine

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/adapters/secrets/env"
	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/clock"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// fakeLineageProvider records the endpoint it receives.
type fakeLineageProvider struct {
	noop.Provider
	received *lineage.LineageEndpoint
}

func (f *fakeLineageProvider) Type() string { return "fakelineage" }

func (f *fakeLineageProvider) ConfigureLineage(_ context.Context, _ reconciler.Request, ep lineage.LineageEndpoint) error {
	f.received = &ep
	return nil
}

func envelope(kind, name string, spec map[string]any, observers ...string) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = kind
	e.Metadata.Name = name
	e.Spec = spec
	for _, o := range observers {
		e.Metadata.Observers = append(e.Metadata.Observers, resource.ObserverRef{Name: o})
	}
	return e
}

func newTestEngine(t *testing.T, reg *registry.Registry) *Engine {
	return &Engine{
		Registry:   reg,
		StateStore: localfile.New(filepath.Join(t.TempDir(), "state.json")),
		Clock:      &clock.Fake{T: time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)},
	}
}

func applyAll(t *testing.T, eng *Engine, envelopes []resource.Envelope) Result {
	t.Helper()
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}
	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	p, err := plan.Compute(envelopes, st, g)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	result, err := eng.Apply(context.Background(), p, envelopes, g)
	if err != nil {
		t.Fatalf("apply: %v (failed: %v)", err, result.Failed)
	}
	return result
}

// TestLineageEndpointForwarded covers the Phase 3 exit criterion: a resource
// with metadata.observers whose provider is LineageAware receives a
// correctly-populated LineageEndpoint.
func TestLineageEndpointForwarded(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)

	lineageProv := &fakeLineageProvider{}
	reg.RegisterProvider("fakelineage", func() reconciler.Provider { return lineageProv }, "")
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	envelopes := []resource.Envelope{
		envelope("Provider", "local-marquez", map[string]any{
			"type":          "noop",
			"runtime":       map[string]any{"type": "fake"},
			"configuration": map[string]any{"url": "http://local-marquez:5000"},
		}),
		envelope("Provider", "observed-provider", map[string]any{
			"type":    "fakelineage",
			"runtime": map[string]any{"type": "fake"},
		}, "local-marquez"),
	}

	applyAll(t, newTestEngine(t, reg), envelopes)

	if lineageProv.received == nil {
		t.Fatal("LineageAware provider never received an endpoint")
	}
	if lineageProv.received.URL != "http://local-marquez:5000" {
		t.Errorf("endpoint URL = %q, want %q", lineageProv.received.URL, "http://local-marquez:5000")
	}
	if lineageProv.received.Namespace != "datascape" {
		t.Errorf("endpoint namespace = %q, want %q", lineageProv.received.Namespace, "datascape")
	}
}

// TestLineageNotConsumedCondition covers the Phase 3 exit criterion: an
// observers entry on a resource whose provider is not LineageAware produces
// the informational LineageEndpointDeclaredNotConsumed condition and does not
// block Ready.
func TestLineageNotConsumedCondition(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	envelopes := []resource.Envelope{
		envelope("Provider", "local-marquez", map[string]any{
			"type":          "noop",
			"runtime":       map[string]any{"type": "fake"},
			"configuration": map[string]any{"url": "http://local-marquez:5000"},
		}),
		envelope("Provider", "plain-provider", map[string]any{
			"type":    "noop",
			"runtime": map[string]any{"type": "fake"},
		}, "local-marquez"),
	}

	eng := newTestEngine(t, reg)
	applyAll(t, eng, envelopes)

	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs, ok := st.Resources[resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "plain-provider"}]
	if !ok {
		t.Fatal("plain-provider missing from state")
	}
	if !rs.Status.IsReady() {
		t.Errorf("resource with unconsumed observers is not Ready; conditions: %+v", rs.Status.Conditions)
	}
	foundInfo := false
	for _, c := range rs.Status.Conditions {
		if c.Reason == status.ReasonLineageNotConsumed {
			foundInfo = true
		}
	}
	if !foundInfo {
		t.Errorf("missing %s condition; conditions: %+v", status.ReasonLineageNotConsumed, rs.Status.Conditions)
	}
}

// driftingProvider reports drift until it has been reconciled a second time,
// simulating a resource killed out-of-band and then healed.
type driftingProvider struct {
	noop.Provider
}

func (d *driftingProvider) Type() string { return "drifty" }

func (d *driftingProvider) Probe(_ context.Context, _ reconciler.Request) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	if d.ReconcileCount < 2 {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "GoneMissing"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "GoneMissing"}, now)
		return st, nil
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "Healthy"}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
	return st, nil
}

func driftFixture(t *testing.T) (*Engine, *driftingProvider, []resource.Envelope) {
	t.Helper()
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	prov := &driftingProvider{}
	reg.RegisterProvider("drifty", func() reconciler.Provider { return prov }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	envelopes := []resource.Envelope{
		envelope("Provider", "drifter", map[string]any{
			"type":    "drifty",
			"runtime": map[string]any{"type": "fake"},
		}),
	}
	return newTestEngine(t, reg), prov, envelopes
}

// TestApplyHealsDrift: an unchanged manifest set re-applied with HealDrift
// probes plan-noop resources and re-reconciles the drifted ones.
func TestApplyHealsDrift(t *testing.T) {
	eng, prov, envelopes := driftFixture(t)
	applyAll(t, eng, envelopes)
	if prov.ReconcileCount != 1 {
		t.Fatalf("ReconcileCount after first apply = %d, want 1", prov.ReconcileCount)
	}

	eng.HealDrift = true
	result := applyAll(t, eng, envelopes)
	if prov.ReconcileCount != 2 {
		t.Errorf("ReconcileCount after healing apply = %d, want 2 (drift must trigger re-reconcile)", prov.ReconcileCount)
	}
	if len(result.Succeeded) != 1 {
		t.Errorf("healed resources reported = %d, want 1", len(result.Succeeded))
	}

	// No drift anymore: a further apply must be a true no-op.
	applyAll(t, eng, envelopes)
	if prov.ReconcileCount != 2 {
		t.Errorf("ReconcileCount after clean apply = %d, want 2 (no drift, no reconcile)", prov.ReconcileCount)
	}
}

// TestApplyWithoutHealDriftLeavesDrift: with the gate off, apply trusts
// recorded state and never probes.
func TestApplyWithoutHealDriftLeavesDrift(t *testing.T) {
	eng, prov, envelopes := driftFixture(t)
	applyAll(t, eng, envelopes)
	applyAll(t, eng, envelopes)
	if prov.ReconcileCount != 1 {
		t.Errorf("ReconcileCount = %d, want 1 (HealDrift off must not reconcile)", prov.ReconcileCount)
	}
}

type externalConfigProvider struct {
	noop.Provider
	configured int
}

func (p *externalConfigProvider) Type() string { return "external-config" }

func (p *externalConfigProvider) Reconcile(_ context.Context, req reconciler.Request) (status.Status, error) {
	if req.Resource.Kind != "Provider" {
		return status.Status{}, errors.New("external resource used Reconcile")
	}
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ProviderReady"}, time.Now())
	return st, nil
}

func (p *externalConfigProvider) ConfigureExternal(_ context.Context, req reconciler.Request) (status.Status, error) {
	if req.Resource.Kind != "Dataset" {
		return status.Status{}, errors.New("unexpected external resource")
	}
	p.configured++
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ExternalConfigured"}, time.Now())
	return st, nil
}

func TestExternalProviderRefUsesConfigureExternal(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	prov := &externalConfigProvider{}
	reg.RegisterProvider("external-config", func() reconciler.Provider { return prov }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	envelopes := []resource.Envelope{
		envelope("Provider", "cfg", map[string]any{
			"type":    "external-config",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Dataset", "raw", map[string]any{
			"external":    true,
			"providerRef": map[string]any{"name": "cfg"},
			"bucket":      "raw",
			"format":      "json",
		}),
	}
	applyAll(t, newTestEngine(t, reg), envelopes)
	if prov.configured != 1 {
		t.Fatalf("ConfigureExternal calls = %d, want 1", prov.configured)
	}
}

func TestApplyRefusesLegacyOrphanUnknown(t *testing.T) {
	eng := newTestEngine(t, registry.New(featuregate.NewRegistry()))
	key := resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "legacy"}
	if err := eng.StateStore.Save(context.Background(), state.State{
		Version: state.CurrentVersion,
		Resources: map[resource.Key]state.ResourceState{
			key: {SpecHash: "old", Lifecycle: resource.Managed.String()},
		},
	}); err != nil {
		t.Fatal(err)
	}
	g, err := graph.Build(nil)
	if err != nil {
		t.Fatal(err)
	}
	p := plan.Plan{
		Entries: []plan.Entry{{Key: key, Action: plan.ActionOrphanUnknown}},
		Levels:  [][]resource.Key{{key}},
	}
	result, err := eng.Apply(context.Background(), p, nil, g)
	if err == nil {
		t.Fatal("apply accepted orphan-unknown delete")
	}
	if _, ok := result.Failed[key]; !ok {
		t.Fatalf("result missing failed orphan key: %+v", result.Failed)
	}
}

// TestApplyRefusesProtectedDelete guards docs/planning/08 A5: apply must
// fail an authoritative delete for a resource whose last-applied manifest
// carried metadata.protect: true, naming the resource and the remedy.
func TestApplyRefusesProtectedDelete(t *testing.T) {
	eng := newTestEngine(t, registry.New(featuregate.NewRegistry()))
	protected := envelope("Provider", "protected", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	protected.Metadata.Protect = true
	key := protected.Key()
	if err := eng.StateStore.Save(context.Background(), state.State{
		Version: state.CurrentVersion,
		Resources: map[resource.Key]state.ResourceState{
			key: {SpecHash: "old", Lifecycle: resource.Managed.String(), LastApplied: &protected},
		},
	}); err != nil {
		t.Fatal(err)
	}
	g, err := graph.Build(nil)
	if err != nil {
		t.Fatal(err)
	}
	p := plan.Plan{
		Entries: []plan.Entry{{Key: key, Action: plan.ActionRefused, Reason: "protected"}},
		Levels:  [][]resource.Key{{key}},
	}
	result, err := eng.Apply(context.Background(), p, nil, g)
	if err == nil {
		t.Fatal("apply accepted a protected delete")
	}
	ferr, ok := result.Failed[key]
	if !ok || !strings.Contains(ferr.Error(), "protected") {
		t.Fatalf("result missing protected failure: %+v", result.Failed)
	}
}

// TestDestroyRefusesProtectedResource guards docs/planning/08 A5: destroy
// must fail against a protect: true data-bearing resource.
func TestDestroyRefusesProtectedResource(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	eng := newTestEngine(t, reg)
	protected := envelope("Provider", "protected", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	protected.Metadata.Protect = true
	envelopes := []resource.Envelope{protected}
	applyAll(t, eng, []resource.Envelope{envelope("Provider", "protected", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})})

	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatal(err)
	}
	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p, err := plan.ComputeDestroy(envelopes, st, g, false, false)
	if err != nil {
		t.Fatal(err)
	}
	result, err := eng.Destroy(context.Background(), p, envelopes, g)
	if err == nil {
		t.Fatal("engine destroyed a protected resource")
	}
	ferr, ok := result.Failed[protected.Key()]
	if !ok || !strings.Contains(ferr.Error(), "protect") {
		t.Fatalf("result missing protected failure: %+v", result.Failed)
	}
}

// TestProbeRecordsDrift: Probe merges observed DriftDetected/Ready
// conditions into recorded state so `status` reflects the last observation.
func TestProbeRecordsDrift(t *testing.T) {
	eng, _, envelopes := driftFixture(t)
	applyAll(t, eng, envelopes)

	results, err := eng.Probe(context.Background(), envelopes)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if len(results) != 1 || !HasDrift(results[0].Status) {
		t.Fatalf("probe results = %+v, want one drifted resource", results)
	}

	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rs := st.Resources[envelopes[0].Key()]
	if !HasDrift(rs.Status) {
		t.Errorf("DriftDetected not persisted to state: %+v", rs.Status.Conditions)
	}
	if c, _ := rs.Status.Condition(status.Ready); c.Status != status.False {
		t.Errorf("probed Ready not persisted, got %+v", c)
	}
}

// TestDestroyExternalGuard covers NFR-3's engine half: even when a plan
// marks an External resource for deletion, the engine refuses unless the
// destructive double opt-in was given.
func TestDestroyExternalGuard(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	eng := newTestEngine(t, reg)

	envelopes := []resource.Envelope{
		envelope("SecretReference", "prod-db-conn", map[string]any{
			"backend": "env",
			"keys":    []any{"username", "password"},
		}),
		envelope("Source", "prod-db", map[string]any{
			"engine":        "postgres",
			"external":      true,
			"connectionRef": map[string]any{"name": "prod-db-conn"},
		}),
	}
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatal(err)
	}
	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p, err := plan.ComputeDestroy(envelopes, st, g, true, false)
	if err != nil {
		t.Fatal(err)
	}

	result, err := eng.Destroy(context.Background(), p, envelopes, g)
	if err == nil {
		t.Fatal("engine destroyed an External resource without AllowDestructive")
	}
	ferr, ok := result.Failed[envelopes[1].Key()]
	if !ok || !strings.Contains(ferr.Error(), "yes-i-understand-this-is-destructive") {
		t.Errorf("guard error missing or unclear: %v", ferr)
	}

	// With the double opt-in, an external-no-provider resource is only
	// forgotten, never touched.
	eng.AllowDestructive = true
	if _, err := eng.Destroy(context.Background(), p, envelopes, g); err != nil {
		t.Fatalf("destroy with AllowDestructive failed: %v", err)
	}
}

// slowProvider counts concurrent Reconcile executions.
type slowProvider struct {
	noop.Provider
	mu      sync.Mutex
	active  int
	maxSeen int
}

func (s *slowProvider) Type() string { return "slow" }

func (s *slowProvider) Reconcile(_ context.Context, _ reconciler.Request) (status.Status, error) {
	s.mu.Lock()
	s.active++
	if s.active > s.maxSeen {
		s.maxSeen = s.active
	}
	s.mu.Unlock()
	time.Sleep(30 * time.Millisecond)
	s.mu.Lock()
	s.active--
	s.ReconcileCount++
	s.mu.Unlock()
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "SlowReconciled"}, time.Now())
	return st, nil
}

// TestParallelReconciliation covers Phase 6: independent resources in the
// same topological level reconcile concurrently, bounded by Parallelism,
// with all state persisted correctly.
func TestParallelReconciliation(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	prov := &slowProvider{}
	reg.RegisterProvider("slow", func() reconciler.Provider { return prov }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	var envelopes []resource.Envelope
	for _, n := range []string{"a", "b", "c", "d", "e", "f"} {
		envelopes = append(envelopes, envelope("Provider", "p-"+n, map[string]any{
			"type":    "slow",
			"runtime": map[string]any{"type": "fake"},
		}))
	}

	eng := newTestEngine(t, reg)
	eng.Parallelism = 4
	result := applyAll(t, eng, envelopes)
	if len(result.Succeeded) != len(envelopes) {
		t.Fatalf("succeeded = %d, want %d (failed: %v)", len(result.Succeeded), len(envelopes), result.Failed)
	}
	if prov.maxSeen < 2 {
		t.Errorf("max concurrent reconciles = %d; parallelism 4 over 6 independent resources should overlap", prov.maxSeen)
	}
	if prov.maxSeen > 4 {
		t.Errorf("max concurrent reconciles = %d, exceeding the parallelism bound of 4", prov.maxSeen)
	}

	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Resources) != len(envelopes) {
		t.Errorf("state has %d resources, want %d", len(st.Resources), len(envelopes))
	}
}

// TestPreflightSecretsAggregates: every unresolvable secret is reported in a
// single error so the user fixes them in one pass — the fail-fast guard that
// apply cannot half-apply for want of a credential.
func TestPreflightSecretsAggregates(t *testing.T) {
	eng := newTestEngine(t, registry.New(featuregate.NewRegistry()))
	eng.SecretStore = env.New()
	envs := []resource.Envelope{
		envelope("SecretReference", "creds-a", map[string]any{"backend": "env", "keys": []any{"username", "password"}}),
		envelope("SecretReference", "creds-b", map[string]any{"backend": "env", "keys": []any{"token"}}),
		envelope("Provider", "p", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}}),
	}

	err := eng.PreflightSecrets(context.Background(), envs)
	if err == nil {
		t.Fatal("preflight passed with no secrets set")
	}
	for _, want := range []string{
		"DATASCAPE_SECRET_CREDS_A_USERNAME",
		"DATASCAPE_SECRET_CREDS_A_PASSWORD",
		"DATASCAPE_SECRET_CREDS_B_TOKEN",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("aggregated error missing %s:\n%s", want, err)
		}
	}

	t.Setenv("DATASCAPE_SECRET_CREDS_A_USERNAME", "u")
	t.Setenv("DATASCAPE_SECRET_CREDS_A_PASSWORD", "p")
	t.Setenv("DATASCAPE_SECRET_CREDS_B_TOKEN", "t")
	if err := eng.PreflightSecrets(context.Background(), envs); err != nil {
		t.Errorf("preflight failed with all secrets set: %v", err)
	}
}

func TestSecretReferenceDriftAndApplyRecordsNewFingerprint(t *testing.T) {
	eng := newTestEngine(t, registry.New(featuregate.NewRegistry()))
	eng.SecretStore = env.New()
	envelopes := []resource.Envelope{
		envelope("SecretReference", "db-creds", map[string]any{"backend": "env", "keys": []any{"password"}}),
	}
	key := envelopes[0].Key()

	t.Setenv("DATASCAPE_SECRET_DB_CREDS_PASSWORD", "old-password")
	applyWithSecretHashes(t, eng, envelopes)
	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	oldHash := st.Resources[key].SecretHash
	if oldHash == "" {
		t.Fatal("secret hash was not recorded after apply")
	}

	t.Setenv("DATASCAPE_SECRET_DB_CREDS_PASSWORD", "new-password")
	results, err := eng.Probe(context.Background(), envelopes)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("probe returned %d result(s), want 1", len(results))
	}
	cond, ok := results[0].Status.Condition(status.DriftDetected)
	if !ok || cond.Status != status.True || cond.Reason != "SecretChanged" {
		t.Fatalf("drift condition = %+v, want SecretChanged true", cond)
	}
	st, err = eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Resources[key].SecretHash; got != oldHash {
		t.Fatalf("probe changed stored secret hash to %q, want old hash %q until apply", got, oldHash)
	}

	applyWithSecretHashes(t, eng, envelopes)
	st, err = eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	newHash := st.Resources[key].SecretHash
	if newHash == "" || newHash == oldHash {
		t.Fatalf("apply did not record new secret hash; old=%q new=%q", oldHash, newHash)
	}
}

func applyWithSecretHashes(t *testing.T, eng *Engine, envelopes []resource.Envelope) Result {
	t.Helper()
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}
	st, err := eng.StateStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	secretHashes, err := eng.SecretHashes(context.Background(), envelopes)
	if err != nil {
		t.Fatalf("secret hashes: %v", err)
	}
	p, err := plan.ComputeWithSecretHashes(envelopes, st, g, secretHashes)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	result, err := eng.Apply(context.Background(), p, envelopes, g)
	if err != nil {
		t.Fatalf("apply: %v (failed: %v)", err, result.Failed)
	}
	return result
}

// TestProbeTCPReachable distinguishes a live endpoint (holds the connection),
// a dead-upstream forwarder (accepts then closes immediately), and nothing
// listening — the basis for honest external-resource health.
func TestProbeTCPReachable(t *testing.T) {
	ctx := context.Background()

	// Live server that waits for the client to speak (like Postgres).
	live, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback listen blocked by this environment; skipping (see docs/planning/07 §3.2): %v", err)
	}
	defer live.Close()
	go func() {
		for {
			c, err := live.Accept()
			if err != nil {
				return
			}
			// Hold it open without speaking.
			go func(c net.Conn) { time.Sleep(2 * time.Second); c.Close() }(c)
		}
	}()
	if err := probeTCPReachable(ctx, live.Addr().String()); err != nil {
		t.Errorf("live endpoint reported unreachable: %v", err)
	}

	// Forwarder whose upstream is down: accept, then close immediately.
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer dead.Close()
	go func() {
		for {
			c, err := dead.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	if err := probeTCPReachable(ctx, dead.Addr().String()); err == nil {
		t.Error("dead-upstream forwarder reported reachable")
	}

	// Nothing listening.
	if err := probeTCPReachable(ctx, "127.0.0.1:1"); err == nil {
		t.Error("closed port reported reachable")
	}
}

// TestConnectionDialAddressUsesPublishedFactNotResourceName proves the F4
// fix directly: a managed Connection's forwarder is not guaranteed to be
// named after the Connection resource itself — that was only a convention,
// and re-deriving it from connEnv.Metadata.Name is exactly the K7 mistake
// (docs/planning/08 F4, docs/planning/09 Class 4). Here the runtime object
// is deliberately named differently from the Connection ("orders-db-fwd" vs.
// "orders-db"); connectionDialAddress must resolve via the endpoint fact
// (RuntimeName/ContainerPort) recorded in the Connection's own
// Status.ProviderState, not by guessing the Connection's own name.
func TestConnectionDialAddressUsesPublishedFactNotResourceName(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	fakeRT := fakeruntime.New()
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeRT, nil
	})

	ctx := context.Background()
	const runtimeObjectName = "orders-db-fwd" // deliberately NOT "orders-db"
	if _, err := fakeRT.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  runtimeObjectName,
		Image: "alpine/socat:1.8.0.3",
		Ports: []runtime.PortBinding{{HostPort: 15432, ContainerPort: 5432, Audience: runtime.AudienceHost}},
	}); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}

	eng := newTestEngine(t, reg)
	provEnv := envelope("Provider", "cfg", map[string]any{
		"type":    "noop",
		"runtime": map[string]any{"type": "fake"},
	})
	connEnv := envelope("Connection", "orders-db", map[string]any{
		"providerRef": map[string]any{"name": "cfg"},
		"port":        5432,
		"target":      "upstream:5432",
	})
	facts := endpoint.List{
		{Name: "forward", Scheme: "tcp", RuntimeName: runtimeObjectName, ContainerPort: 5432, Audience: runtime.AudienceHost},
	}.ToState()
	// Status.ProviderState is only ever populated from a real state-file
	// load in production, which round-trips through JSON — []map[string]any
	// becomes []any of map[string]any, not the Go slice type directly; build
	// it the same way so this test exercises the real decode path.
	factsAny := make([]any, len(facts))
	for i, f := range facts {
		factsAny[i] = f
	}
	connEnv.Status.ProviderState = map[string]any{endpoint.Key: factsAny}
	byKey := map[resource.Key]resource.Envelope{
		provEnv.Key(): provEnv,
		connEnv.Key(): connEnv,
	}

	conn, err := connection.FromEnvelope(connEnv)
	if err != nil {
		t.Fatalf("connection.FromEnvelope: %v", err)
	}

	addr, closeFn := eng.connectionDialAddress(ctx, connEnv, conn, byKey)
	if closeFn != nil {
		defer closeFn()
	}
	if addr == "" {
		t.Fatal("connectionDialAddress returned no address; want it to resolve via the published endpoint fact")
	}

	// Sanity check the premise: naming.RuntimeObjectName(connEnv) (the old
	// re-derivation) names a container that does not exist, so a
	// regression back to that behavior would make this test fail with an
	// empty address above, not silently pass for the wrong reason.
	if naming.RuntimeObjectName(connEnv) == runtimeObjectName {
		t.Fatal("test setup invalid: resource name and runtime object name must differ")
	}
}

// fakeSchemaRegistryProvider stands in for redpanda: on reconciling its own
// Provider resource, it publishes a "schema-registry" endpoint fact exactly
// like the real adapter does (docs/planning/08 D1) — no naming/port
// convention re-derived by this test, the same providerState round-trip a
// real StateStore.Save/Load performs.
type fakeSchemaRegistryProvider struct {
	noop.Provider
	internalAddr string
}

func (p *fakeSchemaRegistryProvider) Type() string { return "fakestream" }

func (p *fakeSchemaRegistryProvider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st, err := p.Provider.Reconcile(ctx, req)
	if err != nil || req.Resource.Kind != "Provider" {
		return st, err
	}
	st.ProviderState = map[string]any{
		endpoint.Key: endpoint.List{{Name: "schema-registry", Scheme: "http", Internal: p.internalAddr}}.ToState(),
	}
	return st, nil
}

// fakeCDCProvider stands in for debezium: it records whatever
// req.SchemaRegistryURL the engine resolved for its own Binding reconcile,
// so the test can assert on it without depending on debezium's own
// connector-config wiring.
type fakeCDCProvider struct {
	noop.Provider
	gotSchemaRegistryURL string
	bindingReconciled    bool
}

func (p *fakeCDCProvider) Type() string { return "fakecdc" }

func (p *fakeCDCProvider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	if req.Resource.Kind == "Binding" {
		p.gotSchemaRegistryURL = req.SchemaRegistryURL
		p.bindingReconciled = true
	}
	return p.Provider.Reconcile(ctx, req)
}

// TestResolveSchemaRegistryURLFromEventStreamProvider proves the D1 wiring
// end to end through a real Apply(): the EventStream's own Provider
// (fakestream) is reconciled first by dependency-graph ordering and
// publishes its "schema-registry" endpoint into state; when the dependent
// Binding (options.format: avro) is then reconciled in the *same* Apply
// call, its Request.SchemaRegistryURL carries exactly that published
// endpoint's Internal address — read back from st.Resources (mutated
// in-place across the run), never constructed by string convention.
func TestResolveSchemaRegistryURLFromEventStreamProvider(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	streamProv := &fakeSchemaRegistryProvider{internalAddr: "http://stream-broker:8081"}
	cdcProv := &fakeCDCProvider{}
	reg.RegisterProvider("fakestream", func() reconciler.Provider { return streamProv }, "")
	reg.RegisterProvider("fakecdc", func() reconciler.Provider { return cdcProv }, "")
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	eng := newTestEngine(t, reg)
	envelopes := []resource.Envelope{
		envelope("Provider", "stream-broker", map[string]any{"type": "fakestream", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "cdc-worker", map[string]any{"type": "fakecdc", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "db", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}}),
		envelope("Source", "orders-db", map[string]any{"engine": "postgres", "providerRef": map[string]any{"name": "db"}}),
		envelope("EventStream", "orders-events", map[string]any{"providerRef": map[string]any{"name": "stream-broker"}}),
		envelope("Binding", "orders-cdc", map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "orders-db"},
			"targetRef":   map[string]any{"name": "orders-events"},
			"providerRef": map[string]any{"name": "cdc-worker"},
			"options":     map[string]any{"format": "avro"},
		}),
	}

	applyAll(t, eng, envelopes)

	if !cdcProv.bindingReconciled {
		t.Fatal("test setup invalid: the Binding was never reconciled")
	}
	if cdcProv.gotSchemaRegistryURL != "http://stream-broker:8081" {
		t.Errorf("Request.SchemaRegistryURL = %q, want %q", cdcProv.gotSchemaRegistryURL, "http://stream-broker:8081")
	}
}

// TestResolveSchemaRegistryURLEmptyForJSONFormat: the common (unset/json)
// case never resolves a registry URL at all, even when the upstream
// Provider happens to publish one.
func TestResolveSchemaRegistryURLEmptyForJSONFormat(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	streamProv := &fakeSchemaRegistryProvider{internalAddr: "http://stream-broker:8081"}
	cdcProv := &fakeCDCProvider{}
	reg.RegisterProvider("fakestream", func() reconciler.Provider { return streamProv }, "")
	reg.RegisterProvider("fakecdc", func() reconciler.Provider { return cdcProv }, "")
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	eng := newTestEngine(t, reg)
	envelopes := []resource.Envelope{
		envelope("Provider", "stream-broker", map[string]any{"type": "fakestream", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "cdc-worker", map[string]any{"type": "fakecdc", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "db", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}}),
		envelope("Source", "orders-db", map[string]any{"engine": "postgres", "providerRef": map[string]any{"name": "db"}}),
		envelope("EventStream", "orders-events", map[string]any{"providerRef": map[string]any{"name": "stream-broker"}}),
		envelope("Binding", "orders-cdc", map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "orders-db"},
			"targetRef":   map[string]any{"name": "orders-events"},
			"providerRef": map[string]any{"name": "cdc-worker"},
		}),
	}

	applyAll(t, eng, envelopes)

	if !cdcProv.bindingReconciled {
		t.Fatal("test setup invalid: the Binding was never reconciled")
	}
	if cdcProv.gotSchemaRegistryURL != "" {
		t.Errorf("Request.SchemaRegistryURL = %q, want empty for an unset options.format", cdcProv.gotSchemaRegistryURL)
	}
}

// fakeMetricsProvider stands in for redpanda/minio's own metrics endpoint
// publishing (docs/planning/08 C9): its own Provider reconcile publishes a
// "metrics"-named endpoint fact.
type fakeMetricsProvider struct {
	noop.Provider
	internalAddr string
}

func (p *fakeMetricsProvider) Type() string { return "fakemetricssource" }

func (p *fakeMetricsProvider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st, err := p.Provider.Reconcile(ctx, req)
	if err != nil || req.Resource.Kind != "Provider" {
		return st, err
	}
	st.ProviderState = map[string]any{
		endpoint.Key: endpoint.List{{Name: "metrics", Scheme: "http", Internal: p.internalAddr}}.ToState(),
	}
	return st, nil
}

// fakeMonitoringProvider stands in for prometheus: it records whatever
// req.MetricsTargets the engine resolved for its own Provider reconcile.
type fakeMonitoringProvider struct {
	noop.Provider
	gotTargets []reconciler.MetricsTarget
}

func (p *fakeMonitoringProvider) Type() string { return "fakemonitoring" }

func (p *fakeMonitoringProvider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	if req.Resource.Kind == "Provider" {
		p.gotTargets = req.MetricsTargets
	}
	return p.Provider.Reconcile(ctx, req)
}

// TestResolveMetricsTargetsFromPublishedEndpoints covers docs/planning/08
// C9's engine wiring: two independent Provider resources (no ref between
// either of them and the monitoring Provider — prometheus scrapes whatever
// carries a metrics endpoint, not a declared dependency) are reconciled in
// a first Apply; a second Apply reconciling the monitoring Provider sees
// both already-published "metrics" endpoint facts in its
// Request.MetricsTargets, read back from state (never constructed by
// string convention) — same two-phase-apply shape as the schema-registry
// test above, needed here because the metrics-publishing Providers have no
// dependency edge to the monitoring Provider that would otherwise order a
// single Apply's topological levels for us.
func TestResolveMetricsTargetsFromPublishedEndpoints(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	rp := &fakeMetricsProvider{internalAddr: "http://redpanda:9644/public_metrics"}
	minio := &fakeMetricsProvider{internalAddr: "http://minio:9000/minio/v2/metrics/cluster"}
	mon := &fakeMonitoringProvider{}
	reg.RegisterProvider("fakemetrics-rp", func() reconciler.Provider { return rp }, "")
	reg.RegisterProvider("fakemetrics-minio", func() reconciler.Provider { return minio }, "")
	reg.RegisterProvider("fakemonitoring", func() reconciler.Provider { return mon }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	eng := newTestEngine(t, reg)
	infra := []resource.Envelope{
		envelope("Provider", "redpanda", map[string]any{"type": "fakemetrics-rp", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "minio", map[string]any{"type": "fakemetrics-minio", "runtime": map[string]any{"type": "fake"}}),
	}
	applyAll(t, eng, infra)

	all := append(append([]resource.Envelope{}, infra...),
		envelope("Provider", "prometheus", map[string]any{"type": "fakemonitoring", "runtime": map[string]any{"type": "fake"}}))
	applyAll(t, eng, all)

	if len(mon.gotTargets) != 2 {
		t.Fatalf("MetricsTargets = %+v, want 2 entries", mon.gotTargets)
	}
	byJob := map[string]string{}
	for _, tgt := range mon.gotTargets {
		byJob[tgt.JobName] = tgt.Endpoint.Internal
	}
	if byJob["redpanda"] != "http://redpanda:9644/public_metrics" {
		t.Errorf("redpanda target = %q, want %q", byJob["redpanda"], "http://redpanda:9644/public_metrics")
	}
	if byJob["minio"] != "http://minio:9000/minio/v2/metrics/cluster" {
		t.Errorf("minio target = %q, want %q", byJob["minio"], "http://minio:9000/minio/v2/metrics/cluster")
	}
}

// fakeCatalogProvider stands in for nessie: its Catalog-kind reconcile
// publishes an "iceberg-rest" endpoint fact (docs/planning/08 D10).
type fakeCatalogProvider struct {
	noop.Provider
	internalAddr string
}

func (p *fakeCatalogProvider) Type() string { return "fakecatalog" }

func (p *fakeCatalogProvider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st, err := p.Provider.Reconcile(ctx, req)
	if err != nil || req.Resource.Kind != "Catalog" {
		return st, err
	}
	st.ProviderState = map[string]any{
		endpoint.Key: endpoint.List{{Name: "iceberg-rest", Scheme: "http", Internal: p.internalAddr}}.ToState(),
	}
	return st, nil
}

// fakeWarehouseProvider stands in for s3/minio: its Provider-kind reconcile
// publishes an "s3" endpoint fact and reports type "s3" (the type check
// resolveCatalogFacts uses to auto-infer the sole warehouse candidate).
type fakeWarehouseProvider struct {
	noop.Provider
	internalAddr string
}

func (p *fakeWarehouseProvider) Type() string { return "s3" }

func (p *fakeWarehouseProvider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st, err := p.Provider.Reconcile(ctx, req)
	if err != nil || req.Resource.Kind != "Provider" {
		return st, err
	}
	st.ProviderState = map[string]any{
		endpoint.Key: endpoint.List{{Name: "s3", Scheme: "http", Internal: p.internalAddr}}.ToState(),
	}
	return st, nil
}

// fakeTrinoLikeProvider stands in for trino: it records whatever
// req.CatalogFacts the engine resolved for its own Provider reconcile.
type fakeTrinoLikeProvider struct {
	noop.Provider
	got *reconciler.CatalogFacts
}

func (p *fakeTrinoLikeProvider) Type() string { return "faketrino" }

func (p *fakeTrinoLikeProvider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	if req.Resource.Kind == "Provider" {
		p.got = req.CatalogFacts
	}
	return p.Provider.Reconcile(ctx, req)
}

// TestResolveCatalogFactsFromCatalogRef covers docs/planning/08 D10's
// engine wiring end to end: configuration.catalogRef graph-orders a
// Catalog before the trino-like Provider that reads it (a real dependency
// edge, unlike the metrics test above), but the warehouse-backing S3
// Provider has no such edge to it (warehouseProviderRef is left unset here
// to exercise the "sole s3/minio Provider in the manifest" auto-inference
// path) — so a second Apply is still needed for that fact to already be
// published, the same two-phase shape TestResolveMetricsTargetsFromPublishedEndpoints
// documents.
func TestResolveCatalogFactsFromCatalogRef(t *testing.T) {
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	cat := &fakeCatalogProvider{internalAddr: "catalog-svc:19120/iceberg"}
	minio := &fakeWarehouseProvider{internalAddr: "lake-minio:9000"}
	trino := &fakeTrinoLikeProvider{}
	reg.RegisterProvider("fakecatalog", func() reconciler.Provider { return cat }, "")
	reg.RegisterProvider("s3", func() reconciler.Provider { return minio }, "")
	reg.RegisterProvider("faketrino", func() reconciler.Provider { return trino }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})

	eng := newTestEngine(t, reg)
	infra := []resource.Envelope{
		envelope("Provider", "catalog-svc", map[string]any{"type": "fakecatalog", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "lake-minio", map[string]any{"type": "s3", "runtime": map[string]any{"type": "fake"}}),
		envelope("Catalog", "lakehouse-catalog", map[string]any{"engine": "nessie", "providerRef": map[string]any{"name": "catalog-svc"}}),
	}
	applyAll(t, eng, infra)

	all := append(append([]resource.Envelope{}, infra...),
		envelope("Provider", "lake-trino", map[string]any{
			"type":    "faketrino",
			"runtime": map[string]any{"type": "fake"},
			"configuration": map[string]any{
				"catalogRef": map[string]any{"name": "lakehouse-catalog"},
			},
		}))
	applyAll(t, eng, all)

	if trino.got == nil {
		t.Fatal("CatalogFacts is nil, want it resolved")
	}
	if trino.got.RestInternal != "catalog-svc:19120/iceberg" {
		t.Errorf("RestInternal = %q, want %q", trino.got.RestInternal, "catalog-svc:19120/iceberg")
	}
	if trino.got.S3Internal != "lake-minio:9000" {
		t.Errorf("S3Internal = %q, want %q", trino.got.S3Internal, "lake-minio:9000")
	}
}

// TestExternalConnectionInNetworkReachability covers docs/planning/08 C10:
// an external Connection consumed by an in-network Binding (here, a CDC
// Binding whose sourceRef is the external Source declaring the
// connectionRef) is additionally probed from inside that Binding's realizing
// Provider's own network — distinctly from the pre-existing host-side check
// — and reports the distinct condition reason
// "ExternalEndpointUnreachableInNetwork" when that in-network probe fails,
// never folding it into the host-side "ExternalEndpointUnreachable".
//
// The external Connection's declared host is a real "127.0.0.1:<port>" this
// test process listens on directly — real enough for the pre-existing
// host-side TCP probe to genuinely succeed in both subtests below — while
// also being the literal name of a fake-managed container the fake runtime
// (ADR 015's strict interpreter) resolves ProbeReachable's in-network dial
// against; the two subtests differ only in whether that fake container
// declares the probed port, exactly the under- vs over-declaration
// distinction C10 exists to catch.
func TestExternalConnectionInNetworkReachability(t *testing.T) {
	newFixture := func(t *testing.T) (*Engine, *fakeruntime.Runtime, []resource.Envelope, int) {
		t.Helper()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		port := ln.Addr().(*net.TCPAddr).Port

		gates := featuregate.NewRegistry()
		reg := registry.New(gates)
		fakeRT := fakeruntime.New()
		reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
		reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
			return fakeRT, nil
		})
		if err := fakeRT.EnsureNetwork(context.Background(), runtime.NetworkSpec{Name: "app-net"}); err != nil {
			t.Fatalf("EnsureNetwork: %v", err)
		}

		envelopes := []resource.Envelope{
			envelope("Connection", "db-conn", map[string]any{
				"external": true,
				"host":     "127.0.0.1",
				"port":     port,
			}),
			envelope("Source", "prod-db", map[string]any{
				"engine":        "postgres",
				"external":      true,
				"connectionRef": map[string]any{"name": "db-conn"},
			}),
			envelope("Provider", "connect", map[string]any{
				"type":    "noop",
				"runtime": map[string]any{"type": "fake", "network": "app-net"},
			}),
			envelope("Binding", "cdc", map[string]any{
				"mode":        "cdc",
				"sourceRef":   map[string]any{"name": "prod-db"},
				"targetRef":   map[string]any{"name": "changes"}, // unresolved — nothing dereferences it here
				"providerRef": map[string]any{"name": "connect"},
			}),
		}

		eng := newTestEngine(t, reg)
		sourceKey := envelopes[1].Key()
		if err := eng.StateStore.Save(context.Background(), state.State{
			Version:   state.CurrentVersion,
			Resources: map[resource.Key]state.ResourceState{sourceKey: {SpecHash: "seed"}},
		}); err != nil {
			t.Fatalf("seed state: %v", err)
		}
		return eng, fakeRT, envelopes, port
	}

	probeSource := func(t *testing.T, eng *Engine, envelopes []resource.Envelope) status.Condition {
		t.Helper()
		results, err := eng.Probe(context.Background(), envelopes)
		if err != nil {
			t.Fatalf("Probe: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("Probe results = %+v, want exactly the seeded Source", results)
		}
		c, ok := results[0].Status.Condition(status.Ready)
		if !ok {
			t.Fatalf("Probe result has no Ready condition: %+v", results[0].Status)
		}
		return c
	}

	t.Run("in_network_reachable", func(t *testing.T) {
		eng, fakeRT, envelopes, port := newFixture(t)
		if _, err := fakeRT.EnsureContainer(context.Background(), runtime.ContainerSpec{
			Name:     "127.0.0.1",
			Image:    "postgres:16",
			Networks: []string{"app-net"},
			Ports:    []runtime.PortBinding{{ContainerPort: port, Audience: runtime.AudienceInternal}},
		}); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}

		c := probeSource(t, eng, envelopes)
		if c.Status != status.True || c.Reason != "ExternalEndpointReachable" {
			t.Fatalf("Ready condition = %+v, want True/ExternalEndpointReachable", c)
		}
	})

	t.Run("in_network_unreachable_distinct_from_host_side", func(t *testing.T) {
		eng, fakeRT, envelopes, port := newFixture(t)
		// The fake-managed container attached to the consuming Binding's
		// network exists, but never declares the probed port — the ADR 015
		// strict interpreter's under-declaration refusal — even though the
		// same host:port genuinely answers a real, host-side TCP dial
		// (proven by the reachable subtest above sharing the exact same
		// listener setup).
		if _, err := fakeRT.EnsureContainer(context.Background(), runtime.ContainerSpec{
			Name:     "127.0.0.1",
			Image:    "postgres:16",
			Networks: []string{"app-net"},
		}); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		_ = port

		c := probeSource(t, eng, envelopes)
		if c.Status != status.False || c.Reason != "ExternalEndpointUnreachableInNetwork" {
			t.Fatalf("Ready condition = %+v, want False/ExternalEndpointUnreachableInNetwork", c)
		}
	})
}
