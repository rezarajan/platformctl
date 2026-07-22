package compose

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rezarajan/platformctl/internal/application/blueprint"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// --- local capability stubs (CLAUDE.md's application-test exception: no
// technology adapters imported here, only local doubles — the same
// pattern as internal/application/compatibility/compatibility_test.go's
// versionedStub). ---

type stubProvider struct{ typeName string }

func (s stubProvider) Type() string { return s.typeName }
func (s stubProvider) Reconcile(context.Context, reconciler.Request) (status.Status, error) {
	return status.Status{}, nil
}
func (s stubProvider) Destroy(context.Context, reconciler.Request) error { return nil }
func (s stubProvider) Probe(context.Context, reconciler.Request) (status.Status, error) {
	return status.Status{}, nil
}

type cdcStub struct{ stubProvider }

func (cdcStub) SupportedSourceEngines() []string { return []string{"postgres", "mysql", "mariadb"} }

type sinkStub struct{ stubProvider }

func (sinkStub) SupportedSinkFormats() []string { return []string{"json", "parquet"} }

type connStub struct {
	stubProvider
	schemes []string
}

func (c connStub) SupportedConnectionSchemes() []string { return c.schemes }

type catalogStub struct{ stubProvider }

func (catalogStub) SupportedCatalogEngines() []string { return []string{"nessie"} }

// testResolver mirrors the shape of provider types compose's fixtures use,
// with just enough capability declared for compatibility.Check to pass —
// never a real adapter.
func testResolver(t *testing.T) Resolver {
	t.Helper()
	table := map[string]reconciler.Provider{
		"postgres":   stubProvider{"postgres"},
		"mysql":      stubProvider{"mysql"},
		"mariadb":    stubProvider{"mariadb"},
		"redpanda":   stubProvider{"redpanda"},
		"debezium":   cdcStub{stubProvider{"debezium"}},
		"s3sink":     sinkStub{stubProvider{"s3sink"}},
		"minio":      stubProvider{"minio"},
		"s3":         stubProvider{"s3"},
		"nessie":     catalogStub{stubProvider{"nessie"}},
		"prometheus": stubProvider{"prometheus"},
		"proxy":      connStub{stubProvider{"proxy"}, []string{"tcp"}},
		"ingress":    connStub{stubProvider{"ingress"}, []string{"http"}}, // https NOT supported — mirrors the real ingress provider until ADR 018 §C8 merges
		"jdbcsink":   stubProvider{"jdbcsink"},
		"s3source":   stubProvider{"s3source"},
	}
	return func(providerType string) (reconciler.Provider, error) {
		if p, ok := table[providerType]; ok {
			return p, nil
		}
		return nil, &unknownProviderErr{providerType}
	}
}

type unknownProviderErr struct{ t string }

func (e *unknownProviderErr) Error() string { return "unknown provider type " + e.t }

// writeCDCToLakeFixture writes the real cdc-to-lake blueprint into dir
// (the same embedded templates `platformctl init cdc-to-lake` writes),
// giving compose tests a realistic, validating-with-zero-edits starting
// set to compute reuse candidates and patches against.
func writeCDCToLakeFixture(t *testing.T, dir string) {
	t.Helper()
	if _, err := blueprint.Write("cdc-to-lake", dir, false); err != nil {
		t.Fatalf("writing cdc-to-lake fixture: %v", err)
	}
}

func mustLoad(t *testing.T, dir string) Snapshot {
	t.Helper()
	snap, err := LoadTolerant(dir, testResolver(t))
	if err != nil {
		t.Fatalf("LoadTolerant(%s): %v", dir, err)
	}
	if snap.Warning != "" {
		t.Fatalf("LoadTolerant(%s) degraded unexpectedly: %s", dir, snap.Warning)
	}
	return snap
}

func TestLoadTolerantDegradesOnGraphError(t *testing.T) {
	dir := t.TempDir()
	// A dangling providerRef makes graph.Build fail.
	bad := "apiVersion: datascape.io/v1alpha1\nkind: Source\nmetadata:\n  name: orphan\nspec:\n  engine: postgres\n  providerRef:\n    name: nope\n  postgres:\n    database: x\n"
	if err := os.WriteFile(filepath.Join(dir, "source.yaml"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	snap, err := LoadTolerant(dir, testResolver(t))
	if err != nil {
		t.Fatalf("LoadTolerant should degrade, not error: %v", err)
	}
	if snap.Warning == "" {
		t.Fatal("expected a non-empty Warning for an invalid manifest set (tolerant mode)")
	}
	if snap.Graph != nil {
		t.Fatal("expected Graph to be nil when graph.Build fails")
	}
	if len(snap.Envelopes) != 1 {
		t.Fatalf("expected the one parseable envelope to still be usable for best-effort candidates, got %d", len(snap.Envelopes))
	}
}

func TestLoadTolerantDegradesOnCompatibilityError(t *testing.T) {
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	// A resolver that knows no provider types makes compatibility.Check fail
	// but leaves the graph buildable.
	brokenResolve := func(string) (reconciler.Provider, error) { return nil, errUnknown }
	snap, err := LoadTolerant(dir, brokenResolve)
	if err != nil {
		t.Fatalf("LoadTolerant should degrade, not error: %v", err)
	}
	if snap.Warning == "" {
		t.Fatal("expected a non-empty Warning when compatibility.Check fails")
	}
	if snap.Graph == nil {
		t.Fatal("expected Graph to still be built (only compatibility failed)")
	}
	if len(snap.Envelopes) == 0 {
		t.Fatal("expected envelopes to still be usable for best-effort candidates")
	}
}

var errUnknown = &unknownProviderErr{"?"}

func TestBrokerAndDatasetCandidatesFromExistingBlueprint(t *testing.T) {
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	brokers := snap.BrokerCandidates()
	if len(brokers) != 1 || brokers[0].Name != "broker" {
		t.Fatalf("BrokerCandidates() = %+v, want exactly [broker]", brokers)
	}

	datasets := snap.DatasetCandidates()
	if len(datasets) != 1 || datasets[0].Name != "raw-lake" {
		t.Fatalf("DatasetCandidates() = %+v, want exactly [raw-lake]", datasets)
	}
	if datasets[0].LakeProvider != "lake" {
		t.Errorf("DatasetCandidates()[0].LakeProvider = %q, want %q", datasets[0].LakeProvider, "lake")
	}
	if datasets[0].SinkProvider != "sink" {
		t.Errorf("DatasetCandidates()[0].SinkProvider = %q, want %q", datasets[0].SinkProvider, "sink")
	}
	if datasets[0].Bucket != "raw-events" {
		t.Errorf("DatasetCandidates()[0].Bucket = %q, want %q", datasets[0].Bucket, "raw-events")
	}
}
