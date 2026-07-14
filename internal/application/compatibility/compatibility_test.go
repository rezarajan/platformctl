package compatibility

import (
	"context"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type stubProvider struct{ typeName string }

func (s stubProvider) Type() string { return s.typeName }
func (s stubProvider) Reconcile(context.Context, resource.Envelope, runtime.ContainerRuntime) (status.Status, error) {
	return status.Status{}, nil
}
func (s stubProvider) Destroy(context.Context, resource.Envelope, runtime.ContainerRuntime) error {
	return nil
}
func (s stubProvider) Probe(context.Context, resource.Envelope, runtime.ContainerRuntime) (status.Status, error) {
	return status.Status{}, nil
}

type cdcStub struct{ stubProvider }

func (cdcStub) SupportedSourceEngines() []string { return []string{"postgres", "mysql", "mongodb"} }

type sinkStub struct{ stubProvider }

func (sinkStub) SupportedSinkFormats() []string { return []string{"parquet", "json"} }

func envelope(kind, name string, spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = kind
	e.Metadata.Name = name
	e.Spec = spec
	return e
}

func cdcManifests(engine string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "postgres-cdc", map[string]any{
			"type":    "debezium",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-postgres", map[string]any{
			"type":    "postgres",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Source", "student-database", map[string]any{
			"engine":      engine,
			"providerRef": map[string]any{"name": "local-postgres"},
		}),
		envelope("EventStream", "attendance-events", map[string]any{
			"providerRef": map[string]any{"name": "postgres-cdc"},
		}),
		envelope("Binding", "student-db-to-events", map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "student-database"},
			"targetRef":   map[string]any{"name": "attendance-events"},
			"providerRef": map[string]any{"name": "postgres-cdc"},
		}),
	}
}

func resolver(impl reconciler.Provider) ProviderResolver {
	return func(string) (reconciler.Provider, error) { return impl, nil }
}

// TestUnsupportedEngineErrorFormat covers the Phase 3 exit criterion: the
// validate-time error matches the documented shape exactly
// (docs/planning/02-architecture.md §5.2) — on the character, not in spirit.
func TestUnsupportedEngineErrorFormat(t *testing.T) {
	err := Check(cdcManifests("sqlite"), resolver(cdcStub{stubProvider{"debezium"}}))
	if err == nil {
		t.Fatal("validate accepted an unsupported source engine")
	}
	want := `Binding "student-db-to-events": Provider "postgres-cdc" (type: debezium)
does not support source engine "sqlite" (supported: mongodb, mysql, postgres)`
	if err.Error() != want {
		t.Errorf("error format mismatch\ngot:\n%s\nwant:\n%s", err.Error(), want)
	}
}

// TestNonCDCCapableProviderRejected: a Binding referencing a Provider that
// does not implement CDCCapableProvider fails at validate, not apply.
func TestNonCDCCapableProviderRejected(t *testing.T) {
	err := Check(cdcManifests("postgres"), resolver(stubProvider{"redpanda"}))
	if err == nil {
		t.Fatal("validate accepted a non-CDC-capable provider behind a cdc Binding")
	}
	if !strings.Contains(err.Error(), `does not support mode "cdc"`) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSupportedEngineAccepted(t *testing.T) {
	if err := Check(cdcManifests("postgres"), resolver(cdcStub{stubProvider{"debezium"}})); err != nil {
		t.Fatalf("valid CDC binding rejected: %v", err)
	}
}

func sinkManifests(format string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "s3-sink", map[string]any{
			"type":    "s3sink",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-minio", map[string]any{
			"type":    "minio",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("EventStream", "attendance-events", map[string]any{
			"providerRef": map[string]any{"name": "s3-sink"},
		}),
		envelope("Dataset", "attendance-raw", map[string]any{
			"providerRef": map[string]any{"name": "local-minio"},
			"bucket":      "raw-events",
			"format":      format,
		}),
		envelope("Binding", "attendance-events-to-lake", map[string]any{
			"mode":        "sink",
			"sourceRef":   map[string]any{"name": "attendance-events"},
			"targetRef":   map[string]any{"name": "attendance-raw"},
			"providerRef": map[string]any{"name": "s3-sink"},
		}),
	}
}

// TestUnsupportedSinkFormatErrorFormat covers the Phase 4 exit criterion:
// a Binding(mode: sink) whose Dataset format the provider cannot write fails
// at validate with the documented error shape
// (docs/planning/02-architecture.md §5.2).
func TestUnsupportedSinkFormatErrorFormat(t *testing.T) {
	err := Check(sinkManifests("avro"), resolver(sinkStub{stubProvider{"s3sink"}}))
	if err == nil {
		t.Fatal("validate accepted an unsupported sink format")
	}
	want := `Binding "attendance-events-to-lake": Provider "s3-sink" (type: s3sink)
does not support sink format "avro" (supported: json, parquet)`
	if err.Error() != want {
		t.Errorf("error format mismatch\ngot:\n%s\nwant:\n%s", err.Error(), want)
	}
}

// TestNonSinkCapableProviderRejected: a Binding(mode: sink) referencing a
// Provider that does not implement SinkCapableProvider fails at validate,
// not apply.
func TestNonSinkCapableProviderRejected(t *testing.T) {
	err := Check(sinkManifests("json"), resolver(stubProvider{"redpanda"}))
	if err == nil {
		t.Fatal("validate accepted a non-sink-capable provider behind a sink Binding")
	}
	if !strings.Contains(err.Error(), `does not support mode "sink"`) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSupportedSinkFormatAccepted(t *testing.T) {
	if err := Check(sinkManifests("json"), resolver(sinkStub{stubProvider{"s3sink"}})); err != nil {
		t.Fatalf("valid sink binding rejected: %v", err)
	}
}

type dbSinkStub struct{ stubProvider }

func (dbSinkStub) SupportedSinkEngines() []string { return []string{"postgres"} }

type ingestStub struct{ stubProvider }

func (ingestStub) SupportedIngestFormats() []string { return []string{"json", "parquet"} }

func dbSinkManifests(engine string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "jdbc-sink", map[string]any{
			"type":    "jdbcsink",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-postgres", map[string]any{
			"type":    "postgres",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("EventStream", "attendance-events", map[string]any{
			"providerRef": map[string]any{"name": "jdbc-sink"},
		}),
		envelope("Source", "reporting-db", map[string]any{
			"engine":      engine,
			"providerRef": map[string]any{"name": "local-postgres"},
		}),
		envelope("Binding", "events-to-reporting-db", map[string]any{
			"mode":        "sink",
			"sourceRef":   map[string]any{"name": "attendance-events"},
			"targetRef":   map[string]any{"name": "reporting-db"},
			"providerRef": map[string]any{"name": "jdbc-sink"},
		}),
	}
}

// TestDatabaseAsSink: a Source (an engine-backed database) is a legitimate
// sink-mode target — the taxonomy encodes direction in the Binding, not the
// noun. Capability gates which providers can realize it.
func TestDatabaseAsSink(t *testing.T) {
	if err := Check(dbSinkManifests("postgres"), resolver(dbSinkStub{stubProvider{"jdbcsink"}})); err != nil {
		t.Fatalf("sink Binding into a Source rejected despite capability: %v", err)
	}
	err := Check(dbSinkManifests("postgres"), resolver(sinkStub{stubProvider{"s3sink"}}))
	if err == nil || !strings.Contains(err.Error(), "database-sink capability") {
		t.Errorf("provider without DatabaseSinkCapableProvider accepted, err = %v", err)
	}
	err = Check(dbSinkManifests("mysql"), resolver(dbSinkStub{stubProvider{"jdbcsink"}}))
	if err == nil || !strings.Contains(err.Error(), `does not support sink engine "mysql"`) {
		t.Errorf("unsupported sink engine accepted, err = %v", err)
	}
}

func ingestManifests() []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "s3-ingest", map[string]any{
			"type":    "s3source",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-minio", map[string]any{
			"type":    "minio",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Dataset", "landed-files", map[string]any{
			"providerRef": map[string]any{"name": "local-minio"},
			"bucket":      "raw",
			"format":      "json",
		}),
		envelope("EventStream", "replayed-events", map[string]any{
			"providerRef": map[string]any{"name": "s3-ingest"},
		}),
		envelope("Binding", "files-to-events", map[string]any{
			"mode":        "ingest",
			"sourceRef":   map[string]any{"name": "landed-files"},
			"targetRef":   map[string]any{"name": "replayed-events"},
			"providerRef": map[string]any{"name": "s3-ingest"},
		}),
	}
}

// TestObjectStoreAsSource: a Dataset is a legitimate ingest-mode origin.
func TestObjectStoreAsSource(t *testing.T) {
	if err := Check(ingestManifests(), resolver(ingestStub{stubProvider{"s3source"}})); err != nil {
		t.Fatalf("ingest Binding from a Dataset rejected despite capability: %v", err)
	}
	err := Check(ingestManifests(), resolver(stubProvider{"redpanda"}))
	if err == nil || !strings.Contains(err.Error(), "ingest capability") {
		t.Errorf("provider without IngestCapableProvider accepted, err = %v", err)
	}
}

// TestDisallowedPairingListsAlternatives: nonsense pairings name what the
// mode actually connects.
func TestDisallowedPairingListsAlternatives(t *testing.T) {
	ms := ingestManifests()
	// ingest with the refs swapped: EventStream -> Dataset is not an ingest pairing.
	ms[4] = envelope("Binding", "files-to-events", map[string]any{
		"mode":        "ingest",
		"sourceRef":   map[string]any{"name": "replayed-events"},
		"targetRef":   map[string]any{"name": "landed-files"},
		"providerRef": map[string]any{"name": "s3-ingest"},
	})
	err := Check(ms, resolver(ingestStub{stubProvider{"s3source"}}))
	if err == nil || !strings.Contains(err.Error(), "allowed pairings: Dataset->EventStream") {
		t.Errorf("swapped ingest refs accepted or unclear, err = %v", err)
	}
}
