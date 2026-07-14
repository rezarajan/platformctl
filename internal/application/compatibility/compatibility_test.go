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
