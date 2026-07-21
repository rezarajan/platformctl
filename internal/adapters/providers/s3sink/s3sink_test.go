package s3sink

import (
	"context"
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

func workerEnvelope(name string, configuration map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = "Provider"
	e.Metadata.Name = name
	e.Spec = map[string]any{
		"type":          "s3sink",
		"runtime":       map[string]any{"type": "fake"},
		"configuration": configuration,
	}
	return e
}

// TestReconcileWorkerBootstrapServersInferred mirrors debezium's coverage
// of docs/planning/08 E2: an omitted spec.configuration.bootstrapServers
// falls back to the engine's graph-inferred req.KafkaBootstrapServers, and
// the effective value is published into providerState for visibility.
func TestReconcileWorkerBootstrapServersInferred(t *testing.T) {
	rt := fakeruntime.New()
	env := workerEnvelope("sink", map[string]any{
		"image":                "datascape-s3sink-connect:local",
		"credentialsSecretRef": "creds",
	})
	p := New()
	req := reconciler.Request{
		Resource:              env,
		Provider:              env,
		Runtime:               rt,
		KafkaBootstrapServers: "broker:29092",
	}
	st, err := p.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := st.ProviderState["bootstrapServers"]; got != "broker:29092" {
		t.Errorf("providerState[bootstrapServers] = %v, want %q", got, "broker:29092")
	}
	ctrState, found, err := rt.Inspect(context.Background(), "sink")
	if err != nil || !found {
		t.Fatalf("Inspect: found=%v err=%v", found, err)
	}
	if got := ctrState.Env["BOOTSTRAP_SERVERS"]; got != "broker:29092" {
		t.Errorf("container BOOTSTRAP_SERVERS = %q, want %q", got, "broker:29092")
	}
}

// TestReconcileWorkerBootstrapServersRequiredWithoutInference: no declared
// value and nothing inferable fails clearly rather than starting with an
// empty Kafka address.
func TestReconcileWorkerBootstrapServersRequiredWithoutInference(t *testing.T) {
	rt := fakeruntime.New()
	env := workerEnvelope("sink", map[string]any{
		"image":                "datascape-s3sink-connect:local",
		"credentialsSecretRef": "creds",
	})
	p := New()
	req := reconciler.Request{Resource: env, Provider: env, Runtime: rt}
	if _, err := p.Reconcile(context.Background(), req); err == nil {
		t.Fatal("want an error when bootstrapServers is neither declared nor inferable")
	}
}
