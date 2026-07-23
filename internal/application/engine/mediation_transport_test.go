package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// stubAddressResolver is docs/planning/08 L1's honest fake mediation.
// AddressResolver (ADR 028): deterministic "mediated://<from>-<to>:1"
// addresses, computed purely from the edge's own From/To names — no
// hidden state influences the answer, mirroring the fake runtime's own
// "same inputs, same result" discipline. written tracks each DISTINCT
// edge ever asked for (docs/planning/08 L1's idempotency proof: calling
// DialAddress twice for the identical edge must not behave as if a NEW
// edge were being realized).
type stubAddressResolver struct {
	mu      sync.Mutex
	calls   int
	written map[mediation.AddressEdge]bool
	err     error
}

func (s *stubAddressResolver) DialAddress(_ context.Context, edge mediation.AddressEdge) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return "", s.err
	}
	if s.written == nil {
		s.written = map[mediation.AddressEdge]bool{}
	}
	s.written[edge] = true
	return fmt.Sprintf("mediated://%s-%s:1", edge.From.Name, edge.To.Name), nil
}

func (s *stubAddressResolver) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.written)
}

func (s *stubAddressResolver) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// mtBrokerProvider is the EventStream-realizing broker fake both L1
// resolution surfaces need: KafkaBootstrapAddress (the graph-resolved
// wrapper) computed from its own runtime name + fixed port, mirroring
// kafkabootstrap_test.go's kafkaBootstrapStub convention (":29092").
// SchemaRegistryURL's own (Facts-based) surface reads a published
// "schema-registry" endpoint fact instead — supplied directly in each
// test's state.State (docs/planning/08 I9), not through this provider's
// own Reconcile, since these tests call resolveRequest directly rather
// than driving a full Apply.
type mtBrokerProvider struct{ noop.Provider }

func (mtBrokerProvider) Type() string { return "mtbroker" }

func (mtBrokerProvider) KafkaBootstrapAddress(name string, _ provider.Provider) string {
	return name + ":29092"
}

// mtManifest builds docs/planning/08 L1's worked scenario: a cdc-mode
// Binding (orders-cdc, providerRef: cdc-worker) whose sourceRef->Source
// and targetRef->EventStream(orders-events, providerRef: stream-broker)
// give resolveSchemaRegistryURL and resolveKafkaBootstrapServers each
// exactly one declared edge to resolve — orders-cdc -> stream-broker
// (schema registry, Facts-based) and cdc-worker -> stream-broker (kafka
// bootstrap, graph-resolved). bindingTransport is "" or "direct",
// written straight to the Binding's spec.transport.
func mtManifest(bindingTransport string) (byKey map[resource.Key]resource.Envelope, bindingEnv, workerEnv, brokerEnv resource.Envelope) {
	brokerEnv = envelope("Provider", "stream-broker", map[string]any{"type": "mtbroker", "runtime": map[string]any{"type": "fake"}})
	workerEnv = envelope("Provider", "cdc-worker", map[string]any{"type": "mtworker", "runtime": map[string]any{"type": "fake"}})
	dbProvEnv := envelope("Provider", "db", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	sourceEnv := envelope("Source", "orders-db", map[string]any{"engine": "postgres", "providerRef": map[string]any{"name": "db"}})
	esEnv := envelope("EventStream", "orders-events", map[string]any{"providerRef": map[string]any{"name": "stream-broker"}})
	bindingSpec := map[string]any{
		"mode":        "cdc",
		"sourceRef":   map[string]any{"name": "orders-db"},
		"targetRef":   map[string]any{"name": "orders-events"},
		"providerRef": map[string]any{"name": "cdc-worker"},
		"options":     map[string]any{"format": "avro"},
	}
	if bindingTransport != "" {
		bindingSpec["transport"] = bindingTransport
	}
	bindingEnv = envelope("Binding", "orders-cdc", bindingSpec)

	all := []resource.Envelope{brokerEnv, workerEnv, dbProvEnv, sourceEnv, esEnv, bindingEnv}
	byKey = make(map[resource.Key]resource.Envelope, len(all))
	for _, e := range all {
		byKey[e.Key()] = e
	}
	return byKey, bindingEnv, workerEnv, brokerEnv
}

// mtState publishes brokerEnv's "schema-registry" endpoint fact —
// resolveSchemaRegistryURL's Facts-based read (docs/planning/08 I9).
func mtState(brokerEnv resource.Envelope) *state.State {
	return &state.State{Resources: map[resource.Key]state.ResourceState{
		brokerEnv.Key(): {Provider: map[string]any{
			endpoint.Key: endpoint.List{{Name: "schema-registry", Scheme: "http", Internal: "http://stream-broker:8081"}}.ToState(),
		}},
	}}
}

// mtEngine wires a registry with MediatedTransport registered (enabled
// per gateOn) and the fixed mtbroker/mtworker/noop provider types +
// "fake" runtime mtManifest's envelopes need.
func mtEngine(t *testing.T, gateOn bool) *Engine {
	t.Helper()
	gates := featuregate.NewRegistry()
	gates.Register("MediatedTransport", featuregate.Alpha, false)
	if gateOn {
		if err := gates.Apply("MediatedTransport=true"); err != nil {
			t.Fatalf("enable MediatedTransport: %v", err)
		}
	}
	reg := registry.New(gates)
	reg.RegisterProvider("mtbroker", func() reconciler.Provider { return &mtBrokerProvider{} }, "")
	reg.RegisterProvider("mtworker", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	return newTestEngine(t, reg)
}

// TestMediatedTransportSubstitutesResolvedAddresses is docs/planning/08
// L1's core proof (a): with the gate on and a wired AddressResolver, BOTH
// named resolution surfaces — SchemaRegistryURL (Facts-based) and
// KafkaBootstrapServers (graph-resolved) — resolve to the mediated
// address for their own declared edge, not the unmediated one.
func TestMediatedTransportSubstitutesResolvedAddresses(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	stub := &stubAddressResolver{}
	eng.Mediation = stub
	byKey, bindingEnv, workerEnv, brokerEnv := mtManifest("")
	st := mtState(brokerEnv)
	ctx := context.Background()

	_, bindingReq, err := eng.resolveRequest(ctx, bindingEnv, byKey, st)
	if err != nil {
		t.Fatalf("resolveRequest(Binding): %v", err)
	}
	wantSchema := fmt.Sprintf("mediated://%s-%s:1", bindingEnv.Metadata.Name, brokerEnv.Metadata.Name)
	if bindingReq.SchemaRegistryURL != wantSchema {
		t.Errorf("SchemaRegistryURL = %q, want mediated address %q", bindingReq.SchemaRegistryURL, wantSchema)
	}

	_, workerReq, err := eng.resolveRequest(ctx, workerEnv, byKey, st)
	if err != nil {
		t.Fatalf("resolveRequest(Provider): %v", err)
	}
	wantKafka := fmt.Sprintf("mediated://%s-%s:1", workerEnv.Metadata.Name, brokerEnv.Metadata.Name)
	if workerReq.KafkaBootstrapServers != wantKafka {
		t.Errorf("KafkaBootstrapServers = %q, want mediated address %q", workerReq.KafkaBootstrapServers, wantKafka)
	}
}

// TestMediatedTransportDirectEdgeResolvesUnmediated is proof (b):
// spec.transport: direct on the declaring Binding opts BOTH edges it
// drives out of mediation — the Binding's own SchemaRegistryURL edge, and
// (since orders-cdc is the sole contributing Binding) the worker's
// KafkaBootstrapServers edge too (mediatedKafkaTransportDirect's "every
// contributing Binding" rule).
func TestMediatedTransportDirectEdgeResolvesUnmediated(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	stub := &stubAddressResolver{}
	eng.Mediation = stub
	byKey, bindingEnv, workerEnv, brokerEnv := mtManifest("direct")
	st := mtState(brokerEnv)
	ctx := context.Background()

	_, bindingReq, err := eng.resolveRequest(ctx, bindingEnv, byKey, st)
	if err != nil {
		t.Fatalf("resolveRequest(Binding): %v", err)
	}
	if bindingReq.SchemaRegistryURL != "http://stream-broker:8081" {
		t.Errorf("SchemaRegistryURL = %q, want the unmediated published endpoint (transport: direct)", bindingReq.SchemaRegistryURL)
	}

	_, workerReq, err := eng.resolveRequest(ctx, workerEnv, byKey, st)
	if err != nil {
		t.Fatalf("resolveRequest(Provider): %v", err)
	}
	if workerReq.KafkaBootstrapServers != "stream-broker:29092" {
		t.Errorf("KafkaBootstrapServers = %q, want the unmediated graph-resolved address (transport: direct)", workerReq.KafkaBootstrapServers)
	}
	if stub.callCount() != 0 {
		t.Errorf("stub.callCount() = %d, want 0 — a transport: direct edge must never even ask the mediator", stub.callCount())
	}
}

// TestMediatedTransportGateOffIsByteIdentical is proof (c), pinned the
// same way TestGraphScopedAccessGateOffIsByteIdentical pins H7: gate off
// resolves BOTH surfaces exactly as before this task, even with a fully
// wired, willing AddressResolver present — the gate, not the field or the
// Mediation wiring alone, is what turns substitution on.
func TestMediatedTransportGateOffIsByteIdentical(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, false)
	stub := &stubAddressResolver{}
	eng.Mediation = stub
	byKey, bindingEnv, workerEnv, brokerEnv := mtManifest("")
	st := mtState(brokerEnv)
	ctx := context.Background()

	_, bindingReq, err := eng.resolveRequest(ctx, bindingEnv, byKey, st)
	if err != nil {
		t.Fatalf("resolveRequest(Binding): %v", err)
	}
	if bindingReq.SchemaRegistryURL != "http://stream-broker:8081" {
		t.Errorf("gate-off: SchemaRegistryURL = %q, want byte-identical pre-L1 resolution", bindingReq.SchemaRegistryURL)
	}

	_, workerReq, err := eng.resolveRequest(ctx, workerEnv, byKey, st)
	if err != nil {
		t.Fatalf("resolveRequest(Provider): %v", err)
	}
	if workerReq.KafkaBootstrapServers != "stream-broker:29092" {
		t.Errorf("gate-off: KafkaBootstrapServers = %q, want byte-identical pre-L1 resolution", workerReq.KafkaBootstrapServers)
	}
	if stub.callCount() != 0 {
		t.Errorf("gate-off: stub.callCount() = %d, want 0 — the ENTIRE gate-off cost must be the bool check, docs/planning/08 H7's own promise extended to L1", stub.callCount())
	}
}

// TestMediatedTransportIdempotent is proof (d): resolving the SAME two
// requests twice yields byte-identical addresses both times, and the
// mediator sees exactly two DISTINCT edges ever written (schema + kafka)
// no matter how many times resolveRequest re-derives them — mirroring the
// Ensure*-idempotent contract every mediation.MediationProvider method
// already documents ("a second call ... makes no additional control-plane
// writes once observed state already matches desired state").
func TestMediatedTransportIdempotent(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	stub := &stubAddressResolver{}
	eng.Mediation = stub
	byKey, bindingEnv, workerEnv, brokerEnv := mtManifest("")
	st := mtState(brokerEnv)
	ctx := context.Background()

	var schemaURLs, kafkaAddrs []string
	for range 2 {
		_, bindingReq, err := eng.resolveRequest(ctx, bindingEnv, byKey, st)
		if err != nil {
			t.Fatalf("resolveRequest(Binding): %v", err)
		}
		schemaURLs = append(schemaURLs, bindingReq.SchemaRegistryURL)

		_, workerReq, err := eng.resolveRequest(ctx, workerEnv, byKey, st)
		if err != nil {
			t.Fatalf("resolveRequest(Provider): %v", err)
		}
		kafkaAddrs = append(kafkaAddrs, workerReq.KafkaBootstrapServers)
	}

	if schemaURLs[0] == "" || schemaURLs[0] != schemaURLs[1] {
		t.Errorf("SchemaRegistryURL not idempotent: %v", schemaURLs)
	}
	if kafkaAddrs[0] == "" || kafkaAddrs[0] != kafkaAddrs[1] {
		t.Errorf("KafkaBootstrapServers not idempotent: %v", kafkaAddrs)
	}
	if got := stub.writeCount(); got != 2 {
		t.Errorf("stub.writeCount() = %d, want exactly 2 (one per distinct edge) across two full resolutions", got)
	}
}

// TestMediatedTransportDialAddressErrorFailsResolve proves the seam
// refuses to silently degrade to an unmediated address when the gate is
// on and the mediator genuinely fails: docs/planning/08 L1 promotes
// mediation to the zero-trust plane once flipped on, so a failed dial
// request must fail resolveRequest, not hand the provider a plaintext
// fallback it never declared transport: direct for.
func TestMediatedTransportDialAddressErrorFailsResolve(t *testing.T) {
	t.Parallel()
	eng := mtEngine(t, true)
	stub := &stubAddressResolver{err: errors.New("fabric unavailable")}
	eng.Mediation = stub
	byKey, bindingEnv, _, brokerEnv := mtManifest("")
	st := mtState(brokerEnv)
	ctx := context.Background()

	if _, _, err := eng.resolveRequest(ctx, bindingEnv, byKey, st); err == nil {
		t.Fatal("resolveRequest must fail when the mediator errors and the edge is not transport: direct")
	}
}
