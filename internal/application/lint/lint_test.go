package lint

import (
	"context"
	"encoding/json"
	"sort"
	"testing"

	"github.com/rezarajan/platformctl/internal/application/compatibility"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// --- local provider doubles (mirrors compatibility_test.go's stubProvider
// pattern; CLAUDE.md's layering test-exception forbids importing real
// technology adapters here) -----------------------------------------------

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

func (cdcStub) SupportedSourceEngines() []string { return []string{"postgres"} }

type sinkAndDBStub struct{ stubProvider }

func (sinkAndDBStub) SupportedSinkFormats() []string { return []string{"json"} }
func (sinkAndDBStub) SupportedSinkEngines() []string { return []string{"postgres"} }

type catalogStub struct{ stubProvider }

func (catalogStub) SupportedCatalogEngines() []string { return []string{"stubengine"} }

type connectionStub struct {
	stubProvider
	schemes []string
}

func (c connectionStub) SupportedConnectionSchemes() []string { return c.schemes }

// designLinterStub lets tests exercise runProviderLints without a real
// adapter: it returns a fixed finding set and counts how many times
// LintDesign was called (to prove the "once per distinct type" contract).
type designLinterStub struct {
	stubProvider
	findings []Finding
	calls    *int
}

func (d designLinterStub) LintDesign([]resource.Envelope, *graph.Graph) []Finding {
	if d.calls != nil {
		*d.calls++
	}
	return d.findings
}

func envelope(kind, name string, spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = kind
	e.Metadata.Name = name
	e.Spec = spec
	return e
}

func ref(name string) map[string]any { return map[string]any{"name": name} }

func resolverFor(types map[string]reconciler.Provider) compatibility.ProviderResolver {
	return func(t string) (reconciler.Provider, error) {
		if impl, ok := types[t]; ok {
			return impl, nil
		}
		return stubProvider{typeName: t}, nil
	}
}

func mustGraph(t *testing.T, envelopes []resource.Envelope) *graph.Graph {
	t.Helper()
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	return g
}

func codesOf(findings []Finding) map[string]int {
	out := map[string]int{}
	for _, f := range findings {
		out[f.Code]++
	}
	return out
}

// --- DL001 -----------------------------------------------------------------

func TestDuplicateCapture(t *testing.T) {
	base := []resource.Envelope{
		envelope("Provider", "src-p", map[string]any{"type": "source-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "cdc-p", map[string]any{"type": "cdc-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "broker-p", map[string]any{"type": "broker-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Source", "db", map[string]any{"engine": "postgres", "providerRef": ref("src-p"), "deletionPolicy": "retain"}),
		envelope("EventStream", "es1", map[string]any{"providerRef": ref("broker-p")}),
		envelope("EventStream", "es2", map[string]any{"providerRef": ref("broker-p")}),
	}

	newBinding := func(name string, tables []any) resource.Envelope {
		spec := map[string]any{
			"mode": "cdc", "sourceRef": ref("db"), "targetRef": ref("es" + name[len(name)-1:]), "providerRef": ref("cdc-p"),
		}
		if tables != nil {
			spec["options"] = map[string]any{"tables": tables}
		}
		return envelope("Binding", name, spec)
	}

	t.Run("overlapping tables fire", func(t *testing.T) {
		envelopes := append(append([]resource.Envelope{}, base...),
			newBinding("cdc1", []any{"orders"}),
			newBinding("cdc2", []any{"orders", "users"}),
		)
		g := mustGraph(t, envelopes)
		findings, err := Run(envelopes, g, resolverFor(map[string]reconciler.Provider{"cdc-stub": cdcStub{stubProvider{"cdc-stub"}}}), Options{})
		if err != nil {
			t.Fatal(err)
		}
		n := codesOf(findings)[CodeDuplicateCapture]
		if n != 2 {
			t.Errorf("DL001 findings = %d, want 2 (one per overlapping Binding)", n)
		}
	})

	t.Run("disjoint tables don't fire", func(t *testing.T) {
		envelopes := append(append([]resource.Envelope{}, base...),
			newBinding("cdc1", []any{"orders"}),
			newBinding("cdc2", []any{"users"}),
		)
		g := mustGraph(t, envelopes)
		findings, err := Run(envelopes, g, resolverFor(map[string]reconciler.Provider{"cdc-stub": cdcStub{stubProvider{"cdc-stub"}}}), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodeDuplicateCapture]; n != 0 {
			t.Errorf("DL001 findings = %d, want 0 for disjoint table sets", n)
		}
	})

	t.Run("unset tables means all, always overlaps", func(t *testing.T) {
		envelopes := append(append([]resource.Envelope{}, base...),
			newBinding("cdc1", []any{"orders"}),
			newBinding("cdc2", nil),
		)
		g := mustGraph(t, envelopes)
		findings, err := Run(envelopes, g, resolverFor(map[string]reconciler.Provider{"cdc-stub": cdcStub{stubProvider{"cdc-stub"}}}), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodeDuplicateCapture]; n != 2 {
			t.Errorf("DL001 findings = %d, want 2 (unset tables = all, overlaps everything)", n)
		}
	})
}

// --- DL002 -----------------------------------------------------------------

func TestSinkCollision(t *testing.T) {
	envelopes := []resource.Envelope{
		envelope("Provider", "broker-p", map[string]any{"type": "broker-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "sink-p", map[string]any{"type": "sink-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "lake-p", map[string]any{"type": "lake-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("EventStream", "es1", map[string]any{"providerRef": ref("broker-p")}),
		envelope("EventStream", "es2", map[string]any{"providerRef": ref("broker-p")}),
		envelope("Dataset", "ds", map[string]any{"providerRef": ref("lake-p"), "bucket": "raw", "prefix": "events/", "format": "json", "deletionPolicy": "retain"}),
		envelope("Binding", "sink1", map[string]any{"mode": "sink", "sourceRef": ref("es1"), "targetRef": ref("ds"), "providerRef": ref("sink-p")}),
		envelope("Binding", "sink2", map[string]any{"mode": "sink", "sourceRef": ref("es2"), "targetRef": ref("ds"), "providerRef": ref("sink-p")}),
	}
	g := mustGraph(t, envelopes)
	findings, err := Run(envelopes, g, resolverFor(map[string]reconciler.Provider{"sink-stub": sinkAndDBStub{stubProvider{"sink-stub"}}}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if n := codesOf(findings)[CodeSinkCollision]; n != 2 {
		t.Errorf("DL002 findings = %d, want 2 (sink1 and sink2 both write ds bucket+prefix)", n)
	}
}

// --- DL003 -------------------------------------------------------------------

func TestObserverNotConsumed(t *testing.T) {
	makeEnvelopes := func(cdcType string) []resource.Envelope {
		e := envelope("Binding", "cdc1", map[string]any{
			"mode": "cdc", "sourceRef": ref("db"), "targetRef": ref("es1"), "providerRef": ref("cdc-p"),
		})
		e.Metadata.Observers = []resource.ObserverRef{{Name: "cdc-p"}}
		return []resource.Envelope{
			envelope("Provider", "src-p", map[string]any{"type": "source-stub", "runtime": map[string]any{"type": "fake"}}),
			envelope("Provider", "cdc-p", map[string]any{"type": cdcType, "runtime": map[string]any{"type": "fake"}}),
			envelope("Provider", "broker-p", map[string]any{"type": "broker-stub", "runtime": map[string]any{"type": "fake"}}),
			envelope("Source", "db", map[string]any{"engine": "postgres", "providerRef": ref("src-p"), "deletionPolicy": "retain"}),
			envelope("EventStream", "es1", map[string]any{"providerRef": ref("broker-p")}),
			e,
		}
	}

	t.Run("no LineageAware fires", func(t *testing.T) {
		envelopes := makeEnvelopes("cdc-stub")
		g := mustGraph(t, envelopes)
		findings, err := Run(envelopes, g, resolverFor(map[string]reconciler.Provider{"cdc-stub": cdcStub{stubProvider{"cdc-stub"}}}), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodeObserverNotConsumed]; n != 1 {
			t.Errorf("DL003 findings = %d, want 1", n)
		}
	})
}

// --- DL004 -------------------------------------------------------------------

func TestPlaintextBoundary(t *testing.T) {
	newEnvelopes := func(schemes []string, scheme string) []resource.Envelope {
		spec := map[string]any{"providerRef": ref("edge-p"), "port": 9999, "target": "internal:9999"}
		if scheme != "" {
			spec["scheme"] = scheme
		}
		return []resource.Envelope{
			envelope("Provider", "edge-p", map[string]any{"type": "edge-stub", "runtime": map[string]any{"type": "fake"}}),
			envelope("Connection", "conn", spec),
		}
	}
	resolve := func(schemes []string) compatibility.ProviderResolver {
		return resolverFor(map[string]reconciler.Provider{"edge-stub": connectionStub{stubProvider{"edge-stub"}, schemes}})
	}

	t.Run("plaintext with https-capable provider fires", func(t *testing.T) {
		envelopes := newEnvelopes(nil, "")
		g := mustGraph(t, envelopes)
		findings, err := Run(envelopes, g, resolve([]string{"tcp", "https"}), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodePlaintextBoundary]; n != 1 {
			t.Errorf("DL004 findings = %d, want 1", n)
		}
	})

	t.Run("no https support (today's providers) never fires", func(t *testing.T) {
		envelopes := newEnvelopes(nil, "")
		g := mustGraph(t, envelopes)
		findings, err := Run(envelopes, g, resolve([]string{"tcp"}), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodePlaintextBoundary]; n != 0 {
			t.Errorf("DL004 findings = %d, want 0 (no shipped provider advertises https yet)", n)
		}
	})
}

// --- DL010/011/012/013 -------------------------------------------------------

func TestOrphanedEventStreamAndUnreferenced(t *testing.T) {
	envelopes := []resource.Envelope{
		envelope("Provider", "broker-p", map[string]any{"type": "broker-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "catalog-p", map[string]any{"type": "catalog-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "idle-p", map[string]any{"type": "idle-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("EventStream", "es-orphan", map[string]any{"providerRef": ref("broker-p")}),
		envelope("Catalog", "cat-orphan", map[string]any{"engine": "stubengine", "providerRef": ref("catalog-p")}),
		envelope("SecretReference", "sec-unused", map[string]any{"backend": "env", "keys": []any{"k"}}),
	}
	g := mustGraph(t, envelopes)
	findings, err := Run(envelopes, g, resolverFor(nil), Options{})
	if err != nil {
		t.Fatal(err)
	}
	codes := codesOf(findings)
	if codes[CodeOrphanedEventStream] != 1 {
		t.Errorf("DL010 = %d, want 1", codes[CodeOrphanedEventStream])
	}
	if codes[CodeUnreferencedCatalog] != 1 {
		t.Errorf("DL011 = %d, want 1", codes[CodeUnreferencedCatalog])
	}
	// DL012 fires for idle-p (Provider) and sec-unused (SecretReference) —
	// catalog-p has a dependent (cat-orphan's providerRef).
	if codes[CodeUnusedResource] != 2 {
		t.Errorf("DL012 = %d, want 2 (idle-p Provider + sec-unused SecretReference)", codes[CodeUnusedResource])
	}
}

func TestDeadEndPipeline(t *testing.T) {
	envelopes := []resource.Envelope{
		envelope("Provider", "src-p", map[string]any{"type": "source-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "cdc-p", map[string]any{"type": "cdc-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "broker-p", map[string]any{"type": "broker-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Source", "db", map[string]any{"engine": "postgres", "providerRef": ref("src-p"), "deletionPolicy": "retain"}),
		envelope("EventStream", "es1", map[string]any{"providerRef": ref("broker-p")}),
		envelope("Binding", "cdc1", map[string]any{"mode": "cdc", "sourceRef": ref("db"), "targetRef": ref("es1"), "providerRef": ref("cdc-p")}),
	}
	g := mustGraph(t, envelopes)
	findings, err := Run(envelopes, g, resolverFor(map[string]reconciler.Provider{"cdc-stub": cdcStub{stubProvider{"cdc-stub"}}}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if n := codesOf(findings)[CodeDeadEndPipeline]; n != 1 {
		t.Errorf("DL013 = %d, want 1", n)
	}
}

// --- DL014 -------------------------------------------------------------------

func TestSingleReplicaWithHAGate(t *testing.T) {
	envelopes := []resource.Envelope{
		envelope("Provider", "broker-p", map[string]any{
			"type": "broker-stub", "runtime": map[string]any{"type": "fake"},
			"configuration": map[string]any{"brokers": 1},
		}),
	}
	g := mustGraph(t, envelopes)

	t.Run("gate off never fires", func(t *testing.T) {
		findings, err := Run(envelopes, g, resolverFor(nil), Options{HighAvailabilityEnabled: false})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodeSingleReplicaWithHAGate]; n != 0 {
			t.Errorf("DL014 = %d, want 0 with gate off", n)
		}
	})
	t.Run("gate on fires", func(t *testing.T) {
		findings, err := Run(envelopes, g, resolverFor(nil), Options{HighAvailabilityEnabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodeSingleReplicaWithHAGate]; n != 1 {
			t.Errorf("DL014 = %d, want 1 with gate on", n)
		}
	})
}

// --- DL020/DL021 --------------------------------------------------------------

func TestDeletionPolicyAndProtectUnset(t *testing.T) {
	envelopes := []resource.Envelope{
		envelope("Provider", "src-p", map[string]any{"type": "source-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Source", "db", map[string]any{"engine": "postgres", "providerRef": ref("src-p")}),
	}
	g := mustGraph(t, envelopes)

	t.Run("DL020 fires when deletionPolicy unset", func(t *testing.T) {
		findings, err := Run(envelopes, g, resolverFor(nil), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodeDeletionPolicyUnset]; n != 1 {
			t.Errorf("DL020 = %d, want 1", n)
		}
	})

	t.Run("DL021 needs prior state entries that imply an authoritative delete", func(t *testing.T) {
		findings, err := Run(envelopes, g, resolverFor(nil), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodeProtectUnset]; n != 0 {
			t.Errorf("DL021 = %d, want 0 with no state", n)
		}

		st := &state.State{Resources: map[resource.Key]state.ResourceState{
			{Namespace: "default", Kind: "Dataset", Name: "ghost"}: {},
		}}
		findings, err = Run(envelopes, g, resolverFor(nil), Options{State: st})
		if err != nil {
			t.Fatal(err)
		}
		if n := codesOf(findings)[CodeProtectUnset]; n != 1 {
			t.Errorf("DL021 = %d, want 1 once state implies an authoritative delete", n)
		}
	})
}

// --- Waivers -------------------------------------------------------------------

func TestWaivers(t *testing.T) {
	base := []resource.Envelope{
		envelope("Provider", "broker-p", map[string]any{"type": "broker-stub", "runtime": map[string]any{"type": "fake"}}),
	}

	t.Run("valid waiver suppresses the finding and is marked waived", func(t *testing.T) {
		es := envelope("EventStream", "es-orphan", map[string]any{"providerRef": ref("broker-p")})
		es.Metadata.Annotations = map[string]string{lint.WaiveAnnotation: "DL010: intentionally unread in this fixture"}
		envelopes := append(append([]resource.Envelope{}, base...), es)
		g := mustGraph(t, envelopes)
		findings, err := Run(envelopes, g, resolverFor(nil), Options{})
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, f := range findings {
			if f.Code == CodeOrphanedEventStream {
				found = true
				if !f.Waived || f.WaiverReason == "" {
					t.Errorf("DL010 finding: Waived=%v WaiverReason=%q, want waived with a reason", f.Waived, f.WaiverReason)
				}
			}
		}
		if !found {
			t.Fatal("expected the DL010 finding to still be present (waived), not removed")
		}
	})

	t.Run("empty reason does not waive and produces DL000", func(t *testing.T) {
		es := envelope("EventStream", "es-orphan", map[string]any{"providerRef": ref("broker-p")})
		es.Metadata.Annotations = map[string]string{lint.WaiveAnnotation: "DL010"}
		envelopes := append(append([]resource.Envelope{}, base...), es)
		g := mustGraph(t, envelopes)
		findings, err := Run(envelopes, g, resolverFor(nil), Options{})
		if err != nil {
			t.Fatal(err)
		}
		codes := codesOf(findings)
		if codes[CodeMalformedWaiver] != 1 {
			t.Errorf("DL000 = %d, want 1", codes[CodeMalformedWaiver])
		}
		for _, f := range findings {
			if f.Code == CodeOrphanedEventStream && f.Waived {
				t.Error("DL010 finding should not be marked waived when the waiver's reason is empty")
			}
		}
	})
}

// --- Provider orchestration -----------------------------------------------

func TestProviderDesignLinterCalledOncePerType(t *testing.T) {
	calls := 0
	stub := designLinterStub{
		stubProvider: stubProvider{"multi-stub"},
		findings: []Finding{{
			Code: "DL-multi-001", Severity: lint.Warning,
			Resource: resource.Key{Namespace: "default", Kind: "Provider", Name: "p1"}, Message: "stub finding",
		}},
		calls: &calls,
	}
	envelopes := []resource.Envelope{
		envelope("Provider", "p1", map[string]any{"type": "multi-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "p2", map[string]any{"type": "multi-stub", "runtime": map[string]any{"type": "fake"}}),
	}
	g := mustGraph(t, envelopes)
	findings, err := Run(envelopes, g, resolverFor(map[string]reconciler.Provider{"multi-stub": stub}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("LintDesign called %d times, want exactly 1 (once per distinct provider Type())", calls)
	}
	if n := codesOf(findings)["DL-multi-001"]; n != 1 {
		t.Errorf("DL-multi-001 findings = %d, want 1", n)
	}
}

// --- Determinism + ordering --------------------------------------------------

func TestRunDeterminism(t *testing.T) {
	envelopes, resolve, opts := allCodesFixture(t)
	g := mustGraph(t, envelopes)

	f1, err := Run(envelopes, g, resolve, opts)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := Run(envelopes, g, resolve, opts)
	if err != nil {
		t.Fatal(err)
	}
	j1, _ := json.Marshal(f1)
	j2, _ := json.Marshal(f2)
	if string(j1) != string(j2) {
		t.Fatalf("Run is not deterministic across repeated calls:\n%s\nvs\n%s", j1, j2)
	}
}

func TestFindingsAreSorted(t *testing.T) {
	envelopes, resolve, opts := allCodesFixture(t)
	g := mustGraph(t, envelopes)
	findings, err := Run(envelopes, g, resolve, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !sort.SliceIsSorted(findings, func(i, j int) bool { return lint.Less(findings[i], findings[j]) }) {
		t.Fatal("findings are not sorted by (severity, code, resource key)")
	}
}

// TestAllBuiltinCodesFixture is H1's "fixture manifest set exhibiting every
// DL code, golden-verified" — one manifest set engineered to trigger every
// built-in code (DL000 plus the full ADR 020 §4 table), asserting the exact
// set of codes produced.
func TestAllBuiltinCodesFixture(t *testing.T) {
	envelopes, resolve, opts := allCodesFixture(t)
	g := mustGraph(t, envelopes)
	findings, err := Run(envelopes, g, resolve, opts)
	if err != nil {
		t.Fatal(err)
	}
	codes := codesOf(findings)
	for _, want := range BuiltinCodes {
		if codes[want] == 0 {
			t.Errorf("fixture did not trigger %s — every built-in code must appear at least once", want)
		}
	}
	// Reverse direction: every code the fixture produced is a known one (no
	// typo'd/unexpected code sneaking in).
	known := map[string]bool{}
	for _, c := range BuiltinCodes {
		known[c] = true
	}
	for code := range codes {
		if !known[code] {
			t.Errorf("fixture produced unexpected code %s not in BuiltinCodes", code)
		}
	}
}

// allCodesFixture builds the manifest set + resolver + Options that
// exercises every built-in code exactly once (see TASK_PROGRESS.md's
// design notes for the layout rationale).
func allCodesFixture(t *testing.T) ([]resource.Envelope, compatibility.ProviderResolver, Options) {
	t.Helper()
	envelopes := []resource.Envelope{
		envelope("Provider", "src-p", map[string]any{"type": "source-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "cdc-p", map[string]any{"type": "cdc-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "broker-p", map[string]any{
			"type": "broker-stub", "runtime": map[string]any{"type": "fake"},
			"configuration": map[string]any{"brokers": 1},
		}),
		envelope("Provider", "sink-p", map[string]any{"type": "sink-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "lake-p", map[string]any{"type": "lake-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "catalog-p", map[string]any{"type": "catalog-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "edge-p", map[string]any{"type": "edge-stub", "runtime": map[string]any{"type": "fake"}}),
		envelope("Provider", "idle-p", map[string]any{"type": "idle-stub", "runtime": map[string]any{"type": "fake"}}),

		// DL001 + DL020 + DL021: src-dup has 2 overlapping cdc captures,
		// unset deletionPolicy, and unset protect (state below implies an
		// authoritative delete).
		envelope("Source", "src-dup", map[string]any{"engine": "postgres", "providerRef": ref("src-p")}),
		envelope("EventStream", "es-dup1", map[string]any{"providerRef": ref("broker-p")}),
		envelope("EventStream", "es-dup2", map[string]any{"providerRef": ref("broker-p")}),
		envelope("Binding", "cdc1", map[string]any{
			"mode": "cdc", "sourceRef": ref("src-dup"), "targetRef": ref("es-dup1"), "providerRef": ref("cdc-p"),
			"options": map[string]any{"tables": []any{"orders"}},
		}),
		envelope("Binding", "cdc2", map[string]any{
			"mode": "cdc", "sourceRef": ref("src-dup"), "targetRef": ref("es-dup2"), "providerRef": ref("cdc-p"),
			"options": map[string]any{"tables": []any{"orders", "users"}},
		}),

		// DL002: sink1/sink2 both write Dataset ds-a's bucket+prefix; also
		// keeps es-dup1/es-dup2 from tripping DL013.
		envelope("Dataset", "ds-a", map[string]any{
			"providerRef": ref("lake-p"), "bucket": "raw", "prefix": "events/", "format": "json",
			"deletionPolicy": "retain",
		}),
		envelope("Binding", "sink1", map[string]any{"mode": "sink", "sourceRef": ref("es-dup1"), "targetRef": ref("ds-a"), "providerRef": ref("sink-p")}),
		envelope("Binding", "sink2", map[string]any{"mode": "sink", "sourceRef": ref("es-dup2"), "targetRef": ref("ds-a"), "providerRef": ref("sink-p")}),

		// DL003 + DL013: cdc-deadend's EventStream has no downstream, and
		// cdc-p (its own provider) implements no LineageAware.
		envelope("Source", "src-deadend", map[string]any{"engine": "postgres", "providerRef": ref("src-p"), "deletionPolicy": "retain", "protect": true}),
		envelope("EventStream", "es-deadend", map[string]any{"providerRef": ref("broker-p")}),
		func() resource.Envelope {
			e := envelope("Binding", "cdc-deadend", map[string]any{
				"mode": "cdc", "sourceRef": ref("src-deadend"), "targetRef": ref("es-deadend"), "providerRef": ref("cdc-p"),
			})
			e.Metadata.Observers = []resource.ObserverRef{{Name: "catalog-p"}}
			return e
		}(),

		// DL010: nothing reads or writes es-orphan.
		envelope("EventStream", "es-orphan", map[string]any{"providerRef": ref("broker-p")}),

		// DL011: cat-orphan has no consumer.
		envelope("Catalog", "cat-orphan", map[string]any{"engine": "stubengine", "providerRef": ref("catalog-p")}),

		// DL012: idle-p (Provider) and sec-unused (SecretReference) have no
		// consumer; conn-plaintext (Connection, below) doubles as the
		// Connection sub-case.
		envelope("SecretReference", "sec-unused", map[string]any{"backend": "env", "keys": []any{"k"}}),

		// DL004 (+ DL012's Connection sub-case, + DL000 malformed-waiver
		// annotation on the same resource): plaintext scheme against a
		// provider that also advertises https, unreferenced by anything,
		// carrying a reason-less waiver naming its own DL004 finding.
		func() resource.Envelope {
			e := envelope("Connection", "conn-plaintext", map[string]any{
				"providerRef": ref("edge-p"), "port": 9999, "target": "internal:9999",
			})
			e.Metadata.Annotations = map[string]string{lint.WaiveAnnotation: "DL004"}
			return e
		}(),
	}

	resolve := resolverFor(map[string]reconciler.Provider{
		"cdc-stub":     cdcStub{stubProvider{"cdc-stub"}},
		"sink-stub":    sinkAndDBStub{stubProvider{"sink-stub"}},
		"catalog-stub": catalogStub{stubProvider{"catalog-stub"}},
		"edge-stub":    connectionStub{stubProvider{"edge-stub"}, []string{"tcp", "https"}},
	})

	st := &state.State{Resources: map[resource.Key]state.ResourceState{
		{Namespace: "default", Kind: "Dataset", Name: "ghost-in-state"}: {},
	}}
	return envelopes, resolve, Options{HighAvailabilityEnabled: true, State: st}
}
