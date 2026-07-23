package policy

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	planpkg "github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/policy"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDirMissingIsNoPolicies(t *testing.T) {
	t.Parallel()
	policies, err := LoadDir(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil || policies != nil {
		t.Fatalf("LoadDir(missing) = (%v, %v), want (nil, nil)", policies, err)
	}
}

func TestLoadDirHappyPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, dir, "deny.yaml", `
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: test
spec:
  rules:
    - id: no-plaintext-connections
      match: {kind: Connection}
      assert: {field: spec.scheme, in: [https]}
      effect: deny
      exemptible: true
      message: "TLS required"
`)
	policies, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(policies) != 1 || len(policies[0].Rules()) != 1 {
		t.Fatalf("got %+v", policies)
	}
}

func TestLoadDirSchemaRejectsBadShape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, dir, "bad.yaml", `
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: test
spec:
  rules:
    - id: no-id-effect
      match: {kind: Connection}
`)
	if _, err := LoadDir(dir); err == nil {
		t.Fatal("expected a schema validation error (missing required effect)")
	}
}

func conn(name, scheme string) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Connection"},
		Metadata:         resource.Metadata{Name: name},
		Spec:             map[string]any{"scheme": scheme},
	}
}

func denyPlaintextPolicy(id string, exemptible bool) policy.Policy {
	var p policy.Policy
	p.APIVersion = policy.APIVersion
	p.Kind = policy.KindName
	p.Metadata.Name = "pack"
	p.Spec.Rules = []policy.Rule{{
		ID:         id,
		Match:      &policy.Match{Kind: policy.StringList{"Connection"}},
		Assert:     &policy.Assert{Field: "spec.scheme", In: []any{"https"}},
		Effect:     policy.Deny,
		Exemptible: exemptible,
		Message:    "TLS required",
	}}
	return p
}

func TestRunFieldAssertDeny(t *testing.T) {
	t.Parallel()
	envelopes := []resource.Envelope{conn("plain", "tcp"), conn("secure", "https")}
	decisions, err := Run([]policy.Policy{denyPlaintextPolicy("no-plaintext", false)}, envelopes, nil, nil, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 {
		t.Fatalf("got %d decisions, want 1: %+v", len(decisions), decisions)
	}
	if decisions[0].Resource.Name != "plain" || decisions[0].Effect != policy.Deny {
		t.Errorf("decisions[0] = %+v", decisions[0])
	}
}

func TestRunExemptionOnlyHonoredWhenExemptible(t *testing.T) {
	t.Parallel()
	exempted := conn("plain", "tcp")
	exempted.Metadata.Annotations = map[string]string{
		policy.ExemptAnnotation: "no-plaintext: local dev only",
	}

	// Non-exemptible rule: annotation present but ignored.
	decisions, err := Run([]policy.Policy{denyPlaintextPolicy("no-plaintext", false)}, []resource.Envelope{exempted}, nil, nil, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 || decisions[0].Exempted {
		t.Fatalf("non-exemptible rule: decisions = %+v, want one non-exempted decision", decisions)
	}

	// Exemptible rule: same annotation now honored.
	decisions, err = Run([]policy.Policy{denyPlaintextPolicy("no-plaintext", true)}, []resource.Envelope{exempted}, nil, nil, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 || !decisions[0].Exempted || decisions[0].ExemptReason != "local dev only" {
		t.Fatalf("exemptible rule: decisions = %+v, want one exempted decision with reason", decisions)
	}
}

func TestRunMatchFindingEscalation(t *testing.T) {
	t.Parallel()
	var p policy.Policy
	p.APIVersion = policy.APIVersion
	p.Kind = policy.KindName
	p.Metadata.Name = "pack"
	p.Spec.Rules = []policy.Rule{{
		ID:           "escalate-DL001",
		MatchFinding: &policy.FindingMatch{Code: "DL001"},
		Effect:       policy.Deny,
	}}

	findings := []lint.Finding{
		{Code: "DL001", Severity: lint.Warning, Resource: resource.Key{Namespace: "default", Kind: "Binding", Name: "b1"}, Message: "overlap"},
		{Code: "DL002", Severity: lint.Warning, Resource: resource.Key{Namespace: "default", Kind: "Binding", Name: "b2"}, Message: "collision"},
	}
	decisions, err := Run([]policy.Policy{p}, nil, nil, findings, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 || decisions[0].Resource.Name != "b1" {
		t.Fatalf("got %+v, want exactly one escalated DL001 decision", decisions)
	}
}

func TestRunPlanMatchesActionAndKind(t *testing.T) {
	t.Parallel()
	var p policy.Policy
	p.APIVersion = policy.APIVersion
	p.Kind = policy.KindName
	p.Metadata.Name = "pack"
	p.Spec.Rules = []policy.Rule{{
		ID:        "no-dataset-deletes",
		MatchPlan: &policy.PlanMatch{Action: "delete", Kind: "Dataset"},
		Effect:    policy.Deny,
	}}

	entries := []planpkg.Entry{
		{Key: resource.Key{Namespace: "default", Kind: "Dataset", Name: "raw"}, Action: planpkg.ActionDelete},
		{Key: resource.Key{Namespace: "default", Kind: "Source", Name: "db"}, Action: planpkg.ActionDelete},
		{Key: resource.Key{Namespace: "default", Kind: "Dataset", Name: "curated"}, Action: planpkg.ActionUpdate},
	}
	decisions, err := RunPlan([]policy.Policy{p}, nil, entries)
	if err != nil {
		t.Fatalf("RunPlan: %v", err)
	}
	if len(decisions) != 1 || decisions[0].Resource.Name != "raw" {
		t.Fatalf("got %+v, want exactly one decision for the Dataset delete", decisions)
	}
}

// domainEnv builds a minimal envelope carrying metadata.domain, for the
// crossDomain (docs/adr/022 Ring 0, docs/planning/08 H5) tests below.
func domainEnv(kind, name, domain string, spec map[string]any) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: kind},
		Metadata:         resource.Metadata{Name: name, Domain: domain},
		Spec:             spec,
	}
}

// cdcCrossDomainFixture builds docs/planning/08 H5's owner scenario: a cdc
// Binding whose sourceRef lives in domain "payments" and whose targetRef
// lives in domain "analytics" — plus the graph the two resolve through.
func cdcCrossDomainFixture(t *testing.T, sourceDomain, targetDomain string) ([]resource.Envelope, *graph.Graph) {
	t.Helper()
	src := domainEnv("Source", "pg-src", sourceDomain, map[string]any{"providerRef": map[string]any{"name": "prov"}})
	stream := domainEnv("EventStream", "events", targetDomain, map[string]any{"providerRef": map[string]any{"name": "prov"}})
	prov := domainEnv("Provider", "prov", "", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	binding := domainEnv("Binding", "cdc-binding", "", map[string]any{
		"mode":        "cdc",
		"sourceRef":   map[string]any{"name": "pg-src"},
		"targetRef":   map[string]any{"name": "events"},
		"providerRef": map[string]any{"name": "prov"},
	})
	envelopes := []resource.Envelope{src, stream, prov, binding}
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	return envelopes, g
}

func crossDomainPolicy(id, from, to string, effect policy.Effect) policy.Policy {
	var p policy.Policy
	p.APIVersion = policy.APIVersion
	p.Kind = policy.KindName
	p.Metadata.Name = "cross-domain-pack"
	p.Spec.Rules = []policy.Rule{{
		ID:        id,
		MatchEdge: &policy.EdgeMatch{CrossDomain: &policy.CrossDomainSelector{From: from, To: to}},
		Effect:    effect,
	}}
	return p
}

// TestRunCrossDomainDeniesBindingAcrossDomains is docs/planning/08 H5's
// accept criterion (a): "the owner-scenario's validate half — a cdc Binding
// whose source domain denies the sink's domain is caught at validate" — a
// cdc Binding whose sourceRef lives in domain "payments" and whose
// targetRef lives in domain "analytics", denied by a
// deny{from:payments,to:analytics} matchEdge.crossDomain rule (docs/adr/022
// Ring 0). The Decision must name both domains and the edge.
func TestRunCrossDomainDeniesBindingAcrossDomains(t *testing.T) {
	t.Parallel()
	envelopes, g := cdcCrossDomainFixture(t, "payments", "analytics")
	p := crossDomainPolicy("deny-payments-to-analytics", "payments", "analytics", policy.Deny)

	decisions, err := Run([]policy.Policy{p}, envelopes, g, nil, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 {
		t.Fatalf("got %d decisions, want 1: %+v", len(decisions), decisions)
	}
	d := decisions[0]
	if d.Resource.Kind != "Binding" || d.Resource.Name != "cdc-binding" {
		t.Errorf("decision resource = %+v, want the Binding cdc-binding", d.Resource)
	}
	if d.Effect != policy.Deny {
		t.Errorf("effect = %v, want Deny", d.Effect)
	}
	for _, want := range []string{"payments", "analytics", "pg-src", "events"} {
		if !strings.Contains(d.Message, want) {
			t.Errorf("message %q does not name %q (both domains + the edge)", d.Message, want)
		}
	}
}

// TestRunCrossDomainSameDomainNoDecision proves the selector matches the
// exact (from, to) domain pair only — a Binding whose source and target
// share a domain never fires a crossDomain rule naming two different
// domains, and undeclared (default) domains behave identically.
func TestRunCrossDomainSameDomainNoDecision(t *testing.T) {
	t.Parallel()
	envelopes, g := cdcCrossDomainFixture(t, "payments", "payments")
	p := crossDomainPolicy("deny-payments-to-analytics", "payments", "analytics", policy.Deny)

	decisions, err := Run([]policy.Policy{p}, envelopes, g, nil, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 0 {
		t.Fatalf("got %d decisions, want 0 (same-domain edge never matches a cross-domain selector): %+v", len(decisions), decisions)
	}
}

// TestRunCrossDomainConnectionConsumption proves the second edge shape ADR
// 022 Ring 0 names: "a Connection consumption is an edge" — a resource
// declaring connectionRef in one domain, naming a Connection in another,
// denied the same way a Binding's source/target edge is.
func TestRunCrossDomainConnectionConsumption(t *testing.T) {
	t.Parallel()
	conn := domainEnv("Connection", "ext-db", "analytics", map[string]any{
		"target": "10.0.0.5:5432", "port": 5432, "scheme": "tcp",
	})
	consumer := domainEnv("Provider", "ext-src", "payments", map[string]any{
		"type": "postgres", "external": true, "runtime": map[string]any{"type": "fake"},
		"connectionRef": map[string]any{"name": "ext-db"},
	})
	envelopes := []resource.Envelope{conn, consumer}
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	p := crossDomainPolicy("deny-payments-to-analytics", "payments", "analytics", policy.Deny)

	decisions, err := Run([]policy.Policy{p}, envelopes, g, nil, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 || decisions[0].Resource.Name != "ext-src" {
		t.Fatalf("got %+v, want exactly one decision on the connectionRef-declaring consumer", decisions)
	}
}

// labelEnv builds a minimal envelope carrying metadata.labels, for the
// docs/adr/033 (Stage K2) matchEdge.selector tests below — the label-
// granularity counterpart to domainEnv's metadata.domain.
func labelEnv(kind, name string, labels map[string]string, spec map[string]any) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: kind},
		Metadata:         resource.Metadata{Name: name, Labels: labels},
		Spec:             spec,
	}
}

// goldTierEdgeSelectorFixture builds a connectionRef edge — the same
// crossDomainEdges shape TestRunCrossDomainConnectionConsumption exercises
// — between a consumer and a Connection labeled tier: gold.
func goldTierEdgeSelectorFixture(t *testing.T, consumerLabels map[string]string) ([]resource.Envelope, *graph.Graph) {
	t.Helper()
	conn := labelEnv("Connection", "gold-svc", map[string]string{"tier": "gold"}, map[string]any{
		"external": true, "host": "gold.example.com", "port": float64(5432),
	})
	consumer := labelEnv("Provider", "consumer", consumerLabels, map[string]any{
		"type": "noop", "external": true, "runtime": map[string]any{"type": "fake"},
		"connectionRef": map[string]any{"name": "gold-svc"},
	})
	envelopes := []resource.Envelope{conn, consumer}
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	return envelopes, g
}

func edgeSelectorPolicy(id string, from, to *policy.Selector, effect policy.Effect) policy.Policy {
	var p policy.Policy
	p.APIVersion = policy.APIVersion
	p.Kind = policy.KindName
	p.Metadata.Name = "edge-selector-pack"
	p.Spec.Rules = []policy.Rule{{
		ID:        id,
		MatchEdge: &policy.EdgeMatch{Selector: &policy.EdgeSelector{From: from, To: to}},
		Effect:    effect,
	}}
	return p
}

// TestRunEdgeSelectorDeniesMatchingEdge is docs/planning/08 K2's accept
// criterion (b): an edge matches when the FROM endpoint's labels satisfy
// selector.from AND the TO endpoint's labels satisfy selector.to,
// evaluated over the SAME graph-derived edges crossDomain already uses.
// The Decision must name the edge (accept: "rule id, both selectors, and
// the edge key pair" — RuleID/Resource are the Decision's own fields; the
// Message names both selectors).
func TestRunEdgeSelectorDeniesMatchingEdge(t *testing.T) {
	t.Parallel()
	envelopes, g := goldTierEdgeSelectorFixture(t, nil) // no clearance label at all
	from := &policy.Selector{MatchExpressions: []policy.SelectorRequirement{{Key: "clearance", Operator: policy.SelectorNotIn, Values: []string{"gold"}}}}
	to := &policy.Selector{MatchLabels: map[string]string{"tier": "gold"}}
	p := edgeSelectorPolicy("gold-tier-requires-clearance", from, to, policy.Deny)

	decisions, err := Run([]policy.Policy{p}, envelopes, g, nil, true)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 {
		t.Fatalf("got %d decisions, want 1: %+v", len(decisions), decisions)
	}
	d := decisions[0]
	if d.Resource.Kind != "Provider" || d.Resource.Name != "consumer" {
		t.Errorf("decision resource = %+v, want the connectionRef-declaring consumer", d.Resource)
	}
	if d.Effect != policy.Deny {
		t.Errorf("effect = %v, want Deny", d.Effect)
	}
	for _, want := range []string{"gold-svc", "consumer", "clearance", "tier", "gold"} {
		if !strings.Contains(d.Message, want) {
			t.Errorf("message %q does not name %q (both selectors + the edge)", d.Message, want)
		}
	}
}

// TestRunEdgeSelectorNoMatchNoDecision is the positive polarity: a
// consumer that already satisfies selector.from (carries clearance: gold)
// never triggers the deny.
func TestRunEdgeSelectorNoMatchNoDecision(t *testing.T) {
	t.Parallel()
	envelopes, g := goldTierEdgeSelectorFixture(t, map[string]string{"clearance": "gold"})
	from := &policy.Selector{MatchExpressions: []policy.SelectorRequirement{{Key: "clearance", Operator: policy.SelectorNotIn, Values: []string{"gold"}}}}
	to := &policy.Selector{MatchLabels: map[string]string{"tier": "gold"}}
	p := edgeSelectorPolicy("gold-tier-requires-clearance", from, to, policy.Deny)

	decisions, err := Run([]policy.Policy{p}, envelopes, g, nil, true)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 0 {
		t.Fatalf("got %d decisions, want 0 (consumer already satisfies selector.from): %+v", len(decisions), decisions)
	}
}

// TestRunLabelScopedAccessGateOffIsByteIdentical is docs/planning/08 K2's
// gate-off pin (accept: "gate-off is byte-identical"), mirroring
// internal/application/engine/graphscoped_test.go's
// TestGraphScopedAccessGateOffIsByteIdentical shape at this package's own
// level: a policy set mixing a pre-existing matchEdge.crossDomain rule
// (unaffected by this gate) with a new matchEdge.selector rule (this
// gate's own vocabulary) produces the SAME decisions with the gate off as
// a policy set that never had the selector rule at all — the selector
// rule is skipped outright, not silently degraded.
func TestRunLabelScopedAccessGateOffIsByteIdentical(t *testing.T) {
	t.Parallel()
	crossDomainEnvelopes, g := cdcCrossDomainFixture(t, "payments", "analytics")
	crossDomain := crossDomainPolicy("deny-payments-to-analytics", "payments", "analytics", policy.Deny)

	baseline, err := Run([]policy.Policy{crossDomain}, crossDomainEnvelopes, g, nil, false)
	if err != nil {
		t.Fatalf("Run (baseline, no selector rule): %v", err)
	}

	edgeEnvelopes, _ := goldTierEdgeSelectorFixture(t, nil)
	edgeSelector := edgeSelectorPolicy("gold-tier-requires-clearance",
		&policy.Selector{MatchExpressions: []policy.SelectorRequirement{{Key: "clearance", Operator: policy.SelectorNotIn, Values: []string{"gold"}}}},
		&policy.Selector{MatchLabels: map[string]string{"tier": "gold"}}, policy.Deny)

	// Combine both fixtures/policies into one Run call, gate OFF: the
	// crossDomain decision must still fire (pre-existing behavior
	// untouched) while the selector rule produces nothing at all.
	combinedEnvelopes := append(append([]resource.Envelope{}, crossDomainEnvelopes...), edgeEnvelopes...)
	combinedGraph, err := graph.Build(combinedEnvelopes)
	if err != nil {
		t.Fatalf("graph.Build(combined): %v", err)
	}
	gateOff, err := Run([]policy.Policy{crossDomain, edgeSelector}, combinedEnvelopes, combinedGraph, nil, false)
	if err != nil {
		t.Fatalf("Run (gate off): %v", err)
	}
	if len(gateOff) != len(baseline) {
		t.Fatalf("gate-off: got %d decisions, want %d (byte-identical to the selector rule never existing): %+v", len(gateOff), len(baseline), gateOff)
	}
	for _, d := range gateOff {
		if d.RuleID == "gold-tier-requires-clearance" {
			t.Errorf("gate-off: the selector rule must never fire, got decision %+v", d)
		}
	}

	// Sanity: the SAME combined policy set + graph with the gate ON
	// produces MORE decisions than gate-off — proving the gate-off run
	// above genuinely suppressed something real, not that the selector
	// rule was inert for an unrelated reason.
	gateOn, err := Run([]policy.Policy{crossDomain, edgeSelector}, combinedEnvelopes, combinedGraph, nil, true)
	if err != nil {
		t.Fatalf("Run (gate on, sanity): %v", err)
	}
	if len(gateOn) <= len(gateOff) {
		t.Errorf("gate-on sanity: expected MORE decisions than gate-off, got %d vs %d", len(gateOn), len(gateOff))
	}
}

func grantPolicy(id, namespace string, effect policy.Effect) policy.Policy {
	var p policy.Policy
	p.APIVersion = policy.APIVersion
	p.Kind = policy.KindName
	p.Metadata.Name = "grant-pack"
	p.Spec.Rules = []policy.Rule{{
		ID:         id,
		MatchGrant: &policy.GrantMatch{Namespace: namespace},
		Effect:     effect,
	}}
	return p
}

// TestRunMatchGrantDeniesWideGrant pins docs/adr/026 decision 2 (docs/planning/08
// H7): "a matchGrant selector lets organizations deny or constrain wide
// grants" — a Provider declaring spec.access: [{namespace: b}] is denied by
// a matching matchGrant rule, named exactly like evaluateCrossDomain names
// its own edge.
func TestRunMatchGrantDeniesWideGrant(t *testing.T) {
	r1 := domainEnv("Provider", "r1", "", map[string]any{
		"type": "noop", "runtime": map[string]any{"type": "fake"},
		"access": []any{map[string]any{"namespace": "b"}},
	})
	envelopes := []resource.Envelope{r1}
	p := grantPolicy("no-wide-grants-to-b", "b", policy.Deny)

	decisions, err := Run([]policy.Policy{p}, envelopes, nil, nil, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 || decisions[0].Resource.Name != "r1" {
		t.Fatalf("got %+v, want exactly one decision on r1", decisions)
	}
	if decisions[0].Effect != policy.Deny {
		t.Errorf("effect = %v, want Deny", decisions[0].Effect)
	}
	if !strings.Contains(decisions[0].Message, "b") {
		t.Errorf("message %q does not name the granted namespace", decisions[0].Message)
	}
}

// TestRunMatchGrantOtherNamespaceNoDecision proves the selector matches the
// exact namespace only — a grant to a namespace the rule doesn't name never
// fires.
func TestRunMatchGrantOtherNamespaceNoDecision(t *testing.T) {
	r1 := domainEnv("Provider", "r1", "", map[string]any{
		"type": "noop", "runtime": map[string]any{"type": "fake"},
		"access": []any{map[string]any{"namespace": "c"}},
	})
	envelopes := []resource.Envelope{r1}
	p := grantPolicy("no-wide-grants-to-b", "b", policy.Deny)

	decisions, err := Run([]policy.Policy{p}, envelopes, nil, nil, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 0 {
		t.Fatalf("got %d decisions, want 0 (grant to a different namespace never matches): %+v", len(decisions), decisions)
	}
}

// TestRunDeterministic is the golden determinism bar (docs/planning/08 H3
// accept: "determinism golden") — two independent Run calls over the same
// inputs must produce byte-identical (deep-equal, stably ordered) output.
func TestRunDeterministic(t *testing.T) {
	t.Parallel()
	envelopes := []resource.Envelope{conn("a", "tcp"), conn("b", "tcp"), conn("c", "https")}
	policies := []policy.Policy{denyPlaintextPolicy("no-plaintext", false)}

	first, err := Run(policies, envelopes, nil, nil, false)
	if err != nil {
		t.Fatalf("Run (1st): %v", err)
	}
	second, err := Run(policies, envelopes, nil, nil, false)
	if err != nil {
		t.Fatalf("Run (2nd): %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Run is not deterministic:\n1st: %+v\n2nd: %+v", first, second)
	}
	if len(first) != 2 {
		t.Fatalf("got %d decisions, want 2 (a, b)", len(first))
	}
}

// TestDenyWinsPrecedence proves deny-wins: a warn rule and a deny rule
// matching the same resource both fire (no rule suppresses another), and
// deny sorts first (Less's effect rank) — the SCP-style guarantee ADR 021
// §1 names explicitly ("deny cannot be overridden by a later allow").
func TestDenyWinsPrecedence(t *testing.T) {
	t.Parallel()
	var warnPolicy policy.Policy
	warnPolicy.APIVersion = policy.APIVersion
	warnPolicy.Kind = policy.KindName
	warnPolicy.Metadata.Name = "warn-pack"
	warnPolicy.Spec.Rules = []policy.Rule{{
		ID:     "warn-plaintext",
		Match:  &policy.Match{Kind: policy.StringList{"Connection"}},
		Assert: &policy.Assert{Field: "spec.scheme", In: []any{"https"}},
		Effect: policy.Warn,
	}}
	denyPolicy := denyPlaintextPolicy("deny-plaintext", false)

	envelopes := []resource.Envelope{conn("plain", "tcp")}
	decisions, err := Run([]policy.Policy{warnPolicy, denyPolicy}, envelopes, nil, nil, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 2 {
		t.Fatalf("got %d decisions, want 2 (both rules fire independently)", len(decisions))
	}
	if decisions[0].Effect != policy.Deny || decisions[1].Effect != policy.Warn {
		t.Errorf("decisions not deny-first: %+v", decisions)
	}
}
