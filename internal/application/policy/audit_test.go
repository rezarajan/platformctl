package policy

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/policy"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// TestAuditNoPoliciesPermitsEveryDeclaredEdge is Audit's base case: an empty
// policy set names every declared edge Permitted/JustifyNoDeny — "no
// policy governs this yet" is itself a nameable, honest justification
// (docs/planning/08 K5's audit table must never leave a row unexplained).
func TestAuditNoPoliciesPermitsEveryDeclaredEdge(t *testing.T) {
	t.Parallel()
	envelopes, g := cdcCrossDomainFixture(t, "payments", "analytics")

	audits := Audit(nil, envelopes, g, false)
	if len(audits) != 1 {
		t.Fatalf("got %d audits, want 1 (the Binding edge): %+v", len(audits), audits)
	}
	a := audits[0]
	if a.Verdict != Permitted || a.Justification != JustifyNoDeny {
		t.Fatalf("got verdict=%v justification=%v, want Permitted/JustifyNoDeny: %+v", a.Verdict, a.Justification, a)
	}
	if a.Kind != EdgeKindBinding {
		t.Errorf("kind = %v, want EdgeKindBinding", a.Kind)
	}
	if a.RuleID != "" {
		t.Errorf("ruleID = %q, want empty (no rule to name)", a.RuleID)
	}
	for _, want := range []string{"pg-src", "events"} {
		if !strings.Contains(a.Detail, want) {
			t.Errorf("detail %q does not name %q", a.Detail, want)
		}
	}
}

// TestAuditDenyRuleDeniesBindingEdge proves the crossDomain deny path names
// the firing rule.
func TestAuditDenyRuleDeniesBindingEdge(t *testing.T) {
	t.Parallel()
	envelopes, g := cdcCrossDomainFixture(t, "payments", "analytics")
	p := crossDomainPolicy("deny-payments-to-analytics", "payments", "analytics", policy.Deny)

	audits := Audit([]policy.Policy{p}, envelopes, g, false)
	if len(audits) != 1 {
		t.Fatalf("got %d audits, want 1: %+v", len(audits), audits)
	}
	a := audits[0]
	if a.Verdict != Denied || a.Justification != JustifyDenyRule {
		t.Fatalf("got verdict=%v justification=%v, want Denied/JustifyDenyRule: %+v", a.Verdict, a.Justification, a)
	}
	if a.RuleID != "deny-payments-to-analytics" {
		t.Errorf("ruleID = %q, want the firing rule id", a.RuleID)
	}
}

// TestAuditExemptionPermitsDeniedEdge proves ADR 021 §3's exemption
// mechanism flips a would-be-denied edge to Permitted/JustifyExemption,
// naming both the rule and the reason.
func TestAuditExemptionPermitsDeniedEdge(t *testing.T) {
	t.Parallel()
	src := domainEnv("Source", "pg-src", "payments", map[string]any{"providerRef": map[string]any{"name": "prov"}})
	stream := domainEnv("EventStream", "events", "analytics", map[string]any{"providerRef": map[string]any{"name": "prov"}})
	prov := domainEnv("Provider", "prov", "", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	binding := resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Binding"},
		Metadata: resource.Metadata{
			Name:        "cdc-binding",
			Annotations: map[string]string{policy.ExemptAnnotation: "deny-payments-to-analytics: reviewed exception"},
		},
		Spec: map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "pg-src"},
			"targetRef":   map[string]any{"name": "events"},
			"providerRef": map[string]any{"name": "prov"},
		},
	}
	envelopes := []resource.Envelope{src, stream, prov, binding}
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	rule := crossDomainPolicy("deny-payments-to-analytics", "payments", "analytics", policy.Deny)
	rule.Spec.Rules[0].Exemptible = true

	audits := Audit([]policy.Policy{rule}, envelopes, g, false)
	if len(audits) != 1 {
		t.Fatalf("got %d audits, want 1: %+v", len(audits), audits)
	}
	a := audits[0]
	if a.Verdict != Permitted || a.Justification != JustifyExemption {
		t.Fatalf("got verdict=%v justification=%v, want Permitted/JustifyExemption: %+v", a.Verdict, a.Justification, a)
	}
	if a.RuleID != "deny-payments-to-analytics" {
		t.Errorf("ruleID = %q, want the exempted rule id", a.RuleID)
	}
	if a.ExemptReason != "reviewed exception" {
		t.Errorf("exemptReason = %q, want the annotation's reason", a.ExemptReason)
	}
}

// TestAuditNonExemptibleDenyStaysDenied proves an exemption annotation
// naming a rule that does NOT declare exemptible: true is ignored — same
// bar applyExemptions enforces (ADR 021 §3).
func TestAuditNonExemptibleDenyStaysDenied(t *testing.T) {
	t.Parallel()
	src := domainEnv("Source", "pg-src", "payments", map[string]any{"providerRef": map[string]any{"name": "prov"}})
	stream := domainEnv("EventStream", "events", "analytics", map[string]any{"providerRef": map[string]any{"name": "prov"}})
	prov := domainEnv("Provider", "prov", "", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	binding := resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Binding"},
		Metadata: resource.Metadata{
			Name:        "cdc-binding",
			Annotations: map[string]string{policy.ExemptAnnotation: "deny-payments-to-analytics: nice try"},
		},
		Spec: map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "pg-src"},
			"targetRef":   map[string]any{"name": "events"},
			"providerRef": map[string]any{"name": "prov"},
		},
	}
	envelopes := []resource.Envelope{src, stream, prov, binding}
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	// exemptible left false (the default): the annotation must not unblock it.
	rule := crossDomainPolicy("deny-payments-to-analytics", "payments", "analytics", policy.Deny)

	audits := Audit([]policy.Policy{rule}, envelopes, g, false)
	if len(audits) != 1 || audits[0].Verdict != Denied || audits[0].Justification != JustifyDenyRule {
		t.Fatalf("got %+v, want a single Denied/JustifyDenyRule audit (non-exemptible rule ignores the annotation)", audits)
	}
}

// TestAuditGrantJustifiesPermittedEdge proves docs/adr/033's "a permitted
// edge's justification may be a grant, not only a policy rule": a
// spec.access wide grant with no matching matchGrant deny rule is
// Permitted/JustifyGrant, naming the declaring resource and namespace.
func TestAuditGrantJustifiesPermittedEdge(t *testing.T) {
	t.Parallel()
	r1 := domainEnv("Provider", "r1", "", map[string]any{
		"type": "noop", "runtime": map[string]any{"type": "fake"},
		"access": []any{map[string]any{"namespace": "b"}},
	})
	envelopes := []resource.Envelope{r1}

	audits := Audit(nil, envelopes, nil, false)
	if len(audits) != 1 {
		t.Fatalf("got %d audits, want 1 (the grant): %+v", len(audits), audits)
	}
	a := audits[0]
	if a.Kind != EdgeKindGrant {
		t.Errorf("kind = %v, want EdgeKindGrant", a.Kind)
	}
	if a.Verdict != Permitted || a.Justification != JustifyGrant {
		t.Fatalf("got verdict=%v justification=%v, want Permitted/JustifyGrant: %+v", a.Verdict, a.Justification, a)
	}
	if a.To != "namespace/b" {
		t.Errorf("to = %q, want namespace/b", a.To)
	}
	if !strings.Contains(a.Detail, "r1") || !strings.Contains(a.Detail, "b") {
		t.Errorf("detail %q does not name the declaring resource and namespace", a.Detail)
	}
}

// TestAuditMatchGrantDeniesGrantEdge proves a matchGrant deny rule denies
// the grant row, naming the rule.
func TestAuditMatchGrantDeniesGrantEdge(t *testing.T) {
	t.Parallel()
	r1 := domainEnv("Provider", "r1", "", map[string]any{
		"type": "noop", "runtime": map[string]any{"type": "fake"},
		"access": []any{map[string]any{"namespace": "b"}},
	})
	envelopes := []resource.Envelope{r1}
	p := grantPolicy("no-wide-grants-to-b", "b", policy.Deny)

	audits := Audit([]policy.Policy{p}, envelopes, nil, false)
	if len(audits) != 1 {
		t.Fatalf("got %d audits, want 1: %+v", len(audits), audits)
	}
	a := audits[0]
	if a.Verdict != Denied || a.Justification != JustifyDenyRule || a.RuleID != "no-wide-grants-to-b" {
		t.Fatalf("got %+v, want Denied/JustifyDenyRule naming no-wide-grants-to-b", a)
	}
}

// TestAuditEdgeSelectorGatedByLabelScopedAccess proves the audit's
// selector-edge handling mirrors Run's own gate: off, the selector rule
// contributes nothing (Permitted/JustifyNoDeny); on, it denies.
func TestAuditEdgeSelectorGatedByLabelScopedAccess(t *testing.T) {
	t.Parallel()
	conn := labelEnv("Connection", "gold-svc", map[string]string{"tier": "gold"}, map[string]any{
		"external": true, "host": "gold.example.com", "port": 5432,
	})
	consumer := labelEnv("Provider", "consumer", nil, map[string]any{
		"type": "noop", "external": true, "runtime": map[string]any{"type": "fake"},
		"connectionRef": map[string]any{"name": "gold-svc"},
	})
	envelopes := []resource.Envelope{conn, consumer}
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	var p policy.Policy
	p.APIVersion = policy.APIVersion
	p.Kind = policy.KindName
	p.Metadata.Name = "selector-pack"
	p.Spec.Rules = []policy.Rule{{
		ID: "gold-tier-requires-clearance",
		MatchEdge: &policy.EdgeMatch{Selector: &policy.EdgeSelector{
			From: &policy.Selector{MatchExpressions: []policy.SelectorRequirement{{Key: "clearance", Operator: policy.SelectorNotIn, Values: []string{"gold"}}}},
			To:   &policy.Selector{MatchLabels: map[string]string{"tier": "gold"}},
		}},
		Effect: policy.Deny,
	}}

	gateOff := Audit([]policy.Policy{p}, envelopes, g, false)
	if len(gateOff) != 1 || gateOff[0].Verdict != Permitted || gateOff[0].Justification != JustifyNoDeny {
		t.Fatalf("gate off: got %+v, want a single Permitted/JustifyNoDeny row", gateOff)
	}

	gateOn := Audit([]policy.Policy{p}, envelopes, g, true)
	if len(gateOn) != 1 || gateOn[0].Verdict != Denied || gateOn[0].RuleID != "gold-tier-requires-clearance" {
		t.Fatalf("gate on: got %+v, want a single Denied row naming gold-tier-requires-clearance", gateOn)
	}
}

// TestAuditEveryEdgeHasNameableJustification is docs/planning/08 K5's own
// accept bar, taken literally: "an edge with no nameable justification is
// a test failure." Builds a fixture mixing every EdgeKind/Justification
// combination Audit can produce and asserts each row is fully self-
// explanatory: Justification is always one of the four closed values,
// Detail is always non-empty, and RuleID/ExemptReason are populated
// exactly when the Justification implies a rule/reason exists.
func TestAuditEveryEdgeHasNameableJustification(t *testing.T) {
	t.Parallel()
	// Edge 1: Binding, no policy touches it -> Permitted/JustifyNoDeny.
	src := domainEnv("Source", "pg-src", "payments", map[string]any{"providerRef": map[string]any{"name": "prov"}})
	stream := domainEnv("EventStream", "events", "analytics", map[string]any{"providerRef": map[string]any{"name": "prov"}})
	prov := domainEnv("Provider", "prov", "", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	binding := domainEnv("Binding", "cdc-binding", "", map[string]any{
		"mode": "cdc", "sourceRef": map[string]any{"name": "pg-src"}, "targetRef": map[string]any{"name": "events"},
		"providerRef": map[string]any{"name": "prov"},
	})
	// Edge 2: connectionRef, denied outright.
	conn := domainEnv("Connection", "ext-db", "analytics", map[string]any{"target": "10.0.0.5:5432", "port": 5432, "scheme": "tcp"})
	consumer := domainEnv("Provider", "ext-src", "payments", map[string]any{
		"type": "postgres", "external": true, "runtime": map[string]any{"type": "fake"}, "connectionRef": map[string]any{"name": "ext-db"},
	})
	// Edge 3: grant, no matchGrant rule -> Permitted/JustifyGrant.
	granter := domainEnv("Provider", "granter", "", map[string]any{
		"type": "noop", "runtime": map[string]any{"type": "fake"}, "access": []any{map[string]any{"namespace": "shared"}},
	})

	envelopes := []resource.Envelope{src, stream, prov, binding, conn, consumer, granter}
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	deny := crossDomainPolicy("deny-payments-to-analytics", "payments", "analytics", policy.Deny)

	audits := Audit([]policy.Policy{deny}, envelopes, g, false)
	if len(audits) != 3 {
		t.Fatalf("got %d audits, want 3 (Binding + connectionRef + grant): %+v", len(audits), audits)
	}
	for _, a := range audits {
		if a.Verdict != Permitted && a.Verdict != Denied {
			t.Errorf("edge %s->%s: verdict %q is not one of Permitted/Denied", a.From, a.To, a.Verdict)
		}
		switch a.Justification {
		case JustifyNoDeny, JustifyGrant:
			if a.RuleID != "" {
				t.Errorf("edge %s->%s: justification %q should not name a rule, got %q", a.From, a.To, a.Justification, a.RuleID)
			}
		case JustifyDenyRule:
			if a.Verdict != Denied {
				t.Errorf("edge %s->%s: JustifyDenyRule but verdict is %q, want Denied", a.From, a.To, a.Verdict)
			}
			if a.RuleID == "" {
				t.Errorf("edge %s->%s: JustifyDenyRule with no rule id — no nameable justification", a.From, a.To)
			}
		case JustifyExemption:
			if a.RuleID == "" || a.ExemptReason == "" {
				t.Errorf("edge %s->%s: JustifyExemption missing rule id or reason — no nameable justification", a.From, a.To)
			}
		default:
			t.Errorf("edge %s->%s: unrecognized justification %q — no nameable justification", a.From, a.To, a.Justification)
		}
		if a.Detail == "" {
			t.Errorf("edge %s->%s: empty Detail — no human-readable justification", a.From, a.To)
		}
	}
}

// TestAuditDeterministicOrdering is the same determinism bar Run/RunPlan
// hold (docs/planning/08 H3): two independent Audit calls over the same
// inputs produce identical, stably-ordered output.
func TestAuditDeterministicOrdering(t *testing.T) {
	t.Parallel()
	src := domainEnv("Source", "pg-src", "payments", map[string]any{"providerRef": map[string]any{"name": "prov"}})
	stream := domainEnv("EventStream", "events", "analytics", map[string]any{"providerRef": map[string]any{"name": "prov"}})
	prov := domainEnv("Provider", "prov", "", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	binding := domainEnv("Binding", "cdc-binding", "", map[string]any{
		"mode": "cdc", "sourceRef": map[string]any{"name": "pg-src"}, "targetRef": map[string]any{"name": "events"},
		"providerRef": map[string]any{"name": "prov"},
	})
	granter := domainEnv("Provider", "granter", "", map[string]any{
		"type": "noop", "runtime": map[string]any{"type": "fake"}, "access": []any{map[string]any{"namespace": "shared"}},
	})
	envelopes := []resource.Envelope{src, stream, prov, binding, granter}
	g, err := graph.Build(envelopes)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	deny := crossDomainPolicy("deny-payments-to-analytics", "payments", "analytics", policy.Deny)

	first := Audit([]policy.Policy{deny}, envelopes, g, false)
	second := Audit([]policy.Policy{deny}, envelopes, g, false)
	if len(first) != len(second) {
		t.Fatalf("nondeterministic length: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("nondeterministic ordering at index %d: %+v vs %+v", i, first[i], second[i])
		}
	}
}
