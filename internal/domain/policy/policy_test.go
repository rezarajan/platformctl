package policy

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func envelope(kind, name string, spec map[string]any, labels map[string]string) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: kind},
		Metadata:         resource.Metadata{Name: name, Labels: labels},
		Spec:             spec,
	}
}

func TestMatchSelects(t *testing.T) {
	t.Parallel()
	e := envelope("Connection", "db-conn", map[string]any{"scheme": "tcp"}, map[string]string{"env": "prod"})

	cases := []struct {
		name string
		m    *Match
		want bool
	}{
		{"nil match selects everything", nil, true},
		{"kind match", &Match{Kind: StringList{"Connection"}}, true},
		{"kind mismatch", &Match{Kind: StringList{"Provider"}}, false},
		{"kind list match", &Match{Kind: StringList{"Dataset", "Connection"}}, true},
		{"name match", &Match{Name: "db-conn"}, true},
		{"name mismatch", &Match{Name: "other"}, false},
		{"label match", &Match{Label: map[string]string{"env": "prod"}}, true},
		{"label mismatch", &Match{Label: map[string]string{"env": "dev"}}, false},
		{"label key missing", &Match{Label: map[string]string{"missing": "x"}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.m.Selects(e); got != c.want {
				t.Errorf("Selects() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestAssertSatisfied(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		a       Assert
		value   any
		present bool
		want    bool
	}{
		{"equals match", Assert{Equals: true}, true, true, true},
		{"equals mismatch", Assert{Equals: true}, false, true, false},
		{"equals absent violates", Assert{Equals: true}, nil, false, false},
		{"notEquals absent satisfies", Assert{NotEquals: "none"}, nil, false, true},
		{"notEquals match violates", Assert{NotEquals: "none"}, "none", true, false},
		{"notEquals mismatch satisfies", Assert{NotEquals: "none"}, "all", true, true},
		{"in member satisfies", Assert{In: []any{"https"}}, "https", true, true},
		{"in absent violates", Assert{In: []any{"https"}}, nil, false, false},
		{"in non-member violates", Assert{In: []any{"https"}}, "http", true, false},
		{"absent true and field missing satisfies", Assert{Absent: true}, nil, false, true},
		{"absent true and field present violates", Assert{Absent: true}, "x", true, false},
		{"matches present satisfies", Assert{Matches: "^abc"}, "abcdef", true, true},
		{"matches absent violates by default pattern", Assert{Matches: "^abc"}, nil, false, false},
		{"matches absent satisfies with optional pattern", Assert{Matches: "^(abc)?$"}, nil, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.a.Satisfied(c.value, c.present)
			if err != nil {
				t.Fatalf("Satisfied() error: %v", err)
			}
			if got != c.want {
				t.Errorf("Satisfied() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestAssertSatisfiedNoOperatorErrors(t *testing.T) {
	t.Parallel()
	if _, err := (Assert{Field: "spec.x"}).Satisfied("x", true); err == nil {
		t.Fatal("expected an error for an Assert with no operator set")
	}
}

func TestFieldValue(t *testing.T) {
	t.Parallel()
	e := envelope("Connection", "db-conn", map[string]any{
		"scheme": "https",
		"tls":    map[string]any{"selfSigned": true},
	}, nil)
	e.Metadata.Protect = true

	val, present, err := FieldValue(e, "spec.scheme")
	if err != nil || !present || val != "https" {
		t.Fatalf("spec.scheme = (%v, %v), err=%v", val, present, err)
	}
	val, present, err = FieldValue(e, "spec.tls.selfSigned")
	if err != nil || !present || val != true {
		t.Fatalf("spec.tls.selfSigned = (%v, %v), err=%v", val, present, err)
	}
	_, present, err = FieldValue(e, "spec.missing.deeper")
	if err != nil || present {
		t.Fatalf("spec.missing.deeper: present=%v err=%v, want absent/no error", present, err)
	}
	val, present, err = FieldValue(e, "metadata.protect")
	if err != nil || !present || val != true {
		t.Fatalf("metadata.protect = (%v, %v), err=%v", val, present, err)
	}
}

func validPolicy(id string, rule Rule) Policy {
	var p Policy
	p.APIVersion = APIVersion
	p.Kind = KindName
	p.Metadata.Name = id
	p.Spec.Rules = []Rule{rule}
	return p
}

func TestValidateDuplicateRuleID(t *testing.T) {
	t.Parallel()
	rule := Rule{ID: "dup", Effect: Deny, Match: &Match{Kind: StringList{"Connection"}}, Assert: &Assert{Field: "spec.scheme", In: []any{"https"}}}
	policies := []Policy{validPolicy("a", rule), validPolicy("b", rule)}
	if err := Validate(policies); err == nil {
		t.Fatal("expected duplicate rule id error")
	}
}

func TestValidateRequiresExactlyOneSelectorShape(t *testing.T) {
	t.Parallel()
	cases := []Rule{
		{ID: "r1", Effect: Deny}, // none of match+assert/matchFinding/matchPlan
		{ID: "r2", Effect: Deny, Match: &Match{Kind: StringList{"Connection"}}},                                                                                                        // match without assert
		{ID: "r3", Effect: Deny, Match: &Match{Kind: StringList{"Connection"}}, Assert: &Assert{Field: "spec.scheme", In: []any{"https"}}, MatchFinding: &FindingMatch{Code: "DL001"}}, // both
		{ID: "r4", Effect: Deny, Match: &Match{Kind: StringList{"Provider"}}, MatchGrant: &GrantMatch{Namespace: "b"}},                                                                 // match + matchGrant
	}
	for _, r := range cases {
		t.Run(r.ID, func(t *testing.T) {
			if err := Validate([]Policy{validPolicy(r.ID, r)}); err == nil {
				t.Fatalf("rule %s: expected a shape-exclusivity validation error", r.ID)
			}
		})
	}
}

func TestRuleKindMatchEdge(t *testing.T) {
	t.Parallel()
	rule := Rule{ID: "cross-domain", Effect: Deny, MatchEdge: &EdgeMatch{CrossDomain: &CrossDomainSelector{From: "payments", To: "analytics"}}}
	if got := rule.Kind(); got != RuleKindEdge {
		t.Fatalf("Kind() = %v, want RuleKindEdge", got)
	}
	if err := Validate([]Policy{validPolicy("p", rule)}); err != nil {
		t.Fatalf("expected a well-formed matchEdge rule to validate, got %v", err)
	}
}

func TestValidateMatchEdgeRequiresFromAndTo(t *testing.T) {
	t.Parallel()
	cases := []Rule{
		{ID: "no-from", Effect: Deny, MatchEdge: &EdgeMatch{CrossDomain: &CrossDomainSelector{To: "analytics"}}},
		{ID: "no-to", Effect: Deny, MatchEdge: &EdgeMatch{CrossDomain: &CrossDomainSelector{From: "payments"}}},
	}
	for _, r := range cases {
		t.Run(r.ID, func(t *testing.T) {
			if err := Validate([]Policy{validPolicy(r.ID, r)}); err == nil {
				t.Fatalf("rule %s: expected a from/to-required validation error", r.ID)
			}
		})
	}
}

// TestSelectorRequirementSatisfied pins the four docs/adr/033 (Stage K2)
// matchExpressions operators against Kubernetes' own labels.Requirement.Matches
// semantics — In requires the key present with a matching value; NotIn
// matches when the key is ABSENT too (not just present-with-a-different-
// value), which is what lets a matchExpressions entry express a negative
// audience condition (e.g. "consumer lacks clearance: gold").
func TestSelectorRequirementSatisfied(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		req    SelectorRequirement
		labels map[string]string
		want   bool
	}{
		{"in-match", SelectorRequirement{Key: "clearance", Operator: SelectorIn, Values: []string{"gold"}}, map[string]string{"clearance": "gold"}, true},
		{"in-no-match", SelectorRequirement{Key: "clearance", Operator: SelectorIn, Values: []string{"gold"}}, map[string]string{"clearance": "silver"}, false},
		{"in-absent", SelectorRequirement{Key: "clearance", Operator: SelectorIn, Values: []string{"gold"}}, map[string]string{}, false},
		{"notin-absent", SelectorRequirement{Key: "clearance", Operator: SelectorNotIn, Values: []string{"gold"}}, map[string]string{}, true},
		{"notin-different-value", SelectorRequirement{Key: "clearance", Operator: SelectorNotIn, Values: []string{"gold"}}, map[string]string{"clearance": "silver"}, true},
		{"notin-same-value", SelectorRequirement{Key: "clearance", Operator: SelectorNotIn, Values: []string{"gold"}}, map[string]string{"clearance": "gold"}, false},
		{"exists-present", SelectorRequirement{Key: "clearance", Operator: SelectorExists}, map[string]string{"clearance": "anything"}, true},
		{"exists-absent", SelectorRequirement{Key: "clearance", Operator: SelectorExists}, map[string]string{}, false},
		{"doesnotexist-absent", SelectorRequirement{Key: "clearance", Operator: SelectorDoesNotExist}, map[string]string{}, true},
		{"doesnotexist-present", SelectorRequirement{Key: "clearance", Operator: SelectorDoesNotExist}, map[string]string{"clearance": "gold"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.req.Satisfied(tc.labels); got != tc.want {
				t.Errorf("Satisfied(%v) = %v, want %v", tc.labels, got, tc.want)
			}
		})
	}
}

// TestSelectorSelectsAndsMatchLabelsAndMatchExpressions proves matchLabels
// and matchExpressions compose with AND (both across the two blocks and
// across every entry within each) — docs/adr/033 decision 1.
func TestSelectorSelectsAndsMatchLabelsAndMatchExpressions(t *testing.T) {
	t.Parallel()
	sel := &Selector{
		MatchLabels: map[string]string{"tier": "gold"},
		MatchExpressions: []SelectorRequirement{
			{Key: "clearance", Operator: SelectorIn, Values: []string{"gold", "platinum"}},
		},
	}
	cases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"both-match", map[string]string{"tier": "gold", "clearance": "platinum"}, true},
		{"matchLabels-only", map[string]string{"tier": "gold"}, false},
		{"matchExpressions-only", map[string]string{"clearance": "gold"}, false},
		{"neither", map[string]string{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sel.Selects(tc.labels); got != tc.want {
				t.Errorf("Selects(%v) = %v, want %v", tc.labels, got, tc.want)
			}
		})
	}
}

// TestSelectorValidateRejectsEmpty and TestSelectorValidateRejectsBadOperator
// pin the docs/planning/08 K2 selector validation fixtures.
func TestSelectorValidateRejectsEmpty(t *testing.T) {
	t.Parallel()
	if err := (&Selector{}).validate(); err == nil {
		t.Fatal("expected an error for an empty selector (matches everything)")
	}
}

func TestSelectorValidateRejectsBadOperator(t *testing.T) {
	t.Parallel()
	sel := &Selector{MatchExpressions: []SelectorRequirement{{Key: "clearance", Operator: "Bogus"}}}
	if err := sel.validate(); err == nil {
		t.Fatal("expected an error for an unknown operator")
	}
}

func TestSelectorValidateRejectsInWithNoValues(t *testing.T) {
	t.Parallel()
	sel := &Selector{MatchExpressions: []SelectorRequirement{{Key: "clearance", Operator: SelectorIn}}}
	if err := sel.validate(); err == nil {
		t.Fatal("expected an error for In with no values")
	}
}

func TestSelectorValidateRejectsExistsWithValues(t *testing.T) {
	t.Parallel()
	sel := &Selector{MatchExpressions: []SelectorRequirement{{Key: "clearance", Operator: SelectorExists, Values: []string{"gold"}}}}
	if err := sel.validate(); err == nil {
		t.Fatal("expected an error for Exists with values set")
	}
}

func TestSelectorValidateAcceptsWellFormed(t *testing.T) {
	t.Parallel()
	sel := &Selector{
		MatchLabels:      map[string]string{"tier": "gold"},
		MatchExpressions: []SelectorRequirement{{Key: "clearance", Operator: SelectorExists}},
	}
	if err := sel.validate(); err != nil {
		t.Fatalf("validate() = %v, want nil", err)
	}
}

// TestMatchSelectsWithSelector proves Match.Selects composes the plain
// Label equality map and the richer Selector — docs/adr/033's "matchResource
// gains the same selector form" alongside the pre-existing Label map.
func TestMatchSelectsWithSelector(t *testing.T) {
	t.Parallel()
	m := &Match{Selector: &Selector{MatchExpressions: []SelectorRequirement{
		{Key: "clearance", Operator: SelectorExists},
	}}}
	withLabel := envelope("Provider", "p1", nil, map[string]string{"clearance": "gold"})
	withoutLabel := envelope("Provider", "p2", nil, nil)
	if !m.Selects(withLabel) {
		t.Error("Selects() = false, want true for a resource carrying the clearance label")
	}
	if m.Selects(withoutLabel) {
		t.Error("Selects() = true, want false for a resource with no clearance label")
	}
}

// TestRuleKindMatchEdgeSelector and TestValidateMatchEdgeSelector pin the
// docs/adr/033 (Stage K2) matchEdge.selector shape — the label-granularity
// generalization of matchEdge.crossDomain, exactly one of the two set.
func TestRuleKindMatchEdgeSelector(t *testing.T) {
	t.Parallel()
	rule := Rule{ID: "gold-tier-requires-clearance", Effect: Deny, MatchEdge: &EdgeMatch{Selector: &EdgeSelector{
		From: &Selector{MatchExpressions: []SelectorRequirement{{Key: "clearance", Operator: SelectorNotIn, Values: []string{"gold"}}}},
		To:   &Selector{MatchLabels: map[string]string{"tier": "gold"}},
	}}}
	if got := rule.Kind(); got != RuleKindEdge {
		t.Fatalf("Kind() = %v, want RuleKindEdge", got)
	}
	if err := Validate([]Policy{validPolicy("p", rule)}); err != nil {
		t.Fatalf("expected a well-formed matchEdge.selector rule to validate, got %v", err)
	}
}

func TestRuleKindInvalidWhenBothCrossDomainAndSelectorSet(t *testing.T) {
	t.Parallel()
	rule := Rule{ID: "both", Effect: Deny, MatchEdge: &EdgeMatch{
		CrossDomain: &CrossDomainSelector{From: "a", To: "b"},
		Selector:    &EdgeSelector{From: &Selector{MatchLabels: map[string]string{"x": "y"}}, To: &Selector{MatchLabels: map[string]string{"x": "y"}}},
	}}
	if got := rule.Kind(); got != RuleKindInvalid {
		t.Fatalf("Kind() = %v, want RuleKindInvalid when both crossDomain and selector are set", got)
	}
}

func TestValidateMatchEdgeSelectorRequiresFromAndTo(t *testing.T) {
	t.Parallel()
	cases := []Rule{
		{ID: "no-from", Effect: Deny, MatchEdge: &EdgeMatch{Selector: &EdgeSelector{To: &Selector{MatchLabels: map[string]string{"x": "y"}}}}},
		{ID: "no-to", Effect: Deny, MatchEdge: &EdgeMatch{Selector: &EdgeSelector{From: &Selector{MatchLabels: map[string]string{"x": "y"}}}}},
	}
	for _, r := range cases {
		t.Run(r.ID, func(t *testing.T) {
			if err := Validate([]Policy{validPolicy(r.ID, r)}); err == nil {
				t.Fatalf("rule %s: expected a from/to-required validation error", r.ID)
			}
		})
	}
}

// TestRuleKindMatchGrant pins docs/adr/026 §2's matchGrant selector
// (docs/planning/08 H7) — the same closed-shape-exclusivity dispatch
// TestRuleKindMatchEdge already pins for matchEdge.
func TestRuleKindMatchGrant(t *testing.T) {
	rule := Rule{ID: "no-wide-grants-to-b", Effect: Deny, MatchGrant: &GrantMatch{Namespace: "b"}}
	if got := rule.Kind(); got != RuleKindGrant {
		t.Fatalf("Kind() = %v, want RuleKindGrant", got)
	}
	if err := Validate([]Policy{validPolicy("p", rule)}); err != nil {
		t.Fatalf("expected a well-formed matchGrant rule to validate, got %v", err)
	}
}

func TestValidateMatchGrantRequiresNamespace(t *testing.T) {
	rule := Rule{ID: "no-namespace", Effect: Deny, MatchGrant: &GrantMatch{}}
	if err := Validate([]Policy{validPolicy("no-namespace", rule)}); err == nil {
		t.Fatal("expected a namespace-required validation error")
	}
}

func TestValidateAssertOperatorExclusivity(t *testing.T) {
	t.Parallel()
	rule := Rule{
		ID:     "multi-op",
		Effect: Deny,
		Match:  &Match{Kind: StringList{"Connection"}},
		Assert: &Assert{Field: "spec.scheme", Equals: "https", NotEquals: "tcp"},
	}
	if err := Validate([]Policy{validPolicy("p", rule)}); err == nil {
		t.Fatal("expected an error for an assert setting two operators")
	}
}

func TestValidateInvalidEffect(t *testing.T) {
	t.Parallel()
	rule := Rule{ID: "bad-effect", Effect: Effect("block"), Match: &Match{Kind: StringList{"Connection"}}, Assert: &Assert{Field: "spec.scheme", In: []any{"https"}}}
	if err := Validate([]Policy{validPolicy("p", rule)}); err == nil {
		t.Fatal("expected an invalid-effect error")
	}
}

func TestValidateBadRegexRejected(t *testing.T) {
	t.Parallel()
	rule := Rule{ID: "bad-regex", Effect: Deny, Match: &Match{Kind: StringList{"Provider"}}, Assert: &Assert{Field: "spec.configuration.image", Matches: "(unclosed"}}
	if err := Validate([]Policy{validPolicy("p", rule)}); err == nil {
		t.Fatal("expected a regex compile error")
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	raw := map[string]any{
		"apiVersion": APIVersion,
		"kind":       KindName,
		"metadata":   map[string]any{"name": "prod-zero-trust"},
		"spec": map[string]any{
			"rules": []any{
				map[string]any{
					"id":     "no-plaintext-connections",
					"match":  map[string]any{"kind": "Connection"},
					"assert": map[string]any{"field": "spec.scheme", "in": []any{"https"}},
					"effect": "deny",
				},
				map[string]any{
					"id":         "protect-data",
					"match":      map[string]any{"kind": []any{"Dataset", "Source"}},
					"assert":     map[string]any{"field": "metadata.protect", "equals": true},
					"effect":     "deny",
					"exemptible": true,
				},
			},
		},
	}
	p, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(p.Rules()) != 2 {
		t.Fatalf("got %d rules, want 2", len(p.Rules()))
	}
	if got := p.Rules()[0].Match.Kind; len(got) != 1 || got[0] != "Connection" {
		t.Errorf("rule 0 match.kind (singular) = %v", got)
	}
	if got := p.Rules()[1].Match.Kind; len(got) != 2 || got[0] != "Dataset" || got[1] != "Source" {
		t.Errorf("rule 1 match.kind (list) = %v", got)
	}
	if err := Validate([]Policy{p}); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestDecodeRejectsWrongAPIVersion(t *testing.T) {
	t.Parallel()
	raw := map[string]any{
		"apiVersion": "datascape.io/v1alpha1",
		"kind":       KindName,
		"metadata":   map[string]any{"name": "x"},
		"spec":       map[string]any{"rules": []any{}},
	}
	if _, err := Decode(raw); err == nil {
		t.Fatal("expected an apiVersion error")
	}
}

func TestSortDecisionsDeterministic(t *testing.T) {
	t.Parallel()
	decisions := []Decision{
		{RuleID: "z-rule", Effect: Warn, Resource: resource.Key{Namespace: "default", Kind: "Connection", Name: "b"}},
		{RuleID: "a-rule", Effect: Deny, Resource: resource.Key{Namespace: "default", Kind: "Connection", Name: "a"}},
		{RuleID: "a-rule", Effect: Deny, Resource: resource.Key{Namespace: "default", Kind: "Connection", Name: "z"}},
	}
	SortDecisions(decisions)
	if decisions[0].RuleID != "a-rule" || decisions[0].Resource.Name != "a" {
		t.Errorf("decisions[0] = %+v, want a-rule/a first (deny before warn, then rule id, then resource)", decisions[0])
	}
	if decisions[2].Effect != Warn {
		t.Errorf("decisions[2].Effect = %v, want Warn last", decisions[2].Effect)
	}
}

func TestParseExemptions(t *testing.T) {
	t.Parallel()
	anns := map[string]string{
		ExemptAnnotation: "no-plaintext-connections: local dev only\nforbid-env-secret-backend: no vault available\nmalformed-entry\nno-reason:   ",
	}
	got := ParseExemptions(anns)
	if len(got) != 2 {
		t.Fatalf("got %d exemptions, want 2 (malformed entries dropped): %+v", len(got), got)
	}
	if got[0].RuleID != "no-plaintext-connections" || got[0].Reason != "local dev only" {
		t.Errorf("got[0] = %+v", got[0])
	}
	if got[1].RuleID != "forbid-env-secret-backend" || got[1].Reason != "no vault available" {
		t.Errorf("got[1] = %+v", got[1])
	}
}

func TestParseExemptionsEmpty(t *testing.T) {
	t.Parallel()
	if got := ParseExemptions(nil); got != nil {
		t.Errorf("ParseExemptions(nil) = %v, want nil", got)
	}
}
