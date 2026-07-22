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
	if _, err := (Assert{Field: "spec.x"}).Satisfied("x", true); err == nil {
		t.Fatal("expected an error for an Assert with no operator set")
	}
}

func TestFieldValue(t *testing.T) {
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
	rule := Rule{ID: "dup", Effect: Deny, Match: &Match{Kind: StringList{"Connection"}}, Assert: &Assert{Field: "spec.scheme", In: []any{"https"}}}
	policies := []Policy{validPolicy("a", rule), validPolicy("b", rule)}
	if err := Validate(policies); err == nil {
		t.Fatal("expected duplicate rule id error")
	}
}

func TestValidateRequiresExactlyOneSelectorShape(t *testing.T) {
	cases := []Rule{
		{ID: "r1", Effect: Deny}, // none of match+assert/matchFinding/matchPlan
		{ID: "r2", Effect: Deny, Match: &Match{Kind: StringList{"Connection"}}},                                                                                                        // match without assert
		{ID: "r3", Effect: Deny, Match: &Match{Kind: StringList{"Connection"}}, Assert: &Assert{Field: "spec.scheme", In: []any{"https"}}, MatchFinding: &FindingMatch{Code: "DL001"}}, // both
	}
	for _, r := range cases {
		t.Run(r.ID, func(t *testing.T) {
			if err := Validate([]Policy{validPolicy(r.ID, r)}); err == nil {
				t.Fatalf("rule %s: expected a shape-exclusivity validation error", r.ID)
			}
		})
	}
}

func TestValidateAssertOperatorExclusivity(t *testing.T) {
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
	rule := Rule{ID: "bad-effect", Effect: Effect("block"), Match: &Match{Kind: StringList{"Connection"}}, Assert: &Assert{Field: "spec.scheme", In: []any{"https"}}}
	if err := Validate([]Policy{validPolicy("p", rule)}); err == nil {
		t.Fatal("expected an invalid-effect error")
	}
}

func TestValidateBadRegexRejected(t *testing.T) {
	rule := Rule{ID: "bad-regex", Effect: Deny, Match: &Match{Kind: StringList{"Provider"}}, Assert: &Assert{Field: "spec.configuration.image", Matches: "(unclosed"}}
	if err := Validate([]Policy{validPolicy("p", rule)}); err == nil {
		t.Fatal("expected a regex compile error")
	}
}

func TestDecodeRoundTrip(t *testing.T) {
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
	if got := ParseExemptions(nil); got != nil {
		t.Errorf("ParseExemptions(nil) = %v, want nil", got)
	}
}
