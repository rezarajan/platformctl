package policy

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	planpkg "github.com/rezarajan/platformctl/internal/application/plan"
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
	policies, err := LoadDir(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil || policies != nil {
		t.Fatalf("LoadDir(missing) = (%v, %v), want (nil, nil)", policies, err)
	}
}

func TestLoadDirHappyPath(t *testing.T) {
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
	envelopes := []resource.Envelope{conn("plain", "tcp"), conn("secure", "https")}
	decisions, err := Run([]policy.Policy{denyPlaintextPolicy("no-plaintext", false)}, envelopes, nil, nil)
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
	exempted := conn("plain", "tcp")
	exempted.Metadata.Annotations = map[string]string{
		policy.ExemptAnnotation: "no-plaintext: local dev only",
	}

	// Non-exemptible rule: annotation present but ignored.
	decisions, err := Run([]policy.Policy{denyPlaintextPolicy("no-plaintext", false)}, []resource.Envelope{exempted}, nil, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 || decisions[0].Exempted {
		t.Fatalf("non-exemptible rule: decisions = %+v, want one non-exempted decision", decisions)
	}

	// Exemptible rule: same annotation now honored.
	decisions, err = Run([]policy.Policy{denyPlaintextPolicy("no-plaintext", true)}, []resource.Envelope{exempted}, nil, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 || !decisions[0].Exempted || decisions[0].ExemptReason != "local dev only" {
		t.Fatalf("exemptible rule: decisions = %+v, want one exempted decision with reason", decisions)
	}
}

func TestRunMatchFindingEscalation(t *testing.T) {
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
	decisions, err := Run([]policy.Policy{p}, nil, nil, findings)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(decisions) != 1 || decisions[0].Resource.Name != "b1" {
		t.Fatalf("got %+v, want exactly one escalated DL001 decision", decisions)
	}
}

func TestRunPlanMatchesActionAndKind(t *testing.T) {
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

// TestRunDeterministic is the golden determinism bar (docs/planning/08 H3
// accept: "determinism golden") — two independent Run calls over the same
// inputs must produce byte-identical (deep-equal, stably ordered) output.
func TestRunDeterministic(t *testing.T) {
	envelopes := []resource.Envelope{conn("a", "tcp"), conn("b", "tcp"), conn("c", "https")}
	policies := []policy.Policy{denyPlaintextPolicy("no-plaintext", false)}

	first, err := Run(policies, envelopes, nil, nil)
	if err != nil {
		t.Fatalf("Run (1st): %v", err)
	}
	second, err := Run(policies, envelopes, nil, nil)
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
	decisions, err := Run([]policy.Policy{warnPolicy, denyPolicy}, envelopes, nil, nil)
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
