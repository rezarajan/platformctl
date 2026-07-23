package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// labelScopedPolicyYAML is docs/planning/08 K2's worked example: a
// matchEdge.selector rule ("gold-tier-requires-clearance", the label-
// granularity generalization of matchEdge.crossDomain) paired with a
// match.selector "who-may-wear-this-label" rule (docs/adr/033's label-
// integrity guardrail) — the two rules TestPolicyTestLabelScopedSelfClaim*
// below exercise together to prove the self-claim pitfall is actually
// closed, not just documented.
const labelScopedPolicyYAML = `
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: label-scoped-test
spec:
  rules:
    - id: gold-tier-requires-clearance
      matchEdge:
        selector:
          from: {matchExpressions: [{key: clearance, operator: NotIn, values: [gold]}]}
          to: {matchLabels: {tier: gold}}
      effect: deny
    - id: who-may-wear-clearance-label
      match:
        selector:
          matchExpressions: [{key: clearance, operator: Exists}]
      assert: {field: metadata.namespace, equals: "trusted"}
      effect: deny
      message: "only resources in namespace trusted may carry a clearance label"
`

// goldConnectionManifest is the shared "audience" target both fixtures
// below reference: an external Connection labeled tier: gold.
const goldConnectionManifest = `
apiVersion: datascape.io/v1alpha1
kind: Connection
metadata:
  name: gold-svc
  labels: {tier: gold}
spec:
  external: true
  host: gold.example.com
  port: 5432
`

func writeManifestDocs(t *testing.T, docs ...string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "manifests.yaml"), []byte(strings.Join(docs, "\n---\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// runPolicyTestJSON runs `platformctl policy test` with both PolicyEngine
// and LabelScopedAccess enabled and parses the -o json decisions document
// — mirrors policy_pack_examples_test.go's assertZeroTrustPackDecisions
// plumbing.
func runPolicyTestJSON(t *testing.T, manifestDir, policiesDir string) (policyDecisionTestOutput, string, error) {
	t.Helper()
	out, _, err := runSplit(t, "policy", "test", manifestDir, "--policies", policiesDir,
		"--feature-gates", "PolicyEngine=true,LabelScopedAccess=true", "-o", "json")
	var parsed policyDecisionTestOutput
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("parse policy test -o json output: %v\n%s", jsonErr, out)
	}
	return parsed, out, err
}

func deniedRuleIDs(out policyDecisionTestOutput) map[string]bool {
	fired := map[string]bool{}
	for _, d := range out.Decisions {
		if d.Effect == "deny" && !d.Exempted {
			fired[d.RuleID] = true
		}
	}
	return fired
}

// TestPolicyTestLabelScopedSelfClaimAttackFails is docs/planning/08 Stage K
// exit criterion 2's CI evidence: a consumer that labels ITSELF
// clearance: gold — self-claiming membership in the matchEdge.selector
// "audience" that would otherwise let it reach the tier:gold Connection —
// must still FAIL policy. The edge selector rule alone is fooled by the
// self-claim (the consumer now carries clearance: gold, so
// gold-tier-requires-clearance's from-selector no longer matches); the
// who-may-wear-clearance-label rule is what actually closes it, denying
// any resource carrying the clearance label outside namespace "trusted" —
// exactly ADR 033's "guardrails, in order of bite" self-claim section.
func TestPolicyTestLabelScopedSelfClaimAttackFails(t *testing.T) {
	t.Parallel()
	policiesDir := writePolicyDir(t, labelScopedPolicyYAML)
	manifestDir := writeManifestDocs(t, goldConnectionManifest, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: sneaky-consumer
  labels: {clearance: gold}
spec:
  type: noop
  external: true
  runtime: {type: fake}
  connectionRef: {name: gold-svc}
`)

	out, raw, err := runPolicyTestJSON(t, manifestDir, policiesDir)
	if err == nil {
		t.Fatalf("expected policy test to fail (nonzero exit) for the self-claim fixture:\n%s", raw)
	}
	fired := deniedRuleIDs(out)
	if !fired["who-may-wear-clearance-label"] {
		t.Errorf("expected who-may-wear-clearance-label to deny the self-claimed label, decisions:\n%s", raw)
	}
	if fired["gold-tier-requires-clearance"] {
		t.Errorf("gold-tier-requires-clearance should NOT fire once the consumer self-claims clearance:gold — that's exactly the loophole who-may-wear-clearance-label exists to close, decisions:\n%s", raw)
	}
	for _, d := range out.Decisions {
		if d.RuleID != "who-may-wear-clearance-label" {
			continue
		}
		if !strings.Contains(d.Resource, "sneaky-consumer") {
			t.Errorf("deny decision resource = %q, want it to name sneaky-consumer", d.Resource)
		}
	}
}

// TestPolicyTestLabelScopedLegitimateConsumerPasses is the positive
// polarity: a consumer legitimately declared in namespace "trusted" with
// clearance: gold reaches the same tier:gold Connection cleanly — neither
// rule fires.
func TestPolicyTestLabelScopedLegitimateConsumerPasses(t *testing.T) {
	t.Parallel()
	policiesDir := writePolicyDir(t, labelScopedPolicyYAML)
	manifestDir := writeManifestDocs(t, goldConnectionManifest, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: trusted-consumer
  namespace: trusted
  labels: {clearance: gold}
spec:
  type: noop
  external: true
  runtime: {type: fake}
  connectionRef: {name: gold-svc, namespace: default}
`)

	out, raw, err := runPolicyTestJSON(t, manifestDir, policiesDir)
	if err != nil {
		t.Fatalf("expected policy test to pass for the legitimate trusted consumer: %v\n%s", err, raw)
	}
	if fired := deniedRuleIDs(out); len(fired) != 0 {
		t.Errorf("expected zero deny decisions for the legitimate consumer, got %v:\n%s", fired, raw)
	}
}

// TestPolicyTestLabelScopedEdgeSelectorDeniesUnclearedConsumer is the
// matchEdge.selector rule firing on its own terms (no label-integrity
// rule needed): a consumer with no clearance label at all reaching a
// tier:gold Connection is denied outright, and the deny message names the
// rule, both selectors, and the edge key pair (docs/planning/08 K2 accept).
func TestPolicyTestLabelScopedEdgeSelectorDeniesUnclearedConsumer(t *testing.T) {
	t.Parallel()
	policiesDir := writePolicyDir(t, labelScopedPolicyYAML)
	manifestDir := writeManifestDocs(t, goldConnectionManifest, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: uncleared-consumer
spec:
  type: noop
  external: true
  runtime: {type: fake}
  connectionRef: {name: gold-svc}
`)

	out, raw, err := runPolicyTestJSON(t, manifestDir, policiesDir)
	if err == nil {
		t.Fatalf("expected policy test to fail for an uncleared consumer reaching a tier:gold Connection:\n%s", raw)
	}
	var found bool
	for _, d := range out.Decisions {
		if d.RuleID != "gold-tier-requires-clearance" || d.Effect != "deny" {
			continue
		}
		found = true
		if !strings.Contains(d.Resource, "uncleared-consumer") {
			t.Errorf("decision resource = %q, want it to name uncleared-consumer", d.Resource)
		}
		for _, want := range []string{"gold-svc", "uncleared-consumer", "clearance", "tier"} {
			if !strings.Contains(d.Message, want) {
				t.Errorf("message %q does not name %q (both selectors + the edge)", d.Message, want)
			}
		}
	}
	if !found {
		t.Fatalf("expected gold-tier-requires-clearance to fire, decisions:\n%s", raw)
	}
}

// TestPolicyTestLabelScopedGateOffIsByteIdentical is docs/planning/08 K2's
// gate-off pin: with LabelScopedAccess disabled, the exact same manifest +
// policy set that fails above (the uncleared-consumer fixture) validates
// cleanly — the new selector-bearing rules produce zero decisions, exactly
// as if they were absent, mirroring the graphscoped gate-off pin's shape
// (internal/application/engine/graphscoped_test.go
// TestGraphScopedAccessGateOffIsByteIdentical).
func TestPolicyTestLabelScopedGateOffIsByteIdentical(t *testing.T) {
	t.Parallel()
	policiesDir := writePolicyDir(t, labelScopedPolicyYAML)
	manifestDir := writeManifestDocs(t, goldConnectionManifest, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: uncleared-consumer
spec:
  type: noop
  external: true
  runtime: {type: fake}
  connectionRef: {name: gold-svc}
`)

	out, _, err := runSplit(t, "policy", "test", manifestDir, "--policies", policiesDir,
		"--feature-gates", "PolicyEngine=true", "-o", "json") // LabelScopedAccess left at its Alpha/disabled default
	if err != nil {
		t.Fatalf("gate-off: expected policy test to pass unaffected by the selector-bearing rules: %v\n%s", err, out)
	}
	var parsed policyDecisionTestOutput
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("parse policy test -o json output: %v\n%s", jsonErr, out)
	}
	if len(parsed.Decisions) != 0 {
		t.Fatalf("gate-off: got %d decision(s), want 0 (byte-identical to a policy set without the new rules): %+v", len(parsed.Decisions), parsed.Decisions)
	}
}

// TestPolicyInitZeroTrustIncludesWhoMayWearRule is docs/planning/08 Stage K
// exit criterion 2's other half: the shipped zero-trust pack itself (not
// just a synthetic test policy) ships a who-may-wear-this-label rule
// shape.
func TestPolicyInitZeroTrustIncludesWhoMayWearRule(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err, code := run(t, "policy", "init", "zero-trust", "--dir", dir); err != nil || code != 0 {
		t.Fatalf("policy init zero-trust failed (code %d): %v", code, err)
	}
	data, readErr := os.ReadFile(filepath.Join(dir, "policy.yaml"))
	if readErr != nil {
		t.Fatalf("read written pack: %v", readErr)
	}
	if !strings.Contains(string(data), "who-may-wear") {
		t.Fatalf("expected the shipped zero-trust pack to include a who-may-wear-this-label rule; got:\n%s", data)
	}
}
