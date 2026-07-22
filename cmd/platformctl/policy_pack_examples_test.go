package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/rezarajan/platformctl/internal/application/blueprint"
	apppolicy "github.com/rezarajan/platformctl/internal/application/policy"
)

// knownNonExemptiblePostureFindings records the zero-trust pack's
// non-exemptible rule ids that fire against every shipped example and
// blueprint by design (docs/planning/08 §7.7 H4 evaluation) — a real,
// currently-open posture finding, not a bug in this test or a bypass of
// the pack:
//
//   - protect-data (metadata.protect: true required on Dataset/Source):
//     no shipped example/blueprint sets it, and setting it would also
//     unconditionally refuse `destroy` for that resource (plan.go's
//     isProtected/ComputeDestroy have no override) — breaking every
//     dev/test teardown, not just tightening posture.
//   - secrets-from-vault-or-k8s (SecretReference.spec.backend must be
//     vault|kubernetes): every shipped example/blueprint uses backend:
//     env for local credentials. The pack's OWN exemptible
//     forbid-env-secret-backend rule covers the identical fact, but ADR
//     021 §3 makes exemptions rule-scoped — the exemptible rule's waiver
//     cannot silence its non-exemptible twin's decision on the same
//     resource.
//
// Both are recorded here deliberately (docs/planning/08 H4's Done-note)
// rather than routed around: the pack is not weakened (no exemptible flag
// flip) and no exemption annotation is added for either rule id (ADR 021
// §3: a non-exemptible deny has no in-manifest escape, and
// applyExemptions ignores any annotation naming one anyway). This test's
// job is to catch *new* posture regressions — any unexempted deny whose
// rule id is not in this set fails the build.
var knownNonExemptiblePostureFindings = map[string]bool{
	"protect-data":              true,
	"secrets-from-vault-or-k8s": true,
}

type policyDecisionTestOutput struct {
	Decisions []struct {
		RuleID       string `json:"ruleId"`
		Effect       string `json:"effect"`
		Resource     string `json:"resource"`
		Message      string `json:"message"`
		Exempted     bool   `json:"exempted"`
		ExemptReason string `json:"exemptReason"`
	} `json:"decisions"`
}

// assertZeroTrustPackDecisions runs `policy test` (PolicyEngine gate
// enabled for this invocation only, per docs/planning/08 H4) against dir
// with the zero-trust pack (policiesDir) and asserts every unexempted
// deny is a known, recorded posture finding
// (knownNonExemptiblePostureFindings) — anything else is a regression.
// A nonzero exit from `policy test` itself is expected and not asserted
// on here (ADR 021 §3: it exits nonzero whenever any unexempted deny
// fires, which the two known findings guarantee) — only the decisions
// document's content is checked.
func assertZeroTrustPackDecisions(t *testing.T, label, dir, policiesDir string) {
	t.Helper()
	out, _, err := runSplit(t, "policy", "test", dir, "--policies", policiesDir, "--feature-gates", "PolicyEngine=true", "-o", "json")
	_ = err // see doc comment: an error here just means a deny fired, expected

	var parsed policyDecisionTestOutput
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("%s: parse policy test -o json output: %v\n%s", label, jsonErr, out)
	}
	for _, d := range parsed.Decisions {
		if d.Effect != "deny" {
			continue // warn-effect rules never block; nothing to assert here
		}
		if d.Exempted {
			if d.ExemptReason == "" {
				t.Errorf("%s: %s on %s is marked exempted but carries no reason", label, d.RuleID, d.Resource)
			}
			continue
		}
		if !knownNonExemptiblePostureFindings[d.RuleID] {
			t.Errorf("%s: posture regression — unexempted deny %s on %s: %s", label, d.RuleID, d.Resource, d.Message)
		}
	}
}

// TestZeroTrustPackAgainstExamplesAndBlueprints is docs/planning/08 H4's
// CI evaluation: the shipped zero-trust pack (`platformctl policy init
// zero-trust`) run via `platformctl policy test` against every shipped
// example (examples/*) and blueprint (internal/application/blueprint/
// templates/*), gate-enabled explicitly for this invocation only
// (PolicyEngine stays Alpha/disabled by default everywhere else). Every
// dev-flavored violation the pack denies (plaintext connections,
// env-backend secrets, unpinned/non-corp-registry images) carries a
// recorded `policy.datascape.io/exempt` waiver directly on the offending
// resource, with a reason naming why it's acceptable in a dev
// example/blueprint — grep the examples/ and templates/ trees for
// `policy.datascape.io/exempt` to review them. The two rules that cannot
// be waived (protect-data, secrets-from-vault-or-k8s) are recorded as an
// open posture finding in knownNonExemptiblePostureFindings above, not
// routed around. A *new* unexempted deny appearing anywhere — a
// regression, not this known baseline — fails this test and therefore
// the CI job that runs it (pure evaluation, no Docker: trivial runtime
// cost).
func TestZeroTrustPackAgainstExamplesAndBlueprints(t *testing.T) {
	policiesDir := t.TempDir()
	if _, err := apppolicy.WritePack("zero-trust", policiesDir, false); err != nil {
		t.Fatalf("write zero-trust pack: %v", err)
	}

	t.Run("example/cdc-attendance", func(t *testing.T) {
		assertZeroTrustPackDecisions(t, "example/cdc-attendance", "../../examples/cdc-attendance", policiesDir)
	})
	t.Run("example/lakehouse", func(t *testing.T) {
		assertZeroTrustPackDecisions(t, "example/lakehouse", "../../examples/lakehouse", policiesDir)
	})

	for _, name := range blueprint.Names() {
		name := name
		t.Run("blueprint/"+name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), name)
			if out, err, code := run(t, "init", name, "--dir", dir); err != nil || code != 0 {
				t.Fatalf("init %s failed (code %d): %v\n%s", name, code, err, out)
			}
			assertZeroTrustPackDecisions(t, "blueprint/"+name, dir, policiesDir)
		})
	}
}
