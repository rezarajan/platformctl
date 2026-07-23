package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// TestValidateLogFormatJSONEmitsPolicyDecisionEvent is docs/planning/08 K5's
// core structured-decision-event bar: a policy decision fired during
// validate (Run's own evaluation point) is logged as one parseable JSON
// event on the SAME I11 slog seam reconciliation actions use, carrying the
// resource/rule/effect/exempted facts (ADR 033 decision 5).
func TestValidateLogFormatJSONEmitsPolicyDecisionEvent(t *testing.T) {
	t.Parallel()
	dir := writePolicyDir(t, alwaysDenyPolicyYAML)

	root := newRootCmd(defaultWiring)
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"validate", "testdata/noop-scenario", "--policies", dir,
		"--feature-gates", "PolicyEngine=true", "--log-format", "json"})
	// The deny is expected to fail validate (ADR 021 §3) — the point of
	// this test is that the decision was LOGGED before that happened, not
	// that validate succeeds.
	_ = root.Execute()

	trimmed := strings.TrimRight(errBuf.String(), "\n")
	if trimmed == "" {
		t.Fatalf("expected at least one policy decision event on stderr, got none")
	}
	var found bool
	for _, line := range strings.Split(trimmed, "\n") {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("stderr line is not parseable JSON: %v\nline: %s", err, line)
		}
		rule, _ := event["rule"].(string)
		if rule != "always-deny-provider" {
			continue
		}
		found = true
		for _, key := range []string{"resource", "rule", "effect", "outcome", "exempted", "msg"} {
			if _, ok := event[key]; !ok {
				t.Errorf("event missing %q key: %v", key, event)
			}
		}
		if event["effect"] != "deny" {
			t.Errorf("effect = %v, want deny", event["effect"])
		}
		if event["outcome"] != "deny" {
			t.Errorf("outcome = %v, want deny", event["outcome"])
		}
		if event["exempted"] != false {
			t.Errorf("exempted = %v, want false", event["exempted"])
		}
	}
	if !found {
		t.Fatalf("no event named rule always-deny-provider on stderr:\n%s", trimmed)
	}
}

// TestValidateLogFormatTextIncludesRuleAndResource proves the text-format
// (default) rendering carries the same facts in prose, since text mode
// drops slog attrs entirely (I11's byte-compatible textLineHandler).
func TestValidateLogFormatTextIncludesRuleAndResource(t *testing.T) {
	t.Parallel()
	dir := writePolicyDir(t, alwaysDenyPolicyYAML)

	root := newRootCmd(defaultWiring)
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"validate", "testdata/noop-scenario", "--policies", dir,
		"--feature-gates", "PolicyEngine=true"})
	_ = root.Execute()

	stderr := errBuf.String()
	for _, want := range []string{"always-deny-provider", "test-noop-provider", "deny"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("text-format stderr %q does not contain %q", stderr, want)
		}
	}
}

// TestPolicyDecisionEventsGateOffEmitsNoEvents proves the K5 seam inherits
// PolicyEngine's own off-switch semantics (docs/planning/08 H3): with the
// gate disabled (the default), evaluation never runs at all, so zero
// decision events are logged even though the same denying policy dir is
// passed.
func TestPolicyDecisionEventsGateOffEmitsNoEvents(t *testing.T) {
	t.Parallel()
	dir := writePolicyDir(t, alwaysDenyPolicyYAML)

	root := newRootCmd(defaultWiring)
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"validate", "testdata/noop-scenario", "--policies", dir, "--log-format", "json"})
	if err := root.Execute(); err != nil {
		t.Fatalf("gate off: validate should succeed unaffected by the denying policy dir: %v\n%s", err, errBuf.String())
	}
	if errBuf.Len() != 0 {
		t.Fatalf("gate off: expected zero stderr output, got:\n%s", errBuf.String())
	}
}

// TestPlanLogFormatJSONEmitsMatchPlanDecisionEvent proves the RunPlan half
// (matchPlan rules, evaluated at plan/apply/destroy) is logged too — a
// warn-effect rule so the plan command itself still succeeds, letting this
// test observe a non-blocking decision event end to end.
func TestPlanLogFormatJSONEmitsMatchPlanDecisionEvent(t *testing.T) {
	t.Parallel()
	dir := writePolicyDir(t, `
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: warn-on-provider-create
spec:
  rules:
    - id: warn-provider-create
      matchPlan: {action: create, kind: Provider}
      effect: warn
`)
	stateFile := filepath.Join(t.TempDir(), "state.json")

	root := newRootCmd(defaultWiring)
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"plan", "testdata/noop-scenario", "--policies", dir,
		"--feature-gates", "PolicyEngine=true", "--state-file", stateFile,
		"--detect-drift-only", "--log-format", "json"})
	if err := root.Execute(); err != nil {
		t.Fatalf("plan with a warn-effect matchPlan rule should still succeed: %v\n%s", err, errBuf.String())
	}

	trimmed := strings.TrimRight(errBuf.String(), "\n")
	if trimmed == "" {
		t.Fatalf("expected a policy decision event on stderr, got none")
	}
	var found bool
	for _, line := range strings.Split(trimmed, "\n") {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("stderr line is not parseable JSON: %v\nline: %s", err, line)
		}
		if event["rule"] == "warn-provider-create" {
			found = true
			if event["effect"] != "warn" || event["outcome"] != "warn" {
				t.Errorf("effect/outcome = %v/%v, want warn/warn", event["effect"], event["outcome"])
			}
		}
	}
	if !found {
		t.Fatalf("no event named rule warn-provider-create on stderr:\n%s", trimmed)
	}
}
