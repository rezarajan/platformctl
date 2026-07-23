package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type policyAuditTestOutput struct {
	Edges []struct {
		Owner         string `json:"owner"`
		From          string `json:"from"`
		To            string `json:"to"`
		Kind          string `json:"kind"`
		Verdict       string `json:"verdict"`
		Justification string `json:"justification"`
		RuleID        string `json:"ruleId"`
		Detail        string `json:"detail"`
		ExemptReason  string `json:"exemptReason"`
	} `json:"edges"`
}

func runPolicyAuditJSON(t *testing.T, manifestDir, policiesDir string) (policyAuditTestOutput, string, error) {
	t.Helper()
	args := []string{"policy", "audit", manifestDir, "--feature-gates", "PolicyEngine=true,LabelScopedAccess=true", "-o", "json"}
	if policiesDir != "" {
		args = append(args, "--policies", policiesDir)
	}
	out, _, err := runSplit(t, args...)
	var parsed policyAuditTestOutput
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("parse policy audit -o json output: %v\n%s", jsonErr, out)
	}
	return parsed, out, err
}

// TestPolicyAuditRequiresPolicyEngineGate proves audit shares the same
// Alpha off-switch every other policy command requires.
func TestPolicyAuditRequiresPolicyEngineGate(t *testing.T) {
	t.Parallel()
	out, err, code := run(t, "policy", "audit", "testdata/noop-scenario")
	if err == nil || code == 0 {
		t.Fatalf("expected policy audit to require the PolicyEngine gate (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(err.Error(), "PolicyEngine") {
		t.Errorf("expected the error to name the PolicyEngine gate, got: %v", err)
	}
}

// TestPolicyAuditNamesDenyRuleJustification is docs/planning/08 K5's core
// accept bar through the real CLI: a denied edge names the specific rule
// that denied it, and the command itself does not fail (audit reports,
// never blocks — validate/plan/apply own the blocking behavior).
func TestPolicyAuditNamesDenyRuleJustification(t *testing.T) {
	t.Parallel()
	policiesDir := writePolicyDir(t, `
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: audit-deny-pack
spec:
  rules:
    - id: deny-payments-to-analytics
      matchEdge: {crossDomain: {from: payments, to: analytics}}
      effect: deny
`)
	manifestDir := t.TempDir()
	manifest := `
apiVersion: datascape.io/v1alpha1
kind: SecretReference
metadata:
  name: cdc-pg-admin
spec:
  backend: env
  keys: [username, password]
---
apiVersion: datascape.io/v1alpha1
kind: SecretReference
metadata:
  name: cdc-pg-repl
spec:
  backend: env
  keys: [username, password]
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: audit-rp
  domain: analytics
spec:
  type: redpanda
  runtime: {type: docker}
  configuration: {image: "docker.redpanda.com/redpandadata/redpanda:v24.2.1"}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: audit-pg
  domain: payments
spec:
  type: postgres
  runtime: {type: docker}
  configuration:
    version: "16"
    superuserSecretRef: cdc-pg-admin
    replicationSecretRef: cdc-pg-repl
  secretRefs: [cdc-pg-admin, cdc-pg-repl]
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: audit-dbz
spec:
  type: debezium
  runtime: {type: docker}
  configuration:
    image: "quay.io/debezium/connect:2.7"
    bootstrapServers: "audit-rp:29092"
    replicationSecretRef: cdc-pg-repl
  secretRefs: [cdc-pg-repl]
---
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: audit-src
  domain: payments
spec:
  engine: postgres
  providerRef: {name: audit-pg}
  postgres: {database: attendance, schema: public}
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: audit-events
  domain: analytics
spec:
  providerRef: {name: audit-rp}
  partitions: 1
  retention: {duration: 1d}
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: audit-binding
spec:
  mode: cdc
  sourceRef: {name: audit-src}
  targetRef: {name: audit-events}
  providerRef: {name: audit-dbz}
  options: {tables: [students], snapshotMode: initial}
`
	if err := os.WriteFile(filepath.Join(manifestDir, "manifests.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}

	out, raw, err := runPolicyAuditJSON(t, manifestDir, policiesDir)
	if err != nil {
		t.Fatalf("policy audit itself must not fail on a denied edge (report-only): %v\n%s", raw, raw)
	}
	var found bool
	for _, e := range out.Edges {
		if !strings.Contains(e.Owner, "audit-binding") {
			continue
		}
		found = true
		if e.Verdict != "denied" {
			t.Errorf("verdict = %q, want denied", e.Verdict)
		}
		if e.Justification != "deny-rule" {
			t.Errorf("justification = %q, want deny-rule", e.Justification)
		}
		if e.RuleID != "deny-payments-to-analytics" {
			t.Errorf("ruleId = %q, want the firing rule", e.RuleID)
		}
	}
	if !found {
		t.Fatalf("expected a row for the audit-binding edge, got:\n%s", raw)
	}
}

// TestPolicyAuditNamesNoMatchingDenyJustification proves the default-
// permit case (no policy denies the edge) names itself as such rather
// than leaving the row unexplained.
func TestPolicyAuditNamesNoMatchingDenyJustification(t *testing.T) {
	t.Parallel()
	manifestDir := writeManifestDocs(t, goldConnectionManifest, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: plain-consumer
spec:
  type: noop
  external: true
  runtime: {type: fake}
  connectionRef: {name: gold-svc}
`)

	out, raw, err := runPolicyAuditJSON(t, manifestDir, "")
	if err != nil {
		t.Fatalf("policy audit with no policies loaded must not fail: %v\n%s", err, raw)
	}
	var found bool
	for _, e := range out.Edges {
		if !strings.Contains(e.Owner, "plain-consumer") {
			continue
		}
		found = true
		if e.Verdict != "permitted" || e.Justification != "no-matching-deny" {
			t.Errorf("got verdict=%q justification=%q, want permitted/no-matching-deny", e.Verdict, e.Justification)
		}
		if e.Detail == "" {
			t.Error("detail is empty — no nameable justification")
		}
	}
	if !found {
		t.Fatalf("expected a row for plain-consumer, got:\n%s", raw)
	}
}

// TestPolicyAuditNamesGrantJustification proves a spec.access grant with
// no denying matchGrant rule is named Permitted/grant — ADR 033's "a
// permitted edge's justification may be a grant, not only a policy rule."
func TestPolicyAuditNamesGrantJustification(t *testing.T) {
	t.Parallel()
	manifestDir := writeManifestDocs(t, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: grant-owner
  namespace: shared
spec:
  type: noop
  runtime: {type: fake}
  access: [{namespace: default}]
`)

	out, raw, err := runPolicyAuditJSON(t, manifestDir, "")
	if err != nil {
		t.Fatalf("policy audit must not fail: %v\n%s", err, raw)
	}
	var found bool
	for _, e := range out.Edges {
		if e.Kind != "grant" {
			continue
		}
		found = true
		if e.Verdict != "permitted" || e.Justification != "grant" {
			t.Errorf("got verdict=%q justification=%q, want permitted/grant", e.Verdict, e.Justification)
		}
		if !strings.Contains(e.To, "namespace/default") {
			t.Errorf("to = %q, want it to name the granted namespace", e.To)
		}
	}
	if !found {
		t.Fatalf("expected a grant row, got:\n%s", raw)
	}
}
