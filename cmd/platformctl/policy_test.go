package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/cliutil"
)

// alwaysDenyPolicyYAML is a Policy document guaranteed to fire against
// testdata/noop-scenario's Provider (spec.type is "noop", never "impossible").
const alwaysDenyPolicyYAML = `
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: always-deny
spec:
  rules:
    - id: always-deny-provider
      match: {kind: Provider}
      assert: {field: spec.type, equals: "impossible"}
      effect: deny
      message: "synthetic always-firing deny for tests"
`

func writePolicyDir(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "policy.yaml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestPolicyGateDisabledIsFullOffSwitch is docs/planning/08 H3's explicit
// test bullet: "gate-disabled = no evaluation with clear off-switch
// semantics". PolicyEngine defaults to disabled — a manifest set that would
// otherwise be denied by a --policies dir must validate/apply exactly as if
// no --policies flag were given at all, with zero evaluation.
func TestPolicyGateDisabledIsFullOffSwitch(t *testing.T) {
	t.Parallel()
	dir := writePolicyDir(t, alwaysDenyPolicyYAML)

	out, err, code := run(t, "validate", "testdata/noop-scenario", "--policies", dir)
	if err != nil || code != 0 {
		t.Fatalf("validate with the gate off should be unaffected by a denying policy dir (code %d): %v\n%s", code, err, out)
	}
}

// TestPolicyGateEnabledDeniesAtValidate proves the deny path actually
// blocks once PolicyEngine is enabled, via the standard validation-error
// exit path naming the rule id, message, and resource.
func TestPolicyGateEnabledDeniesAtValidate(t *testing.T) {
	t.Parallel()
	dir := writePolicyDir(t, alwaysDenyPolicyYAML)

	out, err, code := run(t, "validate", "testdata/noop-scenario", "--policies", dir, "--feature-gates", "PolicyEngine=true")
	if code != cliutil.ExitValidation {
		t.Fatalf("validate exit code = %d, want %d (ExitValidation); err=%v\n%s", code, cliutil.ExitValidation, err, out)
	}
	if err == nil || !strings.Contains(err.Error(), "always-deny-provider") {
		t.Fatalf("expected the error to name the denying rule id, got: %v", err)
	}
}

// TestPolicyExemptionUnblocksValidate proves ADR 021 §3's exemption
// mechanism end-to-end: an exemptible deny rule, once the target resource
// carries a matching exemption annotation, no longer blocks validate.
func TestPolicyExemptionUnblocksValidate(t *testing.T) {
	t.Parallel()
	dir := writePolicyDir(t, `
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: exemptible-deny
spec:
  rules:
    - id: exemptible-always-deny
      match: {kind: Provider}
      assert: {field: spec.type, equals: "impossible"}
      effect: deny
      exemptible: true
`)

	manifestDir := t.TempDir()
	manifest := `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: exempted-provider
  annotations:
    policy.datascape.io/exempt: "exemptible-always-deny: test exemption"
spec:
  type: noop
  runtime:
    type: fake
`
	if err := os.WriteFile(filepath.Join(manifestDir, "manifests.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err, code := run(t, "validate", manifestDir, "--policies", dir, "--feature-gates", "PolicyEngine=true")
	if err != nil || code != 0 {
		t.Fatalf("validate with an honored exemption should succeed (code %d): %v\n%s", code, err, out)
	}
}

// TestPolicyMatchPlanDeniesApply proves matchPlan rules are wired into
// apply on the computed diff (docs/planning/08 H3): a delete-scoped policy
// blocks a destroy plan that deletes the matched kind, even though the
// manifest set itself validates cleanly (no field/finding rule fires).
func TestPolicyMatchPlanDeniesApply(t *testing.T) {
	t.Parallel()
	dir := writePolicyDir(t, `
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: no-provider-deletes
spec:
  rules:
    - id: no-provider-deletes-in-ci
      matchPlan: {action: delete, kind: Provider}
      effect: deny
`)
	stateFile := filepath.Join(t.TempDir(), "state.json")
	gateArgs := []string{"--feature-gates", "PolicyEngine=true", "--policies", dir}

	if _, err, code := run(t, append([]string{"apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"}, gateArgs...)...); err != nil || code != 0 {
		t.Fatalf("initial apply (no policy denial expected — a create plan) failed (code %d): %v", code, err)
	}

	// destroy plans a Provider delete: matchPlan should deny it.
	out, err, code := run(t, append([]string{"destroy", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"}, gateArgs...)...)
	if code != cliutil.ExitValidation {
		t.Fatalf("destroy exit code = %d, want %d (ExitValidation); err=%v\n%s", code, cliutil.ExitValidation, err, out)
	}
	if err == nil || !strings.Contains(err.Error(), "no-provider-deletes-in-ci") {
		t.Fatalf("expected the error to name the denying matchPlan rule id, got: %v", err)
	}
}

// TestPolicyCrossDomainDeniesCDCBindingAtValidate is docs/planning/08 H5's
// accept criterion (a) end-to-end through the real CLI: the owner's exact
// scenario (docs/adr/022 Ring 0) — a cdc Binding whose sourceRef lives in
// domain "payments" and whose sink chain (its targetRef EventStream) lives
// in domain "analytics" — denied at `validate` by a
// deny{from:payments,to:analytics} matchEdge.crossDomain rule, before any
// infrastructure exists (runtime: docker, never touched by validate).
func TestPolicyCrossDomainDeniesCDCBindingAtValidate(t *testing.T) {
	t.Parallel()
	dir := writePolicyDir(t, `
apiVersion: policy.datascape.io/v1alpha1
kind: Policy
metadata:
  name: cross-domain-pack
spec:
  rules:
    - id: deny-payments-to-analytics
      matchEdge: {crossDomain: {from: payments, to: analytics}}
      effect: deny
      message: "payments may not feed analytics directly"
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
  name: cdc-rp
  domain: analytics
spec:
  type: redpanda
  runtime: {type: docker}
  configuration: {image: "docker.redpanda.com/redpandadata/redpanda:v24.2.1"}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: cdc-pg
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
  name: cdc-dbz
spec:
  type: debezium
  runtime: {type: docker}
  configuration:
    image: "quay.io/debezium/connect:2.7"
    bootstrapServers: "cdc-rp:29092"
    replicationSecretRef: cdc-pg-repl
  secretRefs: [cdc-pg-repl]
---
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: pg-src
  domain: payments
spec:
  engine: postgres
  providerRef: {name: cdc-pg}
  postgres: {database: attendance, schema: public}
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: events
  domain: analytics
spec:
  providerRef: {name: cdc-rp}
  partitions: 1
  retention: {duration: 1d}
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: cdc-binding
spec:
  mode: cdc
  sourceRef: {name: pg-src}
  targetRef: {name: events}
  providerRef: {name: cdc-dbz}
  options: {tables: [students], snapshotMode: initial}
`
	if err := os.WriteFile(filepath.Join(manifestDir, "manifests.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err, code := run(t, "validate", manifestDir, "--policies", dir, "--feature-gates", "PolicyEngine=true")
	if code != cliutil.ExitValidation {
		t.Fatalf("validate exit code = %d, want %d (ExitValidation); err=%v\n%s", code, cliutil.ExitValidation, err, out)
	}
	if err == nil {
		t.Fatal("expected a denial error")
	}
	for _, want := range []string{"deny-payments-to-analytics", "payments", "analytics", "cdc-binding"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("validate error %q missing %q (rule id, both domains, and the edge)", err.Error(), want)
		}
	}
}
