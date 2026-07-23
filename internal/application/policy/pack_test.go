package policy

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func TestWritePackZeroTrust(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	written, err := WritePack("zero-trust", dir, false)
	if err != nil {
		t.Fatalf("WritePack: %v", err)
	}
	if len(written) != 1 {
		t.Fatalf("got %d file(s), want 1: %v", len(written), written)
	}

	// The written pack must itself load and validate cleanly through the
	// exact same channel a real --policies dir would (schema + domain
	// Validate) — proves the shipped pack isn't just well-formed YAML but
	// an actually-loadable policy set.
	policies, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir(written pack): %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("got %d policies, want 1", len(policies))
	}
	if got := len(policies[0].Rules()); got != 12 {
		t.Errorf("zero-trust pack has %d rules, want 12", got)
	}
}

func TestWritePackRefusesOverwriteWithoutForce(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err := WritePack("zero-trust", dir, false); err != nil {
		t.Fatalf("first WritePack: %v", err)
	}
	if _, err := WritePack("zero-trust", dir, false); err == nil {
		t.Fatal("expected a refusal on the second WritePack without --force")
	}
	if _, err := WritePack("zero-trust", dir, true); err != nil {
		t.Fatalf("WritePack with force: %v", err)
	}
}

func TestWritePackUnknownName(t *testing.T) {
	t.Parallel()
	if _, err := WritePack("no-such-pack", t.TempDir(), false); err == nil {
		t.Fatal("expected an unknown-pack error")
	}
}

// fixtureEnvelopes is the golden fixture (docs/planning/08 H3 accept:
// "golden fixture triggering every built-in rule") crafted so every
// field/finding-scoped built-in rule fires against it — see
// TestBuiltinPackDeniesEveryFieldOrFindingRule.
func fixtureEnvelopes() []resource.Envelope {
	gvk := resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1"}
	return []resource.Envelope{
		{
			// no-plaintext-connections (scheme tcp) + external-allowlist
			// (host not on the placeholder allowlist).
			GroupVersionKind: resource.GroupVersionKind{APIVersion: gvk.APIVersion, Kind: "Connection"},
			Metadata:         resource.Metadata{Name: "plain"},
			Spec:             map[string]any{"scheme": "tcp", "external": true, "host": "not-allowed.example.com"},
		},
		{
			// images-from-corp-registry + require-digest-pins (no digest,
			// wrong registry) + no-isolation-optout (networkPolicy: none).
			GroupVersionKind: resource.GroupVersionKind{APIVersion: gvk.APIVersion, Kind: "Provider"},
			Metadata:         resource.Metadata{Name: "img"},
			Spec: map[string]any{
				"configuration": map[string]any{"image": "docker.io/library/redis:latest"},
				"runtime":       map[string]any{"networkPolicy": "none"},
			},
		},
		{
			// protect-data (metadata.protect unset).
			GroupVersionKind: resource.GroupVersionKind{APIVersion: gvk.APIVersion, Kind: "Dataset"},
			Metadata:         resource.Metadata{Name: "data"},
			Spec:             map[string]any{},
		},
		{
			// forbid-env-secret-backend (backend == "env").
			GroupVersionKind: resource.GroupVersionKind{APIVersion: gvk.APIVersion, Kind: "SecretReference"},
			Metadata:         resource.Metadata{Name: "sec"},
			Spec:             map[string]any{"backend": "env"},
		},
	}
}

// fixtureFindings supplies the lint findings the escalate-duplicate-capture,
// ha-replication-floor, and insecure-endpoint rules promote to deny.
func fixtureFindings() []lint.Finding {
	return []lint.Finding{
		{Code: "DL001", Severity: lint.Warning, Resource: resource.Key{Namespace: "default", Kind: "Binding", Name: "capture"}, Message: "overlap"},
		{Code: "DL014", Severity: lint.Info, Resource: resource.Key{Namespace: "default", Kind: "Provider", Name: "img"}, Message: "single replica with HA"},
		{Code: "DL004", Severity: lint.Warning, Resource: resource.Key{Namespace: "default", Kind: "Connection", Name: "plain"}, Message: "plaintext boundary"},
	}
}

func TestBuiltinRuleIDs(t *testing.T) {
	t.Parallel()
	ids, err := BuiltinRuleIDs()
	if err != nil {
		t.Fatalf("BuiltinRuleIDs: %v", err)
	}
	if len(ids) != 12 {
		t.Fatalf("got %d rule ids, want 12: %v", len(ids), ids)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i-1] >= ids[i] {
			t.Fatalf("BuiltinRuleIDs not sorted: %v", ids)
		}
	}
}

// TestBuiltinPackDeniesEveryRuleOnATriggeringFixture is the golden fixture
// (docs/planning/08 H3 accept: "golden fixture triggering every built-in
// rule"): a small synthetic manifest set crafted to violate all nine of the
// pack's field/finding-scoped rules (the three matchPlan-scoped rules —
// no-dataset-deletes-in-ci, and none else — are plan-scoped and exercised
// separately in cmd/platformctl's policy command tests, since Run alone
// never sees a plan).
func TestBuiltinPackDeniesEveryFieldOrFindingRule(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, err := WritePack("zero-trust", dir, false); err != nil {
		t.Fatalf("WritePack: %v", err)
	}
	policies, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}

	fixture := fixtureEnvelopes()
	findings := fixtureFindings()

	decisions, err := Run(policies, fixture, nil, findings, false)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	fired := map[string]bool{}
	for _, d := range decisions {
		fired[d.RuleID] = true
	}
	want := []string{
		"no-plaintext-connections",
		"images-from-corp-registry",
		"protect-data",
		"no-isolation-optout",
		"escalate-duplicate-capture",
		"require-digest-pins",
		"ha-replication-floor",
		"insecure-endpoint",
		"external-allowlist",
		"forbid-env-secret-backend",
	}
	for _, id := range want {
		if !fired[id] {
			t.Errorf("rule %q did not fire against the triggering fixture", id)
		}
	}
}
