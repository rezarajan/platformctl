package s3

import (
	"testing"

	"github.com/minio/minio-go/v7"
	minlifecycle "github.com/minio/minio-go/v7/pkg/lifecycle"

	"github.com/rezarajan/platformctl/internal/domain/dataset"
)

// TestLifecycleRuleIDDeterministic covers docs/planning/08 D7's idempotency
// requirement: the same Dataset name must always produce the same rule ID
// so re-applying an unchanged spec issues zero redundant PUTs (via
// ruleMatches), and a shared bucket's other Dataset's rule (a different
// name, a different ID) is never touched by this Dataset's own reconcile.
func TestLifecycleRuleIDDeterministic(t *testing.T) {
	if lifecycleRuleID("attendance-raw") != lifecycleRuleID("attendance-raw") { //nolint:staticcheck // SA4000: deliberate same-input-twice determinism check, not a copy-paste bug
		t.Error("lifecycleRuleID is not deterministic for the same name")
	}
	if lifecycleRuleID("a") == lifecycleRuleID("b") {
		t.Error("lifecycleRuleID collided for two different Dataset names")
	}
}

func TestDesiredLifecycleRule(t *testing.T) {
	ds := dataset.Dataset{
		Prefix:    "attendance/",
		Lifecycle: dataset.Lifecycle{ExpireAfterDays: 30},
	}
	rule := desiredLifecycleRule("datascape-attendance-raw", ds)
	if rule.ID != "datascape-attendance-raw" {
		t.Errorf("rule.ID = %q, want %q", rule.ID, "datascape-attendance-raw")
	}
	if rule.Status != "Enabled" {
		t.Errorf("rule.Status = %q, want Enabled", rule.Status)
	}
	if rule.RuleFilter.Prefix != "attendance/" {
		t.Errorf("rule.RuleFilter.Prefix = %q, want %q", rule.RuleFilter.Prefix, "attendance/")
	}
	if int(rule.Expiration.Days) != 30 {
		t.Errorf("rule.Expiration.Days = %d, want 30", rule.Expiration.Days)
	}
}

func TestRuleMatches(t *testing.T) {
	ds := dataset.Dataset{Prefix: "p/", Lifecycle: dataset.Lifecycle{ExpireAfterDays: 7}}
	want := desiredLifecycleRule("id", ds)

	if ruleMatches(nil, want) {
		t.Error("ruleMatches(nil, want) = true, want false")
	}
	same := want
	if !ruleMatches(&same, want) {
		t.Error("ruleMatches with an identical rule = false, want true")
	}
	changed := want
	changed.Expiration.Days = minlifecycle.ExpirationDays(14)
	if ruleMatches(&changed, want) {
		t.Error("ruleMatches with a different expiration = true, want false")
	}
	changedPrefix := want
	changedPrefix.RuleFilter.Prefix = "other/"
	if ruleMatches(&changedPrefix, want) {
		t.Error("ruleMatches with a different prefix = true, want false")
	}
}

func TestVersioningStatus(t *testing.T) {
	if versioningStatus(dataset.VersioningEnabled) != minio.Enabled {
		t.Errorf("versioningStatus(enabled) = %q, want %q", versioningStatus(dataset.VersioningEnabled), minio.Enabled)
	}
	if versioningStatus(dataset.VersioningSuspended) != minio.Suspended {
		t.Errorf("versioningStatus(suspended) = %q, want %q", versioningStatus(dataset.VersioningSuspended), minio.Suspended)
	}
}

func TestFindRule(t *testing.T) {
	cfg := &minlifecycle.Configuration{Rules: []minlifecycle.Rule{
		{ID: "a"}, {ID: "b"},
	}}
	if r := findRule(cfg, "b"); r == nil || r.ID != "b" {
		t.Errorf("findRule(cfg, \"b\") = %v, want rule b", r)
	}
	if r := findRule(cfg, "missing"); r != nil {
		t.Errorf("findRule(cfg, \"missing\") = %v, want nil", r)
	}
}
