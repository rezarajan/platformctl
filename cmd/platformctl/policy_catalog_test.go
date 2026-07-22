package main

import (
	"sort"
	"strings"
	"testing"

	apppolicy "github.com/rezarajan/platformctl/internal/application/policy"
	"github.com/rezarajan/platformctl/internal/domain/status"
)

// TestExplainCatalogCoversEveryPolicyRule is docs/planning/08 H3's
// completeness guard, mirroring
// cmd/platformctl/lint_catalog_test.go's TestExplainCatalogCoversEveryLintCode
// for the "policyRule" vocabulary: every built-in zero-trust pack rule id
// (internal/application/policy.BuiltinRuleIDs, parsed from the embedded
// pack itself) must have exactly one status.Catalog entry, and
// status.Catalog must carry no orphan/duplicate policyRule entries.
func TestExplainCatalogCoversEveryPolicyRule(t *testing.T) {
	ids, err := apppolicy.BuiltinRuleIDs()
	if err != nil {
		t.Fatalf("BuiltinRuleIDs: %v", err)
	}
	if len(ids) == 0 {
		t.Fatal("BuiltinRuleIDs() returned zero ids — wiring is broken")
	}

	catalogTokens := map[string]int{}
	for _, e := range status.Catalog {
		if e.Kind == "policyRule" {
			catalogTokens[e.Token]++
		}
	}

	var missing []string
	known := map[string]bool{}
	for _, id := range ids {
		known[id] = true
		if catalogTokens[id] == 0 {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("policy rule id(s) with no status.Catalog entry — add one in internal/domain/status/catalog.go:\n  %s", strings.Join(missing, "\n  "))
	}

	var orphans, duplicates []string
	for token, count := range catalogTokens {
		if !known[token] {
			orphans = append(orphans, token)
		}
		if count > 1 {
			duplicates = append(duplicates, token)
		}
	}
	if len(orphans) > 0 {
		sort.Strings(orphans)
		t.Errorf("status.Catalog policyRule entr(y/ies) with no matching built-in rule id (stale or typo'd Token) — fix or remove:\n  %s", strings.Join(orphans, "\n  "))
	}
	if len(duplicates) > 0 {
		sort.Strings(duplicates)
		t.Errorf("status.Catalog has duplicate policyRule entries for the same Token — merge them:\n  %s", strings.Join(duplicates, "\n  "))
	}
}

// TestEveryPolicyRuleExplains proves `platformctl explain <rule-id>`
// actually resolves every built-in pack rule id end-to-end.
func TestEveryPolicyRuleExplains(t *testing.T) {
	ids, err := apppolicy.BuiltinRuleIDs()
	if err != nil {
		t.Fatalf("BuiltinRuleIDs: %v", err)
	}
	for _, id := range ids {
		id := id
		t.Run(id, func(t *testing.T) {
			out, err, exitCode := run(t, "explain", id)
			if err != nil || exitCode != 0 {
				t.Fatalf("explain %s failed (code %d): %v\n%s", id, exitCode, err, out)
			}
			if !strings.Contains(out, id) {
				t.Errorf("explain %s output missing the rule id itself:\n%s", id, out)
			}
		})
	}
}
