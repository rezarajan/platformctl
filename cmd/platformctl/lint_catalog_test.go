package main

import (
	"sort"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/providers/debezium"
	"github.com/rezarajan/platformctl/internal/adapters/providers/redpanda"
	"github.com/rezarajan/platformctl/internal/adapters/providers/s3sink"
	applint "github.com/rezarajan/platformctl/internal/application/lint"
	"github.com/rezarajan/platformctl/internal/domain/status"
)

// allLintCodes is every design-lint code this binary can produce: the
// built-in set (internal/application/lint.BuiltinCodes) plus every
// provider-contributed code (docs/adr/020-design-lints.md §5). Registered
// here — not derived by AST scanning like reasons.go's completeness test —
// because lint codes are string constants scattered across the
// internal/adapters/providers/* packages that implement
// reconciler.DesignLinter; this file is the one place, alongside
// cmd/platformctl's own registry wiring, that already imports every
// adapter, so it is the natural home for the completeness enumeration
// (CLAUDE.md: only cmd/platformctl and internal/application/registry import
// concrete adapters).
func allLintCodes() []string {
	var codes []string
	codes = append(codes, applint.BuiltinCodes...)
	codes = append(codes, debezium.LintCodes...)
	codes = append(codes, redpanda.LintCodes...)
	codes = append(codes, s3sink.LintCodes...)
	return codes
}

// TestExplainCatalogCoversEveryLintCode is docs/planning/08 H1/H2's
// completeness guard, mirroring
// internal/archtest.TestExplainCatalogCoversEveryReason for the "lintCode"
// vocabulary: every code allLintCodes lists must have exactly one
// status.Catalog entry, and status.Catalog must carry no orphan/duplicate
// lintCode entries.
func TestExplainCatalogCoversEveryLintCode(t *testing.T) {
	codes := allLintCodes()
	if len(codes) == 0 {
		t.Fatal("allLintCodes() returned zero codes — wiring is broken")
	}

	catalogTokens := map[string]int{}
	for _, e := range status.Catalog {
		if e.Kind == "lintCode" {
			catalogTokens[e.Token]++
		}
	}

	var missing []string
	known := map[string]bool{}
	for _, code := range codes {
		known[code] = true
		if catalogTokens[code] == 0 {
			missing = append(missing, code)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("lint code(s) with no status.Catalog entry — add one in internal/domain/status/catalog.go:\n  %s", strings.Join(missing, "\n  "))
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
		t.Errorf("status.Catalog lintCode entr(y/ies) with no matching registered code (stale or typo'd Token) — fix or remove:\n  %s", strings.Join(orphans, "\n  "))
	}
	if len(duplicates) > 0 {
		sort.Strings(duplicates)
		t.Errorf("status.Catalog has duplicate lintCode entries for the same Token — merge them:\n  %s", strings.Join(duplicates, "\n  "))
	}
}

// TestEveryLintCodeExplains proves `platformctl explain <code>` actually
// resolves every registered code end-to-end (schema/CLI path, not just the
// catalog data structure the test above checks).
func TestEveryLintCodeExplains(t *testing.T) {
	for _, code := range allLintCodes() {
		code := code
		t.Run(code, func(t *testing.T) {
			out, err, exitCode := run(t, "explain", code)
			if err != nil || exitCode != 0 {
				t.Fatalf("explain %s failed (code %d): %v\n%s", code, exitCode, err, out)
			}
			if !strings.Contains(out, code) {
				t.Errorf("explain %s output missing the code itself:\n%s", code, out)
			}
		})
	}
}
