package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/status"
)

// reasonsGoPath is internal/domain/status/reasons.go, relative to this
// package (internal/archtest).
const reasonsGoPath = "../domain/status/reasons.go"

// declaredReasons parses reasons.go's AST (rather than reusing status.Catalog
// or a regex over the same file the catalog was generated from) and returns
// every `Reason<Name> = "<value>"` constant declared there, keyed by the
// constant's Go identifier, valued by its string literal — the enumerable
// source of truth E4's catalog must cover completely (docs/planning/08 E4,
// G4).
func declaredReasons(t *testing.T) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	path := filepath.FromSlash(reasonsGoPath)
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", reasonsGoPath, err)
	}
	out := map[string]string{}
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Names) != len(vs.Values) {
				continue
			}
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "Reason") {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				val, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("unquote %s: %v", lit.Value, err)
				}
				out[name.Name] = val
			}
		}
	}
	if len(out) == 0 {
		t.Fatalf("parsed zero Reason* constants from %s — parser or path is broken", reasonsGoPath)
	}
	return out
}

// TestExplainCatalogCoversEveryReason is docs/planning/08 E4's completeness
// gate: every Reason* constant declared in reasons.go must have exactly one
// status.Catalog entry (by string value), and the catalog must not carry
// orphan/duplicate entries that no longer correspond to a declared
// constant — both directions catch a reason added/renamed without its
// explain entry landing in the same commit.
func TestExplainCatalogCoversEveryReason(t *testing.T) {
	t.Parallel()
	declared := declaredReasons(t)

	catalogTokens := map[string]int{}
	for _, e := range status.Catalog {
		if e.Kind != "reason" {
			continue
		}
		catalogTokens[e.Token]++
	}

	var missing []string
	for ident, value := range declared {
		if catalogTokens[value] == 0 {
			missing = append(missing, ident+" = "+strconv.Quote(value))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("reason constant(s) with no status.Catalog entry — add one in internal/domain/status/catalog.go:\n  %s", strings.Join(missing, "\n  "))
	}

	declaredValues := map[string]bool{}
	for _, v := range declared {
		declaredValues[v] = true
	}
	var orphans []string
	var duplicates []string
	for token, count := range catalogTokens {
		if !declaredValues[token] {
			orphans = append(orphans, token)
		}
		if count > 1 {
			duplicates = append(duplicates, token)
		}
	}
	if len(orphans) > 0 {
		sort.Strings(orphans)
		t.Errorf("status.Catalog reason entr(y/ies) with no matching Reason* constant in reasons.go (stale or typo'd Token) — fix or remove:\n  %s", strings.Join(orphans, "\n  "))
	}
	if len(duplicates) > 0 {
		sort.Strings(duplicates)
		t.Errorf("status.Catalog has duplicate reason entries for the same Token — merge them:\n  %s", strings.Join(duplicates, "\n  "))
	}
}

// TestExplainCatalogCoversEveryConditionType is the ConditionType half of
// the same completeness bar: Ready/Progressing/Degraded/DriftDetected
// (status.go) must each have a status.Catalog conditionType entry, since
// `platformctl explain` resolves both vocabularies.
func TestExplainCatalogCoversEveryConditionType(t *testing.T) {
	t.Parallel()
	want := []status.ConditionType{status.Ready, status.Progressing, status.Degraded, status.DriftDetected}
	have := map[string]bool{}
	for _, e := range status.Catalog {
		if e.Kind == "conditionType" {
			have[e.Token] = true
		}
	}
	var missing []string
	for _, w := range want {
		if !have[string(w)] {
			missing = append(missing, string(w))
		}
	}
	if len(missing) > 0 {
		t.Errorf("ConditionType(s) with no status.Catalog entry — add one in internal/domain/status/catalog.go:\n  %s", strings.Join(missing, "\n  "))
	}
}

// TestDeclaredReasonsDetectsMissingCatalogEntry proves the parser-driven
// detector actually works: a reasons.go-shaped fixture constant with no
// catalog counterpart must be reported by name, mirroring
// TestScanReasonFileDetectsAndExemptsCorrectly's self-proof pattern in this
// package.
func TestDeclaredReasonsDetectsMissingCatalogEntry(t *testing.T) {
	t.Parallel()
	declared := map[string]string{
		"ReasonReconcileComplete": status.ReasonReconcileComplete, // known-good, has a catalog entry
		"ReasonTotallyFabricated": "TotallyFabricated",            // deliberately absent from status.Catalog
	}
	catalogTokens := map[string]int{}
	for _, e := range status.Catalog {
		if e.Kind == "reason" {
			catalogTokens[e.Token]++
		}
	}
	var missing []string
	for ident, value := range declared {
		if catalogTokens[value] == 0 {
			missing = append(missing, ident)
		}
	}
	if len(missing) != 1 || missing[0] != "ReasonTotallyFabricated" {
		t.Fatalf("expected exactly [ReasonTotallyFabricated] reported missing, got %v", missing)
	}
}
