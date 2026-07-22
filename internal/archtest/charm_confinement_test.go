package archtest

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// charmImportPrefixes are the charm.land/huh/bubbletea/bubbles/lipgloss
// TUI ecosystem's import path prefixes (docs/adr/024-interactive-
// composition.md "Interaction layer": Huh v2 for prompts, Bubble Tea v2
// underneath). Confined to cmd/platformctl and internal/cliutil so the
// composition *engine* (internal/application/compose: candidate
// computation, patch generation) stays headless and unit-testable without
// a TTY — the same engine/TUI seam a future visual composer (E10) would
// build on.
var charmImportPrefixes = []string{
	`"charm.land/`,
	`"github.com/charmbracelet/`,
}

// charmAllowedDirs (relative to the repo root) are the only packages
// permitted to import the charm ecosystem, plus this package's own
// directory — its detector and self-proof fixture below necessarily
// *mention* charm import strings as text, the same reason
// TestNoConstructedLoopbackAddresses's scanDirs never names
// internal/archtest itself.
var charmAllowedDirs = []string{
	filepath.FromSlash("cmd/platformctl"),
	filepath.FromSlash("internal/cliutil"),
	filepath.FromSlash("internal/archtest"),
}

// TestCharmImportsConfinedToCLIAndCliutil enforces ADR 024's confinement
// rule with a repo-wide sweep, the same shape as
// TestNoConstructedLoopbackAddresses (loopback_test.go) — an import-line
// regex scan rather than a full internal/archtest/... it is deliberately
// simple text matching, not go/importer type-checking: a charm import
// string is unambiguous and this test needs no build tags or module
// resolution to run fast in every CI leg.
func TestCharmImportsConfinedToCLIAndCliutil(t *testing.T) {
	repoRoot, err := repoRootDir()
	if err != nil {
		t.Fatal(err)
	}
	var violations []string
	walkErr := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "vendor" || name == "bin" {
				return filepath.SkipDir
			}
			rel, relErr := filepath.Rel(repoRoot, path)
			if relErr == nil && isAllowedCharmDir(rel) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		return scanForCharmImports(path, &violations)
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}
	if len(violations) > 0 {
		t.Fatalf("charm.land/github.com/charmbracelet import(s) found outside cmd/platformctl and internal/cliutil (docs/adr/024-interactive-composition.md: composition engine must stay headless/TUI-free):\n%s", strings.Join(violations, "\n"))
	}
}

// TestScanForCharmImportsDetectsAndExemptsCorrectly proves the detector
// itself works (the same self-proof loopback_test.go's
// TestScanFileDetectsAndExemptsCorrectly gives its own scanner) — a rule
// with no positive-case coverage can silently rot into a no-op. It scans a
// standalone fixture file directly, rather than adding a real violation to
// the tree, exactly as this task's report describes: the fixture below IS
// the "documented, not committed as a real violation" proof.
func TestScanForCharmImportsDetectsAndExemptsCorrectly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	content := `package fixture

import (
	huh "charm.land/huh/v2"
	"github.com/charmbracelet/bubbles/v2/textinput"
	"fmt"
)

func useForm() {
	_ = huh.NewForm()
	_ = fmt.Sprintf
	_ = textinput.New
}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var violations []string
	if err := scanForCharmImports(path, &violations); err != nil {
		t.Fatal(err)
	}
	if len(violations) != 2 {
		t.Fatalf("expected 2 flagged import lines (huh + bubbles), got %d: %v", len(violations), violations)
	}
}

func isAllowedCharmDir(rel string) bool {
	for _, allowed := range charmAllowedDirs {
		if rel == allowed || strings.HasPrefix(rel, allowed+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func scanForCharmImports(path string, violations *[]string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		for _, prefix := range charmImportPrefixes {
			if strings.Contains(line, prefix) {
				*violations = append(*violations, path+":"+strconv.Itoa(lineNo)+": "+strings.TrimSpace(line))
				break
			}
		}
	}
	return sc.Err()
}

// repoRootDir resolves the repository root from internal/archtest's own
// working directory (go test runs with cwd = the package directory).
func repoRootDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(wd, "..", ".."), nil
}
