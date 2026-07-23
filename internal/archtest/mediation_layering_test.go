package archtest

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// zitiCouplingPatterns are the ways OpenZiti — the FIRST (and, at
// authorship time, only) adapter behind internal/ports/mediation
// (docs/adr/027: "MediationProvider is a PORT... OpenZiti is the FIRST
// adapter... nothing outside the adapter may import or name Ziti") — could
// leak into Go source: an import path naming the upstream project, or a
// package-qualified reference to this repo's own openziti package
// (`openziti.` followed by an exported identifier — the Go
// package-selector convention). The second pattern deliberately does NOT
// match a bare `"openziti"` string (a provider-type key in a map, a schema
// filename) — those name the provider by its chosen type string, not by
// importing/referencing the technology's package, and are legitimate in
// registry/schema wiring the same way "wireguard"/"postgres" string keys
// are. Mirrors TestCharmImportsConfinedToCLIAndCliutil's own "simple text
// matching, not go/importer type-checking" posture.
var zitiCouplingPatterns = []*regexp.Regexp{
	regexp.MustCompile(`"github\.com/openziti/`),
	regexp.MustCompile(`\bopenziti\.[A-Z]`),
}

// zitiAllowedDirs are the only packages permitted to name OpenZiti at all
// — the adapter itself, main.go's registration call site (the same
// exception every other adapter's own confinement test would need — see
// CLAUDE.md: "Only cmd/platformctl and internal/application/registry
// import concrete adapters"), and this package's own directory (its
// detector and self-proof fixture below necessarily mention the string,
// the same reason charm_confinement_test.go exempts internal/archtest).
var zitiAllowedDirs = []string{
	filepath.FromSlash("internal/adapters/providers/openziti"),
	filepath.FromSlash("cmd/platformctl"),
	filepath.FromSlash("internal/archtest"),
}

// TestZitiImportsConfinedToOpenzitiAdapter enforces docs/adr/027's layering
// doctrine mechanically: internal/ports/mediation and
// internal/application/graphaccess (the technology-silent port and
// graph-derivation packages docs/planning/08 H6 introduces) must never
// import or name Ziti — a future SPIRE/Consul-intentions adapter must be
// able to implement mediation.MediationProvider without touching either.
// Doc comments and this test's own identifier lists are exempt by
// construction (only .go files are scanned, and this file lives in an
// allowed dir) — the point is production code coupling, not prose.
func TestZitiImportsConfinedToOpenzitiAdapter(t *testing.T) {
	t.Parallel()
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
			if strings.HasPrefix(name, ".") && path != repoRoot {
				return filepath.SkipDir
			}
			if name == "vendor" || name == "bin" {
				return filepath.SkipDir
			}
			rel, relErr := filepath.Rel(repoRoot, path)
			if relErr == nil && isAllowedZitiDir(rel) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		return scanForZitiCoupling(path, &violations)
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}
	if len(violations) > 0 {
		t.Fatalf("Ziti coupling found outside internal/adapters/providers/openziti (docs/adr/027: \"nothing outside the adapter may import or name Ziti\"):\n%s", strings.Join(violations, "\n"))
	}
}

// TestScanForZitiCouplingDetectsAndExemptsCorrectly is the positive-case
// self-proof TestCharmImportsConfinedToCLIAndCliutil's own detector test
// gives — a fixture the scanner is run against directly, not a committed
// violation.
func TestScanForZitiCouplingDetectsAndExemptsCorrectly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	content := `package fixture

import (
	sdk "github.com/openziti/sdk-golang/ziti"
	"fmt"
)

func dial() {
	_ = sdk.NewContext
	_ = fmt.Sprintf
	openziti.New()
}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var violations []string
	if err := scanForZitiCoupling(path, &violations); err != nil {
		t.Fatal(err)
	}
	if len(violations) != 2 {
		t.Fatalf("expected 2 flagged lines (the sdk-golang import + the bare openziti. reference), got %d: %v", len(violations), violations)
	}
}

func isAllowedZitiDir(rel string) bool {
	for _, allowed := range zitiAllowedDirs {
		if rel == allowed || strings.HasPrefix(rel, allowed+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func scanForZitiCoupling(path string, violations *[]string) error {
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
		for _, pattern := range zitiCouplingPatterns {
			if pattern.MatchString(line) {
				*violations = append(*violations, path+":"+strconv.Itoa(lineNo)+": "+strings.TrimSpace(line))
				break
			}
		}
	}
	return sc.Err()
}
