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

// reasonLiteral matches a `Reason:` struct-literal field followed directly
// by a quoted string — i.e. a hardcoded condition reason, as opposed to a
// named constant (`Reason: status.ReasonFoo`) or a variable/expression
// (`Reason: reason`, `Reason: status.ReasonConnectorState + state`).
var reasonLiteral = regexp.MustCompile(`Reason:\s*"[^"]*"`)

// reasonScanDirs are swept for banned `Reason:` string literals. Relative to
// this package (internal/archtest).
var reasonScanDirs = []string{
	"..",        // internal/...
	"../../cmd", // cmd/...
}

// TestNoConditionReasonStringLiterals enforces docs/planning/08 G4: every
// status.Condition{...} constructed outside internal/domain/status must set
// Reason to one of the named constants declared in
// internal/domain/status/reasons.go, never an inline string literal. Before
// this catalog existed, ~156 construction sites carried ~52 semantically
// distinct reasons with inconsistent spellings for the same concept
// (postgres WALNotLogical vs mysql BinlogNotRowFormat, etc.) — an
// unenumerable set that E4's `explain` catalog needs to walk exhaustively.
//
// Two exemptions:
//   - _test.go files (fake providers/fixtures legitimately invent reasons
//     like "FakeReconciled" that have no production meaning).
//   - internal/domain/status itself, where the constants are declared.
//   - a line explicitly marked "archtest:allow-reason-literal: <reason>",
//     scoped per-line like loopback's exemption, for a genuine future
//     exception.
func TestNoConditionReasonStringLiterals(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var violations []string
	for _, dir := range reasonScanDirs {
		abs := filepath.Join(root, dir)
		walkErr := filepath.WalkDir(abs, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			if isStatusPackageFile(path) {
				return nil
			}
			return scanReasonFile(path, &violations)
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("hardcoded condition Reason string literal(s) found — use a named constant from internal/domain/status/reasons.go instead (docs/planning/08 G4); if this is a genuine exception, mark the line \"archtest:allow-reason-literal: <reason>\":\n%s", strings.Join(violations, "\n"))
	}
}

// isStatusPackageFile reports whether path is inside internal/domain/status
// (using a path separator on both sides so a package like
// internal/domain/statusreport wouldn't false-positive).
func isStatusPackageFile(path string) bool {
	sep := string(filepath.Separator)
	return strings.Contains(path, sep+filepath.Join("domain", "status")+sep)
}

func scanReasonFile(path string, violations *[]string) error {
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
		if !reasonLiteral.MatchString(line) {
			continue
		}
		if strings.Contains(line, "archtest:allow-reason-literal") {
			continue
		}
		*violations = append(*violations, path+":"+strconv.Itoa(lineNo)+": "+strings.TrimSpace(line))
	}
	return sc.Err()
}

// TestScanReasonFileDetectsAndExemptsCorrectly proves the detector itself
// works, mirroring TestScanFileDetectsAndExemptsCorrectly in
// loopback_test.go.
func TestScanReasonFileDetectsAndExemptsCorrectly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	content := `package fixture

import "github.com/rezarajan/platformctl/internal/domain/status"

func violation() status.Condition {
	return status.Condition{Type: status.Ready, Status: status.True, Reason: "SomethingHardcoded"}
}

func constantExempt() status.Condition {
	return status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonNoDrift}
}

func markedExempt() status.Condition {
	return status.Condition{Type: status.Ready, Status: status.True, Reason: "Grandfathered"} // archtest:allow-reason-literal: test fixture
}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var violations []string
	if err := scanReasonFile(path, &violations); err != nil {
		t.Fatal(err)
	}
	if len(violations) != 1 {
		t.Fatalf("got %d violations, want exactly 1 (the unexempted literal): %v", len(violations), violations)
	}
	if !strings.Contains(violations[0], `"SomethingHardcoded"`) {
		t.Fatalf("violation %q does not name the offending literal", violations[0])
	}
}

// TestIsStatusPackageFile proves the status-package exemption is scoped to
// the actual package directory and doesn't over-match a similarly named one.
func TestIsStatusPackageFile(t *testing.T) {
	cases := map[string]bool{
		filepath.FromSlash("internal/domain/status/reasons.go"):    true,
		filepath.FromSlash("internal/domain/status/status.go"):     true,
		filepath.FromSlash("internal/domain/statusreport/foo.go"):  false,
		filepath.FromSlash("internal/adapters/providers/s3/s3.go"): false,
	}
	for path, want := range cases {
		if got := isStatusPackageFile(path); got != want {
			t.Errorf("isStatusPackageFile(%q) = %v, want %v", path, got, want)
		}
	}
}
