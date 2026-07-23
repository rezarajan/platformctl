package archtest

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRuntimeObjectNamesAreMintedNotConcatenated enforces docs/adr/030:
// derived runtime object names come from naming.Derived (which guarantees
// lowercase RFC 1123 and the 63-char bound), and name-embedded timestamps
// from naming.Timestamp — never per-site concatenation or a per-site
// format string, the exact pattern that produced an invalid Kubernetes
// object name live at the I15 merge gate. Same grep-scan shape as the
// loopback and charm-confinement guards: an import-line-level rule this
// simple needs no type checking to stay fast in every CI leg.
//
// Two patterns are forbidden outside internal/domain/naming:
//  1. concatenating onto RuntimeObjectName(...) on the same line
//  2. the timestamp layout string "20060102t150405z" (any case)
func TestRuntimeObjectNamesAreMintedNotConcatenated(t *testing.T) {
	t.Parallel()
	repoRoot, err := repoRootDir()
	if err != nil {
		t.Fatal(err)
	}
	scanRoots := []string{
		filepath.Join(repoRoot, "internal", "adapters"),
		filepath.Join(repoRoot, "internal", "application"),
		filepath.Join(repoRoot, "cmd"),
	}
	var violations []string
	for _, root := range scanRoots {
		walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if strings.HasPrefix(d.Name(), ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") {
				return nil
			}
			return scanForNamingViolations(path, &violations)
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("runtime object name built outside the naming authority (docs/adr/030: use naming.Derived / naming.Timestamp — per-site concatenation is how the I15 invalid-name failure shipped):\n%s", strings.Join(violations, "\n"))
	}
}

// TestScanForNamingViolationsDetectsCorrectly is the detector's own
// positive-case self-proof, mirroring the loopback and charm scanners' —
// a rule with no positive coverage can rot into a no-op.
func TestScanForNamingViolationsDetectsCorrectly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	content := `package fixture

func names() {
	jobName := naming.RuntimeObjectName(env) + "-backup-" + ts
	okName := naming.Derived(naming.RuntimeObjectName(env), "backup", ts)
	stamp := now.Format("20060102t150405z")
	_ = jobName
	_ = okName
	_ = stamp
}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var violations []string
	if err := scanForNamingViolations(path, &violations); err != nil {
		t.Fatal(err)
	}
	if len(violations) != 2 {
		t.Fatalf("expected 2 flagged lines (the concatenation + the format string), got %d: %v", len(violations), violations)
	}
}

func scanForNamingViolations(path string, violations *[]string) error {
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
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		bad := false
		if i := strings.Index(line, "RuntimeObjectName("); i >= 0 && strings.Contains(line[i:], ") +") {
			bad = true
		}
		if strings.Contains(strings.ToLower(line), "20060102t150405z") {
			bad = true
		}
		if bad {
			*violations = append(*violations, path+":"+strconv.Itoa(lineNo)+": "+trimmed)
		}
	}
	return sc.Err()
}
