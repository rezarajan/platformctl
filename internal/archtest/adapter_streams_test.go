package archtest

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestAdaptersDoNotWriteProcessGlobalStreams enforces docs/adr/031: an
// adapter or application-layer package may not write to os.Stderr /
// os.Stdout or use fmt.Print* — presentation belongs to the host (the CLI
// wires Engine.Warnings to stderr; stdout is the machine-parsed output
// contract, the H8 lesson). Providers report non-fatal outcomes through
// Request.Warnf; the engine logs through its Logger seam. Same grep-scan
// shape as the loopback/charm/naming guards.
//
// referencing os.Stdout/os.Stderr for anything (writes, Fprintf targets)
// counts: there is no legitimate read of either in these layers.
func TestAdaptersDoNotWriteProcessGlobalStreams(t *testing.T) {
	t.Parallel()
	repoRoot, err := repoRootDir()
	if err != nil {
		t.Fatal(err)
	}
	scanRoots := []string{
		filepath.Join(repoRoot, "internal", "adapters"),
		filepath.Join(repoRoot, "internal", "application"),
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
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			return scanForStreamViolations(path, &violations)
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("process-global stream use in adapter/application code (docs/adr/031: providers use Request.Warnf, the engine uses its Logger/Warnings seams — the host owns presentation):\n%s", strings.Join(violations, "\n"))
	}
}

// TestScanForStreamViolationsDetectsCorrectly is the detector's
// positive-case self-proof, same as every other archtest scanner's.
func TestScanForStreamViolationsDetectsCorrectly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	content := `package fixture

func emit() {
	fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	fmt.Println("direct")
	req.Warnf("fine: %v", err)
	fmt.Fprintf(buf, "fine too")
}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var violations []string
	if err := scanForStreamViolations(path, &violations); err != nil {
		t.Fatal(err)
	}
	if len(violations) != 2 {
		t.Fatalf("expected 2 flagged lines (os.Stderr target + fmt.Println), got %d: %v", len(violations), violations)
	}
}

func scanForStreamViolations(path string, violations *[]string) error {
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
		if strings.Contains(line, "os.Stderr") || strings.Contains(line, "os.Stdout") ||
			strings.Contains(line, "fmt.Print") {
			*violations = append(*violations, path+":"+strconv.Itoa(lineNo)+": "+trimmed)
		}
	}
	return sc.Err()
}
