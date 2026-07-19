// Package archtest holds repo-wide architectural invariant tests that don't
// belong to any single package under test.
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

var loopbackLiteral = regexp.MustCompile(`"[^"]*(127\.0\.0\.1|localhost)[^"]*"`)

// scanDirs are swept for banned constructed-address literals. Runtime
// adapters (internal/adapters/runtime/...) are deliberately excluded: they
// *observe* the runtime's real bind address (ContainerState.Ports), they
// don't guess it.
var scanDirs = []string{
	"../domain",
	"../adapters/providers",
}

// TestNoConstructedLoopbackAddresses enforces docs/planning/08 F1
// (docs/planning/09 Class 1): a provider or domain-layer file may not
// hardcode a "127.0.0.1"/"localhost" network address literal — that guess
// is correct only on Docker, and every current instance of it was a real
// bug found by live Kubernetes testing (docs/planning/09 §1). The only
// legitimate way to learn a dialable address is runtime.EnsureReachable /
// runtime.WithReachable.
//
// Two exemptions, both scoped per-line so they can't accidentally cover a
// real violation next to them:
//   - a healthcheck command (the same line also contains "CMD-SHELL") — it
//     executes *inside* the container, where loopback is correct by
//     definition.
//   - a line explicitly marked "archtest:allow-loopback" with a reason —
//     e.g. redpanda's advertisedAddr(), a sentinel string that is never
//     dialed directly, only matched and redirected by a custom kgo.Dialer.
func TestNoConstructedLoopbackAddresses(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var violations []string
	for _, dir := range scanDirs {
		abs := filepath.Join(root, dir)
		walkErr := filepath.WalkDir(abs, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			return scanFile(path, &violations)
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("constructed loopback/localhost address literal(s) found — reachability must come from runtime.EnsureReachable/WithReachable, not a guess (docs/planning/09 Class 1, docs/planning/08 F1); if this is a genuine exception (e.g. a sentinel never dialed directly), mark the line \"archtest:allow-loopback: <reason>\":\n%s", strings.Join(violations, "\n"))
	}
}

func scanFile(path string, violations *[]string) error {
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
		if !loopbackLiteral.MatchString(line) {
			continue
		}
		if strings.Contains(line, "CMD-SHELL") || strings.Contains(line, "archtest:allow-loopback") {
			continue
		}
		*violations = append(*violations, path+":"+strconv.Itoa(lineNo)+": "+strings.TrimSpace(line))
	}
	return sc.Err()
}

// TestScanFileDetectsAndExemptsCorrectly proves the detector itself works —
// a rule with no positive-case coverage can silently rot into a no-op.
func TestScanFileDetectsAndExemptsCorrectly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	content := `package fixture

func violation() string { return "127.0.0.1:" + "9092" }

func healthcheckExempt() []string {
	return []string{"CMD-SHELL", "pg_isready -h 127.0.0.1 -U admin"}
}

func markedExempt() string {
	return "http://localhost:8080" // archtest:allow-loopback: test fixture
}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var violations []string
	if err := scanFile(path, &violations); err != nil {
		t.Fatal(err)
	}
	if len(violations) != 1 {
		t.Fatalf("got %d violations, want exactly 1 (the unexempted literal): %v", len(violations), violations)
	}
	if !strings.Contains(violations[0], `"127.0.0.1:"`) {
		t.Fatalf("violation %q does not name the offending literal", violations[0])
	}
}
