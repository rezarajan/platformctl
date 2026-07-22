package archtest

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// testImpactScriptPath is scripts/test-impact.sh, relative to this package
// (internal/archtest).
const testImpactScriptPath = "../../scripts/test-impact.sh"

// suiteMapping is one suite row of scripts/test-impact.sh's map, parsed
// from its `id|scope|cmd` heredoc line: which package directories its `go
// test` invocation targets, and the `-run` regex (if any) that filters
// which tests within those directories actually execute. The scope column
// (which files trigger the suite's SELECTION) is deliberately not modeled
// here — completeness cares about whether a test would ever RUN under this
// suite's own `cmd`, not whether some diff would select it.
type suiteMapping struct {
	id    string
	dirs  []dirSpec
	runRE *regexp.Regexp // nil means the suite's cmd has no -run filter (every test in scope dirs runs)
}

type dirSpec struct {
	path      string // slash-separated, relative to repo root, no trailing "/..." or "/"
	recursive bool   // true when the cmd targeted "<path>/..." (covers subdirectories too)
}

var suiteRunFlag = regexp.MustCompile(`-run\s+(?:'([^']*)'|(\S+))`)
var suiteDirToken = regexp.MustCompile(`\./\S+`)

// parseSuiteMap extracts the suites() heredoc body from
// scripts/test-impact.sh's own source text and parses each `id|scope|cmd`
// line — the script file is the single source of truth (doc 08 G7); this
// function must never duplicate the map's data, only read it.
func parseSuiteMap(t *testing.T, script string) []suiteMapping {
	t.Helper()
	start := strings.Index(script, "cat <<'EOF'\n")
	if start == -1 {
		t.Fatalf("could not find the suites() heredoc start (\"cat <<'EOF'\") in %s — did its shape change?", testImpactScriptPath)
	}
	body := script[start+len("cat <<'EOF'\n"):]
	end := strings.Index(body, "\nEOF")
	if end == -1 {
		t.Fatalf("could not find the suites() heredoc end (\"EOF\") in %s — did its shape change?", testImpactScriptPath)
	}
	body = body[:end]

	var suites []suiteMapping
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 3)
		if len(parts) != 3 {
			t.Fatalf("suite line does not have 3 |-delimited fields (id|scope|cmd): %q", line)
		}
		id, cmd := parts[0], parts[2]

		var runRE *regexp.Regexp
		if m := suiteRunFlag.FindStringSubmatch(cmd); m != nil {
			pattern := m[1]
			if pattern == "" {
				pattern = m[2]
			}
			re, err := regexp.Compile(pattern)
			if err != nil {
				t.Fatalf("suite %s: -run pattern %q does not compile: %v", id, pattern, err)
			}
			runRE = re
		}

		var dirs []dirSpec
		for _, tok := range suiteDirToken.FindAllString(cmd, -1) {
			d := strings.TrimSuffix(tok, "/")
			recursive := strings.HasSuffix(d, "/...")
			d = strings.TrimSuffix(d, "/...")
			d = strings.TrimPrefix(d, "./")
			dirs = append(dirs, dirSpec{path: d, recursive: recursive})
		}
		suites = append(suites, suiteMapping{id: id, dirs: dirs, runRE: runRE})
	}
	return suites
}

// coveringSuites returns the ids of every suite whose `go test` invocation
// would actually execute a test named name living in package directory
// dir — mirroring real `go test -run` semantics (an unanchored regex
// search over the test name, not a full-string match).
func coveringSuites(name, dir string, suites []suiteMapping) []string {
	var hits []string
	for _, s := range suites {
		for _, d := range s.dirs {
			inScope := (d.recursive && (dir == d.path || strings.HasPrefix(dir, d.path+"/"))) || (!d.recursive && dir == d.path)
			if !inScope {
				continue
			}
			if s.runRE == nil || s.runRE.MatchString(name) {
				hits = append(hits, s.id)
			}
			break
		}
	}
	return hits
}

// integrationTestExemptions lists integration Test* functions that the
// current scripts/test-impact.sh map does not run under any suite's `go
// test` command, with the reason each is exempted rather than fixed here.
// Every entry was verified against the REAL map (not a synthetic one) when
// this guard was added (docs/planning/08 G7) — see TASK_PROGRESS.md's
// Finding for the full accounting. This task's file-ownership boundary
// forbids editing scripts/test-impact.sh's suites() heredoc (concurrently
// active provider agents append rows there), so pre-existing map gaps are
// recorded here for a maintainer to fix in the map itself, rather than
// worked around silently.
//
// Key is "TestName@package/dir/path" (disambiguates same-named tests in
// different directories, e.g. the three TestConformance).
var integrationTestExemptions = map[string]string{
	// state-s3 suite's `-run 'TestSharedState'` filter applies uniformly
	// to BOTH its target dirs (./internal/adapters/state/... and
	// ./cmd/platformctl/); it never matches this package's own
	// conformance/lock tests even though the directory is in scope.
	"TestConformance@internal/adapters/state/s3":             "state-s3 suite's -run 'TestSharedState' filter doesn't match this package's own tests despite the dir being in scope (docs/planning/08 G7 finding)",
	"TestForceUnlock@internal/adapters/state/s3":             "state-s3 suite's -run 'TestSharedState' filter doesn't match this package's own tests despite the dir being in scope (docs/planning/08 G7 finding)",
	"TestLockReclaimsAfterExpiry@internal/adapters/state/s3": "state-s3 suite's -run 'TestSharedState' filter doesn't match this package's own tests despite the dir being in scope (docs/planning/08 G7 finding)",

	// docker-conformance suite's `-run Conformance` filter only runs
	// Conformance-named tests in internal/adapters/runtime/docker/; these
	// real tests in the same directory/package the suite's scope already
	// claims aren't matched by that filter.
	"TestEnsureNetworkRefusesUnmanagedExisting@internal/adapters/runtime/docker": "docker-conformance suite's -run Conformance filter doesn't match this test despite the dir being in scope (docs/planning/08 G7 finding)",
	"TestEnsureVolumeRefusesUnmanagedExisting@internal/adapters/runtime/docker":  "docker-conformance suite's -run Conformance filter doesn't match this test despite the dir being in scope (docs/planning/08 G7 finding)",
	"TestImagePullAuthPullsFromPrivateRegistry@internal/adapters/runtime/docker": "docker-conformance suite's -run Conformance filter doesn't match this test despite the dir being in scope (docs/planning/08 G7 finding)",
	"TestNetworkAliasResolvesInNetwork@internal/adapters/runtime/docker":         "docker-conformance suite's -run Conformance filter doesn't match this test despite the dir being in scope (docs/planning/08 G7 finding)",
	"TestOutOfBandKillSurfacesUnhealthy@internal/adapters/runtime/docker":        "docker-conformance suite's -run Conformance filter doesn't match this test despite the dir being in scope (docs/planning/08 G7 finding)",
	"TestPublishedPortBindsToLoopbackByDefault@internal/adapters/runtime/docker": "docker-conformance suite's -run Conformance filter doesn't match this test despite the dir being in scope (docs/planning/08 G7 finding)",
	"TestPullPolicyNeverFailsFastOnAbsentImage@internal/adapters/runtime/docker": "docker-conformance suite's -run Conformance filter doesn't match this test despite the dir being in scope (docs/planning/08 G7 finding)",

	// cmd/platformctl tests with no matching suite -run pattern at all.
	"TestDockerProviderEndToEnd@cmd/platformctl":                                     "no suite row's -run pattern matches this test name (docs/planning/08 G7 finding)",
	"TestDriftDetectsDebeziumConnectorConfigMismatch@cmd/platformctl":                "no suite row's -run pattern matches this test name (docs/planning/08 G7 finding)",
	"TestDriftDetectsMariaDBReplicationCredentialMismatch@cmd/platformctl":           "no suite row's -run pattern matches this test name (docs/planning/08 G7 finding)",
	"TestDriftDetectsRedpandaRetentionMismatch@cmd/platformctl":                      "no suite row's -run pattern matches this test name (docs/planning/08 G7 finding)",
	"TestRedpandaKubernetesEndToEnd@cmd/platformctl":                                 "no suite row's -run pattern matches this test name (docs/planning/08 G7 finding)",
	"TestRedpandaKubernetesPortForwardEndToEnd@cmd/platformctl":                      "no suite row's -run pattern matches this test name (docs/planning/08 G7 finding)",
	"TestValidateFailsFastOnBadKubernetesContext@cmd/platformctl":                    "no suite row's -run pattern matches this test name (docs/planning/08 G7 finding)",
	"TestValidatePassesWithReachableKubernetesCluster@cmd/platformctl":               "no suite row's -run pattern matches this test name (docs/planning/08 G7 finding)",
	"TestValidateRefusesKubernetesRuntimeWhenGateExplicitlyDisabled@cmd/platformctl": "no suite row's -run pattern matches this test name (docs/planning/08 G7 finding)",

	// Directories no suite row references at all.
	"TestResolveLiveCluster@internal/adapters/secrets/kubernetes": "no suite row targets this directory at all (docs/planning/08 G7 finding)",
	"TestVaultResolve@internal/adapters/secrets/vault":            "no suite row targets this directory at all (docs/planning/08 G7 finding)",
}

var funcTestRE = regexp.MustCompile(`^func (Test[A-Za-z0-9_]+)\(`)

// integrationTestFuncs walks the repo from root looking for Go files whose
// path matches cmd/platformctl/*_integration_test.go, or that carry a
// `//go:build integration` build tag anywhere in the repo (the two classes
// G7's Do line names), and returns every top-level `func Test*` declared in
// them, keyed by "name@dir".
func integrationTestFuncs(t *testing.T, repoRoot string) map[string]struct{ name, dir string } {
	t.Helper()
	out := map[string]struct{ name, dir string }{}
	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == ".claude" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, relErr := filepath.Rel(repoRoot, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		isCmdIntegration := strings.HasPrefix(rel, "cmd/platformctl/") && strings.HasSuffix(rel, "_integration_test.go")
		if !isCmdIntegration && !hasIntegrationBuildTag(t, path) {
			return nil
		}
		dir := filepath.ToSlash(filepath.Dir(rel))
		names, scanErr := scanTestFuncNames(path)
		if scanErr != nil {
			return scanErr
		}
		for _, name := range names {
			out[name+"@"+dir] = struct{ name, dir string }{name, dir}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", repoRoot, err)
	}
	return out
}

// hasIntegrationBuildTag reports whether path carries a `//go:build
// integration` tag in its leading comment block.
func hasIntegrationBuildTag(t *testing.T, path string) bool {
	t.Helper()
	if !strings.HasSuffix(path, "_test.go") {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "//") {
			break
		}
		if line == "//go:build integration" {
			return true
		}
	}
	return false
}

func scanTestFuncNames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var names []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if m := funcTestRE.FindStringSubmatch(sc.Text()); m != nil {
			names = append(names, m[1])
		}
	}
	return names, sc.Err()
}

// TestIntegrationSuiteMapCoversEveryTest is docs/planning/08 G7's
// completeness guard: every `func Test*` in
// cmd/platformctl/*_integration_test.go, and in any other
// `//go:build integration`-tagged package repo-wide, must be matched by at
// least one scripts/test-impact.sh suite's `go test ... -run ...`
// invocation, or be named on integrationTestExemptions with a reason.
func TestIntegrationSuiteMapCoversEveryTest(t *testing.T) {
	script, err := os.ReadFile(filepath.FromSlash(testImpactScriptPath))
	if err != nil {
		t.Fatalf("read %s: %v", testImpactScriptPath, err)
	}
	suites := parseSuiteMap(t, string(script))

	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	tests := integrationTestFuncs(t, repoRoot)

	var missing []string
	for key, tf := range tests {
		if len(coveringSuites(tf.name, tf.dir, suites)) > 0 {
			continue
		}
		if _, exempt := integrationTestExemptions[key]; exempt {
			continue
		}
		missing = append(missing, key)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("integration test(s) not covered by any scripts/test-impact.sh suite and not on integrationTestExemptions (internal/archtest/test_impact_completeness_test.go) — add a suite row (or widen an existing -run pattern) or an exemption with a reason:\n  %s", strings.Join(missing, "\n  "))
	}

	// Every exemption must still name a test that actually exists — a
	// stale exemption (test renamed/removed) should be cleaned up, not
	// silently keep suppressing nothing.
	var stale []string
	for key := range integrationTestExemptions {
		if _, ok := tests[key]; !ok {
			stale = append(stale, key)
		}
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		t.Errorf("integrationTestExemptions entr(y/ies) name a test that no longer exists — remove them:\n  %s", strings.Join(stale, "\n  "))
	}
}

// TestParseSuiteMapAndCoverage proves the parser+coverage logic itself
// works, against synthetic input rather than the real (large) script —
// mirrors this package's other self-proof tests.
func TestParseSuiteMapAndCoverage(t *testing.T) {
	fixture := "suites() {\n  cat <<'EOF'\n" +
		"docker-conformance|internal/adapters/runtime/docker|go test -tags integration -run Conformance ./internal/adapters/runtime/docker/\n" +
		"cdc|internal/adapters/providers/postgres|go test -tags integration -run 'TestCDC|TestMariaDBCDCEndToEnd' ./cmd/platformctl/\n" +
		"state-s3|internal/adapters/state|go test -tags integration ./internal/adapters/state/... ./cmd/platformctl/ -run 'TestSharedState'\n" +
		"EOF\n}\n"

	suites := parseSuiteMap(t, fixture)
	if len(suites) != 3 {
		t.Fatalf("parsed %d suites, want 3: %+v", len(suites), suites)
	}

	cases := []struct {
		name, dir string
		wantHit   bool
	}{
		{"TestConformance", "internal/adapters/runtime/docker", true},
		{"TestSomethingElse", "internal/adapters/runtime/docker", false}, // -run filter excludes it
		{"TestCDCEndToEnd", "cmd/platformctl", true},                     // substring match, like real go test -run
		{"TestMariaDBCDCEndToEnd", "cmd/platformctl", true},
		{"TestSharedStateBackendEndToEnd", "cmd/platformctl", true},
		{"TestConformance", "internal/adapters/state/s3", false}, // recursive dir in scope, but -run excludes it
		{"TestSharedStateBackendEndToEnd", "internal/adapters/state/s3", true},
		{"TestUnrelated", "internal/unrelated/pkg", false}, // no suite targets this dir at all
	}
	for _, c := range cases {
		got := len(coveringSuites(c.name, c.dir, suites)) > 0
		if got != c.wantHit {
			t.Errorf("coveringSuites(%q, %q) hit=%v, want %v", c.name, c.dir, got, c.wantHit)
		}
	}
}
