package archtest

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestCIScenarioShardsPartitionKubernetesTests guards the 2026-07-23
// scenarios-shard split (.github/workflows/ci.yml): the two scenario -run
// patterns must PARTITION the set of Kubernetes-named integration tests in
// cmd/platformctl — every such test matches exactly one shard. A test
// matching neither silently loses its CI coverage (the pre-split single
// pattern was `-run Kubernetes`, so anything Kubernetes-named ran); a test
// matching both doubles wall-clock on the slowest job for nothing.
func TestCIScenarioShardsPartitionKubernetesTests(t *testing.T) {
	t.Parallel()
	repoRoot, err := repoRootDir()
	if err != nil {
		t.Fatal(err)
	}
	ci, err := os.ReadFile(filepath.Join(repoRoot, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}

	shardPatterns := regexp.MustCompile(`-run '([^']+)'`).FindAllStringSubmatch(string(ci), -1)
	var core, apps *regexp.Regexp
	for _, m := range shardPatterns {
		switch {
		case strings.Contains(m[1], "TestRedpanda"):
			core = regexp.MustCompile(m[1])
		case strings.Contains(m[1], "ExampleOnKubernetes"):
			apps = regexp.MustCompile(m[1])
		}
	}
	if core == nil || apps == nil {
		t.Fatalf("could not locate both scenario shard -run patterns in ci.yml (found %d -run patterns) — if the shard split was restructured, update this guard in the same commit", len(shardPatterns))
	}

	kubernetesTest := regexp.MustCompile(`func (Test\w*Kubernetes\w*)\(`)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "cmd", "platformctl"))
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Join(repoRoot, "cmd", "platformctl", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(string(src), "//go:build integration") {
			continue
		}
		for _, m := range kubernetesTest.FindAllStringSubmatch(string(src), -1) {
			name := m[1]
			checked++
			inCore, inApps := core.MatchString(name), apps.MatchString(name)
			switch {
			case inCore && inApps:
				t.Errorf("%s matches BOTH scenario shards — it runs twice; tighten one pattern in ci.yml", name)
			case !inCore && !inApps:
				t.Errorf("%s matches NEITHER scenario shard — it has silently lost CI coverage; extend a pattern in ci.yml (this is the exact class the pre-split `-run Kubernetes` never had)", name)
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no Kubernetes-named integration tests in cmd/platformctl — the scanner is broken, not the tree")
	}
}
