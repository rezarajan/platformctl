package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestREADMECLISurfaceInSync guards the thrice-recurred failure class
// recorded in docs/remediation/F-003-readme-cli-surface-stale.md (and
// re-found by the 2026-07 production review, doc 11): new top-level
// commands land without a row in README's "CLI surface" table. Every
// visible top-level command must have a table row, and every command
// named by a row must exist — drift in either direction fails.
func TestREADMECLISurfaceInSync(t *testing.T) {
	readme, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	section := string(readme)
	if i := strings.Index(section, "## 🖥 CLI surface"); i >= 0 {
		section = section[i:]
		if j := strings.Index(section[1:], "\n## "); j >= 0 {
			section = section[:j+1]
		}
	} else {
		t.Fatal("README.md has no '## 🖥 CLI surface' section")
	}

	// First backticked token of each table row, first word only
	// ("state inspect" rows document subcommands of "state").
	rowRe := regexp.MustCompile("(?m)^\\| `([a-z]+)")
	documented := map[string]bool{}
	for _, m := range rowRe.FindAllStringSubmatch(section, -1) {
		documented[m[1]] = true
	}

	root := newRootCmd(defaultWiring)
	registered := map[string]bool{}
	for _, c := range root.Commands() {
		if c.Hidden || c.Name() == "help" || c.Name() == "completion" {
			continue
		}
		registered[c.Name()] = true
	}

	for name := range registered {
		if !documented[name] {
			t.Errorf("command %q is registered but has no row in README's CLI surface table (the F-003 failure class)", name)
		}
	}
	for name := range documented {
		if !registered[name] {
			t.Errorf("README's CLI surface table documents %q but no such command is registered", name)
		}
	}
}
