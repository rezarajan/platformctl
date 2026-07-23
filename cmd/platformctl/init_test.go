package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/rezarajan/platformctl/internal/application/blueprint"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

// TestInitBlueprintValidatesWithNoEdits is the e2e-per-blueprint accept
// criterion from docs/planning/08 §E1: `init <blueprint>` followed by
// `validate` on the freshly written directory must be green with zero
// manifest edits, for every shipped blueprint. No Docker required — this
// only exercises manifest → schema → graph → compatibility validation.
func TestInitBlueprintValidatesWithNoEdits(t *testing.T) {
	t.Parallel()
	for _, name := range blueprint.Names() {
		name := name
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, name)

			out, err, code := run(t, "init", name, "--dir", target)
			if err != nil || code != 0 {
				t.Fatalf("init %s failed (code %d): %v\n%s", name, code, err, out)
			}

			out, err, code = run(t, "validate", target)
			if err != nil || code != 0 {
				t.Fatalf("validate on freshly-init'd %s failed (code %d): %v\n%s", name, code, err, out)
			}
			if !strings.Contains(out, "resource(s) valid") {
				t.Errorf("validate output for %s missing the success line:\n%s", name, out)
			}
		})
	}
}

// TestInitListHumanOutput is the default (prose) rendering of --list: one
// line per blueprint, name then summary, no JSON/YAML.
func TestInitListHumanOutput(t *testing.T) {
	t.Parallel()
	out, err, code := run(t, "init", "--list")
	if err != nil || code != 0 {
		t.Fatalf("init --list failed (code %d): %v\n%s", code, err, out)
	}
	for _, name := range blueprint.Names() {
		if !strings.Contains(out, name) {
			t.Errorf("init --list output missing blueprint %q:\n%s", name, out)
		}
	}
}

// TestInitListStructuredOutput guards the docs/planning/08 §2 / §E1
// machine-output contract mirrored from output_contract_test.go: -o
// json|yaml must produce exactly one parseable document on stdout.
func TestInitListStructuredOutput(t *testing.T) {
	t.Parallel()
	out, _, err := runSplit(t, "init", "--list", "-o", "json")
	if err != nil {
		t.Fatalf("init --list -o json: %v", err)
	}
	var parsed struct {
		Blueprints []struct {
			Name      string   `json:"name"`
			Summary   string   `json:"summary"`
			Providers []string `json:"providers"`
		} `json:"blueprints"`
	}
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("init --list -o json did not produce valid JSON: %v\noutput:\n%s", jsonErr, out)
	}
	if len(parsed.Blueprints) != len(blueprint.Names()) {
		t.Errorf("init --list -o json blueprint count = %d, want %d", len(parsed.Blueprints), len(blueprint.Names()))
	}
	for _, b := range parsed.Blueprints {
		if b.Name == "" || b.Summary == "" || len(b.Providers) == 0 {
			t.Errorf("init --list -o json produced an incomplete entry: %+v", b)
		}
	}

	out, _, err = runSplit(t, "init", "--list", "-o", "yaml")
	if err != nil {
		t.Fatalf("init --list -o yaml: %v", err)
	}
	var yamlParsed map[string]any
	if yamlErr := yaml.Unmarshal([]byte(out), &yamlParsed); yamlErr != nil {
		t.Fatalf("init --list -o yaml did not produce valid YAML: %v\noutput:\n%s", yamlErr, out)
	}
	if _, ok := yamlParsed["blueprints"]; !ok {
		t.Errorf("init --list -o yaml missing blueprints key:\n%s", out)
	}
}

// TestInitWriteStructuredOutput: `init <blueprint> -o json` (no --list)
// also emits exactly one parseable document naming the files written.
func TestInitWriteStructuredOutput(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "stream-basics")
	out, _, err := runSplit(t, "init", "stream-basics", "--dir", target, "-o", "json")
	if err != nil {
		t.Fatalf("init stream-basics -o json: %v", err)
	}
	var parsed struct {
		Blueprint string   `json:"blueprint"`
		Dir       string   `json:"dir"`
		Files     []string `json:"files"`
	}
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("init -o json did not produce valid JSON: %v\noutput:\n%s", jsonErr, out)
	}
	if parsed.Blueprint != "stream-basics" {
		t.Errorf("blueprint = %q, want stream-basics", parsed.Blueprint)
	}
	if len(parsed.Files) == 0 {
		t.Error("files list empty")
	}
}

// TestInitUnknownBlueprintFails asserts a clear validation-exit error, not
// a panic or an opaque failure, for an unknown blueprint name.
func TestInitUnknownBlueprintFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, err, code := run(t, "init", "does-not-exist", "--dir", dir)
	if err == nil {
		t.Fatal("init on an unknown blueprint succeeded, want an error")
	}
	if code != cliutil.ExitValidation {
		t.Errorf("exit code = %d, want %d (ExitValidation)", code, cliutil.ExitValidation)
	}
	if !strings.Contains(err.Error(), "unknown blueprint") {
		t.Errorf("error message missing 'unknown blueprint': %v", err)
	}
}

// TestInitRefusesOverwriteWithoutForce and TestInitForceOverwrites cover
// the collision/--force contract at the CLI layer (blueprint_test.go
// covers it at the package layer).
func TestInitRefusesOverwriteWithoutForce(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "stream-basics")
	if _, err, code := run(t, "init", "stream-basics", "--dir", target); err != nil || code != 0 {
		t.Fatalf("first init failed: %v (code %d)", err, code)
	}
	out, err, code := run(t, "init", "stream-basics", "--dir", target)
	if err == nil {
		t.Fatalf("second init without --force succeeded, want a refusal:\n%s", out)
	}
	_ = code
}

func TestInitForceOverwrites(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "stream-basics")
	if _, err, code := run(t, "init", "stream-basics", "--dir", target); err != nil || code != 0 {
		t.Fatalf("first init failed: %v (code %d)", err, code)
	}
	if err := os.WriteFile(filepath.Join(target, "README.md"), []byte("mutated"), 0o644); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	if _, err, code := run(t, "init", "stream-basics", "--dir", target, "--force"); err != nil || code != 0 {
		t.Fatalf("forced init failed: %v (code %d)", err, code)
	}
	data, err := os.ReadFile(filepath.Join(target, "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	if string(data) == "mutated" {
		t.Error("--force did not overwrite the mutated file")
	}
}
