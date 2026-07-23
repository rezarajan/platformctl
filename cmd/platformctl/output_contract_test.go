package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// runSplit is run's stdout/stderr-separated counterpart: the root output
// contract (docs/planning/07 §0.5) is that stdout alone must parse as a
// single JSON/YAML document when -o json|yaml is in effect, so a test
// asserting that contract must not merge stderr into the same buffer the
// way run() does for human-readable assertions.
func runSplit(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	root := newRootCmd(defaultWiring)
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetArgs(args)
	err = root.Execute()
	return outBuf.String(), errBuf.String(), err
}

// TestGraphStructuredOutput guards docs/planning/07 §0.5 / F-001: -o
// json|yaml must override --format with a single parseable document on
// stdout, for every graph format request.
func TestGraphStructuredOutput(t *testing.T) {
	t.Parallel()
	out, _, err := runSplit(t, "graph", "../../examples/cdc-attendance", "--feature-gates", "SchemaRegistrySupport=true", "-o", "json")
	if err != nil {
		t.Fatalf("graph -o json: %v", err)
	}
	var parsed struct {
		Nodes []any `json:"nodes"`
		Edges []any `json:"edges"`
	}
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("graph -o json did not produce valid JSON: %v\noutput:\n%s", jsonErr, out)
	}
	if len(parsed.Nodes) == 0 {
		t.Errorf("graph -o json produced no nodes:\n%s", out)
	}

	out, _, err = runSplit(t, "graph", "../../examples/cdc-attendance", "--feature-gates", "SchemaRegistrySupport=true", "-o", "yaml")
	if err != nil {
		t.Fatalf("graph -o yaml: %v", err)
	}
	var yamlParsed map[string]any
	if yamlErr := yaml.Unmarshal([]byte(out), &yamlParsed); yamlErr != nil {
		t.Fatalf("graph -o yaml did not produce valid YAML: %v\noutput:\n%s", yamlErr, out)
	}
	if _, ok := yamlParsed["nodes"]; !ok {
		t.Errorf("graph -o yaml missing nodes key:\n%s", out)
	}
}

// TestGraphDefaultOutputUnchanged is the regression guard: graph with no
// -o override must still render the tree view, not JSON.
func TestGraphDefaultOutputUnchanged(t *testing.T) {
	t.Parallel()
	out, _, err := runSplit(t, "graph", "../../examples/cdc-attendance", "--feature-gates", "SchemaRegistrySupport=true")
	if err != nil {
		t.Fatalf("graph: %v", err)
	}
	if !strings.HasPrefix(out, "DATA FLOW") {
		t.Errorf("graph default output changed, want tree view starting with DATA FLOW:\n%s", out)
	}
}

// TestGraphFormatFlagStillWorks: --format continues to select the
// non-structured presentation when -o is not json/yaml.
func TestGraphFormatFlagStillWorks(t *testing.T) {
	t.Parallel()
	out, _, err := runSplit(t, "graph", "../../examples/cdc-attendance", "--feature-gates", "SchemaRegistrySupport=true", "--format", "dot")
	if err != nil {
		t.Fatalf("graph --format dot: %v", err)
	}
	if !strings.HasPrefix(out, "digraph") {
		t.Errorf("graph --format dot did not render DOT:\n%s", out)
	}
}

// TestValidateStructuredOutput guards docs/planning/07 §0.5 / F-001.
func TestValidateStructuredOutput(t *testing.T) {
	t.Parallel()
	out, _, err := runSplit(t, "validate", "../../examples/cdc-attendance", "--feature-gates", "SchemaRegistrySupport=true", "-o", "json")
	if err != nil {
		t.Fatalf("validate -o json: %v", err)
	}
	var parsed struct {
		Valid     bool `json:"valid"`
		Resources int  `json:"resources"`
	}
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("validate -o json did not produce valid JSON: %v\noutput:\n%s", jsonErr, out)
	}
	if !parsed.Valid {
		t.Errorf("validate -o json valid = false, want true")
	}
	if parsed.Resources == 0 {
		t.Errorf("validate -o json resources = 0, want > 0")
	}
}

// TestValidateDefaultOutputUnchanged is the regression guard for the
// existing prose contract.
func TestValidateDefaultOutputUnchanged(t *testing.T) {
	t.Parallel()
	out, _, err := runSplit(t, "validate", "../../examples/cdc-attendance", "--feature-gates", "SchemaRegistrySupport=true")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !strings.Contains(out, "resource(s) valid") {
		t.Errorf("validate default output changed:\n%s", out)
	}
}

// TestInventoryForStructuredOutput guards docs/planning/07 §0.5 / F-001:
// --for with -o json must still emit exactly one parseable document, even
// though the underlying snippet is prose.
func TestInventoryForStructuredOutput(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	out, _, err := runSplit(t, "inventory", "testdata/redpanda-scenario", "--state-file", stateFile, "--for", "spark", "-o", "json")
	if err != nil {
		t.Fatalf("inventory --for spark -o json: %v", err)
	}
	var parsed struct {
		Tool   string `json:"tool"`
		Config string `json:"config"`
	}
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("inventory --for -o json did not produce valid JSON: %v\noutput:\n%s", jsonErr, out)
	}
	if parsed.Tool != "spark" {
		t.Errorf("tool = %q, want spark", parsed.Tool)
	}
	if !strings.Contains(parsed.Config, "spark-defaults.conf") {
		t.Errorf("config field missing rendered snippet:\n%s", parsed.Config)
	}
}

// TestInventoryForDefaultOutputUnchanged is the regression guard: --for
// without -o json still writes the raw snippet to stdout.
func TestInventoryForDefaultOutputUnchanged(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	out, _, err := runSplit(t, "inventory", "testdata/redpanda-scenario", "--state-file", stateFile, "--for", "spark")
	if err != nil {
		t.Fatalf("inventory --for spark: %v", err)
	}
	if !strings.HasPrefix(out, "# spark-defaults.conf") {
		t.Errorf("inventory --for default output changed:\n%s", out)
	}
}
