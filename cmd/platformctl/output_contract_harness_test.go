package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// This file is the generic command-output harness docs/planning/08 A7 (doc
// 07 §0.5/§3.2) called for: graph/validate/inventory --for already had
// dedicated tests (output_contract_test.go) proving the -o json|yaml
// contract for the three paths that were once broken, but nothing swept
// every registered command systematically. commandScenarios is that sweep,
// driven against the fake runtime (testdata/noop-scenario, examples/
// cdc-attendance) — real infrastructure exit paths (e.g. drift actually
// observed on live infra) stay with the integration suites, which is a
// deliberate scope line, not an oversight.
//
// Adding a new cobra command without adding it here fails
// TestOutputContractHarnessCoversEveryCommand — see that test.

// commandScenario is one leaf command's coverage entry, keyed by its full
// cobra path (e.g. "docs build").
type commandScenario struct {
	// structured is whether the command supports -o json|yaml at all.
	structured bool
	// reason explains a nil run: a command intentionally not exercised live
	// (e.g. it starts a blocking server), never an unexplained gap.
	reason string
	// run performs the live scenario and makes its own assertions. Nil only
	// when reason is set.
	run func(t *testing.T)
}

// assertJSON fails unless out is exactly one parseable JSON document.
func assertJSON(t *testing.T, label, out string) {
	t.Helper()
	if strings.TrimSpace(out) == "" {
		t.Fatalf("%s: stdout is empty, want a parseable JSON document", label)
	}
	var v any
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Errorf("%s: stdout is not valid JSON: %v\noutput:\n%s", label, err, out)
	}
}

// assertYAML fails unless out is exactly one parseable YAML document.
func assertYAML(t *testing.T, label, out string) {
	t.Helper()
	if strings.TrimSpace(out) == "" {
		t.Fatalf("%s: stdout is empty, want a parseable YAML document", label)
	}
	var v any
	if err := yaml.Unmarshal([]byte(out), &v); err != nil {
		t.Errorf("%s: stdout is not valid YAML: %v\noutput:\n%s", label, err, out)
	}
}

// runBothFormats runs args once with -o json and once with -o yaml,
// asserting stdout parses each time and prose never leaked onto stdout
// (humanWriter routes it to stderr under structured output).
func runBothFormats(t *testing.T, label string, args ...string) {
	t.Helper()
	out, _, _ := runSplit(t, append(append([]string{}, args...), "-o", "json")...)
	assertJSON(t, label+" -o json", out)
	out, _, _ = runSplit(t, append(append([]string{}, args...), "-o", "yaml")...)
	assertYAML(t, label+" -o yaml", out)
}

var commandScenarios = map[string]commandScenario{
	"validate": {
		structured: true,
		run: func(t *testing.T) {
			runBothFormats(t, "validate", "validate", "../../examples/cdc-attendance", "--feature-gates", "SchemaRegistrySupport=true")
		},
	},
	"plan": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			// Changes pending (exit 1, ExitError with nil Err — stdout still
			// carries the plan document).
			out, _, _ := runSplit(t, "plan", "testdata/noop-scenario", "--state-file", stateFile, "-o", "json")
			assertJSON(t, "plan (pending) -o json", out)

			if _, err, code := run(t, "apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
				t.Fatalf("apply failed (code %d): %v", code, err)
			}
			// No-op (exit 0).
			out, _, err := runSplit(t, "plan", "testdata/noop-scenario", "--state-file", stateFile, "-o", "yaml")
			if err != nil {
				t.Fatalf("plan (no-op) -o yaml: %v", err)
			}
			assertYAML(t, "plan (no-op) -o yaml", out)
		},
	},
	"apply": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			// Changed: first apply creates.
			out, _, err := runSplit(t, "apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve", "-o", "json")
			if err != nil {
				t.Fatalf("apply (changed) -o json: %v", err)
			}
			assertJSON(t, "apply (changed) -o json", out)

			// No-op: second apply.
			out, _, err = runSplit(t, "apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve", "-o", "yaml")
			if err != nil {
				t.Fatalf("apply (no-op) -o yaml: %v", err)
			}
			assertYAML(t, "apply (no-op) -o yaml", out)

			// Cancelled: a fresh state, declining the prompt.
			cancelState := filepath.Join(t.TempDir(), "state.json")
			root := newRootCmd(defaultWiring)
			var outBuf, errBuf strings.Builder
			root.SetOut(&outBuf)
			root.SetErr(&errBuf)
			root.SetIn(strings.NewReader("no\n"))
			root.SetArgs([]string{"apply", "testdata/noop-scenario", "--state-file", cancelState, "-o", "json"})
			if err := root.Execute(); err != nil {
				t.Fatalf("apply (cancelled) -o json: %v", err)
			}
			var cancelled struct {
				Cancelled bool `json:"cancelled"`
			}
			assertJSON(t, "apply (cancelled) -o json", outBuf.String())
			if jsonErr := json.Unmarshal([]byte(outBuf.String()), &cancelled); jsonErr != nil || !cancelled.Cancelled {
				t.Errorf("apply (cancelled) -o json: cancelled = %v (err %v), want true\n%s", cancelled.Cancelled, jsonErr, outBuf.String())
			}
		},
	},
	"destroy": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			if _, err, code := run(t, "apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
				t.Fatalf("apply failed (code %d): %v", code, err)
			}
			// Changed: destroy what was applied.
			out, _, err := runSplit(t, "destroy", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve", "-o", "json")
			if err != nil {
				t.Fatalf("destroy (changed) -o json: %v", err)
			}
			assertJSON(t, "destroy (changed) -o json", out)

			// No-op: nothing left to destroy.
			out, _, err = runSplit(t, "destroy", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve", "-o", "yaml")
			if err != nil {
				t.Fatalf("destroy (no-op) -o yaml: %v", err)
			}
			assertYAML(t, "destroy (no-op) -o yaml", out)
		},
	},
	"status": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			// Empty: nothing applied yet.
			out, _, err := runSplit(t, "status", "testdata/noop-scenario", "--state-file", stateFile, "-o", "json")
			if err != nil {
				t.Fatalf("status (empty) -o json: %v", err)
			}
			assertJSON(t, "status (empty) -o json", out)

			if _, err, code := run(t, "apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
				t.Fatalf("apply failed (code %d): %v", code, err)
			}
			out, _, err = runSplit(t, "status", "testdata/noop-scenario", "--state-file", stateFile, "-o", "yaml")
			if err != nil {
				t.Fatalf("status (applied) -o yaml: %v", err)
			}
			assertYAML(t, "status (applied) -o yaml", out)
		},
	},
	"drift": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			if _, err, code := run(t, "apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
				t.Fatalf("apply failed (code %d): %v", code, err)
			}
			// Clean: the fake runtime never drifts on its own. Drifted-exit
			// coverage lives in the chaos/CDC integration suites (A8), which
			// mutate real infrastructure out-of-band — not reproducible
			// cheaply against the fake runtime this harness uses.
			out, _, err := runSplit(t, "drift", "testdata/noop-scenario", "--state-file", stateFile, "-o", "json")
			if err != nil {
				t.Fatalf("drift (clean) -o json: %v", err)
			}
			assertJSON(t, "drift (clean) -o json", out)

			out, _, err = runSplit(t, "drift", "testdata/noop-scenario", "--state-file", stateFile, "-o", "yaml")
			if err != nil {
				t.Fatalf("drift (clean) -o yaml: %v", err)
			}
			assertYAML(t, "drift (clean) -o yaml", out)
		},
	},
	"import": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			out, _, err := runSplit(t, "import", "Provider/test-noop-provider", "testdata/noop-scenario",
				"--from", "test-noop-provider", "--state-file", stateFile, "-o", "json")
			if err != nil {
				t.Fatalf("import -o json: %v", err)
			}
			assertJSON(t, "import -o json", out)
		},
	},
	"backup": {
		structured: true,
		reason:     "needs a real postgres/mysql/s3 instance and object-store destination reachable on Docker; covered by the integration-tagged round-trip tests (cmd/platformctl/backup_integration_test.go).",
	},
	"restore": {
		structured: true,
		reason:     "needs a real postgres/mysql/s3 instance and object-store source reachable on Docker; covered by the integration-tagged round-trip tests (cmd/platformctl/backup_integration_test.go). The restore-over-existing-data refusal itself needs no infra and is unit-tested directly (TestRestoreRefusesWithoutOverwriteFlag).",
	},
	"init": {
		structured: true,
		run: func(t *testing.T) {
			// --list: enumerates blueprints.
			runBothFormats(t, "init --list", "init", "--list")

			// Writing a blueprint: files-written document.
			dir := filepath.Join(t.TempDir(), "bp")
			out, _, err := runSplit(t, "init", "stream-basics", "--dir", dir, "-o", "json")
			if err != nil {
				t.Fatalf("init stream-basics -o json: %v", err)
			}
			assertJSON(t, "init -o json", out)
		},
	},
	"graph": {
		structured: true,
		run: func(t *testing.T) {
			runBothFormats(t, "graph", "graph", "../../examples/cdc-attendance", "--feature-gates", "SchemaRegistrySupport=true")
		},
	},
	"inventory": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			// Empty: nothing applied, so zero endpoints — the doc 07 §0.5
			// case that a bare-nil slice must still marshal as [].
			out, _, err := runSplit(t, "inventory", "testdata/noop-scenario", "--state-file", stateFile, "-o", "json")
			if err != nil {
				t.Fatalf("inventory (empty) -o json: %v", err)
			}
			assertJSON(t, "inventory (empty) -o json", out)
			var parsed struct {
				Endpoints []any `json:"endpoints"`
			}
			if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
				t.Fatalf("inventory (empty) -o json: %v", jsonErr)
			}
			if parsed.Endpoints == nil {
				t.Errorf("inventory (empty) -o json: endpoints = null, want []")
			}

			// --for: a rendered snippet still comes back as one JSON document.
			out, _, err = runSplit(t, "inventory", "testdata/redpanda-scenario", "--state-file", stateFile, "--for", "spark", "-o", "yaml")
			if err != nil {
				t.Fatalf("inventory --for -o yaml: %v", err)
			}
			assertYAML(t, "inventory --for -o yaml", out)
		},
	},
	"docs build": {
		structured: false,
		reason:     "docs has no -o json|yaml support (renders markdown/HTML files, not a data payload) — still exercised as a smoke check.",
		run: func(t *testing.T) {
			out, err, code := run(t, "docs", "build", "--out", t.TempDir())
			if err != nil || code != 0 {
				t.Fatalf("docs build failed (code %d): %v\n%s", code, err, out)
			}
			if !strings.Contains(out, "wrote") {
				t.Errorf("docs build did not report what it wrote:\n%s", out)
			}
		},
	},
	"docs serve": {
		structured: false,
		reason:     "starts a blocking HTTP server (http.ListenAndServe); not exercised in the automated harness — docs build covers the same rendering path non-interactively.",
	},
	"gc plan": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			// The fake runtime is a fresh, empty in-memory instance per
			// a.reg.Runtime(...) call, so it can't carry a pre-existing
			// orphan across separate CLI invocations the way a real Docker
			// daemon does — this smoke-tests the output contract (an empty
			// orphan list still parses as one document) and flag wiring;
			// live orphan detection is covered by the Docker integration
			// test (gc_integration_test.go, docs/planning/08 A2).
			runBothFormats(t, "gc plan", "gc", "plan", "--runtime", "fake", "--state-file", stateFile)
		},
	},
	"gc apply": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			if _, _, err := runSplit(t, "gc", "apply", "--runtime", "fake", "--state-file", stateFile, "-o", "json"); err == nil {
				t.Fatal("gc apply accepted without --yes-i-understand-this-is-destructive")
			}
			out, _, err := runSplit(t, "gc", "apply", "--runtime", "fake", "--state-file", stateFile,
				"--yes-i-understand-this-is-destructive", "-o", "json")
			if err != nil {
				t.Fatalf("gc apply -o json: %v", err)
			}
			assertJSON(t, "gc apply -o json", out)
		},
	},
	"state inspect": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			if _, err, code := run(t, "apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
				t.Fatalf("apply failed (code %d): %v", code, err)
			}
			runBothFormats(t, "state inspect", "state", "inspect", "--state-file", stateFile)
		},
	},
	"state doctor": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			// Healthy: nothing applied yet, so nothing to check — a clean
			// exit and a parseable empty report. The full defect-class
			// sweep and the doctor/repair round-trip live in state_test.go.
			runBothFormats(t, "state doctor", "state", "doctor", "--state-file", stateFile, "--runtime", "fake")
		},
	},
	"state repair": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			out, err, code := run(t, "state", "repair", "--state-file", stateFile, "--runtime", "fake", "--yes", "-o", "json")
			if err != nil || code != 0 {
				t.Fatalf("state repair (healthy no-op) failed (code %d): %v\n%s", code, err, out)
			}
			assertJSON(t, "state repair -o json", out)
		},
	},
	"state unlock": {
		structured: true,
		run: func(t *testing.T) {
			stateFile := filepath.Join(t.TempDir(), "state.json")
			out, err, code := run(t, "state", "unlock", "--state-file", stateFile, "-o", "json")
			if err != nil || code != 0 {
				t.Fatalf("state unlock failed (code %d): %v\n%s", code, err, out)
			}
			assertJSON(t, "state unlock -o json", out)
		},
	},
}

// TestOutputContractHarness runs every registered scenario.
func TestOutputContractHarness(t *testing.T) {
	for name, scenario := range commandScenarios {
		t.Run(name, func(t *testing.T) {
			if scenario.run == nil {
				if scenario.reason == "" {
					t.Fatalf("%s: registered with no run() and no reason — every skip must explain itself", name)
				}
				t.Skipf("not exercised live: %s", scenario.reason)
			}
			scenario.run(t)
		})
	}
}

// leafCommandPaths walks a cobra command tree and returns the full
// space-joined path of every leaf command, excluding cobra's own
// auto-added "help" and "completion" commands (not part of this product's
// command surface).
func leafCommandPaths(cmd *cobra.Command) []string {
	var out []string
	var walk func(c *cobra.Command, prefix string)
	walk = func(c *cobra.Command, prefix string) {
		name := c.Name()
		if name == "help" || name == "completion" {
			return
		}
		path := name
		if prefix != "" {
			path = prefix + " " + name
		}
		if !c.HasSubCommands() {
			out = append(out, path)
			return
		}
		for _, sub := range c.Commands() {
			walk(sub, path)
		}
	}
	for _, sub := range cmd.Commands() {
		walk(sub, "")
	}
	return out
}

// TestOutputContractHarnessCoversEveryCommand is the completeness guard A7
// asked for: every cobra command registered on the real root must have a
// commandScenarios entry, or this fails naming exactly what's missing.
func TestOutputContractHarnessCoversEveryCommand(t *testing.T) {
	root := newRootCmd(defaultWiring)
	var missing []string
	for _, path := range leafCommandPaths(root) {
		if _, ok := commandScenarios[path]; !ok {
			missing = append(missing, path)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("command(s) missing from the output-contract harness table (register in commandScenarios in output_contract_harness_test.go): %v", missing)
	}
}

// TestLeafCommandPathsCatchesUnregisteredCommand proves the completeness
// guard actually works, against a synthetic tree rather than mutating the
// real CLI: a command absent from a table must be reported, by name.
func TestLeafCommandPathsCatchesUnregisteredCommand(t *testing.T) {
	fakeRoot := &cobra.Command{Use: "fake"}
	fakeRoot.AddCommand(
		&cobra.Command{Use: "known", Run: func(*cobra.Command, []string) {}},
		&cobra.Command{Use: "surprise", Run: func(*cobra.Command, []string) {}},
	)
	table := map[string]bool{"known": true}

	var missing []string
	for _, path := range leafCommandPaths(fakeRoot) {
		if !table[path] {
			missing = append(missing, path)
		}
	}
	if len(missing) != 1 || missing[0] != "surprise" {
		t.Fatalf("expected exactly [surprise] reported missing, got %v", missing)
	}
}
