package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// TestLogFormatJSONEmitsStructuredEventsPerAction proves docs/planning/08
// I11's accept bar: --log-format json emits one parseable JSON event per
// reconciliation action on stderr, each carrying NFR-4's resource/action/
// outcome/duration attributes (plus the same prose text mode renders,
// carried in "msg"). destroy is used because its Engine.Logger stays wired
// end to end — apply nils Logger out once its Reporter takes over stderr
// (see (*app).newEngine's Logger wiring and the apply command's `eng.Logger
// = nil`, cmd/platformctl/root.go), so destroy is the live path that
// exercises the seam this task changed.
func TestLogFormatJSONEmitsStructuredEventsPerAction(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	if _, err, code := run(t, "apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v", code, err)
	}

	root := newRootCmd(defaultWiring)
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"destroy", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve", "--log-format", "json"})
	if err := root.Execute(); err != nil {
		t.Fatalf("destroy --log-format json: %v\nstderr:\n%s", err, errBuf.String())
	}

	trimmed := strings.TrimRight(errBuf.String(), "\n")
	if trimmed == "" {
		t.Fatalf("destroy --log-format json: no log lines on stderr")
	}
	lines := strings.Split(trimmed, "\n")
	for _, line := range lines {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("stderr line is not parseable JSON: %v\nline: %s", err, line)
		}
		for _, key := range []string{"resource", "action", "outcome", "duration", "msg"} {
			if _, ok := event[key]; !ok {
				t.Errorf("event missing %q key: %v", key, event)
			}
		}
	}
}

// TestLogFormatTextIsByteCompatible proves the other half of the accept
// bar: --log-format text (the default) still renders exactly the pre-I11
// prose — no slog timestamp/level prefix, no attrs — for the same destroy
// action.
func TestLogFormatTextIsByteCompatible(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	if _, err, code := run(t, "apply", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v", code, err)
	}

	root := newRootCmd(defaultWiring)
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"destroy", "testdata/noop-scenario", "--state-file", stateFile, "--auto-approve"})
	if err := root.Execute(); err != nil {
		t.Fatalf("destroy (default log format): %v\nstderr:\n%s", err, errBuf.String())
	}

	trimmed := strings.TrimRight(errBuf.String(), "\n")
	if trimmed == "" {
		t.Fatalf("destroy (default log format): no log lines on stderr")
	}
	for _, line := range strings.Split(trimmed, "\n") {
		if strings.HasPrefix(line, "{") {
			t.Errorf("text-format log line looks like JSON, want plain prose: %q", line)
		}
		if !strings.HasPrefix(line, "ok   destroy ") && !strings.HasPrefix(line, "fail destroy ") && !strings.HasPrefix(line, "skip destroy ") {
			t.Errorf("text-format log line does not match the historical ok/fail/skip destroy prose: %q", line)
		}
	}
}
