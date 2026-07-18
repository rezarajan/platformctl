package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeStateFixture writes raw JSON directly, bypassing the StateStore, so
// each test controls the on-disk format precisely (including a stale
// version, an orphan, or a corrupt key) — the shape `state doctor`/`state
// repair` are supposed to diagnose and fix.
func writeStateFixture(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestStateDoctorReportsAllDefectClasses covers docs/planning/08 A3's
// accept criterion: a fixture state file containing every defect class
// doctor is supposed to catch, in one pass. The Provider entry's
// "gone object" status is deterministic against the fake runtime, which is
// a fresh empty instance per invocation — exactly what makes it useful here
// (no real Docker needed to prove the check fires).
func TestStateDoctorReportsAllDefectClasses(t *testing.T) {
	// A genuine v1 file predates namespaces entirely, so its raw keys are
	// bare "Kind/Name" (no namespace segment) — a 3-part key here would be
	// misinterpreted by the v1 migration, which is exactly the kind of
	// fixture bug this comment exists to prevent re-introducing.
	stateFile := writeStateFixture(t, `{
  "version": 1,
  "resources": {
    "Provider/legacy-orphan": {
      "specHash": "old",
      "lifecycle": "Managed"
    },
    "EventStream/mismatched": {
      "specHash": "abc",
      "lifecycle": "Managed",
      "lastApplied": {
        "apiVersion": "datascape.io/v1alpha1",
        "kind": "EventStream",
        "metadata": {"name": "actually-different-name", "namespace": "default"},
        "spec": {}
      }
    },
    "Provider/gone-provider": {
      "specHash": "abc",
      "lifecycle": "Managed",
      "lastApplied": {
        "apiVersion": "datascape.io/v1alpha1",
        "kind": "Provider",
        "metadata": {"name": "gone-provider", "namespace": "default"},
        "spec": {"type": "noop", "runtime": {"type": "fake"}}
      }
    }
  }
}`)

	out, _, code := run(t, "state", "doctor", "--state-file", stateFile, "--runtime", "fake", "-o", "json")
	if code != 1 {
		t.Fatalf("state doctor exit = %d, want 1 (defects found)\n%s", code, out)
	}
	var report stateDoctorReport
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("decode doctor report: %v\n%s", err, out)
	}
	if !report.StaleFormat || report.FileVersion != 1 {
		t.Errorf("StaleFormat/FileVersion = %v/%d, want true/1", report.StaleFormat, report.FileVersion)
	}
	if len(report.LegacyOrphans) != 1 || report.LegacyOrphans[0] != "default/Provider/legacy-orphan" {
		t.Errorf("LegacyOrphans = %v, want exactly [default/Provider/legacy-orphan]", report.LegacyOrphans)
	}
	if len(report.CorruptEntries) != 1 || report.CorruptEntries[0] != "default/EventStream/mismatched" {
		t.Errorf("CorruptEntries = %v, want exactly [default/EventStream/mismatched]", report.CorruptEntries)
	}
	if len(report.GoneObjects) != 1 || report.GoneObjects[0] != "default/Provider/gone-provider" {
		t.Errorf("GoneObjects = %v, want exactly [default/Provider/gone-provider]", report.GoneObjects)
	}
}

// TestStateRepairFixesStaleVersionAndDropsGoneObjects is the doctor/repair
// round-trip: repair must persist the migrated format and drop the
// confirmed-gone Provider entry, but must never touch the legacy-orphan or
// corrupt entries — those have no safe automatic fix (doc 08 A3's own
// scoping: "re-key legacy entries, drop entries for confirmed-gone
// objects", nothing else).
func TestStateRepairFixesStaleVersionAndDropsGoneObjects(t *testing.T) {
	stateFile := writeStateFixture(t, `{
  "version": 1,
  "resources": {
    "Provider/legacy-orphan": {
      "specHash": "old",
      "lifecycle": "Managed"
    },
    "Provider/gone-provider": {
      "specHash": "abc",
      "lifecycle": "Managed",
      "lastApplied": {
        "apiVersion": "datascape.io/v1alpha1",
        "kind": "Provider",
        "metadata": {"name": "gone-provider", "namespace": "default"},
        "spec": {"type": "noop", "runtime": {"type": "fake"}}
      }
    }
  }
}`)

	out, err, code := run(t, "state", "repair", "--state-file", stateFile, "--runtime", "fake", "--yes", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("state repair failed (code %d): %v\n%s", code, err, out)
	}
	var result repairOutput
	if jsonErr := json.Unmarshal([]byte(out), &result); jsonErr != nil {
		t.Fatalf("decode repair output: %v\n%s", jsonErr, out)
	}
	actions := map[string]string{}
	for _, a := range result.Applied {
		actions[a.Action] = a.Detail
	}
	if actions["migrated-format"] != "v1 -> v2" {
		t.Errorf("repair applied = %+v, want a migrated-format v1 -> v2 action", result.Applied)
	}
	if actions["dropped-gone-object"] != "default/Provider/gone-provider" {
		t.Errorf("repair applied = %+v, want dropped-gone-object for default/Provider/gone-provider", result.Applied)
	}

	// The legacy orphan survives repair untouched — doctor should still
	// report it (no safe automatic fix), and the file version is now
	// current.
	out, _, code = run(t, "state", "doctor", "--state-file", stateFile, "--runtime", "fake", "-o", "json")
	if code != 1 {
		t.Fatalf("post-repair doctor exit = %d, want 1 (legacy orphan remains)\n%s", code, out)
	}
	var report stateDoctorReport
	if jsonErr := json.Unmarshal([]byte(out), &report); jsonErr != nil {
		t.Fatalf("decode post-repair doctor report: %v\n%s", jsonErr, out)
	}
	if report.StaleFormat {
		t.Errorf("post-repair StaleFormat = true, want false (repair should have persisted v2)")
	}
	if len(report.LegacyOrphans) != 1 || report.LegacyOrphans[0] != "default/Provider/legacy-orphan" {
		t.Errorf("post-repair LegacyOrphans = %v, want the legacy orphan to survive untouched", report.LegacyOrphans)
	}
	if len(report.GoneObjects) != 0 {
		t.Errorf("post-repair GoneObjects = %v, want empty (already dropped)", report.GoneObjects)
	}

	raw, ioErr := os.ReadFile(stateFile)
	if ioErr != nil {
		t.Fatal(ioErr)
	}
	if !strings.Contains(string(raw), `"version": 2`) {
		t.Errorf("state file was not persisted at the migrated version:\n%s", raw)
	}
	if strings.Contains(string(raw), "gone-provider") {
		t.Errorf("dropped entry still present in the persisted state file:\n%s", raw)
	}
}

// TestStateRepairNoopOnHealthyState guards the "no write on healthy state"
// contract: a state file with no defects (current version, every entry has
// a matching last-applied manifest, no Provider entries to check liveness
// against) must come back untouched byte-for-byte.
func TestStateRepairNoopOnHealthyState(t *testing.T) {
	healthy := `{
  "version": 2,
  "resources": {
    "default/EventStream/healthy": {
      "specHash": "abc",
      "lifecycle": "Managed",
      "lastApplied": {
        "apiVersion": "datascape.io/v1alpha1",
        "kind": "EventStream",
        "metadata": {"name": "healthy", "namespace": "default"},
        "spec": {}
      }
    }
  }
}`
	stateFile := writeStateFixture(t, healthy)
	before, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}

	out, err, code := run(t, "state", "repair", "--state-file", stateFile, "--runtime", "fake", "--yes", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("state repair failed (code %d): %v\n%s", code, err, out)
	}
	var result repairOutput
	if jsonErr := json.Unmarshal([]byte(out), &result); jsonErr != nil {
		t.Fatalf("decode repair output: %v\n%s", jsonErr, out)
	}
	if len(result.Applied) != 0 {
		t.Errorf("repair on healthy state applied = %+v, want none", result.Applied)
	}

	after, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("repair wrote to a healthy state file\nbefore:\n%s\nafter:\n%s", before, after)
	}

	// Doctor agrees it's healthy.
	_, err, code = run(t, "state", "doctor", "--state-file", stateFile, "--runtime", "fake", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("state doctor on healthy state (code %d): %v", code, err)
	}
}

// TestStateInspectStructuredOutput is the -o json contract check for the
// third state subcommand (doctor/repair are covered above and in the
// output-contract harness).
func TestStateInspectStructuredOutput(t *testing.T) {
	stateFile := writeStateFixture(t, `{
  "version": 2,
  "resources": {
    "default/Provider/p": {
      "specHash": "abc",
      "lifecycle": "Managed",
      "imported": true
    }
  }
}`)
	out, err, code := run(t, "state", "inspect", "--state-file", stateFile, "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("state inspect failed (code %d): %v\n%s", code, err, out)
	}
	var parsed stateInspectOutput
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("decode state inspect output: %v\n%s", jsonErr, out)
	}
	if parsed.Version != 2 || len(parsed.Resources) != 1 {
		t.Fatalf("state inspect output = %+v, want version 2 and one resource", parsed)
	}
	if r := parsed.Resources[0]; r.Key != "default/Provider/p" || !r.Imported {
		t.Errorf("state inspect resource = %+v, want key default/Provider/p, imported true", r)
	}
}
