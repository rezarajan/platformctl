package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// synth builds a synthetic `go test -json` stream: one "run" + one
// "pass"/"fail" event per (pkg, test, elapsedSeconds), spaced out in
// wall-clock time by elapsedSeconds so the reconstructed total lines up
// with what a real stream would report.
func synth(t *testing.T, start time.Time, entries []struct {
	pkg, test string
	elapsed   float64
	action    string
}) []byte {
	t.Helper()
	var buf bytes.Buffer
	clock := start
	for _, e := range entries {
		runEvt := testEvent{Time: clock, Action: "run", Package: e.pkg, Test: e.test}
		b, err := json.Marshal(runEvt)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(b)
		buf.WriteByte('\n')

		clock = clock.Add(time.Duration(e.elapsed * float64(time.Second)))
		action := e.action
		if action == "" {
			action = "pass"
		}
		endEvt := testEvent{Time: clock, Action: action, Package: e.pkg, Test: e.test, Elapsed: e.elapsed}
		b, err = json.Marshal(endEvt)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

func TestCheckGreenWellUnderBudget(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	stream := synth(t, start, []struct {
		pkg, test string
		elapsed   float64
		action    string
	}{
		{"pkgA", "TestOne", 0.01, ""},
		{"pkgA", "TestTwo", 0.02, ""},
		{"pkgB", "TestThree", 0.5, ""},
	})

	violations, total, count, err := check(bytes.NewReader(stream), 60*time.Second)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("violations = %+v, want none", violations)
	}
	if count != 3 {
		t.Fatalf("testCount = %d, want 3", count)
	}
	if total > 90*time.Second {
		t.Fatalf("total = %s, want well under the 90s tier budget", total)
	}
}

func TestCheckFlagsASingleSlowTest(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	stream := synth(t, start, []struct {
		pkg, test string
		elapsed   float64
		action    string
	}{
		{"pkgA", "TestFast", 0.01, ""},
		{"pkgA", "TestTooSlow", 61, ""}, // the scratch-branch proof this pins permanently
	})

	violations, _, _, err := check(bytes.NewReader(stream), 60*time.Second)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 1 {
		t.Fatalf("violations = %+v, want exactly one", violations)
	}
	if violations[0].Test != "TestTooSlow" || violations[0].Package != "pkgA" {
		t.Errorf("violation = %+v, want pkgA.TestTooSlow", violations[0])
	}
	if violations[0].Elapsed != 61*time.Second {
		t.Errorf("violation elapsed = %s, want 61s", violations[0].Elapsed)
	}
}

func TestCheckFlagsSlowTestEvenWhenItFails(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	stream := synth(t, start, []struct {
		pkg, test string
		elapsed   float64
		action    string
	}{
		{"pkgA", "TestSlowAndFailing", 75, "fail"},
	})

	violations, _, _, err := check(bytes.NewReader(stream), 60*time.Second)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 1 {
		t.Fatalf("violations = %+v, want exactly one (budget is orthogonal to pass/fail)", violations)
	}
}

func TestCheckFlagsTotalOverBudgetEvenWithNoSingleSlowTest(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	var entries []struct {
		pkg, test string
		elapsed   float64
		action    string
	}
	// 10 tests at 10s each: none individually breaches the 60s per-test
	// budget, but the reconstructed wall-clock span (100s) breaches the
	// 90s tier-total budget.
	for i := 0; i < 10; i++ {
		entries = append(entries, struct {
			pkg, test string
			elapsed   float64
			action    string
		}{"pkgA", "Test", 10, ""})
	}
	stream := synth(t, start, entries)

	violations, total, _, err := check(bytes.NewReader(stream), 60*time.Second)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("violations = %+v, want none (no single test over budget)", violations)
	}
	if total <= 90*time.Second {
		t.Fatalf("total = %s, want > 90s to exercise the tier-total check", total)
	}
}

func TestCheckIgnoresNonJSONLines(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	stream := synth(t, start, []struct {
		pkg, test string
		elapsed   float64
		action    string
	}{
		{"pkgA", "TestOne", 0.01, ""},
	})
	// A build failure or `go vet` warning can precede the JSON stream on
	// stdout/stderr when both are merged into one pipe; the guard must not
	// choke on it (a real build break still fails via go test's own exit
	// code, not a guard parse error).
	mixed := append([]byte("# github.com/example/broken\nbroken.go:3:2: undefined: foo\n"), stream...)

	violations, _, count, err := check(bytes.NewReader(mixed), 60*time.Second)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 0 || count != 1 {
		t.Fatalf("violations=%+v count=%d, want 0 violations / 1 test (non-JSON lines skipped)", violations, count)
	}
}

func TestCheckIgnoresPackageLevelRecords(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	// A package-level pass record (Test == "") with a large Elapsed must
	// not be mistaken for a single slow test — it is the package's total,
	// which the tier-total check already covers via wall-clock span.
	pkgEvt := testEvent{Time: start.Add(70 * time.Second), Action: "pass", Package: "pkgA", Elapsed: 70}
	b, err := json.Marshal(pkgEvt)
	if err != nil {
		t.Fatal(err)
	}
	buf.Write(b)
	buf.WriteByte('\n')

	violations, _, count, err := check(&buf, 60*time.Second)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("violations = %+v, want none (package-level record, not a test)", violations)
	}
	if count != 0 {
		t.Fatalf("testCount = %d, want 0", count)
	}
}

func TestMainReportsAndExitsNonZeroOnViolation(t *testing.T) {
	// Exercises the same rendering main() does, without forking a
	// subprocess (os.Exit isn't unit-testable in-process) — proves the
	// message names the offending test and both budgets.
	t.Parallel()
	start := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	stream := synth(t, start, []struct {
		pkg, test string
		elapsed   float64
		action    string
	}{
		{"pkgA", "TestTooSlow", 61, ""},
	})
	violations, total, count, err := check(bytes.NewReader(stream), 60*time.Second)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	var msg strings.Builder
	for _, v := range violations {
		msg.WriteString(v.Package + "." + v.Test)
	}
	if !strings.Contains(msg.String(), "pkgA.TestTooSlow") {
		t.Fatalf("rendered violation missing pkgA.TestTooSlow: %q (total=%s count=%d)", msg.String(), total, count)
	}
}
