// Command testbudget is the CI fast-tier budget guard (ADR 028; doc 08
// §7.8 J1). It reads a `go test -json` event stream from stdin (piped
// straight from `go test -json ./...`, the fast tier's own invocation —
// no -tags integration) and fails if any single test's reported Elapsed
// exceeds -per-test, or the tier's observed wall-clock (the span between
// the stream's first and last event timestamp) exceeds -total. A slow
// test is a fast-tier defect the same way a failing test is: this guard
// makes that a CI failure instead of an aspiration.
//
// Usage (docs/planning/06 §2.1 discipline: mechanical checks over
// judgment where the check is mechanical):
//
//	go test -json ./... | go run ./internal/tools/testbudget
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)

// testEvent mirrors the subset of cmd/test2json's TestEvent fields this
// guard needs (see `go doc cmd/test2json`). Extra fields are ignored by
// encoding/json by default.
type testEvent struct {
	Time    time.Time
	Action  string
	Package string
	Test    string
	Elapsed float64 // seconds; only set on pass/fail/skip
}

// violation is one test whose Elapsed exceeded the per-test budget.
type violation struct {
	Package string
	Test    string
	Elapsed time.Duration
}

// check reads newline-delimited go test -json events from r and reports
// every test exceeding perTestBudget, plus the stream's observed
// wall-clock span (first event's Time to last event's Time — the same
// number a human watching `time go test -json ./...` would see) and how
// many individual test results were observed.
//
// A violation is reported even for a test that ultimately passed: this
// guard enforces a time budget, not correctness — `go test`'s own exit
// code already covers correctness, and a slow *failing* test would be
// caught there too, so double-reporting it here would be noise.
//
// Non-JSON lines (go test can interleave plain-text build-failure output
// ahead of the JSON stream) are skipped rather than treated as a parse
// error, so a build break surfaces via `go test`'s own non-zero exit
// instead of a confusing guard crash.
func check(r io.Reader, perTestBudget time.Duration) (violations []violation, total time.Duration, testCount int, err error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	var first, last time.Time
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e testEvent
		if jsonErr := json.Unmarshal(line, &e); jsonErr != nil {
			continue
		}
		if e.Time.IsZero() {
			continue
		}
		if first.IsZero() || e.Time.Before(first) {
			first = e.Time
		}
		if e.Time.After(last) {
			last = e.Time
		}

		if e.Test == "" {
			continue // package-level record (build/pass/fail for the whole package), not a single test
		}
		switch e.Action {
		case "pass", "fail", "skip":
			testCount++
			elapsed := time.Duration(e.Elapsed * float64(time.Second))
			if elapsed > perTestBudget {
				violations = append(violations, violation{Package: e.Package, Test: e.Test, Elapsed: elapsed})
			}
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, 0, 0, scanErr
	}
	if !first.IsZero() && !last.IsZero() {
		total = last.Sub(first)
	}
	sort.Slice(violations, func(i, j int) bool { return violations[i].Elapsed > violations[j].Elapsed })
	return violations, total, testCount, nil
}

func main() {
	perTest := flag.Duration("per-test", 60*time.Second, "fail if any single test's Elapsed exceeds this")
	totalBudget := flag.Duration("total", 90*time.Second, "fail if the tier's observed wall-clock exceeds this")
	flag.Parse()

	violations, total, testCount, err := check(os.Stdin, *perTest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testbudget: reading go test -json stream: %v\n", err)
		os.Exit(2)
	}

	fail := false
	for _, v := range violations {
		fmt.Fprintf(os.Stderr, "testbudget: FAIL %s.%s took %s, budget is %s\n", v.Package, v.Test, v.Elapsed, *perTest)
		fail = true
	}
	if total > *totalBudget {
		fmt.Fprintf(os.Stderr, "testbudget: FAIL fast tier took %s total, budget is %s\n", total, *totalBudget)
		fail = true
	}
	fmt.Fprintf(os.Stderr, "testbudget: %d tests observed, %s total (budget: %s/test, %s total)\n", testCount, total, *perTest, *totalBudget)
	if fail {
		os.Exit(1)
	}
}
