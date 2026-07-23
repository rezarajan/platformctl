package main

import (
	"strings"
	"testing"
)

// assertAllStatusReady replaces 25 hand-rolled copies of a line-dumb
// status parser (2026-07-23 gate: H8's isolation note — legitimate,
// documented status/stderr output — was misread as a resource row by
// every copy). It checks ONLY resource-table rows: the header is
// skipped, and any diagnostics line (isolation notes, warnings, slog
// spillover in combined captures) is ignored by shape, not by luck.
func assertAllStatusReady(t *testing.T, out, context string) {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		t.Errorf("status output has no resource rows (%s):\n%s", context, out)
		return
	}
	for _, line := range lines[1:] {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" ||
			strings.HasPrefix(trimmed, "network isolation") ||
			strings.HasPrefix(trimmed, "WARNING:") {
			continue
		}
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready (%s): %s", context, line)
		}
	}
}
