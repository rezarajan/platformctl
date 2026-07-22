package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/rezarajan/platformctl/internal/application/blueprint"
)

// TestBlueprintsLintClean is docs/planning/08 H1's blueprint CI test (ADR
// 020's consequence: "Blueprints gain a CI test: every shipped blueprint
// lints clean"). "Lint-clean" means zero *unwaived* findings — a waived
// finding with a documented reason is exactly ADR 020's "do as they please,
// but informed" mechanism, not a failure; every waiver a shipped blueprint
// carries must itself be reviewed (grep the templates/ tree for
// lint.datascape.io/waive to see the reasons).
func TestBlueprintsLintClean(t *testing.T) {
	for _, name := range blueprint.Names() {
		name := name
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, name)
			if out, err, code := run(t, "init", name, "--dir", target); err != nil || code != 0 {
				t.Fatalf("init %s failed (code %d): %v\n%s", name, code, err, out)
			}

			out, _, err := runSplit(t, "lint", target, "-o", "json")
			if err != nil {
				t.Fatalf("lint %s -o json: %v\n%s", name, err, out)
			}
			var parsed struct {
				Findings []struct {
					Code         string `json:"code"`
					Severity     string `json:"severity"`
					Resource     string `json:"resource"`
					Message      string `json:"message"`
					Waived       bool   `json:"waived"`
					WaiverReason string `json:"waiverReason"`
				} `json:"findings"`
			}
			if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
				t.Fatalf("lint %s -o json: %v\n%s", name, jsonErr, out)
			}
			for _, f := range parsed.Findings {
				if !f.Waived {
					t.Errorf("blueprint %q: unwaived %s finding %s on %s: %s", name, f.Severity, f.Code, f.Resource, f.Message)
				} else if f.WaiverReason == "" {
					t.Errorf("blueprint %q: %s on %s is marked waived but carries no reason", name, f.Code, f.Resource)
				}
			}
		})
	}
}
