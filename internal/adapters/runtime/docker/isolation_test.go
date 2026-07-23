package docker

import (
	"context"
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestObserveIsolationEnforcement is docs/planning/08 H8's "Docker path
// unit-covered" accept leg: Docker's isolation claim is enforced by
// construction (network membership IS the mechanism), so this needs no
// Docker daemon at all — a zero-value *Runtime (no client) proves the
// method never touches r.cli.
func TestObserveIsolationEnforcement(t *testing.T) {
	r := &Runtime{}
	status, err := r.ObserveIsolationEnforcement(context.Background())
	if err != nil {
		t.Fatalf("ObserveIsolationEnforcement: %v", err)
	}
	if status.State != runtime.IsolationEnforced {
		t.Errorf("State = %q, want %q", status.State, runtime.IsolationEnforced)
	}
	if status.Reason == "" {
		t.Error("Reason is empty — Docker's enforced-by-construction claim should explain itself")
	}
}
