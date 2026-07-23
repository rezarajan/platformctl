package docker

import (
	"context"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// ObserveIsolationEnforcement implements runtime.IsolationObserver
// (docs/adr/027-enforcement-layering.md, docs/planning/08 H8). Docker has
// no separate network-policy enforcement layer that could be silently
// absent the way a Kubernetes CNI's NetworkPolicy support can be: a
// container only ever reaches what EnsureContainer/EnsureNetwork actually
// attached it to (topology-as-ACL — ADR 027's own framing), so isolation
// here is enforced by construction. There is nothing to probe, and
// probing would only add a canary round-trip to prove something the
// daemon guarantees unconditionally — this always returns Enforced
// without touching the daemon at all.
func (r *Runtime) ObserveIsolationEnforcement(_ context.Context) (runtime.IsolationStatus, error) {
	return runtime.IsolationStatus{
		State:  runtime.IsolationEnforced,
		Reason: "Docker network membership is the isolation mechanism itself (topology-as-ACL) — a container only reaches the networks it is attached to, with no separate enforcement layer to verify", // archtest:allow-reason-literal: IsolationStatus.Reason is free-text detail, not a status.Condition reason token (docs/planning/08 H8)
	}, nil
}
