// Package noop is a trivial provider used only for testing the engine —
// Phase 0's stand-in before any real technology provider exists.
package noop

import (
	"context"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type Provider struct {
	// ReconcileCount lets engine tests assert idempotency behavior.
	ReconcileCount int
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "noop" }

func (p *Provider) Reconcile(_ context.Context, _ resource.Envelope, _ runtime.ContainerRuntime) (status.Status, error) {
	p.ReconcileCount++
	st := status.Status{}
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "NoopReconciled"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	return st, nil
}

func (p *Provider) Destroy(_ context.Context, _ resource.Envelope, _ runtime.ContainerRuntime) error {
	return nil
}

func (p *Provider) Probe(_ context.Context, _ resource.Envelope, _ runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "NoopHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
	return st, nil
}
