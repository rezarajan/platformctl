// Package noop is a trivial provider used only for testing the engine —
// Phase 0's stand-in before any real technology provider exists.
package noop

import (
	"context"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

type Provider struct {
	// ReconcileCount lets engine tests assert idempotency behavior.
	ReconcileCount int
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "noop" }

func (p *Provider) Reconcile(_ context.Context, _ reconciler.Request) (status.Status, error) {
	p.ReconcileCount++
	st := status.Status{}
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonNoopReconciled}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	return st, nil
}

func (p *Provider) Destroy(_ context.Context, _ reconciler.Request) error {
	return nil
}

func (p *Provider) Probe(_ context.Context, _ reconciler.Request) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonNoopHealthy}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}
