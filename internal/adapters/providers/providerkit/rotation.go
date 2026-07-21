package providerkit

import (
	"context"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// CredentialRotation implements the try-desired → try-previous-bootstrap →
// rotate-live → retry state machine shared by every credential-bearing
// single-container provider that must keep working across an operator
// changing a SecretReference's value out from under a running instance
// (postgres's ensureSuperuser, mysql's ensureRootPassword). The SQL/engine
// specifics — how to open a connection, what "ping" means, how to actually
// rotate the live credential — stay with the caller as callbacks; this owns
// only the control flow and the four timeouts, which have been identical
// across every provider that has needed this dance so far.
type CredentialRotation struct {
	Runtime runtime.ContainerRuntime
	Name    string
	Port    int
	// NoPreviousOrUnchanged is true when there is no previously observed
	// live credential (a fresh instance), or the previous credential
	// already equals desired — skips straight to the plain wait-and-verify
	// path with no fallback/rotation attempt.
	NoPreviousOrUnchanged bool
	// PingDesired/PingPrevious dial a freshly-resolved "host:port" and
	// authenticate with the respective credential set.
	PingDesired  func(ctx context.Context, addr string) error
	PingPrevious func(ctx context.Context, addr string) error
	// Rotate authenticates at addr with the previous credential and sets
	// the desired one live. Only called once PingDesired has failed and
	// PingPrevious has succeeded.
	Rotate func(ctx context.Context, addr string) error
	// Exhausted wraps the error from the previous-credential wait when
	// neither credential set authenticates — engine-specific wording naming
	// the technology and what manual recovery means for it.
	Exhausted func(err error) error
}

// Run executes the state machine.
func (c CredentialRotation) Run(ctx context.Context) error {
	if c.NoPreviousOrUnchanged {
		return WaitReachable(ctx, c.Runtime, c.Name, c.Port, 60*time.Second, c.PingDesired)
	}
	if err := WaitReachable(ctx, c.Runtime, c.Name, c.Port, 5*time.Second, c.PingDesired); err == nil {
		return nil
	}
	if err := WaitReachable(ctx, c.Runtime, c.Name, c.Port, 60*time.Second, c.PingPrevious); err != nil {
		return c.Exhausted(err)
	}
	addr, closeAddr, err := ReachableAddr(ctx, c.Runtime, c.Name, c.Port)
	if err != nil {
		return err
	}
	defer closeAddr()
	if err := c.Rotate(ctx, addr); err != nil {
		return err
	}
	return WaitReachable(ctx, c.Runtime, c.Name, c.Port, 30*time.Second, c.PingDesired)
}

// WaitReachable re-resolves a fresh EnsureReachable address on every attempt
// (docs/planning/09 Class 2 / F1) and calls probe against it until it
// succeeds or timeout elapses — the shape every provider's own
// waitReadyReachable already had, generalized so CredentialRotation and a
// provider's own SQL-layer retry loop share one implementation of the
// re-resolve-on-every-attempt rule.
func WaitReachable(ctx context.Context, rt runtime.ContainerRuntime, name string, port int, timeout time.Duration, probe func(ctx context.Context, addr string) error) error {
	return runtime.WithReachable(ctx, rt, name, port, runtime.ReachableOptions{Timeout: timeout}, probe)
}
