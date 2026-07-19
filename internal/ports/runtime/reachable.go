package runtime

import (
	"context"
	"fmt"
	"time"
)

// Default bounds for WithReachable when ReachableOptions leaves a field
// zero.
const (
	DefaultReachableTimeout  = 30 * time.Second
	DefaultReachableInterval = time.Second
)

// ReachableOptions configures WithReachable's retry behavior. The zero
// value uses the package defaults.
type ReachableOptions struct {
	// Timeout bounds the whole wait. 0 = DefaultReachableTimeout.
	Timeout time.Duration
	// Interval is the pause between attempts. 0 = DefaultReachableInterval.
	Interval time.Duration
}

// WithReachable resolves a fresh EnsureReachable address for name:port,
// invokes fn with it, and closes the tunnel — repeating on failure until
// opts.Timeout elapses. It re-resolves the address on every attempt rather
// than reusing one across the whole wait: a port-forward tunnel opened
// while the target is still starting can end up silently dead for the rest
// of its life even once the target comes up (found live against a real
// cluster — docs/planning/08 B8, docs/planning/09 Class 2), while a fresh
// tunnel opened moments later against the same, by-then-ready target works
// every time.
//
// This is the single retry/readiness primitive providers should use
// instead of a bespoke wait loop built on a once-resolved address: on
// Docker/fake, EnsureReachable is a cheap no-op per call, so retrying here
// costs nothing extra there.
func WithReachable(ctx context.Context, rt ContainerRuntime, name string, port int, opts ReachableOptions, fn func(ctx context.Context, addr string) error) error {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultReachableTimeout
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = DefaultReachableInterval
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		lastErr = attemptReachable(ctx, rt, name, port, fn)
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s:%d not reachable within %s: %w", name, port, timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func attemptReachable(ctx context.Context, rt ContainerRuntime, name string, port int, fn func(ctx context.Context, addr string) error) error {
	addr, closeAddr, err := rt.EnsureReachable(ctx, name, port)
	if err != nil {
		return err
	}
	defer closeAddr()
	return fn(ctx, addr)
}
