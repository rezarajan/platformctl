package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

// stubReachableRuntime implements only the EnsureReachable behavior
// WithReachable exercises; every other ContainerRuntime method is an inert
// stub so the type satisfies the interface without pulling in an adapter.
type stubReachableRuntime struct {
	ContainerRuntime
	resolveCalls int
	closeCalls   int
	resolveErr   error
}

func (s *stubReachableRuntime) EnsureReachable(ctx context.Context, name string, port int) (string, func() error, error) {
	s.resolveCalls++
	if s.resolveErr != nil {
		return "", nil, s.resolveErr
	}
	return "resolved:addr", func() error { s.closeCalls++; return nil }, nil
}

func TestWithReachableSucceedsFirstTry(t *testing.T) {
	t.Parallel()
	rt := &stubReachableRuntime{}
	calls := 0
	err := WithReachable(context.Background(), rt, "svc", 1234, ReachableOptions{}, func(ctx context.Context, addr string) error {
		calls++
		if addr != "resolved:addr" {
			t.Fatalf("unexpected addr %q", addr)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 || rt.resolveCalls != 1 || rt.closeCalls != 1 {
		t.Fatalf("calls=%d resolveCalls=%d closeCalls=%d, want 1/1/1", calls, rt.resolveCalls, rt.closeCalls)
	}
}

// TestWithReachableReResolvesPerAttempt is the contract test for the K11
// class (docs/planning/09): a fn failure must trigger a fresh
// EnsureReachable call on the next attempt, not a retry against the same
// tunnel/address.
func TestWithReachableReResolvesPerAttempt(t *testing.T) {
	t.Parallel()
	rt := &stubReachableRuntime{}
	attempt := 0
	opts := ReachableOptions{Timeout: time.Second, Interval: time.Millisecond}
	err := WithReachable(context.Background(), rt, "svc", 1234, opts, func(ctx context.Context, addr string) error {
		attempt++
		if attempt < 3 {
			return errors.New("not ready yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempt != 3 {
		t.Fatalf("fn called %d times, want 3", attempt)
	}
	if rt.resolveCalls != 3 {
		t.Fatalf("EnsureReachable called %d times, want 3 (one fresh resolve per attempt)", rt.resolveCalls)
	}
	if rt.closeCalls != 3 {
		t.Fatalf("close called %d times, want 3 (every resolved tunnel closed, including failed attempts)", rt.closeCalls)
	}
}

func TestWithReachableTimesOut(t *testing.T) {
	t.Parallel()
	rt := &stubReachableRuntime{}
	opts := ReachableOptions{Timeout: 20 * time.Millisecond, Interval: 5 * time.Millisecond}
	err := WithReachable(context.Background(), rt, "svc", 1234, opts, func(ctx context.Context, addr string) error {
		return errors.New("permanently unready")
	})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestWithReachableSurfacesResolveErrors(t *testing.T) {
	t.Parallel()
	rt := &stubReachableRuntime{resolveErr: errors.New("container not found")}
	opts := ReachableOptions{Timeout: 10 * time.Millisecond, Interval: 5 * time.Millisecond}
	err := WithReachable(context.Background(), rt, "svc", 1234, opts, func(ctx context.Context, addr string) error {
		t.Fatal("fn must not be called when EnsureReachable itself fails")
		return nil
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestWithReachableRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	rt := &stubReachableRuntime{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opts := ReachableOptions{Timeout: time.Second, Interval: time.Second}
	err := WithReachable(ctx, rt, "svc", 1234, opts, func(ctx context.Context, addr string) error {
		return errors.New("not ready")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
