package providerkit

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestPreflightWithRefreshResolvesFreshPerTransientAttempt is the regression
// guard for the k8s-scenarios-apps "connection refused": a mediated
// Connection's preflight must resolve a FRESH reachable tunnel on every
// transient retry (a k8s port-forward goes dead for good if its pod churns),
// not dial one static tunnel for the whole window.
func TestPreflightWithRefreshResolvesFreshPerTransientAttempt(t *testing.T) {
	t.Parallel()
	var resolves, closes int32
	resolve := func(context.Context) (string, func() error, error) {
		atomic.AddInt32(&resolves, 1)
		return "127.0.0.1:1", func() error { atomic.AddInt32(&closes, 1); return nil }, nil
	}
	var attempts int32
	verify := func(context.Context, string) error {
		if atomic.AddInt32(&attempts, 1) < 3 {
			return errors.New("dial tcp 127.0.0.1:1: connect: connection refused") // transient
		}
		return nil // the by-then-fresh tunnel works
	}
	if err := preflightWithRefresh(context.Background(), 5*time.Second, time.Millisecond, resolve, verify); err != nil {
		t.Fatalf("want success after transient retries, got %v", err)
	}
	if got := atomic.LoadInt32(&resolves); got < 3 {
		t.Errorf("resolve called %d times, want >= 3 (a fresh tunnel per attempt)", got)
	}
	if got := atomic.LoadInt32(&closes); got < 2 {
		t.Errorf("closeAddr called %d times, want >= 2 (each stale tunnel closed)", got)
	}
}

// TestPreflightWithRefreshFailsFastOnVerdict: an auth/TLS verdict is returned
// immediately, with no retry — retrying a real refusal only delays the honest
// error and hammers a lockout counter.
func TestPreflightWithRefreshFailsFastOnVerdict(t *testing.T) {
	t.Parallel()
	var resolves int32
	resolve := func(context.Context) (string, func() error, error) {
		atomic.AddInt32(&resolves, 1)
		return "127.0.0.1:1", func() error { return nil }, nil
	}
	verify := func(context.Context, string) error {
		return errors.New("password authentication failed for user \"xd_super\"")
	}
	err := preflightWithRefresh(context.Background(), 5*time.Second, time.Millisecond, resolve, verify)
	if err == nil || !strings.Contains(err.Error(), "password authentication failed") {
		t.Fatalf("want the verdict returned immediately, got %v", err)
	}
	if got := atomic.LoadInt32(&resolves); got != 1 {
		t.Errorf("resolve called %d times, want exactly 1 (fail fast, no retry)", got)
	}
}

// TestPreflightWithRefreshTimesOutReturningLastError: an always-transient
// target is retried until the deadline, then the last transport error is
// surfaced (bounded, not infinite).
func TestPreflightWithRefreshTimesOutReturningLastError(t *testing.T) {
	t.Parallel()
	resolve := func(context.Context) (string, func() error, error) {
		return "127.0.0.1:1", func() error { return nil }, nil
	}
	verify := func(context.Context, string) error {
		return errors.New("unexpected EOF") // transient, never resolves
	}
	err := preflightWithRefresh(context.Background(), 40*time.Millisecond, time.Millisecond, resolve, verify)
	if err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("want the last transient error after timeout, got %v", err)
	}
}
