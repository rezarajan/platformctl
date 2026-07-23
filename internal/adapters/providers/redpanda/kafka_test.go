package redpanda

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestRetryTransientProbe pins the probe-side transient discipline
// (doc 11 / CI heal-window race): errors are retried within the window,
// verdicts — clean or drifted — return immediately, and a persistent
// error surfaces honestly after the window.
func TestRetryTransientProbe(t *testing.T) {
	t.Parallel()
	oldW, oldI := topicProbeRetryWindow, topicProbeRetryInterval
	topicProbeRetryWindow, topicProbeRetryInterval = 200*time.Millisecond, 10*time.Millisecond
	defer func() { topicProbeRetryWindow, topicProbeRetryInterval = oldW, oldI }()

	// Transient error then clean: returns clean, no error.
	calls := 0
	drifted, _, err := retryTransientProbe(context.Background(), func() (bool, string, error) {
		calls++
		if calls < 3 {
			return false, "", fmt.Errorf("broker closed the connection during negotiation")
		}
		return false, "", nil
	})
	if err != nil || drifted {
		t.Fatalf("transient-then-clean: drifted=%v err=%v (calls=%d)", drifted, err, calls)
	}

	// A drift verdict is determined — returned immediately, never retried.
	calls = 0
	drifted, reason, err := retryTransientProbe(context.Background(), func() (bool, string, error) {
		calls++
		return true, "PartitionCountMismatch(2!=3)", nil
	})
	if err != nil || !drifted || calls != 1 {
		t.Fatalf("drift verdict: drifted=%v reason=%q calls=%d err=%v", drifted, reason, calls, err)
	}

	// Persistent error surfaces after the window with the last error.
	drifted, _, err = retryTransientProbe(context.Background(), func() (bool, string, error) {
		return false, "", fmt.Errorf("cluster unreachable")
	})
	if err == nil || drifted {
		t.Fatalf("persistent error: drifted=%v err=%v", drifted, err)
	}

	// Context cancellation is honored mid-window.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = retryTransientProbe(ctx, func() (bool, string, error) {
		return false, "", fmt.Errorf("still failing")
	})
	if err == nil {
		t.Fatal("cancelled ctx: want error")
	}
}
