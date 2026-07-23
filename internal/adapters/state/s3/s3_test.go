package s3

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRenewLoop pins the lease-renewal discipline (doc 11): renews every
// tick, stops on release, stops permanently when the lease is lost, and
// keeps trying through transient failures.
func TestRenewLoop(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	calls := 0
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		renewLoop(5*time.Millisecond, stop, func() error {
			mu.Lock()
			calls++
			mu.Unlock()
			return nil
		})
		close(done)
	}()
	time.Sleep(40 * time.Millisecond)
	close(stop)
	<-done
	mu.Lock()
	n := calls
	mu.Unlock()
	if n < 3 {
		t.Fatalf("expected several renewals, got %d", n)
	}

	// Lease lost -> loop exits on its own without stop.
	done2 := make(chan struct{})
	go func() {
		renewLoop(2*time.Millisecond, make(chan struct{}), func() error { return errLeaseLost })
		close(done2)
	}()
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("renewLoop did not exit on errLeaseLost")
	}

	// Transient failure does not stop the loop.
	var tcalls int32
	stop3 := make(chan struct{})
	done3 := make(chan struct{})
	go func() {
		renewLoop(2*time.Millisecond, stop3, func() error {
			if atomic.AddInt32(&tcalls, 1) == 1 {
				return fmt.Errorf("transient")
			}
			return nil
		})
		close(done3)
	}()
	time.Sleep(20 * time.Millisecond)
	close(stop3)
	<-done3
	if atomic.LoadInt32(&tcalls) < 2 {
		t.Fatal("loop stopped after a transient failure")
	}
}
