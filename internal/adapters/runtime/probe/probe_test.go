package probe

import (
	"context"
	"net"
	"testing"
	"time"
)

// listenerAddr starts a TCP listener on an OS-assigned local port and
// returns its address; the caller closes it when done.
func listenerAddr(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return ln.Addr().String(), func() { _ = ln.Close() }
}

// TestDialableSucceedsAgainstOpenPort covers the base case: a listener is
// up, Dialable reports true.
func TestDialableSucceedsAgainstOpenPort(t *testing.T) {
	addr, closeLn := listenerAddr(t)
	defer closeLn()

	if !Dialable(context.Background(), addr) {
		t.Fatal("want Dialable=true against an open listener")
	}
}

// TestDialableFailsAgainstClosedPort covers the base case: nothing is
// listening, Dialable reports false quickly rather than hanging for the
// full 2s default.
func TestDialableFailsAgainstClosedPort(t *testing.T) {
	addr, closeLn := listenerAddr(t)
	closeLn() // release the port so nothing answers there anymore

	if Dialable(context.Background(), addr) {
		t.Fatal("want Dialable=false against a closed port")
	}
}

// TestDialableCapsToRemainingDeadline is docs/planning/08 I5's ctx-aware
// semantics — the behavior debezium's Docker leg always had and Kubernetes'
// dialable now shares: a ctx deadline shorter than the 2s default caps the
// dial timeout instead of the full 2s always applying.
func TestDialableCapsToRemainingDeadline(t *testing.T) {
	addr, closeLn := listenerAddr(t)
	defer closeLn()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	ok := Dialable(ctx, addr)
	elapsed := time.Since(start)

	if !ok {
		t.Fatal("want Dialable=true against an open listener even with a short deadline")
	}
	// A successful local dial should complete near-instantly; this just
	// guards against the deadline cap being silently ignored (which would
	// let a slow/hanging dial run past it).
	if elapsed > 2*time.Second {
		t.Errorf("Dialable took %s, want well under the uncapped 2s default", elapsed)
	}
}

// TestDialableRefusesExpiredDeadline covers the "timeout <= 0" branch: a
// ctx whose deadline has already passed refuses to dial at all rather than
// attempting one with a non-positive timeout.
func TestDialableRefusesExpiredDeadline(t *testing.T) {
	addr, closeLn := listenerAddr(t)
	defer closeLn()

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	// Ensure the deadline is unambiguously in the past.
	time.Sleep(1 * time.Millisecond)

	if Dialable(ctx, addr) {
		t.Fatal("want Dialable=false when ctx's deadline has already passed")
	}
}

// TestExecArgsAndCommand covers the shared argv builders both adapters
// exec/schedule — a caller-supplied host/port must reach the target
// literally (not be reinterpreted as shell syntax), which is why
// ExecArgs passes them as positional args to `sh -c`, not interpolated
// into the script string itself.
func TestExecArgsAndCommand(t *testing.T) {
	got := ExecArgs("db.example.com", "5432")
	want := []string{"sh", "-c", TCPDialScript, "sh", "db.example.com", "5432"}
	if len(got) != len(want) {
		t.Fatalf("ExecArgs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ExecArgs[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	gotCmd := Command("db.example.com", "5432")
	wantCmd := []string{"nc", "-z", "-w3", "db.example.com", "5432"}
	if len(gotCmd) != len(wantCmd) {
		t.Fatalf("Command = %v, want %v", gotCmd, wantCmd)
	}
	for i := range wantCmd {
		if gotCmd[i] != wantCmd[i] {
			t.Fatalf("Command[%d] = %q, want %q", i, gotCmd[i], wantCmd[i])
		}
	}
}
