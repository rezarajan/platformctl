package proxy

import (
	"net"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/reconciler/conformance"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// listenAndHoldOpen is the "fake technology" this exemplar provides
// (docs/planning/08 E6, ADR 028): proxy's own Connection settledness check
// (waitForwarderServing) dials THROUGH the fake container's reported host
// address to prove an upstream answers — the fake runtime never runs a real
// socat process, so nothing would ever be listening unless the test itself
// opens the socket. A real net.Listener on 127.0.0.1, bound to the exact
// port the Connection fixture also declares as its own listen port, is the
// standing-in-for-a-live-upstream trick TestReconcileConnectionSucceedsWhen
// UpstreamAnswers (proxy_test.go) already established; this reuses it
// verbatim as the conformance suite's fake-technology harness. Returns the
// bound port; the listener and its held-open session are torn down via
// t.Cleanup.
func listenAndHoldOpen(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	holdOpen := make(chan struct{})
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer c.Close()
		<-holdOpen // keep the accepted session open for the subtest's lifetime
	}()
	t.Cleanup(func() {
		close(holdOpen)
		_ = ln.Close()
	})
	return port
}

// TestConformance is the settledness/dial-through exemplar (docs/planning/08
// E6, ADR 028): drives the Connection kind of Reconcile/Probe/Destroy — the
// richer of the three exemplars, since reconcileConnection's Ready
// determination genuinely dials through the forwarder to a fake-technology
// upstream (waitForwarderServing/probeThroughForwarder) rather than trusting
// container health alone, making NFR-11 settledness a non-trivial proof
// here. See internal/adapters/providers/noop (trivial) and
// internal/adapters/providers/redpanda (container-lifecycle) for the other
// two points on the provider-complexity spectrum this suite is proven
// against.
func TestConformance(t *testing.T) {
	// Shrink the real dial-through's read-deadline wait once, for this
	// test's entire subtree — never per-subtest, since conformance.Run's
	// subtests run t.Parallel() and concurrently mutating a shared package
	// var would race. A single write here, before any subtest starts, and
	// t.Cleanup's single restore after every subtest has finished (Go runs
	// a parent test's Cleanup only once its parallel children complete) is
	// race-free: every subtest only ever reads the already-shrunk value.
	prevDeadline := probeReadDeadline
	probeReadDeadline = 20 * time.Millisecond
	t.Cleanup(func() { probeReadDeadline = prevDeadline })

	conformance.Run(t, conformance.Harness{
		NewRuntime: func() runtime.ContainerRuntime { return fakeruntime.New() },
		Provider:   func() reconciler.Provider { return New() },
		Resource: func(rt runtime.ContainerRuntime, namePrefix string, i int) reconciler.Request {
			name := namePrefix + "-a"
			if i == 1 {
				name = namePrefix + "-b"
			}
			port := listenAndHoldOpen(t)
			provEnv := providerEnvelope(name + "-provider")
			// target is never actually dialed by this fixture — a via-less
			// Connection's dialTarget IS conn.Target, but
			// listenAndHoldOpen's real listener is reached through
			// ctr.HostAddr(conn.Port) (the forwarder's OWN listen port,
			// matched to the real listener's port above), exactly the trick
			// proxy_test.go's TestReconcileConnectionSucceedsWhenUpstreamAnswers
			// established.
			connEnv := connectionEnvelope(name, provEnv.Metadata.Name, port, "127.0.0.1:1")
			return reconciler.Request{
				Resource: connEnv,
				Provider: provEnv,
				Runtime:  rt,
				Facts:    reconciler.StaticFacts{},
			}
		},
		// CapabilityChecks: nil — proxy's declared capabilities
		// (ConnectionCapableProvider.SupportedConnectionSchemes,
		// ViaConsumingProvider.ConsumesVia) return values, not errors; it
		// declares no SpecValidator/StreamReplicationValidator-shaped
		// interface for this suite to exercise.
	})
}
