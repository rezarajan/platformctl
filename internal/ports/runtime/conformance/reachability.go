package conformance

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// runReachabilityAudience registers
// PortBinding_audience_internal_never_host_bound, proving the core F2
// invariant across every adapter: a port declared Audience: internal never
// gets a host-reachable address, no matter how permissive the adapter
// otherwise is (docs/planning/08 F2, docs/planning/09 K10). Audience: host
// is accepted alongside it in the same spec so the test also proves
// EnsureContainer tolerates both audiences declared at once — the shape
// every multi-listener provider (e.g. redpanda) actually sends.
func runReachabilityAudience(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("PortBinding_audience_internal_never_host_bound", func(t *testing.T) {
		ctx := fx.ctx
		name := fx.namePrefix + "-audience-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{fx.netSpec.Name},
			Ports: []runtime.PortBinding{
				{HostPort: 28998, ContainerPort: 81, Audience: runtime.AudienceHost},
				{ContainerPort: 82, Audience: runtime.AudienceInternal},
			},
			Labels: fx.labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("second EnsureContainer with identical audience spec: %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Errorf("second EnsureContainer with identical audience spec mutated state (NFR-2 violation)")
		}

		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect: found=%v err=%v", found, err)
		}
		for _, p := range st.Ports {
			if p.ContainerPort == 82 && p.HostPort != 0 {
				t.Errorf("Audience: internal port 82 reported a host binding: %+v", p)
			}
		}
	})
}

// runReachabilityDial registers
// EnsureReachable_dialable_immediately_after_WaitHealthy, proving the F3
// contract: once WaitHealthy returns, the very first EnsureReachable call —
// no caller-side retry loop — must hand back an address that accepts a real
// connection right now (docs/planning/08 F3, docs/planning/09 Class 2 / K3 /
// K11). commandRunner-gated the same way volume.go's
// Volume_persists_across_container_update is: only an adapter whose
// containers actually run a process can be dialed for real; the fake proves
// the plumbing (EnsureReachable succeeds, returns a non-empty address)
// without claiming to prove networking it doesn't have.
func runReachabilityDial(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("EnsureReachable_dialable_immediately_after_WaitHealthy", func(t *testing.T) {
		ctx := fx.ctx
		name := fx.namePrefix + "-reachable-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })

		type commandRunner interface {
			RunsContainerCommands() bool
		}
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()

		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "nginx:1.27-alpine", // listens on 80 the instant its process starts — no artificial healthy-before-listening gap
			Networks: []string{fx.netSpec.Name},
			Ports:    []runtime.PortBinding{{ContainerPort: 80, Audience: runtime.AudienceHost}},
			Labels:   fx.labels,
		}
		if !execCapable {
			// The fake never runs a real process; nginx would never
			// actually listen, and the fake doesn't simulate Docker's
			// ephemeral host-port assignment for HostPort: 0. Exercise the
			// same spec shape with an explicit host port so the fake still
			// proves EnsureContainer/EnsureReachable's contract plumbing,
			// just not real dialability.
			spec.Image = "alpine:3.20"
			spec.Cmd = []string{"sleep", "300"}
			spec.Ports = []runtime.PortBinding{{HostPort: 28997, ContainerPort: 80, Audience: runtime.AudienceHost}}
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}

		addr, closeFn, err := rt.EnsureReachable(ctx, name, 80)
		if err != nil {
			t.Fatalf("EnsureReachable immediately after WaitHealthy: %v", err)
		}
		defer func() { _ = closeFn() }()
		if addr == "" {
			t.Fatal("EnsureReachable returned an empty address")
		}
		if !execCapable {
			return
		}
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %q immediately after EnsureReachable: %v (address was not actually dialable)", addr, err)
		}
		_ = conn.Close()
	})
}

// runReachabilityDelayedListen registers
// DelayedListenReadiness_HealthyBeforeListening, backfilling the D1/K11
// class (docs/planning/08 F3, docs/planning/09 Class 2): "healthy" (no
// declared HealthCheck means healthy-when-running, the same contract
// postgres's own pg_isready-over-the-unix-socket gap exercised live) can
// report true before the container's declared port actually accepts
// connections — a container that sleeps briefly before opening its
// listener reproduces the shape generically, documenting whatever gap this
// adapter has (t.Skip when the adapter happens to have none) and proving
// runtime.WithReachable (F1/F3) absorbs it regardless: a caller that dials
// once right after WaitHealthy can lose the race, but one using
// WithReachable does not.
func runReachabilityDelayedListen(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("DelayedListenReadiness_HealthyBeforeListening", func(t *testing.T) {
		ctx := fx.ctx
		type commandRunner interface {
			RunsContainerCommands() bool
		}
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()
		if !execCapable {
			// The fake reports healthy without ever running a process, so
			// it has no healthy-vs-listening gap to document.
			return
		}

		name := fx.namePrefix + "-delayed-listen-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		const listenDelay = 5 * time.Second
		spec := runtime.ContainerSpec{
			Name:  name,
			Image: "alpine:3.20",
			// No HealthCheck declared: healthy means running, the instant
			// the process starts — well before the delayed listener opens.
			Cmd:      []string{"sh", "-c", fmt.Sprintf("sleep %d && nc -l -p 8080", int(listenDelay.Seconds()))},
			Networks: []string{fx.netSpec.Name},
			Ports:    []runtime.PortBinding{{HostPort: 28995, ContainerPort: 8080, Audience: runtime.AudienceHost}},
			Labels:   fx.labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		start := time.Now()
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}
		if elapsed := time.Since(start); elapsed >= listenDelay {
			t.Skipf("WaitHealthy itself took %s (>= the %s listen delay); this adapter/environment left no healthy-before-listening gap to document here", elapsed, listenDelay)
		}

		err := runtime.WithReachable(ctx, rt, name, 8080, runtime.ReachableOptions{Timeout: listenDelay + 15*time.Second, Interval: 500 * time.Millisecond}, func(ctx context.Context, addr string) error {
			conn, derr := net.DialTimeout("tcp", addr, 2*time.Second)
			if derr != nil {
				return derr
			}
			return conn.Close()
		})
		if err != nil {
			t.Fatalf("WithReachable never dialed the delayed listener despite the container being healthy well before it opened: %v", err)
		}
	})
}

// runReachabilityProbe registers
// ProbeReachable_InNetwork_reachable_and_undeclared_errors, proving the C10
// contract (docs/planning/08 C10, ADR 015): a target actually reachable
// from an in-network vantage point reports nil, and an undeclared/
// unreachable target errors — on every adapter, without ever falling back to
// a host-side dial (which this container deliberately has no host-reachable
// binding for, Audience: internal, so a host-side fallback would either dial
// nothing or answer the wrong audience's question).
func runReachabilityProbe(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("ProbeReachable_InNetwork_reachable_and_undeclared_errors", func(t *testing.T) {
		ctx := fx.ctx
		probeNet := fx.namePrefix + "-probe-net"
		name := fx.namePrefix + "-probe-ctr"
		t.Cleanup(func() {
			_ = rt.Remove(ctx, name)
			_ = rt.RemoveNetwork(ctx, probeNet)
		})
		if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: probeNet, Labels: fx.labels}); err != nil {
			t.Fatalf("EnsureNetwork: %v", err)
		}

		type commandRunner interface{ RunsContainerCommands() bool }
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()

		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{probeNet},
			// Audience: internal — no host binding exists for this port at
			// all, so a ProbeReachable that quietly fell back to a host-side
			// dial would find nothing to dial and either error for the wrong
			// reason or (worse, against a more permissive future adapter)
			// report the host audience's answer instead of the network's.
			Ports:  []runtime.PortBinding{{ContainerPort: 8080, Audience: runtime.AudienceInternal}},
			Labels: fx.labels,
		}
		if execCapable {
			// A real listener a real dial can succeed against — busybox nc,
			// re-listening forever so both the reachable and (if it ever
			// raced) a repeat probe still find it up.
			spec.Cmd = []string{"sh", "-c", "while true; do nc -l -p 8080; done"}
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}

		target := fmt.Sprintf("%s:%d", name, 8080)
		if err := rt.ProbeReachable(ctx, probeNet, target); err != nil {
			t.Fatalf("ProbeReachable(%q, %q) = %v, want reachable", probeNet, target, err)
		}

		undeclared := fmt.Sprintf("%s:%d", name, 8081)
		if err := rt.ProbeReachable(ctx, probeNet, undeclared); err == nil {
			t.Fatalf("ProbeReachable(%q, %q) succeeded against an undeclared port; want error", probeNet, undeclared)
		}

		unknownHost := fmt.Sprintf("%s-does-not-exist:8080", fx.namePrefix)
		if err := rt.ProbeReachable(ctx, probeNet, unknownHost); err == nil {
			t.Fatalf("ProbeReachable(%q, %q) succeeded against an unknown host; want error", probeNet, unknownHost)
		}
	})
}
