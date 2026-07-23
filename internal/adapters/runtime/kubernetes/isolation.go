// This file holds ObserveIsolationEnforcement — the Layer 2 honesty probe
// (docs/adr/027-enforcement-layering.md, docs/planning/08 H8) that
// productizes networkpolicy_integration_test.go's
// TestNetworkPolicyEnforcementIsLive mechanism into something any command
// can call on demand: pick two already-managed namespaces that carry the
// default-deny NetworkPolicy wall (EnsureNetwork's default path), schedule
// a bounded, ephemeral canary listener in one of them, and prove
// enforcement by dialing it from both namespaces via the runtime's own
// ProbeReachable — enforced only when the same-namespace dial succeeds
// (basic connectivity isn't broken) AND the cross-namespace dial fails
// (the default-deny wall actually holds).
package kubernetes

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	// isolationCanaryName is fixed, not generated per call: the probe is
	// idempotent at the object-name level like every other Ensure*-style
	// object this adapter creates, and a fixed name means a canary left
	// behind by a killed process (context cancelled mid-probe, before the
	// deferred cleanup ran) is cleanly replaced by the next call rather
	// than accumulating.
	isolationCanaryName = "datascape-isolation-canary"
	// isolationCanaryImage pins the same alpine/socat image/digest
	// networkpolicy_integration_test.go's own listener uses
	// (scripts/pinned-images.txt) — a real, tiny, already-vetted listener,
	// not a new pinned-image dependency.
	isolationCanaryImage = "alpine/socat:1.8.0.3@sha256:beb4a68d9e4fe6b0f21ea774a0fde6c31f580dde6368939ed70100c5385b015e"
	isolationCanaryPort  = 9999
)

// ObserveIsolationEnforcement implements runtime.IsolationObserver. It
// never returns a non-nil error: every failure mode degrades to
// IsolationStatus{State: IsolationUnknown, Reason: ...} instead of
// aborting the caller (docs/planning/08 H8's own contract, restated on the
// port interface) — the honesty probe itself must never be the reason an
// apply/status/drift command fails outright.
//
// The two namespaces it cross-checks are chosen from ListManagedNetworks
// (this platform's own applied Providers), filtered to those actually
// carrying the default-deny NetworkPolicy (EnsureNetwork's default path;
// a namespace opted out via IsolationNone has nothing to prove and is
// skipped) — never freshly created scratch namespaces, so the probe
// reflects this platform's real applied topology, not a synthetic one.
// Fewer than two such namespaces means there is nothing to cross-namespace
// probe yet, reported as Unknown, not guessed at.
func (r *Runtime) ObserveIsolationEnforcement(ctx context.Context) (runtimeport.IsolationStatus, error) {
	walled, err := r.walledManagedNamespaces(ctx)
	if err != nil {
		return runtimeport.IsolationStatus{State: runtimeport.IsolationUnknown, Reason: fmt.Sprintf("list managed namespaces: %v", err)}, nil
	}
	if len(walled) < 2 {
		return runtimeport.IsolationStatus{
			State: runtimeport.IsolationUnknown,
			Reason: fmt.Sprintf(
				"only %d managed namespace(s) provision the default-deny NetworkPolicy wall — a cross-namespace probe needs at least 2; apply another isolated Provider on this cluster, or this manifest set genuinely has nothing to cross-check yet",
				len(walled)),
		}, nil
	}
	nsIn, nsOut := walled[0], walled[1]

	probeCtx, cancel := context.WithTimeout(ctx, runtimeport.ScaledWait(90*time.Second))
	defer cancel()

	if err := r.ensureIsolationCanary(probeCtx, nsIn); err != nil {
		return runtimeport.IsolationStatus{State: runtimeport.IsolationUnknown, Reason: fmt.Sprintf("create isolation canary listener in %q: %v", nsIn, err)}, nil
	}
	// Cleanup is unconditional, even if probeCtx (derived from the
	// caller's ctx) is already done by the time we get here — a probe
	// must never leak infrastructure just because its caller's deadline
	// fired (docs/planning/08 H8's explicit "canaries cleaned up
	// unconditionally, context.WithoutCancel on teardown").
	defer func() {
		delCtx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer dcancel()
		_ = r.clientset.CoreV1().Pods(nsIn).Delete(delCtx, isolationCanaryName, metav1.DeleteOptions{})
	}()

	podIP, err := r.waitIsolationCanaryRunning(probeCtx, nsIn, runtimeport.ScaledWait(60*time.Second))
	if err != nil {
		return runtimeport.IsolationStatus{State: runtimeport.IsolationUnknown, Reason: fmt.Sprintf("isolation canary listener never became ready: %v", err)}, nil
	}
	target := net.JoinHostPort(podIP, strconv.Itoa(isolationCanaryPort))

	// Same-namespace dial: proves basic connectivity (and the
	// allow-same-namespace half of the pair) is not itself broken —
	// a failure here says nothing about cross-namespace enforcement.
	if err := r.ProbeReachable(probeCtx, nsIn, target); err != nil {
		return runtimeport.IsolationStatus{
			State:  runtimeport.IsolationUnknown,
			Reason: fmt.Sprintf("same-namespace dial (basic connectivity, not the boundary under test) failed in %q: %v — cannot conclude anything about cross-namespace enforcement", nsIn, err),
		}, nil
	}

	// Cross-namespace dial: the actual boundary under test.
	crossErr := r.ProbeReachable(probeCtx, nsOut, target)
	if crossErr == nil {
		return runtimeport.IsolationStatus{
			State:  runtimeport.IsolationNotEnforced,
			Reason: fmt.Sprintf("a pod in namespace %q dialed a listener in default-deny namespace %q and SUCCEEDED — this cluster's CNI does not enforce NetworkPolicy", nsOut, nsIn),
		}, nil
	}
	if probeCtx.Err() != nil {
		// The bounded window itself expired somewhere inside the
		// cross-namespace dial — that is "inconclusive," not "the CNI
		// blocked it," even though ProbeReachable also returned an error
		// in this case; checking probeCtx directly (rather than pattern-
		// matching ProbeReachable's own error variety) reliably tells the
		// two apart.
		return runtimeport.IsolationStatus{
			State:  runtimeport.IsolationUnknown,
			Reason: fmt.Sprintf("cross-namespace probe from %q did not complete within the bounded window: %v", nsOut, crossErr),
		}, nil
	}
	return runtimeport.IsolationStatus{
		State:  runtimeport.IsolationEnforced,
		Reason: fmt.Sprintf("a cross-namespace dial from %q into default-deny namespace %q was blocked, as declared", nsOut, nsIn),
	}, nil
}

// walledManagedNamespaces returns the names of every managed namespace
// (ListManagedNetworks' own selection) that currently carries the
// default-deny NetworkPolicy — i.e. was provisioned with
// IsolationDefault, not opted out via IsolationNone.
func (r *Runtime) walledManagedNamespaces(ctx context.Context) ([]string, error) {
	managed, err := r.ListManagedNetworks(ctx)
	if err != nil {
		return nil, err
	}
	var walled []string
	for _, ns := range managed {
		if _, err := r.clientset.NetworkingV1().NetworkPolicies(ns.Name).Get(ctx, denyAllIngressPolicyName, metav1.GetOptions{}); err == nil {
			walled = append(walled, ns.Name)
		} else if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get default-deny policy in %q: %w", ns.Name, err)
		}
	}
	return walled, nil
}

// ensureIsolationCanary creates (or replaces, if one was left behind by a
// killed prior probe) the canary listener Pod in ns — a bare Pod, not a
// managed EnsureContainer Deployment/Service pair, since it never needs to
// be discoverable, drift-checked, or GC'd as a platform resource: it lives
// for the duration of one probe call and is deleted unconditionally
// afterward.
func (r *Runtime) ensureIsolationCanary(ctx context.Context, ns string) error {
	_ = r.clientset.CoreV1().Pods(ns).Delete(ctx, isolationCanaryName, metav1.DeleteOptions{}) // best-effort: clear any stale leftover first
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      isolationCanaryName,
			Namespace: ns,
			Labels:    withOwnership(map[string]string{runtimeport.LabelGeneration: "isolation-canary"}),
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  "canary",
				Image: isolationCanaryImage,
				// Args, not Command: leaves the image's own ENTRYPOINT
				// (socat) in place and appends these, the same
				// Cmd-appends-not-replaces convention every other
				// ContainerSpec.Cmd site in this adapter follows.
				Args: []string{fmt.Sprintf("tcp-listen:%d,fork,reuseaddr", isolationCanaryPort), "exec:'echo ok'"},
			}},
		},
	}
	if _, err := r.clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return err
	}
	return nil
}

// waitIsolationCanaryRunning polls the canary Pod until it reports Running
// with an assigned IP, or timeout expires — the same bounded, honest-
// timeout poll style as ephemeralProbe/serviceReachableAddr (never a
// fixed-duration sleep that assumes completion, docs/planning/02 §4.1).
func (r *Runtime) waitIsolationCanaryRunning(ctx context.Context, ns string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		p, err := r.clientset.CoreV1().Pods(ns).Get(ctx, isolationCanaryName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("get canary pod: %w", err)
		}
		if p.Status.Phase == corev1.PodRunning && p.Status.PodIP != "" {
			return p.Status.PodIP, nil
		}
		if p.Status.Phase == corev1.PodFailed {
			return "", fmt.Errorf("canary pod failed: %s", p.Status.Reason)
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("canary pod did not reach Running within %s (last observed phase: %s)", timeout, p.Status.Phase)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
