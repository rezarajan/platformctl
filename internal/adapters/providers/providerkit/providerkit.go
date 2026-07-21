// Package providerkit holds adapter-layer scaffolding shared by the
// technology providers under internal/adapters/providers — the copy-pasted
// shape docs/planning/08 G1 identified across postgres, mysql, redpanda,
// nessie, s3, s3sink, and debezium: resolving a configurable host port,
// resolving the shared network name, dialing an instance from this process,
// the single-container reconcile-instance skeleton, and the credential
// try-desired/try-previous/rotate state machine.
//
// This is an intra-adapter helper package in the same sense as
// internal/adapters/kafkaconnect (docs/adr/008 consequences): providers may
// import it; internal/domain and internal/ports must not (and never need
// to — nothing here is part of any port). Only what is byte-identical (or
// identical modulo a caller-supplied config key/port/callback) across at
// least two providers lives here; anything a provider computes differently
// stays local to that provider (docs/planning/08 G1: "a helper with more
// parameters than lines saved is worse than the duplication").
package providerkit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Network resolves spec.runtime.network (RuntimeConfig["network"]), falling
// back to the shared default network every provider joins unless overridden.
func Network(cfg provider.Provider) string {
	if n, ok := cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

// HostPort resolves a configurable host-port override at
// cfg.Configuration[key] (e.g. "port", "kafkaPort", "apiPort",
// "connectPort") against hostport.Resolve's deterministic
// configured-or-derived fallback. JSON-decoded configuration carries numbers
// as float64; a hand-built provider.Provider (tests, in-process callers) may
// carry a plain int — both are honored.
func HostPort(cfg provider.Provider, name, key string) int {
	configured := 0
	if v, ok := cfg.Configuration[key]; ok {
		switch n := v.(type) {
		case int:
			configured = n
		case float64:
			configured = int(n)
		}
	}
	return hostport.Resolve(configured, name)
}

// ReachableAddr returns a "host:port" this process can dial right now to
// reach the container's port, plus a close func that must always be called
// (docs/planning/08 B8: Docker's is a cheap no-op; Kubernetes may tear down
// a port-forward tunnel opened just for this call). Unlike a broker-style
// protocol with its own redirect (redpanda's Kafka admin connection), every
// caller of this helper can use the returned address directly for the whole
// call, no placeholder/dialer-interception trick needed.
func ReachableAddr(ctx context.Context, rt runtime.ContainerRuntime, name string, port int) (string, func() error, error) {
	return rt.EnsureReachable(ctx, name, port)
}

// ReachableURL is ReachableAddr for an HTTP-only dependent (a REST API with
// no wire protocol of its own) — it dials the same way and prefixes the
// resolved address with "http://".
func ReachableURL(ctx context.Context, rt runtime.ContainerRuntime, name string, port int) (string, func() error, error) {
	addr, closeAddr, err := rt.EnsureReachable(ctx, name, port)
	if err != nil {
		return "", nil, err
	}
	return "http://" + addr, closeAddr, nil
}

// ReachableURLs is ReachableURL's counterpart for a spec that may have opted
// into ADR 004's `Replicas > 1, StableIdentity: false` replica-set shape —
// interchangeable pure-compute members with no per-ordinal storage or
// identity beyond the ordinal name itself (docs/planning/08 C3, the first
// real consumer: debezium/s3sink's spec.configuration.workers). members <= 1
// (the field undeclared, or declared as 1) keeps today's exact single-address
// ReachableURL path, wrapped in a one-element slice, byte-for-byte; members >
// 1 iterates ordinals 0..N-1 via EnsureReachable and returns every one that
// currently resolves, skipping (not failing on) any that don't — a dead
// member just isn't offered as a failover candidate to the caller, the same
// "proceed against the survivors" rule redpanda's clusterDial uses for its
// own multi-broker dial map (docs/adr/017 §a.4). Erroring only when zero
// members are reachable at all. The returned close func must always be
// called and closes every opened tunnel.
func ReachableURLs(ctx context.Context, rt runtime.ContainerRuntime, name string, port, members int) ([]string, func() error, error) {
	if members <= 1 {
		url, closeURL, err := ReachableURL(ctx, rt, name, port)
		if err != nil {
			return nil, nil, err
		}
		return []string{url}, closeURL, nil
	}
	urls := make([]string, 0, members)
	closers := make([]func() error, 0, members)
	for i := 0; i < members; i++ {
		url, closeURL, err := ReachableURL(ctx, rt, runtime.OrdinalName(name, i), port)
		if err != nil {
			continue
		}
		urls = append(urls, url)
		closers = append(closers, closeURL)
	}
	if len(urls) == 0 {
		return nil, nil, fmt.Errorf("no member of %q (%d ordinals) is currently reachable", name, members)
	}
	return urls, func() error {
		for _, c := range closers {
			_ = c()
		}
		return nil
	}, nil
}

// ProbeConnectWorkerSet is the Provider-kind probe for a declared
// spec.configuration.workers > 1 Kafka Connect worker set (docs/planning/08
// C3), shared verbatim by debezium and s3sink — both reconcile a Connect
// worker via the identical `Replicas: N, StableIdentity: false` shape and
// already share the ReasonConnectWorkerMissing/ReasonConnectWorkerHealthy
// reason constants (internal/domain/status's "Shared Kafka Connect
// connector lifecycle" block), so this is genuinely byte-identical across
// both callers (docs/planning/08 G1's bar for a providerkit extraction).
// Mirrors redpanda.probeBrokerSet's presence check: every ordinal must be
// present and running. There is no Kafka Connect REST equivalent of "list
// group members" to check further (unlike redpanda's admin-API broker
// list), so per-ordinal container presence is the whole signal — a live
// worker's own group membership is Connect's problem to self-heal via its
// rebalance protocol, not something this probe verifies.
func ProbeConnectWorkerSet(ctx context.Context, rt runtime.ContainerRuntime, name string, n int, now time.Time) (status.Status, error) {
	st := status.Status{}
	var missing []string
	for i := 0; i < n; i++ {
		ord := runtime.OrdinalName(name, i)
		ordState, found, err := rt.Inspect(ctx, ord)
		if err != nil {
			return st, err
		}
		if !found || !ordState.Running {
			missing = append(missing, ord)
		}
	}
	if len(missing) > 0 {
		reason := fmt.Sprintf("%s(%s)", status.ReasonConnectWorkerMissing, strings.Join(missing, ","))
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: reason}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: reason}, now)
		return st, nil
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectWorkerHealthy}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}
