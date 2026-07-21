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

	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/provider"
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
