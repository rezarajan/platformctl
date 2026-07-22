// Package wireguard realizes managed Connection resources whose upstream is
// only reachable through a WireGuard tunnel (docs/adr/002's addendum,
// docs/adr/023). Provider(type: wireguard) anchors the shared network and
// configuration defaults; each Connection realized by it gets its own
// tunnel container (one per Connection, mirroring proxy's "one socat
// forwarder container per route" — every existing ConnectionCapableProvider
// in this codebase creates one runtime object named after the Connection
// itself, never after the Provider, and other providers (debezium's
// Connection-address resolution) depend on that naming to work at all — see
// this file's reconcileConnection doc comment): the container is the tunnel
// *initiator*, joining configuration.peerNetwork and dialing an externally
// operated WireGuard peer (the "responder" — never provisioned by this
// provider; docs/adr/023 Decision 7), routing configuration.allowedIPs
// through it. NET_ADMIN is required: the container creates and manages its
// own wg0 interface and iptables NAT/forward rules — a real, broad
// capability grant, scoped by the ordinary boundary every container
// capability grant in this codebase relies on: a Linux network namespace,
// one per container (docs/adr/023 Decision 2).
//
// The forwarder itself is one iptables PREROUTING DNAT rule (spec.port ->
// spec.target, reachable via the tunnel's routed AllowedIPs) baked into the
// SAME container's boot script alongside the wg-quick config — not a
// second socat/nc process. The pinned image ships neither, and installing
// one at apply time would break image pinning (scripts/pinned-images.txt);
// iptables is already required for the tunnel's own routing (docs/adr/023
// Decision 4). spec.target must be an IP:port pair reachable via
// configuration.allowedIPs — unlike proxy's Connection.Target, iptables
// --to-destination does not resolve DNS names.
//
// The private key is resolved from a SecretReference and placed only in the
// wg-quick config file mounted via runtime.FileMount — never Env
// (docker-inspect-visible), never status.ProviderState, never logged
// (docs/adr/023 Decision 3, mirroring
// internal/adapters/providers/postgres's bootstrap-credential file-mount
// discipline — read-only reference, not imported). Key rotation is
// deliberately a container recreate (the existing spec-hash mechanism), not
// a live in-place `wg set` call: a WireGuard tunnel has no live
// authenticated session to preserve the way a database connection does.
//
// Implements ConnectionCapableProvider (scheme: tcp) and
// TunnelCapableProvider (Connection.spec.via's structural capability — not
// yet consumed by any realizing provider; see docs/adr/023's "Scope"
// section).
package wireguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// defaultImage is linuxserver/wireguard, pinned by digest (docs/adr/023
// Decision 1): Alpine-based, ships wireguard-tools (wg/wg-quick) and
// iptables. This provider does not use the image's own PEERS/SERVERURL
// auto-config path — it drives wg-quick directly via a custom entrypoint
// script for full control and testability.
const defaultImage = "linuxserver/wireguard:1.0.20260223@sha256:2868ae5e3dd9065ea3b1e44b4214b33b02b7ce5ebcb9e4f33e1132b75007f39c"

const (
	// wgConfPath is deliberately NOT under /etc/wireguard: the pinned
	// image symlinks /etc/wireguard -> /config/wg_confs (an s6-overlay
	// init-time convenience for its own auto-config path, unused here),
	// and that target doesn't exist in the image's base layer — a
	// FileMount write through it fails opaquely ("Could not find the file
	// /", found live) since Docker's pre-start file-copy has to resolve
	// the full symlink chain to a real directory. wg-quick accepts any
	// absolute path (only a bare name with no "/" is looked up under
	// /etc/wireguard/<name>.conf), so a plain, unshadowed path sidesteps
	// the symlink entirely; the interface still comes up as "wg0" (from
	// the file's basename).
	wgConfPath         = "/etc/datascape/wg0.conf"
	entrypointPath     = "/etc/datascape/entrypoint.sh"
	handshakeStatePath = "/var/run/datascape/handshake-status"
	defaultKeepalive   = 25
	// handshakeStaleFactor: a handshake older than this many multiples of
	// the configured PersistentKeepalive is treated as stale
	// (docs/adr/023 Decision 6).
	handshakeStaleFactor = 3
)

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "wireguard" }

// SupportedConnectionSchemes implements ConnectionCapableProvider: the
// forwarder is a raw TCP relay (iptables DNAT), so anything TCP-framed
// works through it, exactly like proxy's socat forwarder.
func (p *Provider) SupportedConnectionSchemes() []string { return []string{"tcp"} }

// SupportsTunnelChaining implements TunnelCapableProvider — see
// docs/adr/023's "Scope" section for what this does and does not mean yet.
func (p *Provider) SupportsTunnelChaining() []string { return []string{"tcp"} }

func image(cfg provider.Provider) string {
	if img, ok := cfg.Configuration["image"].(string); ok && img != "" {
		return img
	}
	return defaultImage
}

// tunnelConfig is Provider.spec.configuration, parsed.
type tunnelConfig struct {
	peerNetwork   string
	peerPublicKey string
	peerEndpoint  string
	address       string
	allowedIPs    []string
	keepalive     int
}

func parseConfig(cfg provider.Provider) (tunnelConfig, error) {
	tc := tunnelConfig{keepalive: defaultKeepalive}
	tc.peerNetwork, _ = cfg.Configuration["peerNetwork"].(string)
	tc.peerPublicKey, _ = cfg.Configuration["peerPublicKey"].(string)
	tc.peerEndpoint, _ = cfg.Configuration["peerEndpoint"].(string)
	tc.address, _ = cfg.Configuration["address"].(string)
	tc.allowedIPs = stringList(cfg.Configuration["allowedIPs"])
	if v, ok := cfg.Configuration["keepalive"]; ok {
		switch n := v.(type) {
		case int:
			tc.keepalive = n
		case float64:
			tc.keepalive = int(n)
		}
	}
	var missing []string
	if tc.peerNetwork == "" {
		missing = append(missing, "peerNetwork")
	}
	if tc.peerPublicKey == "" {
		missing = append(missing, "peerPublicKey")
	}
	if tc.peerEndpoint == "" {
		missing = append(missing, "peerEndpoint")
	}
	if tc.address == "" {
		missing = append(missing, "address")
	}
	if len(tc.allowedIPs) == 0 {
		missing = append(missing, "allowedIPs")
	}
	if len(missing) > 0 {
		return tc, fmt.Errorf("configuration.%s is required", strings.Join(missing, ", configuration."))
	}
	return tc, nil
}

func stringList(v any) []string {
	switch vv := v.(type) {
	case []string:
		return vv
	case []any:
		out := make([]string, 0, len(vv))
		for _, e := range vv {
			if s, ok := e.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if vv == "" {
			return nil
		}
		return []string{vv}
	default:
		return nil
	}
}

// ValidateSpec implements SpecValidator: required configuration.* keys and
// configuration.privateKeySecretRef's spec.secretRefs membership, checked
// at validate time — before anything is scheduled.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if _, err := parseConfig(cfg); err != nil {
		return err
	}
	ref, _ := cfg.Configuration["privateKeySecretRef"].(string)
	if ref != "" {
		if !cfg.HasSecretRef(ref) {
			return fmt.Errorf("configuration.privateKeySecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
		}
	} else if len(cfg.SecretRefs) == 0 {
		return fmt.Errorf("spec.secretRefs must name at least one SecretReference (the WireGuard private key; configuration.privateKeySecretRef selects one explicitly)")
	}
	return nil
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileInstance(ctx, req)
	case "Connection":
		return p.reconcileConnection(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("wireguard provider cannot reconcile kind %s", req.Resource.Kind)
	}
}

// reconcileInstance: the wireguard Provider has no central container of its
// own — like proxy, its Provider resource only anchors the shared network
// and the configuration every Connection's tunnel container inherits.
// req.Provider is re-parsed (parseConfig) and req.Secrets re-resolved
// (resolvePrivateKey) per Connection reconcile too, not cached here — this
// provider holds no cross-call state (docs/planning/08 F5).
func (p *Provider) reconcileInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	tc, err := parseConfig(cfg)
	if err != nil {
		return st, fmt.Errorf("Provider %q (type: wireguard): %w", name, err)
	}
	platformNetwork := providerkit.Network(cfg)
	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", name, name)
	if err := req.Runtime.EnsureNetwork(ctx, runtime.NetworkSpec{Name: platformNetwork, Labels: labels}); err != nil {
		return st, err
	}
	if err := req.Runtime.EnsureNetwork(ctx, runtime.NetworkSpec{Name: tc.peerNetwork, Labels: labels}); err != nil {
		return st, err
	}
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelSurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{"peerNetwork": tc.peerNetwork}
	return st, nil
}

// reconcileConnection runs the Connection's own tunnel container: named
// after the Connection (naming.RuntimeObjectName(res)) — not the Provider —
// so it answers exactly where every consumer expects a managed Connection
// to answer (connection.Connection.Endpoint's contract: "managed: its own
// name"). debezium's Source-Connection resolution
// (internal/adapters/providers/debezium, read-only reference) calls
// naming.RuntimeObjectName on the Connection envelope and dials that name
// directly via runtime.EnsureReachable/WithReachable — a container named
// after this Provider instead would silently break that resolution for
// every existing and future ConnectionCapableProvider consumer, which is
// why this mirrors proxy's per-Connection-container shape exactly rather
// than the single-shared-container design a first draft of this provider
// used (docs/adr/023 records the correction).
func (p *Provider) reconcileConnection(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	conn, err := connection.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	providerName := naming.RuntimeObjectName(req.Provider)
	tc, err := parseConfig(cfg)
	if err != nil {
		return st, fmt.Errorf("Provider %q (type: wireguard): %w", providerName, err)
	}
	privateKey, err := resolvePrivateKey(cfg, req.Secrets, providerName)
	if err != nil {
		return st, err
	}

	name := naming.RuntimeObjectName(res)
	platformNetwork := providerkit.Network(cfg)
	providerLabels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", providerName, providerName)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: platformNetwork, Labels: providerLabels}); err != nil {
		return st, err
	}
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: tc.peerNetwork, Labels: providerLabels}); err != nil {
		return st, err
	}
	networks := []string{platformNetwork}
	if tc.peerNetwork != platformNetwork {
		networks = append(networks, tc.peerNetwork)
	}

	wgConf := buildWireGuardConfig(tc, privateKey)
	script := buildEntrypointScript(conn.Port, conn.Target)
	connLabels := runtime.ManagedLabels(res.Metadata.Namespace, res.Kind, name, name)

	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:       name,
		Image:      image(cfg),
		Entrypoint: []string{"/bin/sh", entrypointPath},
		Networks:   networks,
		Files: []runtime.FileMount{
			{Path: wgConfPath, Content: []byte(wgConf), Mode: 0o600},
			{Path: entrypointPath, Content: []byte(script), Mode: 0o500},
		},
		Ports: []runtime.PortBinding{{HostPort: conn.Port, ContainerPort: conn.Port, Audience: runtime.AudienceHost}},
		Security: &runtime.SecurityContext{
			CapAdd: []string{"NET_ADMIN"},
		},
		// net.ipv4.ip_forward must be writable at container-create time —
		// docs/adr/023 Decision 5.
		Sysctls: map[string]string{"net.ipv4.ip_forward": "1"},
		HealthCheck: &runtime.HealthCheck{
			Test:     []string{"CMD-SHELL", "wg show wg0 >/dev/null 2>&1"},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
		Labels: connLabels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
		return st, err
	}
	// Ready means serving (docs/planning/01 NFR-11), not just "the container
	// reports its own healthcheck passing" — wg0 existing says nothing about
	// whether the peer has actually handshaked or the upstream answers
	// through the forwarder rule (docs/planning/11 B1 finding 1, CONFIRMED,
	// the redpanda-93fbf14 signature). Settle to the SAME signal Probe
	// verifies before declaring Ready.
	if err := waitTunnelServing(ctx, rt, name, conn, tc); err != nil {
		return st, err
	}

	host, port := conn.Endpoint(name)
	hostAddr := ctrState.HostAddr(conn.Port)
	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelUp}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	// Never the private key, never Env — only host/observed facts.
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"internal":    fmt.Sprintf("%s:%d", host, port),
		"host":        hostAddr,
		"target":      conn.Target,
		endpoint.Key: endpoint.List{
			{Name: "forward", Scheme: conn.Scheme, Host: hostAddr, Internal: fmt.Sprintf("%s:%d", host, port), Insecure: true, RuntimeName: name, ContainerPort: conn.Port, Audience: runtime.AudienceHost},
		}.ToState(),
	}
	return st, nil
}

// resolvePrivateKey resolves the WireGuard private key from
// configuration.privateKeySecretRef (or the first declared secretRef),
// requiring key "privateKey" — never placed anywhere but the caller's
// file-mounted wg-quick config (docs/adr/023 Decision 3).
func resolvePrivateKey(cfg provider.Provider, secrets map[string]map[string]string, name string) (string, error) {
	creds, refName, err := providerkit.ResolveCredential(cfg, secrets, "privateKeySecretRef", name)
	if err != nil {
		return "", err
	}
	key := creds["privateKey"]
	if key == "" {
		return "", fmt.Errorf("Provider %q (type: wireguard): secretRef %q must provide a privateKey key", name, refName)
	}
	return key, nil
}

// buildWireGuardConfig renders a wg-quick config file. The private key is
// the only secret value in it — this string is placed only into a
// runtime.FileMount, never Env, never status.ProviderState, never logged
// (docs/adr/023 Decision 3).
func buildWireGuardConfig(tc tunnelConfig, privateKey string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[Interface]\nPrivateKey = %s\nAddress = %s\n\n", privateKey, tc.address)
	fmt.Fprintf(&b, "[Peer]\nPublicKey = %s\nEndpoint = %s\nAllowedIPs = %s\nPersistentKeepalive = %d\n",
		tc.peerPublicKey, tc.peerEndpoint, strings.Join(tc.allowedIPs, ", "), tc.keepalive)
	return b.String()
}

// buildEntrypointScript renders the container's boot script: bring up the
// tunnel, install this Connection's DNAT+MASQUERADE forwarder rule
// (docs/adr/023 Decision 4), and background a handshake-status poller
// (docs/adr/023 Decision 6 — no ContainerRuntime exec primitive exists, so
// Probe reads this file back via runtime.ReadFile instead of running `wg
// show` itself).
func buildEntrypointScript(port int, target string) string {
	var b strings.Builder
	b.WriteString("#!/bin/sh\nset -e\n")
	b.WriteString("mkdir -p /var/run/datascape\n")
	fmt.Fprintf(&b, "wg-quick up %s\n", wgConfPath)
	fmt.Fprintf(&b, "iptables -t nat -A PREROUTING -p tcp --dport %d -j DNAT --to-destination %s\n", port, target)
	b.WriteString("iptables -t nat -A POSTROUTING -o wg0 -j MASQUERADE\n")
	fmt.Fprintf(&b, "( while true; do wg show wg0 latest-handshakes > %s 2>/dev/null || true; sleep 5; done ) &\n", handshakeStatePath)
	b.WriteString("exec sleep infinity\n")
	return b.String()
}

// handshakeAge reads the handshake-status file the boot script maintains
// (via runtime.ReadFile, not an exec — this ContainerRuntime port has no
// "run a command in a running container" primitive) and returns how long
// ago the peer's latest handshake completed. ok is false when the file
// isn't there yet (still starting) or reports a zero (never-handshaked)
// timestamp.
func handshakeAge(ctx context.Context, rt runtime.ContainerRuntime, name string) (age time.Duration, ok bool) {
	data, err := rt.ReadFile(ctx, name, handshakeStatePath)
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, false
	}
	sec, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	if err != nil || sec == 0 {
		return 0, false
	}
	return time.Since(time.Unix(sec, 0)), true
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	switch req.Resource.Kind {
	case "Provider":
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return err
		}
		if tc, err := parseConfig(cfg); err == nil {
			_ = req.Runtime.RemoveNetwork(ctx, tc.peerNetwork)
		}
		_ = req.Runtime.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	case "Connection":
		return req.Runtime.Remove(ctx, naming.RuntimeObjectName(req.Resource))
	default:
		return fmt.Errorf("wireguard provider cannot destroy kind %s", req.Resource.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	now := time.Now()
	switch res.Kind {
	case "Provider":
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelSurfaceReady}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	case "Connection":
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return st, err
		}
		tc, err := parseConfig(cfg)
		if err != nil {
			return st, err
		}
		conn, err := connection.FromEnvelope(res)
		if err != nil {
			return st, err
		}
		name := naming.RuntimeObjectName(res)

		tss, err := probeTunnelServing(ctx, rt, name, conn, tc)
		if err != nil {
			return st, err
		}
		if !tss.found || !tss.healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonTunnelDown}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonTunnelDown}, now)
			return st, nil
		}
		if tss.dialErr != nil {
			msg := fmt.Sprintf("tunnel forwarder is up but upstream %s is unreachable through it: %v", conn.Target, tss.dialErr)
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonTunnelUpstreamUnreachable, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonTunnelUpstreamUnreachable, Message: msg}, now)
			return st, nil
		}
		if tss.stale {
			// The dial succeeded despite a stale handshake reading —
			// WireGuard's own roaming/NAT-traversal behavior can show this
			// while the tunnel is still functionally up (docs/adr/023
			// Decision 6). Logged as drift, not failed.
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelUp}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonHandshakeStale}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelUp}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	default:
		return st, fmt.Errorf("wireguard provider cannot probe kind %s", res.Kind)
	}
}

// tunnelServingState is the shared "ready means serving" signal for a
// wireguard Connection — forwarder container health, handshake recency, and
// a dial-through-forwarder to the upstream. Both Probe and
// reconcileConnection's settle poll (waitTunnelServing) evaluate this exact
// signal, via probeTunnelServing, never a weaker one (docs/planning/11 B1
// findings 1-3).
type tunnelServingState struct {
	found   bool
	healthy bool
	stale   bool
	dialErr error
}

// probeTunnelServing is the single check both Probe and
// reconcileConnection's settle poll evaluate: container health, handshake
// recency (handshakeAge), and dialUpstream through the forwarder — the same
// three signals Probe has always verified, now extracted so reconcile can
// require them too instead of settling for the container healthcheck alone.
func probeTunnelServing(ctx context.Context, rt runtime.ContainerRuntime, name string, conn connection.Connection, tc tunnelConfig) (tunnelServingState, error) {
	ctr, found, err := rt.Inspect(ctx, name)
	if err != nil {
		return tunnelServingState{}, err
	}
	if !found || !ctr.Healthy {
		return tunnelServingState{found: found}, nil
	}
	age, handshaked := handshakeAge(ctx, rt, name)
	staleThreshold := time.Duration(tc.keepalive*handshakeStaleFactor) * time.Second
	stale := !handshaked || age > staleThreshold
	dialErr := runtime.WithReachable(ctx, rt, name, conn.Port, runtime.ReachableOptions{Timeout: tunnelReachableTimeout, Interval: tunnelReachableInterval}, func(ctx context.Context, addr string) error {
		return dialUpstream(addr)
	})
	return tunnelServingState{found: true, healthy: true, stale: stale, dialErr: dialErr}, nil
}

// tunnelSettleTimeout/tunnelSettlePoll bound reconcileConnection's Ready
// determination to a genuinely serving tunnel — the redpanda
// waitTopicSettled pattern (docs/planning/11 B1 findings 1-3).
// tunnelReachableTimeout/tunnelReachableInterval bound each individual
// probeTunnelServing dial attempt's own internal WithReachable retry
// (unchanged from Probe's prior behavior — 10s/1s, generous enough for a
// K8s port-forward to establish). All four are vars, not consts: tests
// shrink them instead of waiting out real minutes-scale wall time (a down
// upstream would otherwise burn a full tunnelReachableTimeout on every one
// of the settle-poll's own attempts) to exercise the honest-failure path.
var (
	tunnelSettleTimeout     = 45 * time.Second
	tunnelSettlePoll        = 2 * time.Second
	tunnelReachableTimeout  = 10 * time.Second
	tunnelReachableInterval = time.Second
)

// waitTunnelServing re-runs probeTunnelServing until it reports a genuinely
// serving tunnel (found, healthy, non-stale handshake, and a successful
// dial through the forwarder to the upstream) — the SAME signal Probe
// verifies, bounded by tunnelSettleTimeout. A healthy tunnel passes on the
// first attempt (zero added latency); on timeout, reconcile fails honestly
// with the last observed state instead of setting Ready from the container
// healthcheck alone (docs/planning/11 B1 finding 1, the redpanda-93fbf14
// signature).
func waitTunnelServing(ctx context.Context, rt runtime.ContainerRuntime, name string, conn connection.Connection, tc tunnelConfig) error {
	deadline := time.Now().Add(tunnelSettleTimeout)
	var lastReason string
	for {
		tss, err := probeTunnelServing(ctx, rt, name, conn, tc)
		if err != nil {
			return err
		}
		switch {
		case !tss.found || !tss.healthy:
			lastReason = "forwarder container not healthy"
		case tss.dialErr != nil:
			lastReason = fmt.Sprintf("upstream %s unreachable through tunnel: %v", conn.Target, tss.dialErr)
		case tss.stale:
			lastReason = "handshake not yet recent"
		default:
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("tunnel %q did not settle to a serving state within %s (last observed: %s)", name, tunnelSettleTimeout, lastReason)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tunnelSettlePoll):
		}
	}
}

// dialUpstream holds a connection through a short read deadline, mirroring
// proxy.probeThroughForwarder's discipline (read-only reference, not shared
// code): a live TCP accept through the DNAT rule is itself the signal the
// forwarder — and the tunnel behind it — answered.
func dialUpstream(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = c.Read(buf)
	if err == nil {
		return nil
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return nil
	}
	return fmt.Errorf("session closed immediately: %w", err)
}
