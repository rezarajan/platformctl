// Package wireguard realizes managed Connection resources whose upstream is
// only reachable through a WireGuard tunnel (docs/adr/002's addendum,
// docs/adr/023). Provider(type: wireguard) is the tunnel *initiator*: one
// container joining configuration.peerNetwork, dialing an externally
// operated WireGuard peer (the "responder" — never provisioned by this
// provider; docs/adr/023 Decision 7), and routing configuration.allowedIPs
// through it. NET_ADMIN is required: the container creates and manages its
// own wg0 interface and iptables NAT/forward rules — a real, broad
// capability grant, scoped by the ordinary boundary every container
// capability grant in this codebase relies on: a Linux network namespace,
// one per container (docs/adr/023 Decision 2).
//
// A managed Connection realized by this provider (providerRef naming a
// wireguard Provider, scheme tcp) gets a forwarder: one iptables PREROUTING
// DNAT rule (spec.port -> spec.target, reachable via the tunnel's routed
// AllowedIPs) baked into the SAME shared container's boot script — not a
// second socat/nc process. The pinned image ships neither, and installing
// one at apply time would break image pinning (scripts/pinned-images.txt);
// iptables is already required for the tunnel's own routing (docs/adr/023
// Decision 4). spec.target must be an IP:port pair reachable via
// configuration.allowedIPs — unlike proxy's Connection.Target, iptables
// --to-destination does not resolve DNS names.
//
// Every Connection naming this Provider is discovered by scanning
// reconciler.Request.Resources (the full validated resource set, regardless
// of reconcile order — see that field's own doc comment) during the
// Provider's own reconcile; the container's boot script is spec-hashed, so
// any Connection add/remove/edit (including a private-key rotation)
// recreates it — the identical trade-off docs/adr/018 documents for
// prometheus's scrape config, chosen for the same reason: this provider is
// the sole consumer of that config, and a tunnel's own connection count is
// expected to be small. reconcileConnection (the Connection-kind call) does
// no container work at all — the shared container already carries its rule
// by the time a Connection's own reconcile runs (Providers reconcile before
// their dependents in the engine's topological order).
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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
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
	wgConfPath         = "/etc/wireguard/wg0.conf"
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

// forwardRule is one Connection's DNAT rule, baked into the shared
// container's boot script.
type forwardRule struct {
	port   int
	target string
}

// connectionsForProvider scans req.Resources for every managed Connection
// whose providerRef names providerEnv (docs/adr/023's "why the DNAT rules
// live in the Provider-kind boot script" note) — sorted by resource Key so
// the generated script is byte-deterministic regardless of Go's randomized
// map iteration order, which idempotency (a second EnsureContainer call
// with the same content must make zero API calls) depends on.
func connectionsForProvider(providerEnv resource.Envelope, resources map[resource.Key]resource.Envelope) ([]forwardRule, error) {
	providerKey := providerEnv.Key()
	var keys []resource.Key
	byKey := map[resource.Key]resource.Envelope{}
	for k, e := range resources {
		if e.Kind != "Connection" {
			continue
		}
		ref := resource.RefFromSpec(e.Spec, "providerRef")
		if ref.Name == "" || ref.Key(e.Metadata.Namespace, "Provider") != providerKey {
			continue
		}
		keys = append(keys, k)
		byKey[k] = e
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	rules := make([]forwardRule, 0, len(keys))
	for _, k := range keys {
		c, err := connection.FromEnvelope(byKey[k])
		if err != nil {
			return nil, err
		}
		if c.External {
			continue
		}
		rules = append(rules, forwardRule{port: c.Port, target: c.Target})
	}
	return rules, nil
}

func (p *Provider) reconcileInstance(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
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
	privateKey, err := resolvePrivateKey(cfg, req.Secrets, name)
	if err != nil {
		return st, err
	}
	rules, err := connectionsForProvider(req.Provider, req.Resources)
	if err != nil {
		return st, err
	}

	platformNetwork := providerkit.Network(cfg)
	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", name, name)
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: platformNetwork, Labels: labels}); err != nil {
		return st, err
	}
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: tc.peerNetwork, Labels: labels}); err != nil {
		return st, err
	}
	networks := []string{platformNetwork}
	if tc.peerNetwork != platformNetwork {
		networks = append(networks, tc.peerNetwork)
	}

	wgConf := buildWireGuardConfig(tc, privateKey)
	script := buildEntrypointScript(rules)

	ports := make([]runtime.PortBinding, 0, len(rules))
	for _, r := range rules {
		ports = append(ports, runtime.PortBinding{HostPort: r.port, ContainerPort: r.port, Audience: runtime.AudienceHost})
	}

	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:       name,
		Image:      image(cfg),
		Entrypoint: []string{"/bin/sh", entrypointPath},
		Networks:   networks,
		Files: []runtime.FileMount{
			{Path: wgConfPath, Content: []byte(wgConf), Mode: 0o600},
			{Path: entrypointPath, Content: []byte(script), Mode: 0o500},
		},
		Ports: ports,
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
		Labels: labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	age, handshaked := handshakeAge(ctx, rt, name)
	switch {
	case !handshaked:
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelSurfaceReady}, now)
		st.SetCondition(status.Condition{Type: status.Progressing, Status: status.True, Reason: status.ReasonTunnelSurfaceReady}, now)
	case age > time.Duration(tc.keepalive*handshakeStaleFactor)*time.Second:
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelSurfaceReady}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonHandshakeStale}, now)
		st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	default:
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelSurfaceReady}, now)
		st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	}
	// Never the private key, never Env — only host/observed facts.
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"peerNetwork": tc.peerNetwork,
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
// tunnel, install one DNAT+MASQUERADE forwarder rule per Connection
// (docs/adr/023 Decision 4), and background a handshake-status poller
// (docs/adr/023 Decision 6 — no ContainerRuntime exec primitive exists, so
// Probe reads this file back via runtime.ReadFile instead of running `wg
// show` itself). rules is already sorted (connectionsForProvider) so this
// output is byte-deterministic — required for EnsureContainer idempotency.
func buildEntrypointScript(rules []forwardRule) string {
	var b strings.Builder
	b.WriteString("#!/bin/sh\nset -e\n")
	b.WriteString("mkdir -p /var/run/datascape\n")
	fmt.Fprintf(&b, "wg-quick up %s\n", wgConfPath)
	for _, r := range rules {
		fmt.Fprintf(&b, "iptables -t nat -A PREROUTING -p tcp --dport %d -j DNAT --to-destination %s\n", r.port, r.target)
	}
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

// reconcileConnection publishes the Connection's own Ready condition and
// endpoint fact — no container mutation: the shared tunnel container
// already carries this Connection's forwarder rule by the time this runs
// (docs/adr/023's "why the DNAT rules live in the Provider-kind boot
// script" note).
func (p *Provider) reconcileConnection(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	conn, err := connection.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	tunnelName := naming.RuntimeObjectName(req.Provider)
	ctr, found, err := rt.Inspect(ctx, tunnelName)
	if err != nil {
		return st, err
	}
	now := time.Now()
	if !found || !ctr.Healthy {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonTunnelDown}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonTunnelDown}, now)
		return st, nil
	}
	hostAddr := ctr.HostAddr(conn.Port)
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelUp}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	st.ProviderState = map[string]any{
		"tunnelContainerId": ctr.ID,
		"target":            conn.Target,
		endpoint.Key: endpoint.List{
			{Name: "forward", Scheme: conn.Scheme, Host: hostAddr, Internal: fmt.Sprintf("%s:%d", tunnelName, conn.Port), Insecure: true, RuntimeName: tunnelName, ContainerPort: conn.Port, Audience: runtime.AudienceHost},
		}.ToState(),
	}
	return st, nil
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	switch req.Resource.Kind {
	case "Provider":
		name := naming.RuntimeObjectName(req.Resource)
		if err := req.Runtime.Remove(ctx, name); err != nil {
			return err
		}
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
		// No container of its own — see reconcileConnection's doc comment.
		return nil
	default:
		return fmt.Errorf("wireguard provider cannot destroy kind %s", req.Resource.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	now := time.Now()
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	tc, err := parseConfig(cfg)
	if err != nil {
		return st, err
	}
	tunnelName := naming.RuntimeObjectName(req.Provider)

	ctr, found, err := rt.Inspect(ctx, tunnelName)
	if err != nil {
		return st, err
	}
	if !found || !ctr.Healthy {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonTunnelDown}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonTunnelDown}, now)
		return st, nil
	}

	age, handshaked := handshakeAge(ctx, rt, tunnelName)
	staleThreshold := time.Duration(tc.keepalive*handshakeStaleFactor) * time.Second
	stale := !handshaked || age > staleThreshold

	switch res.Kind {
	case "Provider":
		if stale {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelSurfaceReady}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonHandshakeStale}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonTunnelSurfaceReady}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	case "Connection":
		conn, err := connection.FromEnvelope(res)
		if err != nil {
			return st, err
		}
		dialErr := runtime.WithReachable(ctx, rt, tunnelName, conn.Port, runtime.ReachableOptions{Timeout: 10 * time.Second}, func(ctx context.Context, addr string) error {
			return dialUpstream(addr)
		})
		if dialErr != nil {
			msg := fmt.Sprintf("tunnel forwarder is up but upstream %s is unreachable through it: %v", conn.Target, dialErr)
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonTunnelUpstreamUnreachable, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonTunnelUpstreamUnreachable, Message: msg}, now)
			return st, nil
		}
		if stale {
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
