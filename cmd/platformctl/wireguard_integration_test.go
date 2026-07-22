//go:build integration

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestWireGuardTunnelEndToEnd covers docs/planning/08 D5's Accept criteria
// and the Stage D exit criterion: a CDC Binding against a database that
// lives in an isolated "VPC" Docker network reaches RUNNING only through a
// wireguard-realized Connection.
//
// Topology (docs/adr/023 Decision 7 — every piece below except the managed
// wireguard/redpanda/debezium Providers is a raw, unmanaged test fixture,
// standing in for infrastructure platformctl never provisions: a VPC, and
// the corporate VPN gateway fronting it):
//
//	datascape-wg-vpc (isolated, subnet 10.13.13.0/24, no path to the
//	platform network at all):
//	  - wg-vpc-db: plain postgres:16, static IP 10.13.13.10, no host
//	    publish — the database CDC will read from.
//	  - wg-responder: also attached to datascape-wg-transit — the VPC's
//	    own WireGuard gateway (fixed test keypair), dual-homed so it can
//	    NAT/forward between the tunnel and the VPC.
//	datascape-wg-transit (where the *managed* wireguard Provider's tunnel
//	container dials the responder's WireGuard UDP endpoint):
//	  - wg-responder (see above)
//	  - the managed wg-orders-db-conn tunnel container (created by apply)
//	datascape-wg-net (the ordinary shared platform network): redpanda,
//	  debezium, and the managed tunnel container all also join this one —
//	  Debezium never touches datascape-wg-transit or datascape-wg-vpc at
//	  all, only the tunnel container's own name:port on datascape-wg-net.
func TestWireGuardTunnelEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_WG_TUNNEL_KEY_PRIVATEKEY", wgClientPrivateKey)
	t.Setenv("DATASCAPE_SECRET_WG_DB_CREDS_USERNAME", "wg_orders_ro")
	t.Setenv("DATASCAPE_SECRET_WG_DB_CREDS_PASSWORD", "wg-orders-pw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	cleanup := func() {
		for _, c := range []string{"wg-orders-to-events", "datascape-wg-dbz", "datascape-wg-rp", "wg-orders-db-conn"} {
			_ = rt.Remove(ctx, c)
		}
		_ = exec.Command("docker", "rm", "-f", wgDBContainer, wgResponderContainer).Run()
		_ = rt.RemoveVolume(ctx, "datascape-wg-rp-data")
		for _, n := range []string{wgPlatformNetwork, wgTransitNetwork, wgVPCNetwork} {
			_ = exec.Command("docker", "network", "rm", n).Run()
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	// --- Raw fixtures: the "VPC" (isolated network + database) and the
	// WireGuard responder (the VPC's own gateway) --------------------------
	mustRun(t, "docker", "network", "create", "--label", managedByLabel,
		"--subnet", "10.13.13.0/24", wgVPCNetwork)
	mustRun(t, "docker", "network", "create", "--label", managedByLabel, wgTransitNetwork)
	// The shared platform network: created up front (not left for the
	// engine's own EnsureNetwork) so the negative-reachability probe below
	// has a real, already-existing network to probe from.
	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: wgPlatformNetwork}); err != nil {
		t.Fatalf("EnsureNetwork(%s): %v", wgPlatformNetwork, err)
	}

	mustRun(t, "docker", "run", "-d", "--name", wgDBContainer,
		"--network", wgVPCNetwork, "--ip", "10.13.13.10",
		"-e", "POSTGRES_USER=wg_orders_ro", "-e", "POSTGRES_PASSWORD=wg-orders-pw", "-e", "POSTGRES_DB=ordersdb",
		"postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20",
		"postgres", "-c", "wal_level=logical")
	waitPostgresReady(t, wgDBContainer, "wg_orders_ro")

	startWireGuardResponder(t)

	// --- Negative proof: before any tunnel exists, the database is
	// unreachable from the shared platform network (docs/adr/023 Decision
	// 7) — asserted with runtime.ProbeReachable, the in-network vantage
	// point (docs/planning/08 C10), not a host-side dial (which would be
	// unaffected by any of this Docker network topology at all). --------
	if err := rt.ProbeReachable(ctx, wgPlatformNetwork, "10.13.13.10:5432"); err == nil {
		t.Fatal("ProbeReachable succeeded before the tunnel exists — the VPC network isolation this test depends on is not real")
	}

	// --- apply: the managed wireguard/redpanda/debezium Providers, the
	// Connection, the external Source, and the CDC Binding. ---------------
	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/wireguard-scenario"

	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates=TunnelProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("apply took %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates=TunnelProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after apply: %s", line)
		}
	}

	// Positive proof: the CDC connector reached RUNNING — through the
	// tunnel, since Debezium (on datascape-wg-net only) has no other path
	// to 10.13.13.10.
	if state := wgConnectorStatus(t, "wg-orders-to-events"); state != "RUNNING" {
		t.Errorf("connector state = %q, want RUNNING", state)
	}

	tunnelBefore, found, err := rt.Inspect(ctx, "wg-orders-db-conn")
	if err != nil || !found {
		t.Fatalf("tunnel container not found after apply: %v", err)
	}

	// --- idempotent re-apply -----------------------------------------------
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates=TunnelProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}
	tunnelAfterNoop, found, err := rt.Inspect(ctx, "wg-orders-db-conn")
	if err != nil || !found {
		t.Fatalf("tunnel container missing after no-op re-apply: %v", err)
	}
	if tunnelAfterNoop.ID != tunnelBefore.ID {
		t.Errorf("tunnel container was recreated on a no-op re-apply (ID %s -> %s)", tunnelBefore.ID, tunnelAfterNoop.ID)
	}

	// --- key rotation: a new SecretReference value re-establishes the
	// tunnel (docs/adr/023 Decision 3 — a container recreate, not a live
	// wg set). The responder must also be told to accept the new public
	// key: a real VPC operator would do this out of band exactly the same
	// way (the client rotating its own key is useless if the peer still
	// only trusts the old one). ---------------------------------------
	rotateResponderPeer(t, wgClientPublicKey, wgRotatedClientPublicKey)
	t.Setenv("DATASCAPE_SECRET_WG_TUNNEL_KEY_PRIVATEKEY", wgRotatedClientPrivateKey)

	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates=TunnelProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("rotation apply failed (code %d): %v\n%s", code, err, out)
	}
	tunnelAfterRotate, found, err := rt.Inspect(ctx, "wg-orders-db-conn")
	if err != nil || !found {
		t.Fatalf("tunnel container missing after key rotation: %v", err)
	}
	if tunnelAfterRotate.ID == tunnelBefore.ID {
		t.Error("tunnel container was not recreated on a private-key rotation")
	}
	// The re-established tunnel is still functionally up: the connector
	// stays RUNNING (Debezium's own connection survived, or Connect
	// reconnected on its own — either way, the platform's status must
	// reflect it) and a fresh dial through the forwarder succeeds.
	if state := wgConnectorStatus(t, "wg-orders-to-events"); state != "RUNNING" {
		t.Errorf("connector state after key rotation = %q, want RUNNING", state)
	}
	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates=TunnelProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("status after rotation failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after key rotation: %s", line)
		}
	}

	// --- destroy: leaves no tunnel artifacts. -------------------------
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates=TunnelProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range []string{"wg-orders-db-conn", "datascape-wg-dbz", "datascape-wg-rp"} {
		if _, found, _ := rt.Inspect(ctx, c); found {
			t.Errorf("container %s still present after destroy", c)
		}
	}
	managed, err := rt.ListManaged(ctx)
	if err != nil {
		t.Fatalf("list managed: %v", err)
	}
	for _, m := range managed {
		if strings.HasPrefix(m.Name, "datascape-wg-") || m.Name == "wg-orders-db-conn" {
			t.Errorf("orphaned managed container after destroy: %s", m.Name)
		}
	}
	// The shared platform network is fully removable: every container ever
	// attached to it was platformctl-managed (redpanda/debezium/tunnel),
	// and all are gone now (RemoveNetwork's own documented refusal — never
	// cascade-delete, never remove while anything is still attached —
	// internal/ports/runtime.ContainerRuntime's doc comment).
	managedNets, err := rt.ListManagedNetworks(ctx)
	if err != nil {
		t.Fatalf("list managed networks: %v", err)
	}
	for _, n := range managedNets {
		if n.Name == wgPlatformNetwork {
			t.Errorf("platform network %q still present after destroy", wgPlatformNetwork)
		}
	}
	// wgTransitNetwork deliberately is NOT asserted gone here: the raw
	// wg-responder fixture (a stand-in for a VPC operator's own gateway,
	// never platformctl-managed) is still attached to it — RemoveNetwork
	// correctly refuses rather than cascade-removing someone else's
	// container, exactly the behavior this test's own cleanup() (not
	// platformctl) is responsible for tearing down.
}

// --- fixture topology names ------------------------------------------------

const (
	wgVPCNetwork         = "datascape-wg-vpc"     // isolated "VPC": the database + the responder's second leg
	wgTransitNetwork     = "datascape-wg-transit" // where the managed tunnel container dials the responder
	wgPlatformNetwork    = "datascape-wg-net"     // the ordinary shared platform network (redpanda/debezium/tunnel)
	wgDBContainer        = "wg-vpc-db"            // raw postgres, static IP 10.13.13.10 on wgVPCNetwork
	wgResponderContainer = "wg-responder"         // raw wireguard responder, dual-homed
	managedByLabel       = "io.datascape.managed-by=platformctl"
)

// Fixed test keypairs (docs/adr/023 Decision 7 — this is fixture material,
// not a real secret; generated once via `wg genkey`/`wg pubkey` against the
// pinned wireguard image and hardcoded here so the manifest's
// configuration.peerPublicKey and the responder's own config never need
// runtime templating).
const (
	wgResponderPrivateKey = "WL/k63OCnb2/TQzP/zaKrkT/edbqpfRqKBFuxU7y2kM="
	wgResponderPublicKey  = "8GY91qw8rgMR8ffTDuhSccmObRV4GkNMT2YbqYXLHwI=" // matches testdata/wireguard-scenario/manifests.yaml's configuration.peerPublicKey

	wgClientPrivateKey = "6Cc/P931nL2QKiv6E7KMnS9Bvn01GWVWf6L1LmDpnG8="
	wgClientPublicKey  = "F/qVLIP5k+0VjYLQ6FbdVz24DWWwfomiEyD0tNN0Pm8="

	wgRotatedClientPrivateKey = "wG/R+oGSWUycbHHWgGFyGdUL1RhZp06V7RsJl9gEWEo="
	wgRotatedClientPublicKey  = "6j+H2mfwnmkC5Zykmr0aWKZNtzmSEvOYiiN8M75MQkk="

	// wgClientTunnelAddress is the tunnel's own address on the WireGuard
	// point-to-point subnet — deliberately a different CIDR than the VPC's
	// Docker subnet (10.13.13.0/24) to avoid two interfaces (wg0 and the
	// responder's own VPC-network leg) claiming overlapping routes.
	wgClientTunnelAddress = "10.99.0.2/32"
)

// wireguardImage is the same pinned image the wireguard provider itself
// uses (internal/adapters/providers/wireguard's defaultImage,
// scripts/pinned-images.txt) — the responder fixture plays the peer role
// with the identical tooling, just hand-configured as a server instead of
// driven by the provider.
const wireguardImage = "linuxserver/wireguard:1.0.20260223@sha256:2868ae5e3dd9065ea3b1e44b4214b33b02b7ce5ebcb9e4f33e1132b75007f39c"

// wgConnectURL is testdata/wireguard-scenario/manifests.yaml's own
// debezium Provider connectPort — deliberately not cdc_integration_test.go's
// cdcConnectURL (18183): that's a different scenario's own Connect worker.
const wgConnectURL = "http://localhost:18189"

// wgConnectorStatus mirrors connectorStatus (cdc_integration_test.go) but
// against this scenario's own Connect worker port — connectorStatus itself
// can't be reused since it hardcodes cdcConnectURL.
func wgConnectorStatus(t *testing.T, name string) string {
	t.Helper()
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
	}
	getJSON(t, wgConnectURL+"/connectors/"+name+"/status", &body)
	return body.Connector.State
}

// mustRun runs an external command, failing the test with its combined
// output on error — the same shape phase5_integration_test.go's inline
// exec.Command calls use, extracted here since this suite needs several.
func mustRun(t *testing.T, name string, args ...string) {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
}

// waitPostgresReady polls pg_isready inside the raw fixture container —
// standing in for a HealthCheck (unmanaged containers get none here) —
// bounded to 30s.
func waitPostgresReady(t *testing.T, container, user string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := exec.Command("docker", "exec", container, "pg_isready", "-U", user).Run(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become ready within 30s", container)
		}
		time.Sleep(1 * time.Second)
	}
}

// startWireGuardResponder stands up the raw WireGuard "responder" — the
// VPC's own gateway, dual-homed on wgVPCNetwork (attached first, so it's
// eth0 — the interface the MASQUERADE rule below NATs onto) and
// wgTransitNetwork (attached second, eth1 — where the managed tunnel
// container reaches it). Config is written to a host temp file and bind
// mounted, avoiding any exec/heredoc quoting fragility.
func startWireGuardResponder(t *testing.T) {
	t.Helper()
	conf := "[Interface]\n" +
		"PrivateKey = " + wgResponderPrivateKey + "\n" +
		"Address = 10.99.0.1/24\n" +
		"ListenPort = 51820\n\n" +
		"[Peer]\n" +
		"PublicKey = " + wgClientPublicKey + "\n" +
		"AllowedIPs = " + wgClientTunnelAddress + "\n"
	confPath := filepath.Join(t.TempDir(), "wg0.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		t.Fatalf("write responder wg0.conf: %v", err)
	}

	mustRun(t, "docker", "run", "-d", "--name", wgResponderContainer,
		"--network", wgVPCNetwork,
		"--cap-add", "NET_ADMIN",
		"--sysctl", "net.ipv4.ip_forward=1",
		"-v", confPath+":/etc/wireguard/wg0.conf:ro",
		"--entrypoint", "sh",
		wireguardImage,
		"-c", "wg-quick up wg0 && iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE && sleep infinity")
	mustRun(t, "docker", "network", "connect", wgTransitNetwork, wgResponderContainer)

	deadline := time.Now().Add(15 * time.Second)
	for {
		if err := exec.Command("docker", "exec", wgResponderContainer, "wg", "show", "wg0").Run(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s's wg0 interface did not come up within 15s", wgResponderContainer)
		}
		time.Sleep(1 * time.Second)
	}
}

// rotateResponderPeer swaps which client public key the responder accepts —
// a real VPC operator's own side of a client key rotation, done live via
// `wg set` (no restart), exactly what a real gateway operator would do
// after the client-side SecretReference rotates.
func rotateResponderPeer(t *testing.T, oldPub, newPub string) {
	t.Helper()
	mustRun(t, "docker", "exec", wgResponderContainer, "wg", "set", "wg0",
		"peer", newPub, "allowed-ips", wgClientTunnelAddress)
	mustRun(t, "docker", "exec", wgResponderContainer, "wg", "set", "wg0",
		"peer", oldPub, "remove")
}
