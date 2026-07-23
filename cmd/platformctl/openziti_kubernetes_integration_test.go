//go:build integration

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestOpenZitiMediatedConnectionOnKubernetesEndToEnd is docs/adr/027's
// substrate-equality proof: the SAME MediationProvider port + OpenZiti
// adapter, on runtime.type: kubernetes, delivering the identical Layer-1
// guarantee it delivers on Docker (TestOpenZitiMediatedConnectionEndToEnd).
// That equality IS ADR 027's point — "enforcement travels with the
// workloads... which is why the guarantee is identical on Docker,
// Kubernetes, a VM fleet, or Terraform-provisioned cloud infrastructure."
//
// The adapter speaks ONLY the ContainerRuntime port (EnsureContainer/
// EnsureNetwork/EnsureVolume/EnsureReachable) — no kubernetes-special path
// (internal/archtest/mediation_layering_test.go holds the fence). The one
// change this leg required was routing the controller's Edge Management API
// through runtime.EnsureReachable instead of ContainerState.HostAddr:
// HostAddr is "" on a K8s ClusterIP Service, so the Docker-only HostAddr
// path silently produced "https://" (empty host) here — the fix is in the
// adapter's USE of the port, not a K8s branch.
//
// Proofs (same accept bar as Docker):
//  1. apply -> Ready for every resource;
//  2. CDC RUNNING through the mediated Connection (Layer 1 works on K8s);
//  3. wrong-identity refusal — a canary workload holding an unauthorized
//     Ziti identity, in the SAME namespace as the legitimate dial-side
//     tunneler, is refused (the identity check, not network reachability).
//
// Reachability negative (Docker's isolated-network proof): on THIS
// cluster's bridge CNI, NetworkPolicy is not enforced (verified: no CNI
// enforcement — the H8 isolation probe reports not-enforced), so Layer-2
// network isolation is inert here. Per ADR 027's claims table that is
// exactly the "Layer 2 only, enforcement observed absent" row — Layer 1 is
// the guarantee, and proof #3 (wrong-identity refusal) is that guarantee,
// substrate-independent. The test records this explicitly rather than
// faking a network-isolation assertion the cluster cannot honor.
func TestOpenZitiMediatedConnectionOnKubernetesEndToEnd(t *testing.T) {
	requireK8s(t)
	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()

	t.Setenv("DATASCAPE_SECRET_ZK8S_MESH_ADMIN_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_ZK8S_MESH_ADMIN_PASSWORD", "zk8s-admin-pw")
	t.Setenv("DATASCAPE_SECRET_ZK8S_PG_SUPER_USERNAME", "zk8s_super")
	t.Setenv("DATASCAPE_SECRET_ZK8S_PG_SUPER_PASSWORD", "zk8s-super-pw")

	const ns = "datascape-zk8s"
	const gates = "MediatedConnections=true,KubernetesRuntime=true"
	manifests := "testdata/openziti-k8s-scenario"
	stateFile := t.TempDir() + "/state.json"

	cleanup := func() {
		_ = rt.Remove(ctx, "zk8s-canary")
		_, _, _ = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gates)
		_ = rt.RemoveNetwork(ctx, ns)
	}
	cleanup()
	t.Cleanup(cleanup)

	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("k8s apply took %s", time.Since(start).Round(time.Second))

	assertDriftCleanExceptExternalProbeRace(t, manifests, stateFile, "--feature-gates", gates)

	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after apply: %s", line)
		}
	}

	// --- Positive proof: CDC connector RUNNING through the mediated
	// Connection (Debezium dials the external Source's connectionRef -> the
	// mediated entrypoint, never the postgres Service directly). ----------
	if state := zitiK8sConnectorState(t, rt, ctx, "zk8s-cdc"); state != "RUNNING" {
		t.Errorf("connector state = %q, want RUNNING", state)
	}

	// --- Negative proof (identity): a canary with an unauthorized Ziti
	// identity, in the SAME namespace, is refused dialing the SAME service
	// the legitimate dial-side tunneler serves. ---------------------------
	proveWrongIdentityRefusedK8s(t, rt, ctx, ns)
}

// zitiK8sConnectorState reaches the Debezium Connect REST API through an
// ephemeral port-forward (runtime.EnsureReachable — the substrate-neutral
// seam) and returns the named connector's state.
func zitiK8sConnectorState(t *testing.T, rt *k8sruntime.Runtime, ctx context.Context, connector string) string {
	t.Helper()
	var state string
	err := runtime.WithReachable(ctx, rt, "zk8s-dbz", 8083, runtime.ReachableOptions{Timeout: 60 * time.Second, Interval: 3 * time.Second}, func(ctx context.Context, addr string) error {
		resp, err := http.Get("http://" + addr + "/connectors/" + connector + "/status")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		var body struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return err
		}
		if body.Connector.State == "" {
			return fmt.Errorf("connector %q not present yet", connector)
		}
		state = body.Connector.State
		return nil
	})
	if err != nil {
		t.Fatalf("reach Debezium Connect REST for connector %q: %v", connector, err)
	}
	return state
}

// proveWrongIdentityRefusedK8s mints an unauthorized canary identity
// directly against the controller (reached by port-forward), stands up a
// canary ziti-tunnel container in the platform namespace enrolled under it,
// and asserts a dial through the canary's own local proxy port fails — the
// identity check (no Dial service-policy names the canary), not a network
// artifact (the canary has full in-namespace reachability to the router).
func proveWrongIdentityRefusedK8s(t *testing.T, rt *k8sruntime.Runtime, ctx context.Context, ns string) {
	t.Helper()

	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // test-only, mirrors the adapter's own documented trust posture

	var serviceName, canaryJWT string
	err := runtime.WithReachable(ctx, rt, "zk8s-mesh-ctrl", 12890, runtime.ReachableOptions{Timeout: 60 * time.Second, Interval: 3 * time.Second}, func(ctx context.Context, addr string) error {
		token := zitiAuthenticate(t, client, addr, "admin", "zk8s-admin-pw")
		// There is exactly one mediated service in this scenario; pick it
		// rather than recomputing the URI (the target resolves to the
		// postgres Provider, which CompileMediatedConnections excludes from
		// Targets, so the service is named after the Connection — listing
		// avoids re-deriving that subtlety in the test).
		sn, ferr := zitiFirstServiceName(client, addr, token)
		if ferr != nil {
			return ferr
		}
		serviceName = sn
		_, canaryJWT = zitiCreateUnauthorizedIdentity(t, client, addr, token, "zk8s-canary-unauthorized")
		return nil
	})
	if err != nil {
		t.Fatalf("mint canary identity via controller port-forward: %v", err)
	}
	if serviceName == "" {
		t.Fatal("no mediated service found on the controller")
	}

	const canaryPort = 25899
	_, err = rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:     "zk8s-canary",
		Image:    zitiTunnelImage,
		Networks: []string{ns},
		Env: map[string]string{
			"ZITI_ENROLL_TOKEN":      canaryJWT,
			"ZITI_IDENTITY_BASENAME": "canary",
		},
		Cmd:    []string{"proxy", fmt.Sprintf("%s:%d", serviceName, canaryPort)},
		Ports:  []runtime.PortBinding{{ContainerPort: canaryPort, Audience: runtime.AudienceInternal}},
		Labels: runtime.ManagedLabels(ns, "Provider", "zk8s-canary", "zk8s-canary"),
	})
	if err != nil {
		t.Fatalf("create canary tunneler: %v", err)
	}

	// A dial to the canary's proxy port must FAIL: the canary holds a
	// valid, enrolled identity with full network reachability to the
	// router, but no Dial service-policy authorizes it, so the Ziti
	// circuit is refused and no data path is ever established. Probe from
	// an in-namespace vantage (the router's own peers), bounded — a
	// persistent failure across the window is the refusal.
	deadline := time.Now().Add(40 * time.Second)
	var lastErr error
	sawRefusal := false
	for time.Now().Before(deadline) {
		lastErr = rt.ProbeReachable(ctx, ns, fmt.Sprintf("zk8s-canary:%d", canaryPort))
		if lastErr != nil {
			sawRefusal = true
			break
		}
		time.Sleep(3 * time.Second)
	}
	if !sawRefusal {
		t.Fatal("dial through the canary's unauthorized identity unexpectedly succeeded — the per-edge identity check is not enforcing on Kubernetes")
	}
	t.Logf("k8s wrong-identity dial correctly refused (service %q): %v", serviceName, lastErr)
}

// zitiFirstServiceName returns the name of the single mediated service on
// the controller.
func zitiFirstServiceName(client *http.Client, ctrlAddr, token string) (string, error) {
	req, _ := http.NewRequest(http.MethodGet, "https://"+ctrlAddr+"/edge/management/v1/services?limit=100", nil)
	req.Header.Set("zt-session", token)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	for _, s := range out.Data {
		if strings.HasPrefix(s.Name, "spiffe-datascape-") {
			return s.Name, nil
		}
	}
	return "", fmt.Errorf("no datascape-minted service found (got %d services)", len(out.Data))
}
