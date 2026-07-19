//go:build integration

package kubernetes

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestEnsureReachable covers docs/planning/08 B1 (access modes) and B2
// (observed endpoints) against a real cluster: each access mode gets a
// genuinely dialable "outside the cluster" address serving real
// application-layer bytes — not just a plausible-looking string — except
// in-cluster, which must refuse.
func TestEnsureReachable(t *testing.T) {
	require := os.Getenv("PLATFORMCTL_REQUIRE_K8S") != ""
	rt, err := New(nil)
	if err != nil {
		if require {
			t.Fatalf("connect to kubernetes (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("no kubernetes configuration; skipping: %v", err)
	}
	if _, err := rt.clientset.Discovery().ServerVersion(); err != nil {
		if require {
			t.Fatalf("kubernetes cluster unreachable (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("kubernetes cluster unreachable; skipping: %v", err)
	}

	ctx := context.Background()
	const ns = "datascape-reachability-test"
	labels := map[string]string{
		runtimeport.LabelManagedBy:  runtimeport.ManagedByValue,
		runtimeport.LabelGeneration: "reachability-test",
	}
	t.Cleanup(func() { _ = rt.RemoveNetwork(ctx, ns) })

	if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}

	// A real HTTP server (nginx's own image default, no custom command) so
	// a successful dial proves a genuine application-layer round trip, not
	// merely that some socket accepted a TCP handshake.
	deploy := func(t *testing.T, name, accessMode string) {
		t.Helper()
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtimeport.ContainerSpec{
			Name:       name,
			Image:      "nginx:1.27-alpine",
			Networks:   []string{ns},
			Ports:      []runtimeport.PortBinding{{ContainerPort: 80, Audience: runtimeport.AudienceHost}},
			Labels:     labels,
			AccessMode: accessMode,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}
	}

	fetch := func(t *testing.T, addr string) string {
		t.Helper()
		client := http.Client{Timeout: 10 * time.Second}
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatalf("GET http://%s/: %v", addr, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET http://%s/: status %d, body %q", addr, resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "nginx") {
			t.Fatalf("GET http://%s/: body doesn't look like nginx's default page: %q", addr, body)
		}
		return string(body)
	}

	t.Run("port_forward_default_mode_opens_and_closes_a_real_tunnel", func(t *testing.T) {
		const name = "datascape-reach-pf"
		deploy(t, name, runtimeport.AccessPortForward)

		addr, closeFn, err := rt.EnsureReachable(ctx, name, 80)
		if err != nil {
			t.Fatalf("EnsureReachable: %v", err)
		}
		if !strings.HasPrefix(addr, "127.0.0.1:") {
			t.Fatalf("port-forward address = %q, want a 127.0.0.1: local tunnel address", addr)
		}
		fetch(t, addr)
		if err := closeFn(); err != nil {
			t.Fatalf("close: %v", err)
		}
		// The tunnel is torn down: a fresh dial to the same local port must
		// eventually fail (nothing else could legitimately be listening on
		// an OS-assigned ephemeral port we only just released). close()
		// only signals the forwarder's stop channel — actual listener
		// teardown happens asynchronously — so poll briefly rather than
		// asserting it's instantaneous.
		deadline := time.Now().Add(5 * time.Second)
		for {
			conn, dialErr := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if dialErr != nil {
				break
			}
			conn.Close()
			if time.Now().After(deadline) {
				t.Fatalf("still able to dial %q 5s after close(); tunnel was not torn down", addr)
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("node_port_mode_is_reachable_and_observed_by_inspect", func(t *testing.T) {
		const name = "datascape-reach-np"
		deploy(t, name, runtimeport.AccessNodePort)

		addr, closeFn, err := rt.EnsureReachable(ctx, name, 80)
		if err != nil {
			t.Fatalf("EnsureReachable: %v", err)
		}
		defer closeFn()
		if strings.HasPrefix(addr, "127.0.0.1:") {
			t.Fatalf("node-port address = %q, want a real node address, not loopback", addr)
		}
		fetch(t, addr)

		// B2: Inspect must report this same host-reachable address, since
		// `platformctl inventory` is built on Inspect/ListManaged.
		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect: found=%v err=%v", found, err)
		}
		var port *runtimeport.PortBinding
		for i := range st.Ports {
			if st.Ports[i].ContainerPort == 80 {
				port = &st.Ports[i]
			}
		}
		if port == nil {
			t.Fatalf("Inspect did not report container port 80; ports = %+v", st.Ports)
		}
		if port.HostIP == "" || port.HostPort == 0 {
			t.Errorf("Inspect did not report an observed host address for node-port mode: %+v", *port)
		}
		if got := net.JoinHostPort(port.HostIP, strconv.Itoa(port.HostPort)); got != addr {
			t.Errorf("Inspect-observed address %q != EnsureReachable address %q", got, addr)
		}

		svc, err := rt.clientset.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get service: %v", err)
		}
		if svc.Spec.Type != "NodePort" {
			t.Errorf("service type = %q, want NodePort", svc.Spec.Type)
		}
	})

	t.Run("in_cluster_mode_refuses_naming_the_mode", func(t *testing.T) {
		const name = "datascape-reach-ic"
		deploy(t, name, runtimeport.AccessInCluster)

		_, _, err := rt.EnsureReachable(ctx, name, 80)
		if err == nil {
			t.Fatal("EnsureReachable succeeded for in-cluster access mode, want refusal")
		}
		if !strings.Contains(err.Error(), "in-cluster") {
			t.Errorf("refusal error doesn't name the access mode: %v", err)
		}
	})
}
