package ingress

import (
	"context"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// shrinkRouteSettle lowers routeSettleTimeout/routeSettlePoll for the
// duration of a test, restoring them on cleanup — avoids waiting out a
// real 45s timeout to exercise the honest-failure path (docs/planning/11
// B1 finding 2).
func shrinkRouteSettle(t *testing.T) {
	t.Helper()
	prevTimeout, prevPoll := routeSettleTimeout, routeSettlePoll
	routeSettleTimeout = 150 * time.Millisecond
	routeSettlePoll = 20 * time.Millisecond
	t.Cleanup(func() {
		routeSettleTimeout, routeSettlePoll = prevTimeout, prevPoll
	})
}

// mustPort extracts the numeric port an httptest.Server is listening on.
func mustPort(t *testing.T, rawURL string) int {
	t.Helper()
	_, portStr, ok := strings.Cut(strings.TrimPrefix(rawURL, "http://"), ":")
	if !ok {
		t.Fatalf("could not parse port from %q", rawURL)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("could not parse port from %q: %v", rawURL, err)
	}
	return port
}

// freeTCPPort returns a currently-unused TCP port on 127.0.0.1 by binding
// and immediately releasing it — used to simulate "nothing answers here"
// for the shared proxy's http port.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// seedProxyContainer registers the shared Caddy container the Docker path
// expects to already exist (reconcileInstanceDocker's job, out of scope
// here), with its admin port bound to a real fake-Caddy admin server and
// its http port bound to whatever the caller wants dialable.
func seedProxyContainer(t *testing.T, ctx context.Context, rt runtime.ContainerRuntime, name string, adminPort, httpPort int) {
	t.Helper()
	if _, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name: name,
		Ports: []runtime.PortBinding{
			{HostPort: adminPort, ContainerPort: adminContainerPort, Audience: runtime.AudienceHost},
			{HostPort: httpPort, ContainerPort: httpContainerPort, Audience: runtime.AudienceHost},
		},
	}); err != nil {
		t.Fatalf("seed proxy container: %v", err)
	}
}

func ingressProviderEnvelope(name string) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Provider"},
		Metadata:         resource.Metadata{Name: name},
		Spec: map[string]any{
			"type":    "ingress",
			"runtime": map[string]any{"type": "docker"},
		},
	}
}

func ingressConnectionEnvelope(name, providerRef string, port int, target string) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Connection"},
		Metadata:         resource.Metadata{Name: name},
		Spec: map[string]any{
			"scheme":      "http",
			"providerRef": map[string]any{"name": providerRef},
			"port":        port,
			"target":      target,
		},
	}
}

// TestReconcileConnectionDockerFailsHonestlyWhenRouteNeverServes is the I4
// acceptance bar (docs/planning/08 §7.8): reconcile must not set Ready from
// the admin API accepting the route write alone — it must dial through the
// route (probeThroughRoute, the same check probeConnectionDocker uses) and
// fail honestly, naming the last observed state, when nothing ever answers
// on the shared proxy's published http port (docs/planning/11 B1 finding
// 2).
func TestReconcileConnectionDockerFailsHonestlyWhenRouteNeverServes(t *testing.T) {
	shrinkRouteSettle(t)

	ctx := context.Background()
	rt := fakeruntime.New()

	admin := newFakeCaddyAdmin()
	defer admin.Close()

	proxyName := "edge"
	// Nothing listens on this port — reconcileConnectionDocker writes the
	// route fine (against the real fake-admin server) but the settle poll's
	// dial-through-route must never see it answer.
	httpPort := freeTCPPort(t)
	seedProxyContainer(t, ctx, rt, proxyName, mustPort(t, admin.URL), httpPort)

	provEnv := ingressProviderEnvelope(proxyName)
	connEnv := ingressConnectionEnvelope("nessie", proxyName, 8080, "nessie:19120")
	cfg, err := provider.FromEnvelope(provEnv)
	if err != nil {
		t.Fatalf("provider.FromEnvelope: %v", err)
	}

	req := reconciler.Request{Resource: connEnv, Provider: provEnv, Runtime: rt}
	st, err := reconcileConnectionDocker(ctx, req, cfg)
	if err == nil {
		t.Fatal("expected reconcileConnectionDocker to fail honestly when the route never serves, got nil error")
	}
	if !strings.Contains(err.Error(), "did not settle") {
		t.Errorf("error = %q, want it to name the settle timeout (honest failure)", err.Error())
	}
	if ready, ok := st.Condition(status.Ready); ok && ready.Status == status.True {
		t.Error("status must not report Ready when the route never served")
	}
}

// TestReconcileConnectionDockerSucceedsWhenRouteServes is the mirror
// positive case: once the shared proxy's http port actually answers,
// reconcile settles and reports Ready — the settle poll must not regress
// the healthy path.
func TestReconcileConnectionDockerSucceedsWhenRouteServes(t *testing.T) {
	shrinkRouteSettle(t)

	ctx := context.Background()
	rt := fakeruntime.New()

	admin := newFakeCaddyAdmin()
	defer admin.Close()

	// A plain 200-OK server stands in for Caddy actually serving the
	// route: probeThroughRoute only cares that the response isn't a
	// 502/504 (docs/planning/11 B1's "beyond container health, dial
	// through it" discipline), so any non-gateway-error response proves
	// the point without needing a real Caddy route dispatch.
	upstream := httptest.NewServer(nil)
	defer upstream.Close()

	proxyName := "edge"
	seedProxyContainer(t, ctx, rt, proxyName, mustPort(t, admin.URL), mustPort(t, upstream.URL))

	provEnv := ingressProviderEnvelope(proxyName)
	connEnv := ingressConnectionEnvelope("nessie", proxyName, 8080, "nessie:19120")
	cfg, err := provider.FromEnvelope(provEnv)
	if err != nil {
		t.Fatalf("provider.FromEnvelope: %v", err)
	}

	req := reconciler.Request{Resource: connEnv, Provider: provEnv, Runtime: rt}
	st, err := reconcileConnectionDocker(ctx, req, cfg)
	if err != nil {
		t.Fatalf("reconcileConnectionDocker: %v", err)
	}
	if !st.IsReady() {
		t.Error("expected Ready once the route serves")
	}
}
