package wireguard

import (
	"context"
	"strings"
	"testing"
	"time"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// shrinkTunnelSettle lowers tunnelSettleTimeout/tunnelSettlePoll for the
// duration of a test, restoring them on cleanup — avoids waiting out a real
// 45s timeout to exercise the honest-failure path (docs/planning/11 B1
// finding 1).
func shrinkTunnelSettle(t *testing.T) {
	t.Helper()
	prevTimeout, prevPoll, prevReachable, prevInterval := tunnelSettleTimeout, tunnelSettlePoll, tunnelReachableTimeout, tunnelReachableInterval
	tunnelSettleTimeout = 150 * time.Millisecond
	tunnelSettlePoll = 20 * time.Millisecond
	tunnelReachableTimeout = 20 * time.Millisecond
	tunnelReachableInterval = 5 * time.Millisecond
	t.Cleanup(func() {
		tunnelSettleTimeout, tunnelSettlePoll, tunnelReachableTimeout, tunnelReachableInterval = prevTimeout, prevPoll, prevReachable, prevInterval
	})
}

func tunnelProviderEnvelope(name string, port int) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Provider"},
		Metadata:         resource.Metadata{Name: name},
		Spec: map[string]any{
			"type":    "wireguard",
			"runtime": map[string]any{"type": "docker"},
			"configuration": map[string]any{
				"peerNetwork":   "wg-net",
				"peerPublicKey": "cGVlcicgcHVibGljIGtleQ==",
				"peerEndpoint":  "203.0.113.1:51820",
				"address":       "10.10.0.2/24",
				"allowedIPs":    []any{"10.10.0.0/24"},
			},
			"secretRefs": []any{"wg-key"},
		},
	}
}

func tunnelConnectionEnvelope(name, providerRef string, port int, target string) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Connection"},
		Metadata:         resource.Metadata{Name: name},
		Spec: map[string]any{
			"providerRef": map[string]any{"name": providerRef},
			"port":        port,
			"target":      target,
		},
	}
}

// TestReconcileConnectionFailsHonestlyWhenUpstreamNeverAnswers is the I4
// acceptance bar (docs/planning/08 §7.8): reconcile must not set Ready from
// the tunnel container's own healthcheck alone (wg0 existing says nothing
// about whether the peer handshaked or the upstream answers) — it must
// dial through the forwarder to the upstream (dialUpstream, the same check
// Probe uses) and fail honestly, naming the last observed state, when
// nothing ever answers (docs/planning/11 B1 finding 1, CONFIRMED, the
// redpanda-93fbf14 signature).
func TestReconcileConnectionFailsHonestlyWhenUpstreamNeverAnswers(t *testing.T) {
	shrinkTunnelSettle(t)

	ctx := context.Background()
	rt := fakeruntime.New()
	p := New()

	provEnv := tunnelProviderEnvelope("vpc-tunnel", 0)
	// Nothing in this test environment listens on this port — every dial
	// attempt gets connection-refused, simulating an upstream that never
	// answers through the tunnel's forwarder rule.
	connEnv := tunnelConnectionEnvelope("db-tunnel", "vpc-tunnel", 58241, "10.10.0.5:5432")

	req := reconciler.Request{
		Resource: connEnv,
		Provider: provEnv,
		Runtime:  rt,
		Secrets:  map[string]map[string]string{"wg-key": {"privateKey": "cHJpdmF0ZScga2V5"}},
	}
	st, err := p.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected Reconcile to fail honestly when the upstream never answers, got nil error")
	}
	if !strings.Contains(err.Error(), "did not settle") {
		t.Errorf("error = %q, want it to name the settle timeout (honest failure)", err.Error())
	}
	if !strings.Contains(err.Error(), "unreachable through tunnel") {
		t.Errorf("error = %q, want it to name the last observed state (upstream unreachable)", err.Error())
	}
	if ready, ok := st.Condition(status.Ready); ok && ready.Status == status.True {
		t.Error("status must not report Ready when the upstream never answered")
	}
}
