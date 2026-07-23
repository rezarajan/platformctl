package wireguard

import (
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/reconciler/conformance"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestConformance is the E6 conformance-suite retrofit's exemplar for
// wireguard (docs/planning/08 E6 done-note's recorded follow-up; ADR 028):
// drives the Provider kind's Reconcile/Probe/Destroy. Unlike every other
// exemplar in this retrofit, wireguard's own Provider resource anchors no
// container of its own — reconcileInstance is two EnsureNetwork calls plus
// reconcileViaTunnels, which is a documented no-op whenever (as in every
// fixture this suite builds) req.Resources declares no Connection naming
// this Provider via spec.via (docs/planning/08 I1). This makes it the
// LIGHTEST exemplar in this retrofit — lighter even than placeholder's
// single container — and it needed zero real-protocol scoping decision to
// get there.
//
// The Connection kind (reconcileConnection/probeTunnelServing) is OUT of
// this fast-tier suite for a reason one step past every other scoped-out
// Kind in this retrofit: it isn't only a real-protocol dial
// (dialUpstream's raw TCP connect could, on its own, reuse
// internal/adapters/providers/proxy's real-net.Listener trick). Ready also
// requires a genuinely recent WireGuard peer handshake, observed by
// reading a status file (handshakeAge) that the tunnel container's own
// boot script writes AT RUNTIME (buildEntrypointScript's backgrounded `wg
// show` poller) — content no ContainerSpec.Files declaration can pre-seed
// and the fake runtime has no mechanism to fabricate (fake.Runtime.ReadFile
// only ever answers from a container's declared Files or a persisted
// volume, never from something a "process" inside the fake container
// wrote). waitTunnelServing's settle loop requires !stale to ever return
// nil, so this handshake-file gap alone blocks Ready — no amount of
// dial-through faking closes it. Covered instead by the Docker integration
// suite (cmd/platformctl's wireguard scenarios).
//
// CapabilityChecks exercises ValidateSpec's two independent rules: the
// required tunnelConfig fields (peerNetwork/peerPublicKey/peerEndpoint/
// address/allowedIPs) and configuration.privateKeySecretRef's
// spec.secretRefs wiring.
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Harness{
		NewRuntime: func() runtime.ContainerRuntime { return fakeruntime.New() },
		Provider:   func() reconciler.Provider { return New() },
		Resource: func(rt runtime.ContainerRuntime, namePrefix string, i int) reconciler.Request {
			name := namePrefix + "-a"
			if i == 1 {
				name = namePrefix + "-b"
			}
			env := tunnelProviderEnvelope(name, 51820)
			return reconciler.Request{
				Resource: env,
				Provider: env,
				Runtime:  rt,
				Facts:    reconciler.StaticFacts{},
			}
		},
		CapabilityChecks: func(p reconciler.Provider) []conformance.CapabilityCheck {
			sv := p.(reconciler.SpecValidator)
			return []conformance.CapabilityCheck{
				{
					Name: "ValidateSpec/missing-required-tunnel-config",
					Invoke: func() error {
						return sv.ValidateSpec(provider.Provider{
							Type:          "wireguard",
							Configuration: map[string]any{},
							SecretRefs:    []string{"wg-key"},
						})
					},
					WantSubstrings: []string{"configuration.peerNetwork", "is required"},
				},
				{
					Name: "ValidateSpec/privateKeySecretRef-not-wired",
					Invoke: func() error {
						return sv.ValidateSpec(provider.Provider{
							Type: "wireguard",
							Configuration: map[string]any{
								"peerNetwork":         "wg-net",
								"peerPublicKey":       "cGVlcicgcHVibGljIGtleQ==",
								"peerEndpoint":        "203.0.113.1:51820",
								"address":             "10.10.0.2/24",
								"allowedIPs":          []any{"10.10.0.0/24"},
								"privateKeySecretRef": "wg-key",
							},
							SecretRefs: nil, // deliberately not listed
						})
					},
					WantSubstrings: []string{"privateKeySecretRef", "must also be listed in spec.secretRefs"},
				},
			}
		},
	})
}
