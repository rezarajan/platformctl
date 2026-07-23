package kubernetes

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestBuildExternalIngressPolicy is the contract-level reproduction for the
// docs/history/errors.md NodePort/LoadBalancer bug: the namespace default-deny wall (B7)
// silently drops the very external traffic node-port/load-balancer modes (B1)
// exist to admit, so those modes need a per-container hole. The live proof is
// reachability_integration_test.go on a policy-enforcing cluster; this unit
// test pins the pure translation so a regression fails in `go test ./...`.
func TestBuildExternalIngressPolicy(t *testing.T) {
	t.Parallel()
	baseSpec := func(mode string) runtimeport.ContainerSpec {
		return runtimeport.ContainerSpec{
			Name:       "reach",
			Labels:     map[string]string{"io.datascape.kind": "eventstream"},
			AccessMode: mode,
			Ports: []runtimeport.PortBinding{
				{ContainerPort: 80, Protocol: "tcp"},
			},
		}
	}

	t.Run("nil for modes that need no hole", func(t *testing.T) {
		for _, mode := range []string{
			runtimeport.AccessPortForward, // default ""
			runtimeport.AccessInCluster,
		} {
			if p := buildExternalIngressPolicy("ns", baseSpec(mode)); p != nil {
				t.Errorf("mode %q: expected nil policy, got %+v", mode, p)
			}
		}
	})

	t.Run("nil when the external mode declares no ports", func(t *testing.T) {
		spec := baseSpec(runtimeport.AccessNodePort)
		spec.Ports = nil
		if p := buildExternalIngressPolicy("ns", spec); p != nil {
			t.Errorf("no ports: expected nil policy, got %+v", p)
		}
	})

	for _, mode := range []string{runtimeport.AccessNodePort, runtimeport.AccessLoadBalancer} {
		t.Run("opens the wall for "+mode, func(t *testing.T) {
			spec := baseSpec(mode)
			p := buildExternalIngressPolicy("orders", spec)
			if p == nil {
				t.Fatalf("mode %q: expected a policy, got nil", mode)
			}
			if got, want := p.Name, externalIngressPolicyName("reach"); got != want {
				t.Errorf("name = %q, want %q", got, want)
			}
			if got := p.Namespace; got != "orders" {
				t.Errorf("namespace = %q, want %q", got, "orders")
			}
			// The policy must select the pods it means to open — the pod
			// template carries app=<name> (convert.go buildDeployment), so the
			// selector must match on exactly that or the hole opens nothing.
			if got := p.Spec.PodSelector.MatchLabels["app"]; got != "reach" {
				t.Errorf("podSelector app = %q, want %q", got, "reach")
			}
			if got := p.Spec.PolicyTypes; len(got) != 1 || got[0] != networkingv1.PolicyTypeIngress {
				t.Errorf("policyTypes = %v, want [Ingress]", got)
			}
			if got := len(p.Spec.Ingress); got != 1 {
				t.Fatalf("ingress rules = %d, want 1", got)
			}
			rule := p.Spec.Ingress[0]
			// An ingress rule with ports but no `from` admits any source, but
			// only to these ports — the whole point of the hole.
			if len(rule.From) != 0 {
				t.Errorf("ingress From = %v, want empty (admit any source)", rule.From)
			}
			if got := len(rule.Ports); got != 1 {
				t.Fatalf("ingress ports = %d, want 1", got)
			}
			if got := p.Labels[runtimeport.LabelManagedBy]; got != runtimeport.ManagedByValue {
				t.Errorf("missing ownership label: %q = %q", runtimeport.LabelManagedBy, got)
			}
		})
	}

	t.Run("maps udp ports to the UDP protocol", func(t *testing.T) {
		spec := baseSpec(runtimeport.AccessNodePort)
		spec.Ports = []runtimeport.PortBinding{
			{ContainerPort: 53, Protocol: "UDP"}, // case-insensitive
			{ContainerPort: 80, Protocol: "tcp"},
		}
		p := buildExternalIngressPolicy("ns", spec)
		if p == nil {
			t.Fatal("expected a policy, got nil")
		}
		ports := p.Spec.Ingress[0].Ports
		if len(ports) != 2 {
			t.Fatalf("ingress ports = %d, want 2", len(ports))
		}
		if got := *ports[0].Protocol; got != corev1.ProtocolUDP {
			t.Errorf("port[0] protocol = %q, want UDP", got)
		}
		if got := ports[0].Port.IntValue(); got != 53 {
			t.Errorf("port[0] = %d, want 53", got)
		}
		if got := *ports[1].Protocol; got != corev1.ProtocolTCP {
			t.Errorf("port[1] protocol = %q, want TCP", got)
		}
	})

	t.Run("does not mutate the caller's spec labels", func(t *testing.T) {
		spec := baseSpec(runtimeport.AccessNodePort)
		if _, exists := spec.Labels["app"]; exists {
			t.Fatalf("precondition: spec.Labels already has an app key")
		}
		buildExternalIngressPolicy("ns", spec)
		if _, leaked := spec.Labels["app"]; leaked {
			t.Errorf("buildExternalIngressPolicy mutated the caller's spec.Labels")
		}
	})
}

// TestBuildCrossDomainIngressPolicy pins docs/adr/022 Ring 1's Kubernetes
// mapping (docs/planning/08 H5): an allowed cross-domain path opens the
// home namespace's default-deny wall to specific other namespaces by name,
// nothing else — the Kubernetes side of "exactly the holes the mediated
// entrypoint needs" (a Pod lives in exactly one Namespace, unlike a Docker
// container's multi-network attach, so this NetworkPolicy is the mechanism
// instead).
func TestBuildCrossDomainIngressPolicy(t *testing.T) {
	t.Parallel()
	t.Run("nil when no domain is allowed in", func(t *testing.T) {
		if p := buildCrossDomainIngressPolicy("analytics", nil, nil); p != nil {
			t.Errorf("expected nil policy for an empty allow-list, got %+v", p)
		}
	})

	t.Run("admits exactly the named namespaces", func(t *testing.T) {
		p := buildCrossDomainIngressPolicy("datascape-analytics", map[string]string{"io.datascape.kind": "connection"}, []string{"datascape-payments"})
		if p == nil {
			t.Fatal("expected a policy, got nil")
		}
		if got, want := p.Name, crossDomainIngressPolicyName; got != want {
			t.Errorf("name = %q, want %q", got, want)
		}
		if got := p.Namespace; got != "datascape-analytics" {
			t.Errorf("namespace = %q, want %q", got, "datascape-analytics")
		}
		if got := p.Spec.PolicyTypes; len(got) != 1 || got[0] != networkingv1.PolicyTypeIngress {
			t.Errorf("policyTypes = %v, want [Ingress]", got)
		}
		if got := len(p.Spec.Ingress); got != 1 {
			t.Fatalf("ingress rules = %d, want 1", got)
		}
		peers := p.Spec.Ingress[0].From
		if len(peers) != 1 {
			t.Fatalf("peers = %d, want 1", len(peers))
		}
		if peers[0].NamespaceSelector == nil {
			t.Fatal("expected a namespaceSelector peer")
		}
		if got := peers[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"]; got != "datascape-payments" {
			t.Errorf("namespaceSelector match = %q, want %q", got, "datascape-payments")
		}
		if got := p.Labels[runtimeport.LabelManagedBy]; got != runtimeport.ManagedByValue {
			t.Errorf("missing ownership label: %q = %q", runtimeport.LabelManagedBy, got)
		}
	})

	t.Run("multiple allowed namespaces each get their own peer", func(t *testing.T) {
		p := buildCrossDomainIngressPolicy("datascape-analytics", nil, []string{"datascape-payments", "datascape-billing"})
		if p == nil {
			t.Fatal("expected a policy, got nil")
		}
		if got := len(p.Spec.Ingress[0].From); got != 2 {
			t.Fatalf("peers = %d, want 2", got)
		}
	})
}

// TestBuildNetworkPoliciesGraphScoped pins docs/adr/026 H7's core
// Kubernetes claim: "per-edge NetworkPolicies replace allow-same-namespace
// under the gate" — graphScoped=true must drop the allow-same-namespace
// rule entirely (default-deny only), while graphScoped=false keeps
// producing the exact pre-H7 pair, unchanged (the gate-off pin).
func TestBuildNetworkPoliciesGraphScoped(t *testing.T) {
	t.Run("gate off: default-deny + allow-same-namespace, unchanged", func(t *testing.T) {
		policies := buildNetworkPolicies("ns", nil, false)
		if len(policies) != 2 {
			t.Fatalf("got %d policies, want 2 (default-deny + allow-same-namespace)", len(policies))
		}
		names := map[string]bool{}
		for _, p := range policies {
			names[p.Name] = true
		}
		if !names[denyAllIngressPolicyName] || !names[allowSameNamespacePolicyName] {
			t.Errorf("expected both %q and %q, got %v", denyAllIngressPolicyName, allowSameNamespacePolicyName, names)
		}
	})

	t.Run("gate on: default-deny only", func(t *testing.T) {
		policies := buildNetworkPolicies("ns", nil, true)
		if len(policies) != 1 {
			t.Fatalf("got %d policies, want 1 (default-deny only)", len(policies))
		}
		if policies[0].Name != denyAllIngressPolicyName {
			t.Errorf("policy = %q, want %q", policies[0].Name, denyAllIngressPolicyName)
		}
	})
}

// TestBuildGraphScopedIngressPolicy pins the per-container half of
// docs/adr/026 H7's Kubernetes realization: exactly the declared peers,
// each expressed as a namespace-selector + pod-selector pair (a Pod lives
// in exactly one Namespace, so this is the K8s-native way to express "this
// SPECIFIC pod in that namespace, not the whole namespace" —
// buildCrossDomainIngressPolicy's namespace-wide peer generalized down to
// resource granularity).
func TestBuildGraphScopedIngressPolicy(t *testing.T) {
	t.Run("nil when no peer is declared", func(t *testing.T) {
		spec := runtimeport.ContainerSpec{Name: "x"}
		if p := buildGraphScopedIngressPolicy("b", spec); p != nil {
			t.Errorf("expected nil policy for no declared peers, got %+v", p)
		}
	})

	t.Run("admits exactly the declared peers", func(t *testing.T) {
		spec := runtimeport.ContainerSpec{
			Name:   "x",
			Labels: map[string]string{"io.datascape.kind": "provider"},
			AllowFromPeers: []runtimeport.NetworkPeer{
				{Network: "a", Name: "r1"},
				{Network: "a", Name: "r2"},
			},
		}
		p := buildGraphScopedIngressPolicy("b", spec)
		if p == nil {
			t.Fatal("expected a policy, got nil")
		}
		if got, want := p.Name, graphScopedIngressPolicyName("x"); got != want {
			t.Errorf("name = %q, want %q", got, want)
		}
		if got := p.Namespace; got != "b" {
			t.Errorf("namespace = %q, want %q", got, "b")
		}
		if got := p.Spec.PodSelector.MatchLabels["app"]; got != "x" {
			t.Errorf("podSelector app = %q, want %q", got, "x")
		}
		if got := p.Spec.PolicyTypes; len(got) != 1 || got[0] != networkingv1.PolicyTypeIngress {
			t.Errorf("policyTypes = %v, want [Ingress]", got)
		}
		peers := p.Spec.Ingress[0].From
		if len(peers) != 2 {
			t.Fatalf("peers = %d, want 2", len(peers))
		}
		for i, want := range []runtimeport.NetworkPeer{{Network: "a", Name: "r1"}, {Network: "a", Name: "r2"}} {
			if peers[i].NamespaceSelector == nil || peers[i].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != want.Network {
				t.Errorf("peer[%d] namespaceSelector = %+v, want namespace %q", i, peers[i].NamespaceSelector, want.Network)
			}
			if peers[i].PodSelector == nil || peers[i].PodSelector.MatchLabels["app"] != want.Name {
				t.Errorf("peer[%d] podSelector = %+v, want app %q", i, peers[i].PodSelector, want.Name)
			}
		}
		if got := p.Labels[runtimeport.LabelManagedBy]; got != runtimeport.ManagedByValue {
			t.Errorf("missing ownership label: %q = %q", runtimeport.LabelManagedBy, got)
		}
	})

	t.Run("does not mutate the caller's spec labels", func(t *testing.T) {
		spec := runtimeport.ContainerSpec{
			Name:           "x",
			AllowFromPeers: []runtimeport.NetworkPeer{{Network: "a", Name: "r1"}},
		}
		if _, exists := spec.Labels["app"]; exists {
			t.Fatalf("precondition: spec.Labels already has an app key")
		}
		buildGraphScopedIngressPolicy("b", spec)
		if _, leaked := spec.Labels["app"]; leaked {
			t.Errorf("buildGraphScopedIngressPolicy mutated the caller's spec.Labels")
		}
	})
}
