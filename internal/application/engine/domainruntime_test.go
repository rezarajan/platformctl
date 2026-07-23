package engine

import (
	"context"
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func envWithDomain(kind, name, namespace, domain string, spec map[string]any) resource.Envelope {
	if spec == nil {
		spec = map[string]any{}
	}
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: kind},
		Metadata:         resource.Metadata{Name: name, Namespace: namespace, Domain: domain},
		Spec:             spec,
	}
}

// TestDomainRuntimeUndeclaredDomainIsByteIdenticalNoOp is docs/planning/08
// H5's back-compat pin at the decorator itself: a resource with no declared
// domain (or "default") must translate the platform-network token to
// itself, unchanged.
func TestDomainRuntimeUndeclaredDomainIsByteIdenticalNoOp(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := envWithDomain("Provider", "pg", "default", "", nil)
	d := newDomainRuntime(rt, map[string]any{}, env, env, nil, false, false, nil, "fake", nil)

	if err := d.EnsureNetwork(context.Background(), runtime.NetworkSpec{Name: "datascape", Labels: runtime.ManagedLabels("default", "Provider", "pg", "pg")}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}
	nets, err := rt.ListManagedNetworks(context.Background())
	if err != nil {
		t.Fatalf("ListManagedNetworks: %v", err)
	}
	if len(nets) != 1 || nets[0].Name != "datascape" {
		t.Fatalf("ListManagedNetworks() = %v, want exactly [\"datascape\"] (undeclared domain: byte-identical no-op)", nets)
	}
}

// TestDomainRuntimeScopesTheDefaultToken proves Ring 1 (docs/adr/022): a
// resource declaring a non-default domain gets a domain-scoped concrete
// network name for the platform-network token, with zero provider-side
// signal — this decorator is the only place the translation happens.
func TestDomainRuntimeScopesTheDefaultToken(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := envWithDomain("Provider", "pg", "default", "payments", nil)
	d := newDomainRuntime(rt, map[string]any{}, env, env, nil, false, false, nil, "fake", nil)

	if err := d.EnsureNetwork(context.Background(), runtime.NetworkSpec{Name: "datascape", Labels: runtime.ManagedLabels("default", "Provider", "pg", "pg")}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}
	nets, err := rt.ListManagedNetworks(context.Background())
	if err != nil {
		t.Fatalf("ListManagedNetworks: %v", err)
	}
	if len(nets) != 1 || nets[0].Name != "datascape-payments" {
		t.Fatalf("ListManagedNetworks() = %v, want exactly [\"datascape-payments\"]", nets)
	}
}

// TestDomainRuntimePinnedOverrideNeverScoped is docs/planning/08 H5's
// "explicit pin wins" rule (the owner's correction): a configured
// spec.runtime.network override passes through VERBATIM even in a
// non-default domain — never domain-scoped.
func TestDomainRuntimePinnedOverrideNeverScoped(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := envWithDomain("Provider", "pg", "default", "payments", nil)
	d := newDomainRuntime(rt, map[string]any{"network": "custom-net"}, env, env, nil, false, false, nil, "fake", nil)

	if err := d.EnsureNetwork(context.Background(), runtime.NetworkSpec{Name: "custom-net", Labels: runtime.ManagedLabels("default", "Provider", "pg", "pg")}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}
	nets, err := rt.ListManagedNetworks(context.Background())
	if err != nil {
		t.Fatalf("ListManagedNetworks: %v", err)
	}
	if len(nets) != 1 || nets[0].Name != "custom-net" {
		t.Fatalf("ListManagedNetworks() = %v, want exactly [\"custom-net\"] (explicit pin wins in every domain)", nets)
	}
}

// TestDomainRuntimeNonTokenNamePassesThroughVerbatim proves a network name
// a provider computed for its own unrelated purpose (docs/planning/08 I1's
// transit network is the shipped example) is never touched — only a call
// naming EXACTLY the resolved token is domain-scoped.
func TestDomainRuntimeNonTokenNamePassesThroughVerbatim(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	env := envWithDomain("Connection", "bridge", "default", "payments", nil)
	d := newDomainRuntime(rt, map[string]any{}, env, env, nil, false, false, nil, "fake", nil)

	if err := d.EnsureNetwork(context.Background(), runtime.NetworkSpec{Name: "datascape-vpc-transit", Labels: runtime.ManagedLabels("default", "Connection", "bridge", "bridge")}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}
	nets, err := rt.ListManagedNetworks(context.Background())
	if err != nil {
		t.Fatalf("ListManagedNetworks: %v", err)
	}
	if len(nets) != 1 || nets[0].Name != "datascape-vpc-transit" {
		t.Fatalf("ListManagedNetworks() = %v, want exactly [\"datascape-vpc-transit\"] unchanged (not the token)", nets)
	}
}

// TestDomainRuntimeConnectionOpensHolesForCrossDomainConsumers is
// docs/adr/022 Ring 1's core realization, entirely engine-side: a
// Connection in domain "analytics" consumed (via connectionRef) by a
// resource in domain "payments" gets that domain's network attached too —
// "exactly the holes the mediated entrypoint needs" — with zero code in
// internal/adapters/providers/proxy.
func TestDomainRuntimeConnectionOpensHolesForCrossDomainConsumers(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	conn := envWithDomain("Connection", "bridge", "default", "analytics", nil)
	consumer := envWithDomain("Provider", "payments-src", "default", "payments", map[string]any{
		"connectionRef": map[string]any{"name": "bridge"},
	})
	sameDomain := envWithDomain("Provider", "analytics-src", "default", "analytics", map[string]any{
		"connectionRef": map[string]any{"name": "bridge"},
	})
	byKey := map[resource.Key]resource.Envelope{
		consumer.Key():   consumer,
		sameDomain.Key(): sameDomain,
	}
	d := newDomainRuntime(rt, map[string]any{}, conn, conn, byKey, false, false, nil, "fake", nil)

	if err := d.EnsureNetwork(context.Background(), runtime.NetworkSpec{Name: "datascape", Labels: runtime.ManagedLabels("default", "Provider", "pg", "pg")}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}
	if _, err := d.EnsureContainer(context.Background(), runtime.ContainerSpec{
		Name: "bridge", Image: "x", Networks: []string{"datascape"},
		Ports: []runtime.PortBinding{{ContainerPort: 1, Audience: runtime.AudienceInternal}},
	}); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}

	dial := "bridge:1"
	if err := rt.ProbeReachable(context.Background(), "datascape-analytics", dial); err != nil {
		t.Errorf("bridge not attached to its home domain network: %v", err)
	}
	if err := rt.ProbeReachable(context.Background(), "datascape-payments", dial); err != nil {
		t.Errorf("bridge not attached to the consumer's domain network (the hole): %v", err)
	}
	// Blast-minimized: no bare "datascape" (replaced by the home domain
	// network, not added to), and no third domain.
	if err := rt.ProbeReachable(context.Background(), "datascape", dial); err == nil {
		t.Error("bridge must not be attached to the bare (undeclared-domain) network")
	}
	if err := rt.ProbeReachable(context.Background(), "datascape-other", dial); err == nil {
		t.Error("bridge must not be attached to any network beyond [home domain, consumer domain]")
	}
}

// TestDomainRuntimeUsesProviderDomainOfRecord pins the docs/adr/022
// addendum: a dependent resource's reconcile addresses its REALIZING
// PROVIDER's networks — an EventStream declared in one domain but
// realized by a Provider in another must translate the token to the
// provider's domain (the containers live there), never its own.
func TestDomainRuntimeUsesProviderDomainOfRecord(t *testing.T) {
	t.Parallel()
	rt := fakeruntime.New()
	provEnv := envWithDomain("Provider", "broker", "default", "infra", nil)
	esEnv := envWithDomain("EventStream", "events", "default", "analytics", nil)

	d := newDomainRuntime(rt, map[string]any{}, provEnv, esEnv, nil, false, false, nil, "fake", nil)
	if err := d.EnsureNetwork(context.Background(), runtime.NetworkSpec{Name: "datascape", Labels: runtime.ManagedLabels("default", "Provider", "broker", "broker")}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}
	nets, err := rt.ListManagedNetworks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nets) != 1 || nets[0].Name != "datascape-infra" {
		t.Fatalf("token translated to %v; want the PROVIDER's domain network datascape-infra", nets)
	}
}

// TestDomainRuntimeQualifyTargetAddress pins docs/planning/08 H9's
// runtime.AddressQualifier realization — the domain-of-record FQDN fix for
// a mediated Connection's bind-side target, which is handed to the
// mediator's control-plane API directly and therefore never passes through
// this decorator's normal EnsureContainer/EnsureNetwork translation:
//
//   - non-namespaced runtimes (docker/fake): ALWAYS a no-op — reachability
//     there is network membership, which name qualification cannot
//     substitute for;
//   - kubernetes, same domain (or both default): no-op — a bare name
//     resolves in-namespace, and the undeclared-domain byte-identical rule
//     extends here;
//   - kubernetes, different domains: host is qualified to the TARGET's
//     domain-scoped namespace's cluster-DNS FQDN;
//   - an unparseable hostport passes through unchanged, never an error.
func TestDomainRuntimeQualifyTargetAddress(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	target := envWithDomain("Provider", "xd-pg", "default", "payments", nil)
	caller := envWithDomain("Connection", "xd-conn", "default", "analytics", nil)

	// Non-namespaced (fake stands in for docker): verbatim pass-through
	// even across domains.
	dDocker := newDomainRuntime(fakeruntime.New(), map[string]any{}, caller, caller, nil, false, false, nil, "fake", nil).(*domainRuntime)
	if got, err := dDocker.QualifyTargetAddress(ctx, target, caller, "xd-pg:5432"); err != nil || got != "xd-pg:5432" {
		t.Errorf("docker-shaped runtime: QualifyTargetAddress = (%q, %v), want verbatim pass-through", got, err)
	}

	// Kubernetes, cross-domain: qualified to the target's domain-scoped
	// namespace FQDN.
	dK8s := newDomainRuntime(fakeruntime.New(), map[string]any{}, caller, caller, nil, false, false, nil, "kubernetes", nil).(*domainRuntime)
	if got, err := dK8s.QualifyTargetAddress(ctx, target, caller, "xd-pg:5432"); err != nil || got != "xd-pg.datascape-payments.svc.cluster.local:5432" {
		t.Errorf("kubernetes cross-domain: QualifyTargetAddress = (%q, %v), want xd-pg.datascape-payments.svc.cluster.local:5432", got, err)
	}

	// Kubernetes, same domain: no-op.
	sameDomain := envWithDomain("Provider", "xd-pg", "default", "analytics", nil)
	if got, err := dK8s.QualifyTargetAddress(ctx, sameDomain, caller, "xd-pg:5432"); err != nil || got != "xd-pg:5432" {
		t.Errorf("kubernetes same-domain: QualifyTargetAddress = (%q, %v), want verbatim pass-through", got, err)
	}

	// Kubernetes, both undeclared/default: byte-identical no-op.
	defTarget := envWithDomain("Provider", "pg", "default", "", nil)
	defCaller := envWithDomain("Connection", "conn", "default", "", nil)
	if got, err := dK8s.QualifyTargetAddress(ctx, defTarget, defCaller, "pg:5432"); err != nil || got != "pg:5432" {
		t.Errorf("kubernetes default-domain: QualifyTargetAddress = (%q, %v), want byte-identical pass-through", got, err)
	}

	// Unparseable hostport: unchanged, no error.
	if got, err := dK8s.QualifyTargetAddress(ctx, target, caller, "not-a-hostport"); err != nil || got != "not-a-hostport" {
		t.Errorf("unparseable hostport: QualifyTargetAddress = (%q, %v), want unchanged + nil error", got, err)
	}

	// The explicit-override pin (docs/planning/08 H5's configured-value-
	// always-wins rule) SUPPRESSES qualification: a pinned network passes
	// through verbatim in every domain (translate()'s rule), so on
	// Kubernetes every domain shares the one pinned namespace — a bare
	// name already resolves there, and "<pinned>-<domain>" never exists.
	dPinned := newDomainRuntime(fakeruntime.New(), map[string]any{"network": "custom-net"}, caller, caller, nil, false, false, nil, "kubernetes", nil).(*domainRuntime)
	if got, err := dPinned.QualifyTargetAddress(ctx, target, caller, "xd-pg:5432"); err != nil || got != "xd-pg:5432" {
		t.Errorf("pinned network: QualifyTargetAddress = (%q, %v), want verbatim pass-through (every domain shares the pinned namespace)", got, err)
	}
}

// TestDomainRuntimeLabelScopedAccessEnabledIsRawGateRegardlessOfGraphScoped
// pins docs/planning/08 K4's own reason for a SEPARATE labelScopedGate
// field: runtime.LabelScopedAccessQuery must report the LabelScopedAccess
// gate's true value even when GraphScopedAccess is off — unlike the
// existing labelScopedAccessEnabled field (K3), which is deliberately
// zero-valued whenever graphScoped is false because it feeds ONLY the H7
// grant-realization path.
func TestDomainRuntimeLabelScopedAccessEnabledIsRawGateRegardlessOfGraphScoped(t *testing.T) {
	t.Parallel()
	env := envWithDomain("Provider", "pg", "default", "", nil)

	cases := []struct {
		name        string
		graphScoped bool
		labelScoped bool
		want        bool
	}{
		{"both off", false, false, false},
		{"label on, graph off (K4's own case: rides the gate independently)", false, true, true},
		{"both on", true, true, true},
		{"graph on, label off", true, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := newDomainRuntime(fakeruntime.New(), map[string]any{}, env, env, nil, tc.graphScoped, tc.labelScoped, nil, "fake", nil).(*domainRuntime)
			q, ok := runtime.ContainerRuntime(d).(runtime.LabelScopedAccessQuery)
			if !ok {
				t.Fatal("domainRuntime does not implement runtime.LabelScopedAccessQuery")
			}
			if got := q.LabelScopedAccessEnabled(); got != tc.want {
				t.Errorf("LabelScopedAccessEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}
