package graphaccess

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func nameRef(namespace, name string) map[string]any {
	if namespace == "" {
		return map[string]any{"name": name}
	}
	return map[string]any{"name": name, "namespace": namespace}
}

// buildOwnerWorkedExample constructs docs/planning/11's exact scenario
// (recorded 2026-07-22, promoted to docs/adr/026 + doc 08 H7's accept bar):
// A/R1 -> {B/X, C/Y}, A/R2 -> {B/X}, with a third B-namespace resource
// ("other-B") neither R1 nor R2 ever reference — the negative-proof target.
// R1/R2 are Providers (the only Kind-shape whose graph-scoped membership
// can be realized as actual network policy, docs/adr/022 addendum: only
// Provider/Connection Kinds have runtime containers); each "reaches" its
// target via an ordinary Binding realized on itself, sourceRef'd to its own
// local Source and targetRef'd to the target's own Source — the exact
// shape docs/domain/graph.go's refFields already produce for the real cdc
// Binding pairing, no bespoke edge type needed.
func buildOwnerWorkedExample(t *testing.T) (edges []Edge, resources map[resource.Key]resource.Envelope, r1, r2, x, y, otherB resource.Key) {
	t.Helper()
	r1Env := env("a", "Provider", "r1", map[string]any{})
	r2Env := env("a", "Provider", "r2", map[string]any{})
	xEnv := env("b", "Provider", "x", map[string]any{})
	yEnv := env("c", "Provider", "y", map[string]any{})
	otherBEnv := env("b", "Provider", "other-b", map[string]any{})

	inR1 := env("a", "Source", "in-r1", map[string]any{"providerRef": nameRef("", "r1")})
	inR2 := env("a", "Source", "in-r2", map[string]any{"providerRef": nameRef("", "r2")})
	assetX := env("b", "Source", "asset-x", map[string]any{"providerRef": nameRef("", "x")})
	assetY := env("c", "Source", "asset-y", map[string]any{"providerRef": nameRef("", "y")})
	assetOtherB := env("b", "Source", "asset-other-b", map[string]any{"providerRef": nameRef("", "other-b")})

	bindR1X := env("a", "Binding", "bind-r1-x", map[string]any{
		"mode": "cdc", "providerRef": nameRef("", "r1"),
		"sourceRef": nameRef("", "in-r1"), "targetRef": nameRef("b", "asset-x"),
	})
	bindR1Y := env("a", "Binding", "bind-r1-y", map[string]any{
		"mode": "cdc", "providerRef": nameRef("", "r1"),
		"sourceRef": nameRef("", "in-r1"), "targetRef": nameRef("c", "asset-y"),
	})
	bindR2X := env("a", "Binding", "bind-r2-x", map[string]any{
		"mode": "cdc", "providerRef": nameRef("", "r2"),
		"sourceRef": nameRef("", "in-r2"), "targetRef": nameRef("b", "asset-x"),
	})

	all := []resource.Envelope{r1Env, r2Env, xEnv, yEnv, otherBEnv, inR1, inR2, assetX, assetY, assetOtherB, bindR1X, bindR1Y, bindR2X}
	g, err := graph.Build(all)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	resources = make(map[resource.Key]resource.Envelope, len(all))
	for _, e := range all {
		resources[e.Key()] = e
	}
	return DeriveEdges(g), resources, r1Env.Key(), r2Env.Key(), xEnv.Key(), yEnv.Key(), otherBEnv.Key()
}

func keySet(keys []resource.Key) map[resource.Key]bool {
	m := make(map[resource.Key]bool, len(keys))
	for _, k := range keys {
		m[k] = true
	}
	return m
}

// TestMembershipEdgesOwnerWorkedExample pins docs/planning/08 H7's accept
// bar directly at the compiler level (the runtime realization tests pin it
// again end to end, per-runtime): A/R1 reaches exactly {B/X, C/Y}; A/R2
// reaches exactly {B/X}; neither ever reaches the unreferenced "other-B".
func TestMembershipEdgesOwnerWorkedExample(t *testing.T) {
	edges, resources, r1, r2, x, y, otherB := buildOwnerWorkedExample(t)

	r1Peers := keySet(MembershipEdges(edges, r1, resources, false))
	if !r1Peers[x] || !r1Peers[y] {
		t.Errorf("R1 must reach {B/X, C/Y}, got %v", r1Peers)
	}
	if r1Peers[otherB] {
		t.Error("R1 must NOT reach the unreferenced other-B resource (negative proof)")
	}
	if len(r1Peers) != 2 {
		t.Errorf("R1's membership set must be exactly {B/X, C/Y}, got %d peers: %v", len(r1Peers), r1Peers)
	}

	r2Peers := keySet(MembershipEdges(edges, r2, resources, false))
	if !r2Peers[x] {
		t.Errorf("R2 must reach B/X, got %v", r2Peers)
	}
	if r2Peers[y] {
		t.Error("R2 must NOT reach C/Y (negative proof — R2 never declared that reference)")
	}
	if r2Peers[otherB] {
		t.Error("R2 must NOT reach other-B")
	}
	if len(r2Peers) != 1 {
		t.Errorf("R2's membership set must be exactly {B/X}, got %d peers: %v", len(r2Peers), r2Peers)
	}
}

// TestMembershipEdgesSelfNeverIncluded proves ADR 026 decision 1's "brokers
// reach brokers" carve-out: a container's own internal/collapsed resources
// never appear in its own peer set (there is nothing to open a network hole
// for — it's already the same container).
func TestMembershipEdgesSelfNeverIncluded(t *testing.T) {
	edges, resources, r1, _, _, _, _ := buildOwnerWorkedExample(t)
	peers := MembershipEdges(edges, r1, resources, false)
	for _, p := range peers {
		if p == r1 {
			t.Fatal("self must never appear in its own membership set")
		}
	}
}

// TestMembershipEdgesWideGrant pins docs/adr/026 §2: an explicit
// spec.access grant widens reachability to every OTHER container in the
// granted namespace — including one no ordinary graph edge ever reached
// (other-b) — while a resource that never declares the grant stays
// narrowly scoped (R2 still only reaches B/X).
func TestMembershipEdgesWideGrant(t *testing.T) {
	r1Env := env("a", "Provider", "r1", map[string]any{
		"access": []any{map[string]any{"namespace": "b"}},
	})
	xEnv := env("b", "Provider", "x", map[string]any{})
	otherBEnv := env("b", "Provider", "other-b", map[string]any{})

	all := []resource.Envelope{r1Env, xEnv, otherBEnv}
	g, err := graph.Build(all)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	resources := make(map[resource.Key]resource.Envelope, len(all))
	for _, e := range all {
		resources[e.Key()] = e
	}
	edges := DeriveEdges(g)

	peers := keySet(MembershipEdges(edges, r1Env.Key(), resources, false))
	if !peers[xEnv.Key()] || !peers[otherBEnv.Key()] {
		t.Errorf("a namespace-wide grant must reach EVERY container in that namespace, got %v", peers)
	}
}

// --- docs/planning/08 K3: selector-scoped wide grants -----------------------

// envWithLabels is env's sibling for K3's selector-grant fixtures, which
// need metadata.labels (env itself has no labels parameter — adding one
// there would ripple through every existing call site in this file for no
// benefit to tests that don't care about labels).
func envWithLabels(namespace, kind, name string, labels map[string]string, spec map[string]any) resource.Envelope {
	e := env(namespace, kind, name, spec)
	e.Metadata.Labels = labels
	return e
}

// buildSelectorGrantScenario is TestMembershipEdgesWideGrant's fixture,
// narrowed by a matchLabels selector: r1 declares a selector-scoped grant
// to namespace b's "tier: gold" resources only. x carries tier: gold;
// otherB does not — otherB is the K3 negative proof a bare namespace-wide
// grant (TestMembershipEdgesWideGrant, above) could never express.
func buildSelectorGrantScenario(t *testing.T) (edges []Edge, resources map[resource.Key]resource.Envelope, r1, x, otherB resource.Key) {
	t.Helper()
	r1Env := env("a", "Provider", "r1", map[string]any{
		"access": []any{map[string]any{
			"namespace": "b",
			"selector":  map[string]any{"matchLabels": map[string]any{"tier": "gold"}},
		}},
	})
	xEnv := envWithLabels("b", "Provider", "x", map[string]string{"tier": "gold"}, map[string]any{})
	otherBEnv := env("b", "Provider", "other-b", map[string]any{}) // no tier label at all

	all := []resource.Envelope{r1Env, xEnv, otherBEnv}
	g, err := graph.Build(all)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	resources = make(map[resource.Key]resource.Envelope, len(all))
	for _, e := range all {
		resources[e.Key()] = e
	}
	return DeriveEdges(g), resources, r1Env.Key(), xEnv.Key(), otherBEnv.Key()
}

// TestMembershipEdgesSelectorGrantNarrowsAudience is docs/planning/08 K3's
// positive+negative proof, gate ON: a selector-scoped grant admits ONLY the
// namespace members whose labels satisfy the selector (x, tier: gold) and
// excludes the rest of the namespace a bare grant would have widened to
// (otherB, no tier label) — the audience is namespace AND selector, not
// namespace alone.
func TestMembershipEdgesSelectorGrantNarrowsAudience(t *testing.T) {
	edges, resources, r1, x, otherB := buildSelectorGrantScenario(t)
	peers := keySet(MembershipEdges(edges, r1, resources, true))
	if !peers[x] {
		t.Errorf("a selector grant must still reach the labeled member of the namespace, got %v", peers)
	}
	if peers[otherB] {
		t.Error("a selector grant must NOT reach a namespace member whose labels don't satisfy the selector (negative proof)")
	}
}

// TestMembershipEdgesSelectorGrantInertWhenGateOff is docs/planning/08 K3's
// gate-off behavior: ADR 033's "never wider than declared intent when the
// gate is off" answer means a selector-bearing grant is INERT (admits
// nobody at all), never falls back to the wider bare namespace-wide form —
// x must NOT be reached even though it satisfies the selector, because the
// selector itself is never evaluated with the gate off.
func TestMembershipEdgesSelectorGrantInertWhenGateOff(t *testing.T) {
	edges, resources, r1, x, otherB := buildSelectorGrantScenario(t)
	peers := keySet(MembershipEdges(edges, r1, resources, false))
	if peers[x] || peers[otherB] {
		t.Errorf("a selector grant must be INERT (reach nobody) when LabelScopedAccess is off, got %v", peers)
	}
}

// TestIngressPeersSelectorGrantChecksSelfLabels is the IngressPeers-side
// mirror of TestMembershipEdgesSelectorGrantNarrowsAudience: from the
// TARGET's own vantage, a selector grant declared elsewhere only admits
// this container as a peer when THIS container's own labels satisfy the
// grant's selector.
func TestIngressPeersSelectorGrantChecksSelfLabels(t *testing.T) {
	edges, resources, r1, x, otherB := buildSelectorGrantScenario(t)

	xIngress := keySet(IngressPeers(edges, x, resources, true))
	if !xIngress[r1] {
		t.Errorf("B/X (tier: gold) must see A/R1 as an ingress peer under the selector grant, got %v", xIngress)
	}
	otherBIngress := keySet(IngressPeers(edges, otherB, resources, true))
	if otherBIngress[r1] {
		t.Error("B/other-b (no tier label) must NOT see A/R1 as an ingress peer under the selector grant (negative proof)")
	}
}

func TestAccessGrantsReadsNamespaceEntries(t *testing.T) {
	e := env("a", "Provider", "r1", map[string]any{
		"access": []any{
			map[string]any{"namespace": "b"},
			map[string]any{"namespace": "c"},
			map[string]any{}, // malformed: no namespace — dropped, not an error
			"not-an-object",  // malformed shape — dropped
		},
	})
	got := AccessGrants(e)
	if len(got) != 2 || got[0].Namespace != "b" || got[1].Namespace != "c" {
		t.Fatalf("AccessGrants = %+v, want [{b} {c}]", got)
	}
	if got[0].Selector != nil || got[1].Selector != nil {
		t.Fatalf("AccessGrants = %+v, want nil Selector on the bare namespace-wide form", got)
	}
}

// TestAccessGrantsDecodesSelector is docs/planning/08 K3: a grant entry
// carrying a selector block decodes into the SAME internal/domain/
// policy.Selector type K2 already gave the policy engine (reuse, not a
// duplicate selector implementation) — matchLabels and matchExpressions
// both round-trip.
func TestAccessGrantsDecodesSelector(t *testing.T) {
	e := env("a", "Provider", "r1", map[string]any{
		"access": []any{
			map[string]any{
				"namespace": "b",
				"selector": map[string]any{
					"matchLabels": map[string]any{"tier": "gold"},
					"matchExpressions": []any{
						map[string]any{"key": "clearance", "operator": "Exists"},
					},
				},
			},
		},
	})
	got := AccessGrants(e)
	if len(got) != 1 {
		t.Fatalf("AccessGrants = %+v, want exactly one entry", got)
	}
	sel := got[0].Selector
	if sel == nil {
		t.Fatal("Selector = nil, want a decoded selector")
	}
	if sel.MatchLabels["tier"] != "gold" {
		t.Errorf("Selector.MatchLabels[tier] = %q, want %q", sel.MatchLabels["tier"], "gold")
	}
	if len(sel.MatchExpressions) != 1 || sel.MatchExpressions[0].Key != "clearance" {
		t.Errorf("Selector.MatchExpressions = %+v, want one entry keyed \"clearance\"", sel.MatchExpressions)
	}
}

func TestAccessGrantsEmptyWhenUnset(t *testing.T) {
	e := env("a", "Provider", "r1", map[string]any{})
	if got := AccessGrants(e); len(got) != 0 {
		t.Fatalf("AccessGrants on a spec with no access field = %v, want empty", got)
	}
}

func TestContainerOfCollapsesLogicalKinds(t *testing.T) {
	r1 := env("a", "Provider", "r1", map[string]any{})
	src := env("a", "Source", "in-r1", map[string]any{"providerRef": nameRef("", "r1")})
	resources := map[resource.Key]resource.Envelope{r1.Key(): r1, src.Key(): src}

	if got := ContainerOf(r1.Key(), resources); got != r1.Key() {
		t.Errorf("a Provider must resolve to itself, got %v", got)
	}
	if got := ContainerOf(src.Key(), resources); got != r1.Key() {
		t.Errorf("a Source must resolve to its providerRef's Provider, got %v, want %v", got, r1.Key())
	}
}

func TestContainerOfConnectionResolvesToItself(t *testing.T) {
	conn := env("a", "Connection", "c1", map[string]any{"providerRef": nameRef("", "proxy")})
	resources := map[resource.Key]resource.Envelope{conn.Key(): conn}
	if got := ContainerOf(conn.Key(), resources); got != conn.Key() {
		t.Errorf("a Connection must resolve to itself (it realizes its own container), got %v", got)
	}
}

// TestIngressPeersIsDirectional pins the K8s-specific half of H7: X (the
// TARGET R1 dials) must see R1 as an INGRESS peer (someone may dial X),
// while R1 itself gains NO ingress peer from this edge (nobody dials R1 in
// this scenario) — only IngressPeers, not EgressPeers, may ever be
// realized as a Kubernetes NetworkPolicy ingress rule (egress is
// unrestricted by construction in this codebase).
func TestIngressPeersIsDirectional(t *testing.T) {
	edges, resources, r1, _, x, _, _ := buildOwnerWorkedExample(t)

	xIngress := keySet(IngressPeers(edges, x, resources, false))
	if !xIngress[r1] {
		t.Errorf("B/X must see A/R1 as an ingress peer (R1 dials X), got %v", xIngress)
	}

	r1Ingress := keySet(IngressPeers(edges, r1, resources, false))
	if r1Ingress[x] {
		t.Error("A/R1 must NOT see B/X as an ingress peer — R1 dials OUT to X, nothing dials IN to R1 here")
	}

	r1Egress := keySet(EgressPeers(edges, r1, resources, false))
	if !r1Egress[x] {
		t.Errorf("A/R1 must see B/X as an egress peer (R1 dials X), got %v", r1Egress)
	}
	xEgress := keySet(EgressPeers(edges, x, resources, false))
	if xEgress[r1] {
		t.Error("B/X must NOT see A/R1 as an egress peer — X never dials R1 in this scenario")
	}
}

func TestContainerDomainProviderIsOwnDomain(t *testing.T) {
	p := env("a", "Provider", "r1", map[string]any{})
	p.Metadata.Domain = "alpha"
	resources := map[resource.Key]resource.Envelope{p.Key(): p}
	if got := ContainerDomain(p.Key(), resources); got != "alpha" {
		t.Errorf("ContainerDomain(Provider) = %q, want %q", got, "alpha")
	}
}

// TestContainerDomainConnectionUsesRealizingProviderDomain pins docs/adr/022
// addendum: a Connection's OWN declared domain governs graph/policy edges
// only — the container it realizes lives in its REALIZING PROVIDER's
// domain, and ContainerDomain must resolve that, not the Connection's own
// (possibly different) declared domain.
func TestContainerDomainConnectionUsesRealizingProviderDomain(t *testing.T) {
	prov := env("a", "Provider", "proxy", map[string]any{})
	prov.Metadata.Domain = "beta"
	conn := env("a", "Connection", "c1", map[string]any{"providerRef": nameRef("", "proxy")})
	conn.Metadata.Domain = "alpha" // deliberately different from the provider's
	resources := map[resource.Key]resource.Envelope{prov.Key(): prov, conn.Key(): conn}
	if got := ContainerDomain(conn.Key(), resources); got != "beta" {
		t.Errorf("ContainerDomain(Connection) = %q, want the realizing provider's domain %q", got, "beta")
	}
}

func TestMembershipEdgesDeterministic(t *testing.T) {
	edges, resources, r1, _, _, _, _ := buildOwnerWorkedExample(t)
	a := MembershipEdges(edges, r1, resources, false)
	b := MembershipEdges(edges, r1, resources, false)
	if len(a) != len(b) {
		t.Fatalf("not deterministic in length: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("not deterministic at %d: %v vs %v", i, a[i], b[i])
		}
	}
}
