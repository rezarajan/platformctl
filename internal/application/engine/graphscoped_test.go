package engine

import (
	"context"
	"testing"

	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/application/graphaccess"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func gsaEnv(namespace, kind, name string, spec map[string]any) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: kind},
		Metadata:         resource.Metadata{Namespace: namespace, Name: name},
		Spec:             spec,
	}
}

func gsaRef(namespace, name string) map[string]any {
	if namespace == "" {
		return map[string]any{"name": name}
	}
	return map[string]any{"name": name, "namespace": namespace}
}

// buildWorkedExampleResources is docs/planning/11's worked example
// (2026-07-22), promoted to docs/adr/026 + doc 08 H7's accept bar:
// A/R1 -> {B/X, C/Y}, A/R2 -> {B/X}, with an unreferenced third B-namespace
// resource ("other-b") as the negative-proof target — the SAME shape
// internal/application/graphaccess's own TestMembershipEdgesOwnerWorkedExample
// pins at the compiler level; this is the end-to-end version, driven
// through the actual engine decorator against a real (fake) runtime.
func buildWorkedExampleResources(t *testing.T) (byKey map[resource.Key]resource.Envelope, edges []graphaccess.Edge, r1, r2, x, y, otherB resource.Envelope) {
	t.Helper()
	r1 = gsaEnv("a", "Provider", "r1", map[string]any{})
	r2 = gsaEnv("a", "Provider", "r2", map[string]any{})
	x = gsaEnv("b", "Provider", "x", map[string]any{})
	y = gsaEnv("c", "Provider", "y", map[string]any{})
	otherB = gsaEnv("b", "Provider", "other-b", map[string]any{})

	inR1 := gsaEnv("a", "Source", "in-r1", map[string]any{"providerRef": gsaRef("", "r1")})
	inR2 := gsaEnv("a", "Source", "in-r2", map[string]any{"providerRef": gsaRef("", "r2")})
	assetX := gsaEnv("b", "Source", "asset-x", map[string]any{"providerRef": gsaRef("", "x")})
	assetY := gsaEnv("c", "Source", "asset-y", map[string]any{"providerRef": gsaRef("", "y")})
	assetOtherB := gsaEnv("b", "Source", "asset-other-b", map[string]any{"providerRef": gsaRef("", "other-b")})

	bindR1X := gsaEnv("a", "Binding", "bind-r1-x", map[string]any{
		"mode": "cdc", "providerRef": gsaRef("", "r1"),
		"sourceRef": gsaRef("", "in-r1"), "targetRef": gsaRef("b", "asset-x"),
	})
	bindR1Y := gsaEnv("a", "Binding", "bind-r1-y", map[string]any{
		"mode": "cdc", "providerRef": gsaRef("", "r1"),
		"sourceRef": gsaRef("", "in-r1"), "targetRef": gsaRef("c", "asset-y"),
	})
	bindR2X := gsaEnv("a", "Binding", "bind-r2-x", map[string]any{
		"mode": "cdc", "providerRef": gsaRef("", "r2"),
		"sourceRef": gsaRef("", "in-r2"), "targetRef": gsaRef("b", "asset-x"),
	})

	all := []resource.Envelope{r1, r2, x, y, otherB, inR1, inR2, assetX, assetY, assetOtherB, bindR1X, bindR1Y, bindR2X}
	g, err := graph.Build(all)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	byKey = make(map[resource.Key]resource.Envelope, len(all))
	for _, e := range all {
		byKey[e.Key()] = e
	}
	return byKey, graphaccess.DeriveEdges(g), r1, r2, x, y, otherB
}

// ensureWorkedExampleContainer drives EnsureNetwork+EnsureContainer for one
// worked-example Provider through the graph-scoped domainRuntime decorator
// — exactly the sequence resolveRequest's caller (a real provider's
// Reconcile) performs, minus the provider machinery itself.
func ensureWorkedExampleContainer(t *testing.T, rt runtime.ContainerRuntime, env resource.Envelope, byKey map[resource.Key]resource.Envelope, edges []graphaccess.Edge) {
	t.Helper()
	d := newDomainRuntime(rt, map[string]any{}, env, env, byKey, true, edges, "fake")
	ctx := context.Background()
	labels := runtime.ManagedLabels(env.Metadata.Namespace, env.Kind, env.Metadata.Name, env.Metadata.Name)
	if err := d.EnsureNetwork(ctx, runtime.NetworkSpec{Name: "datascape", Labels: labels}); err != nil {
		t.Fatalf("EnsureNetwork(%s): %v", env.Key(), err)
	}
	if _, err := d.EnsureContainer(ctx, runtime.ContainerSpec{
		Name: env.Metadata.Name, Image: "x", Networks: []string{"datascape"}, Labels: labels,
		Ports: []runtime.PortBinding{{ContainerPort: 1, Audience: runtime.AudienceInternal}},
	}); err != nil {
		t.Fatalf("EnsureContainer(%s): %v", env.Key(), err)
	}
}

// TestGraphScopedAccessOwnerWorkedExample is docs/planning/08 H7's accept
// bar, end to end against the (Docker-shaped) fake runtime: A/R1 reaches
// B/X and C/Y; A/R2 reaches only B/X; R2->C/Y and R1->other-B both FAIL —
// negative proofs from the CONSUMER's vantage (ProbeReachable dialing
// FROM the consumer's own attached networks), exactly docs/adr/026
// decision 5's bar. Also proves the flat/home network itself no longer
// carries cross-container reachability under the gate — the mechanism
// that makes Docker's realization actually enforce anything (see
// graphscoped.go's package doc).
func TestGraphScopedAccessOwnerWorkedExample(t *testing.T) {
	rt := fakeruntime.New()
	byKey, edges, r1, r2, x, y, otherB := buildWorkedExampleResources(t)

	for _, p := range []resource.Envelope{r1, r2, x, y, otherB} {
		ensureWorkedExampleContainer(t, rt, p, byKey, edges)
	}
	ctx := context.Background()

	// Positive: R1 reaches B/X and C/Y, each via its own deterministic
	// per-edge network (both endpoints joined it independently).
	r1x := naming.EdgeNetworkName(r1.Key(), x.Key())
	if err := rt.ProbeReachable(ctx, r1x, "x:1"); err != nil {
		t.Errorf("R1 must reach B/X via their per-edge network: %v", err)
	}
	r1y := naming.EdgeNetworkName(r1.Key(), y.Key())
	if err := rt.ProbeReachable(ctx, r1y, "y:1"); err != nil {
		t.Errorf("R1 must reach C/Y via their per-edge network: %v", err)
	}

	// Positive: R2 reaches B/X via the SAME per-edge network X also joined
	// for R1 — X, X, X: two peers, ONE shared target, TWO distinct edge
	// networks (edges are pairs, not stars).
	r2x := naming.EdgeNetworkName(r2.Key(), x.Key())
	if err := rt.ProbeReachable(ctx, r2x, "x:1"); err != nil {
		t.Errorf("R2 must reach B/X via their per-edge network: %v", err)
	}

	// Negative (from the consumer's vantage): R2 never declared C/Y — no
	// edge network for that pair was ever created, so a probe from what
	// WOULD be that network fails outright (target unattached/unknown).
	r2y := naming.EdgeNetworkName(r2.Key(), y.Key())
	if err := rt.ProbeReachable(ctx, r2y, "y:1"); err == nil {
		t.Error("R2 must NOT reach C/Y (negative proof) — no such edge was ever declared")
	}

	// Negative: R1 never declared other-B — no edge network for that pair.
	r1z := naming.EdgeNetworkName(r1.Key(), otherB.Key())
	if err := rt.ProbeReachable(ctx, r1z, "other-b:1"); err == nil {
		t.Error("R1 must NOT reach other-B (negative proof) — no such edge was ever declared")
	}

	// The decisive negative proof: the shared/home "datascape" token no
	// longer grants ANY cross-container reachability under the gate — R1
	// and X are on their OWN exclusive private home networks (see
	// graphscoped.go), never a shared flat one, so dialing X from R1's own
	// home network fails, proving the mechanism didn't just add edge
	// networks on top of a still-flat base. (This assertion is valid here
	// because the fake runtime's ProbeReachable checks the TARGET's own
	// declared Networks list directly — unlike the REAL Docker adapter,
	// which execs a dial from an existing managed container ON the named
	// network and can therefore be confounded by a multi-homed vantage;
	// cmd/platformctl/graphscoped_integration_test.go's live Docker
	// equivalent anchors this same proof on other-b, which declares no
	// edge and so is genuinely single-homed, for exactly that reason.)
	r1Home := naming.PrivateNetworkName("datascape", "", r1.Key())
	if err := rt.ProbeReachable(ctx, r1Home, "x:1"); err == nil {
		t.Error("R1's own private home network must not reach B/X — reachability must come ONLY from the explicit per-edge network")
	}
	xHome := naming.PrivateNetworkName("datascape", "", x.Key())
	if r1Home == xHome {
		t.Fatal("R1 and B/X must have DISTINCT private home networks")
	}
}

// TestGraphScopedAccessWideGrantReachesAllOfNamespace pins docs/adr/026 §2:
// an explicit spec.access grant reaches every container in the granted
// namespace, including one no ordinary graph edge names.
func TestGraphScopedAccessWideGrantReachesAllOfNamespace(t *testing.T) {
	rt := fakeruntime.New()
	r1 := gsaEnv("a", "Provider", "r1", map[string]any{
		"access": []any{map[string]any{"namespace": "b"}},
	})
	x := gsaEnv("b", "Provider", "x", map[string]any{})
	otherB := gsaEnv("b", "Provider", "other-b", map[string]any{})
	all := []resource.Envelope{r1, x, otherB}
	g, err := graph.Build(all)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	byKey := map[resource.Key]resource.Envelope{}
	for _, e := range all {
		byKey[e.Key()] = e
	}
	edges := graphaccess.DeriveEdges(g)

	for _, p := range []resource.Envelope{r1, x, otherB} {
		ensureWorkedExampleContainer(t, rt, p, byKey, edges)
	}
	ctx := context.Background()

	for _, target := range []resource.Envelope{x, otherB} {
		edgeNet := naming.EdgeNetworkName(r1.Key(), target.Key())
		if err := rt.ProbeReachable(ctx, edgeNet, target.Metadata.Name+":1"); err != nil {
			t.Errorf("a namespace-wide grant must reach %s: %v", target.Key(), err)
		}
	}
}

// TestGraphScopedAccessGateOffIsByteIdentical proves the archtest/H5
// precedent holds for H7 too: with graphScoped=false, the decorator takes
// the EXACT pre-H7 code path — no private network, no edge networks, the
// bare shared/domain token reaches everything it always did.
func TestGraphScopedAccessGateOffIsByteIdentical(t *testing.T) {
	rt := fakeruntime.New()
	byKey, _, r1, _, x, _, _ := buildWorkedExampleResources(t)

	ctx := context.Background()
	for _, p := range []resource.Envelope{r1, x} {
		d := newDomainRuntime(rt, map[string]any{}, p, p, byKey, false, nil, "fake")
		labels := runtime.ManagedLabels(p.Metadata.Namespace, p.Kind, p.Metadata.Name, p.Metadata.Name)
		if err := d.EnsureNetwork(ctx, runtime.NetworkSpec{Name: "datascape", Labels: labels}); err != nil {
			t.Fatalf("EnsureNetwork(%s): %v", p.Key(), err)
		}
		if _, err := d.EnsureContainer(ctx, runtime.ContainerSpec{
			Name: p.Metadata.Name, Image: "x", Networks: []string{"datascape"}, Labels: labels,
			Ports: []runtime.PortBinding{{ContainerPort: 1, Audience: runtime.AudienceInternal}},
		}); err != nil {
			t.Fatalf("EnsureContainer(%s): %v", p.Key(), err)
		}
	}
	// Gate off: the bare "datascape" token is the one and only network —
	// both R1 and X are still on it (pre-H7, pre-H5-domains behavior),
	// with no graph-edge declaration required at all.
	if err := rt.ProbeReachable(ctx, "datascape", "x:1"); err != nil {
		t.Errorf("gate-off: the shared token network must still reach everything on it, unchanged: %v", err)
	}
	nets, err := rt.ListManagedNetworks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nets) != 1 || nets[0].Name != "datascape" {
		t.Fatalf("gate-off: ListManagedNetworks() = %v, want exactly [\"datascape\"] (byte-identical pin)", nets)
	}
}
