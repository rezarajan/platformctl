package naming

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// TestRuntimeObjectNameIsTheSingleAuthority pins the current convention
// (named after the realizing resource) and, more importantly, proves it
// lives in exactly one place: changing this function's body is the only
// edit a future convention change would require — every provider and the
// engine call this function rather than re-deriving the name themselves.
func TestRuntimeObjectNameIsTheSingleAuthority(t *testing.T) {
	t.Parallel()
	env := resource.Envelope{}
	env.Metadata.Name = "orders-db"
	env.Metadata.Namespace = "default"

	if got := RuntimeObjectName(env); got != "orders-db" {
		t.Errorf("RuntimeObjectName = %q, want %q", got, "orders-db")
	}
}

// TestNetworkNameDefaultDomainIsByteIdenticalNoOp is docs/planning/08 H5's
// back-compat pin at its narrowest: the default domain (undeclared, "", or
// explicitly "default") must never change the base network name — this is
// the one function every domain-scoped network/namespace name in the repo
// routes through (providerkit.Network, proxy's Connection realization), so
// pinning it here pins the whole back-compat guarantee at its root.
func TestNetworkNameDefaultDomainIsByteIdenticalNoOp(t *testing.T) {
	t.Parallel()
	for _, domain := range []string{"", "default"} {
		if got := NetworkName("datascape", domain); got != "datascape" {
			t.Errorf("NetworkName(%q, %q) = %q, want %q (unchanged)", "datascape", domain, got, "datascape")
		}
	}
}

// TestNetworkNameNonDefaultDomainIsSuffixed proves the actual Ring 1
// segmentation shape (docs/adr/022): a declared, non-default domain gets its
// own network name, and distinct domains get distinct names.
func TestNetworkNameNonDefaultDomainIsSuffixed(t *testing.T) {
	t.Parallel()
	alpha := NetworkName("datascape", "alpha")
	beta := NetworkName("datascape", "beta")
	if alpha != "datascape-alpha" {
		t.Errorf("NetworkName(datascape, alpha) = %q, want %q", alpha, "datascape-alpha")
	}
	if alpha == beta {
		t.Errorf("NetworkName must produce distinct names for distinct domains: alpha=%q beta=%q", alpha, beta)
	}
	if alpha == "datascape" || beta == "datascape" {
		t.Error("a non-default domain must never collide with the undeclared-domain network name")
	}
}

// TestNetworkNameTruncation pins the doc 11 caveat-D fix: domain-scoped
// names exceeding the 63-char DNS-label limit truncate deterministically
// with a full-name hash suffix, so long names neither break Kubernetes
// namespace creation nor silently collide with each other.
func TestNetworkNameTruncation(t *testing.T) {
	t.Parallel()
	longBase := strings.Repeat("a", 50)
	n1 := NetworkName(longBase, "team-analytics-platform")
	n2 := NetworkName(longBase, "team-analytics-products")
	if len(n1) > 63 || len(n2) > 63 {
		t.Fatalf("truncated names exceed 63 chars: %d, %d", len(n1), len(n2))
	}
	if n1 == n2 {
		t.Fatal("distinct long names collided after truncation")
	}
	if n1 != NetworkName(longBase, "team-analytics-platform") {
		t.Fatal("truncation not deterministic")
	}
	if got := NetworkName("datascape", "b"); got != "datascape-b" {
		t.Fatalf("short names must be untouched: %q", got)
	}
}

// TestWorkloadIdentityURIShape pins the exact SPIFFE-aligned form
// docs/adr/022 specifies for an undeclared/default-domain resource:
// spiffe://datascape/<namespace>/<kind>/<name>.
func TestWorkloadIdentityURIShape(t *testing.T) {
	t.Parallel()
	env := resource.Envelope{}
	env.GroupVersionKind.Kind = "Source"
	env.Metadata.Name = "orders-db"
	env.Metadata.Namespace = "payments"

	want := "spiffe://datascape/payments/source/orders-db"
	if got := WorkloadIdentityURI(env); got != want {
		t.Errorf("WorkloadIdentityURI = %q, want %q", got, want)
	}
}

// TestWorkloadIdentityURIIncludesNonDefaultDomain proves the H5-merge
// upgrade: a resource declaring a non-default metadata.domain (docs/planning/08
// H5, docs/adr/022 Ring 0) gets a domain segment in its identity URI —
// spiffe://datascape/<namespace>/<domain>/<kind>/<name> — mirroring
// NetworkName's own "undeclared domain is a no-op, declared domain gets its
// own segment" rule (this file's TestNetworkName* tests, above).
func TestWorkloadIdentityURIIncludesNonDefaultDomain(t *testing.T) {
	t.Parallel()
	env := resource.Envelope{}
	env.GroupVersionKind.Kind = "Source"
	env.Metadata.Name = "orders-db"
	env.Metadata.Namespace = "payments"
	env.Metadata.Domain = "finance"

	want := "spiffe://datascape/payments/finance/source/orders-db"
	if got := WorkloadIdentityURI(env); got != want {
		t.Errorf("WorkloadIdentityURI = %q, want %q", got, want)
	}
}

// TestWorkloadIdentityURIIsDeterministic proves the same graph node always
// derives the same URI — the load-bearing property for ADR 022/027 ("the
// graph node IS the identity subject"): repeated derivation, drift
// detection, and re-apply must never mint a different identity for the
// same resource.
func TestWorkloadIdentityURIIsDeterministic(t *testing.T) {
	t.Parallel()
	env := resource.Envelope{}
	env.GroupVersionKind.Kind = "Binding"
	env.Metadata.Name = "cdc-orders"
	env.Metadata.Namespace = "analytics"

	first := WorkloadIdentityURI(env)
	for i := 0; i < 5; i++ {
		if got := WorkloadIdentityURI(env); got != first {
			t.Fatalf("WorkloadIdentityURI is not deterministic: call %d = %q, first = %q", i, got, first)
		}
	}
}

// TestWorkloadIdentityURIDefaultNamespace proves an unset namespace
// normalizes to "default" exactly like every other namespace-qualified
// name in this codebase (resource.NormalizeNamespace), so an identity is
// never minted with an empty path segment.
func TestWorkloadIdentityURIDefaultNamespace(t *testing.T) {
	t.Parallel()
	env := resource.Envelope{}
	env.GroupVersionKind.Kind = "Connection"
	env.Metadata.Name = "edge"

	want := "spiffe://datascape/default/connection/edge"
	if got := WorkloadIdentityURI(env); got != want {
		t.Errorf("WorkloadIdentityURI = %q, want %q", got, want)
	}
}

// TestWorkloadIdentityURIDistinctForDistinctNodes proves two different
// graph nodes never collide on identity — the collision-free-by-
// construction property docs/adr/022's Consequences section names.
func TestWorkloadIdentityURIDistinctForDistinctNodes(t *testing.T) {
	t.Parallel()
	a := resource.Envelope{}
	a.GroupVersionKind.Kind = "Source"
	a.Metadata.Name = "orders-db"
	a.Metadata.Namespace = "payments"

	b := resource.Envelope{}
	b.GroupVersionKind.Kind = "Source"
	b.Metadata.Name = "orders-db"
	b.Metadata.Namespace = "analytics"

	if WorkloadIdentityURI(a) == WorkloadIdentityURI(b) {
		t.Fatalf("identical URIs for distinct namespaces: %q", WorkloadIdentityURI(a))
	}
}

// TestWorkloadIdentityURIDistinctForDistinctDomains proves two resources
// with the same namespace/kind/name but different domains never collide —
// the same collision-free property, extended to the H5 domain segment.
func TestWorkloadIdentityURIDistinctForDistinctDomains(t *testing.T) {
	t.Parallel()
	a := resource.Envelope{}
	a.GroupVersionKind.Kind = "Source"
	a.Metadata.Name = "orders-db"
	a.Metadata.Namespace = "payments"
	a.Metadata.Domain = "finance"

	b := resource.Envelope{}
	b.GroupVersionKind.Kind = "Source"
	b.Metadata.Name = "orders-db"
	b.Metadata.Namespace = "payments"
	b.Metadata.Domain = "analytics"

	if WorkloadIdentityURI(a) == WorkloadIdentityURI(b) {
		t.Fatalf("identical URIs for distinct domains: %q", WorkloadIdentityURI(a))
	}
}
