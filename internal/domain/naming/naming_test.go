package naming

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// TestRuntimeObjectNameIsTheSingleAuthority pins the current convention
// (named after the realizing resource) and, more importantly, proves it
// lives in exactly one place: changing this function's body is the only
// edit a future convention change would require — every provider and the
// engine call this function rather than re-deriving the name themselves.
func TestRuntimeObjectNameIsTheSingleAuthority(t *testing.T) {
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
