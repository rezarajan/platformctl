package subnet

import (
	"fmt"
	"net"
	"testing"
)

// TestDeterministic pins the addendum's own bar: "same edge -> same
// subnet" — across repeated calls and independent of which side of the
// edge computed the key first (EdgeKey's order-independence).
func TestDeterministic(t *testing.T) {
	key := EdgeKey("default/Provider/a", "default/Provider/b")
	a, err := For(DefaultSupernet, key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := For(DefaultSupernet, key)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("not deterministic: %s != %s", a, b)
	}
}

func TestEdgeKeyOrderIndependent(t *testing.T) {
	if EdgeKey("a", "b") != EdgeKey("b", "a") {
		t.Error("EdgeKey must be symmetric under swap")
	}
}

func TestDistinctEdgesDistinctSubnets(t *testing.T) {
	a, err := For(DefaultSupernet, EdgeKey("default/Provider/a", "default/Provider/b"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := For(DefaultSupernet, EdgeKey("default/Provider/a", "default/Provider/c"))
	if err != nil {
		t.Fatal(err)
	}
	if a == c {
		t.Error("distinct edges collided (unlikely but check)")
	}
}

// TestWithinSupernet proves every allocated /28 actually falls inside the
// declared supernet — the addendum's "explicit subnet at network creation"
// promise is worthless if the math can wander outside the block.
func TestWithinSupernet(t *testing.T) {
	_, supernet, err := net.ParseCIDR(DefaultSupernet)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("edge-%d", i)
		cidr, err := For(DefaultSupernet, key)
		if err != nil {
			t.Fatal(err)
		}
		ip, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			t.Fatalf("allocated invalid CIDR %q: %v", cidr, err)
		}
		if ones, bits := ipnet.Mask.Size(); ones != 28 || bits != 32 {
			t.Fatalf("allocated %q is not a /28", cidr)
		}
		if !supernet.Contains(ip) {
			t.Fatalf("allocated %q falls outside supernet %s", cidr, DefaultSupernet)
		}
	}
}

func TestCapacityMatchesAddendum(t *testing.T) {
	// A /16 supernet holds 4096 distinct /28 blocks — the addendum's own
	// "thousands of edges per daemon" envelope. Hashing N keys into N
	// buckets is a classic balls-into-bins problem: the EXPECTED distinct
	// count is N*(1-1/e) ≈ 2589 of 4096 (birthday-paradox collisions, not a
	// bug) — this smoke-tests that the modulus wasn't accidentally
	// narrowed (which would show up as a much smaller count), not that
	// every block is reachable from 4096 probes alone.
	seen := map[string]bool{}
	for i := 0; i < 4096; i++ {
		cidr, err := For(DefaultSupernet, fmt.Sprintf("capacity-probe-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		seen[cidr] = true
	}
	if len(seen) < 2000 {
		t.Errorf("only %d distinct /28s observed across 4096 probes — capacity looks narrower than the documented /16 (4096 blocks, ~2589 expected distinct)", len(seen))
	}
}

func TestRejectsNonIPv4Supernet(t *testing.T) {
	if _, err := For("2001:db8::/32", "k"); err == nil {
		t.Error("expected an error for an IPv6 supernet")
	}
}

func TestRejectsSupernetSmallerThanBlock(t *testing.T) {
	if _, err := For("10.0.0.0/30", "k"); err == nil {
		t.Error("expected an error for a supernet smaller than a /28")
	}
}

func TestRejectsInvalidSupernet(t *testing.T) {
	if _, err := For("not-a-cidr", "k"); err == nil {
		t.Error("expected an error for an invalid supernet string")
	}
}

// TestConflictDetection mirrors hostport's TestConflictDetection: two
// distinct edge keys hashing to the same /28 within a deliberately tiny
// supernet are reported with both keys and a remedy; a single claimant (the
// common case) reports nothing.
func TestConflictDetection(t *testing.T) {
	tiny := "10.99.0.0/28" // exactly one /28 block: every key collides
	if _, err := For(tiny, "conflict-edge-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := For(tiny, "conflict-edge-b"); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, c := range Conflicts() {
		if c.CIDR != "10.99.0.0/28" {
			continue
		}
		found = true
		if len(c.Edges) < 2 {
			t.Errorf("expected at least 2 colliding edges, got %v", c.Edges)
		}
	}
	if !found {
		t.Error("expected a reported conflict for the single-block supernet")
	}
	if err := ConflictError(); err == nil {
		t.Error("expected ConflictError to report the collision above")
	}
}
