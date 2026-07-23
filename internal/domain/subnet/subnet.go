// Package subnet allocates deterministic Docker per-edge network subnets
// (docs/adr/026, the 2026-07-23 addendum to docs/planning/08 H7).
//
// The problem it solves mirrors internal/domain/hostport exactly, one level
// down the stack: ADR 026's original text bounded Docker's per-edge-network
// realization at "order tens per daemon", but that bound was never a real
// limit — it was Docker's DEFAULT address-pool exhaustion (large default
// subnets, allocated first-come-first-served). Handing NetworkCreate an
// explicit small subnet removes the bound entirely. For gives every edge
// (an unordered pair of endpoint identities) its own deterministic /28 (16
// addresses — exactly two endpoints plus infrastructure) carved out of one
// dedicated supernet, so:
//
//   - different edges get different subnets (their keys differ);
//   - the SAME edge gets the IDENTICAL subnet on every run, computed
//     independently by either endpoint's own reconcile, with no shared
//     state (EdgeKey is order-independent, so it never matters which side
//     computed it first);
//   - a /16 supernet holds 4096 /28s — thousands of edges per daemon, the
//     addendum's honest new envelope (the remaining real bounds, Linux
//     bridge count and fd limits, sit far above any single-daemon
//     platform).
package subnet

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"net"
	"sort"
	"sync"
)

// DefaultSupernet is the dedicated block H7's per-edge Docker networks draw
// /28s from unless a Provider's spec.runtime overrides it — documented in
// docs/planning/03-resource-model-reference.md alongside spec.runtime.network.
// Chosen well clear of Docker's own default bridge (172.17.0.0/16) and this
// codebase's other reserved ranges (docs/adr/023's wireguard transit
// networks use the 172.3x.0.0/16 block); operators whose LANs already use
// 10.94.0.0/16 pin a different supernet.
const DefaultSupernet = "10.94.0.0/16"

// blockPrefix is the fixed /28 block size the addendum specifies: exactly
// two endpoints plus infrastructure (gateway, broadcast, network address)
// comfortably fit in 16 addresses.
const blockPrefix = 28

// For deterministically computes the /28 CIDR (e.g. "10.94.0.16/28") for
// edgeKey within supernetCIDR. edgeKey should already be canonical
// (order-independent) for an unordered endpoint pair — see EdgeKey.
func For(supernetCIDR, edgeKey string) (string, error) {
	_, supernet, err := net.ParseCIDR(supernetCIDR)
	if err != nil {
		return "", fmt.Errorf("subnet: invalid supernet %q: %w", supernetCIDR, err)
	}
	ip4 := supernet.IP.To4()
	if ip4 == nil {
		return "", fmt.Errorf("subnet: supernet %q must be IPv4", supernetCIDR)
	}
	ones, _ := supernet.Mask.Size()
	if ones > blockPrefix {
		return "", fmt.Errorf("subnet: supernet %q is smaller than a /%d", supernetCIDR, blockPrefix)
	}
	blockBits := uint(blockPrefix - ones)
	capacity := uint32(1) << blockBits

	h := fnv.New32a()
	_, _ = h.Write([]byte(edgeKey))
	index := h.Sum32() % capacity

	base := binary.BigEndian.Uint32(ip4)
	blockBase := base + (index << (32 - blockPrefix))
	blockIP := make(net.IP, 4)
	binary.BigEndian.PutUint32(blockIP, blockBase)
	cidr := fmt.Sprintf("%s/%d", blockIP.String(), blockPrefix)
	record(cidr, edgeKey)
	return cidr, nil
}

// EdgeKey canonicalizes an unordered pair of identifiers into one
// deterministic string: swapping a and b yields the identical key, so the
// same edge always allocates the same subnet regardless of which endpoint's
// reconcile computes it first (the same "no shared state, still
// deterministic" property hostport.For gives host ports).
func EdgeKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

// Collision detection — mirrors internal/domain/hostport's Conflicts()
// exactly (same rationale: a hash range is safe at small scale but not
// unbounded; observe rather than silently hope). Two DISTINCT edge keys
// hashing to the same /28 is vanishingly unlikely at the documented
// envelope (4096 blocks in the default /16) but, unlike a host port, isn't
// remediable by asking the user to pin one field — a full accounting is
// still valuable for diagnosis, so a collision here is exposed the same
// way, naming both edges and the fix (a larger or additional supernet).
var (
	claimsMu sync.Mutex
	claims   = map[string]map[string]bool{} // subnet CIDR -> set of claiming edge keys
)

func record(cidr, edgeKey string) {
	claimsMu.Lock()
	defer claimsMu.Unlock()
	if claims[cidr] == nil {
		claims[cidr] = map[string]bool{}
	}
	claims[cidr][edgeKey] = true
}

// Conflict is one /28 subnet claimed by more than one distinct edge key.
type Conflict struct {
	CIDR  string
	Edges []string // sorted
}

// Conflicts reports every allocation collision observed by this process so
// far, deterministically ordered.
func Conflicts() []Conflict {
	claimsMu.Lock()
	defer claimsMu.Unlock()
	var out []Conflict
	for cidr, edges := range claims {
		if len(edges) < 2 {
			continue
		}
		c := Conflict{CIDR: cidr}
		for e := range edges {
			c.Edges = append(c.Edges, e)
		}
		sort.Strings(c.Edges)
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CIDR < out[j].CIDR })
	return out
}

// ConflictError renders Conflicts() as one actionable error, or nil.
func ConflictError() error {
	cs := Conflicts()
	if len(cs) == 0 {
		return nil
	}
	msg := "graph-scoped-access per-edge subnet collision:"
	for _, c := range cs {
		msg += fmt.Sprintf("\n  subnet %s claimed by edges %v — configure a larger or additional supernet (spec.runtime.accessSupernet)", c.CIDR, c.Edges)
	}
	return fmt.Errorf("%s", msg)
}
