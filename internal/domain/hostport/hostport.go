// Package hostport allocates host-side ports for published container ports.
//
// The problem it solves (docs/history/feature-requests.md): when every provider hand-picks
// its host port, two components in a large platform inevitably collide. The
// answer here is that a host port is *optional* — omit it and one is
// allocated deterministically from the component's (unique) name, so:
//
//   - different components get different ports (their names differ);
//   - the same component gets the same port on every reconcile, and any
//     dependent reconcile (e.g. a topic against its broker) computes the
//     identical value without shared state;
//   - nobody hand-specifies a port, so nobody collides one by hand.
//
// In-network addresses (`<container>:<fixed-port>`) remain the stable access
// identifier; the host port is a convenience for host-side tools and is
// surfaced by `platformctl inventory`. A port may still be pinned explicitly
// when an external tool needs a fixed one. On the Docker runtime the port is
// published; another runtime (Kubernetes) would materialise the same intent
// as a Service — the provider states the desire, the runtime realises it.
package hostport

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

const (
	// rangeStart/rangeLen bound the auto-allocation window well clear of the
	// well-known service ports a user's other stacks occupy (5432, 9092, …),
	// giving ~10k slots so hash collisions between a handful of components
	// are unlikely; pin a port explicitly if one ever clashes.
	rangeStart = 20000
	rangeLen   = 10000
)

// For returns the deterministic auto-allocated host port for a component of
// the given (unique) name.
func For(name string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return rangeStart + int(h.Sum32()%rangeLen)
}

// Resolve returns configured if it is non-zero (an explicit pin), else the
// deterministic auto-allocated port for name.
func Resolve(configured int, name string) int {
	if configured > 0 {
		return configured
	}
	p := For(name)
	record(p, name)
	return p
}

// Collision detection (2026-07 production review, doc 11): the hash gives
// ~10k slots, so two dozen components are safe but a large platform is
// not — at ~120 auto-allocated names the birthday-paradox odds of one
// collision pass 50%, and without detection the failure surfaces as a
// cryptic runtime port-bind error on whichever component reconciles
// second. Every Resolve records its claim in a process-level table; the
// engine checks Conflicts() and fails with both names and the pin-a-port
// remedy instead. Determinism is untouched — the table only observes.
var (
	claimsMu sync.Mutex
	claims   = map[int]map[string]bool{} // port -> set of claiming names
)

func record(port int, name string) {
	claimsMu.Lock()
	defer claimsMu.Unlock()
	if claims[port] == nil {
		claims[port] = map[string]bool{}
	}
	claims[port][name] = true
}

// Conflict is one host port claimed by more than one component name.
type Conflict struct {
	Port  int
	Names []string // sorted
}

// Conflicts reports every auto-allocation collision observed by this
// process so far, deterministically ordered.
func Conflicts() []Conflict {
	claimsMu.Lock()
	defer claimsMu.Unlock()
	var out []Conflict
	for port, names := range claims {
		if len(names) < 2 {
			continue
		}
		c := Conflict{Port: port}
		for n := range names {
			c.Names = append(c.Names, n)
		}
		sort.Strings(c.Names)
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Port < out[j].Port })
	return out
}

// ConflictError renders Conflicts() as one actionable error, or nil.
func ConflictError() error {
	cs := Conflicts()
	if len(cs) == 0 {
		return nil
	}
	msg := "host-port auto-allocation collision:"
	for _, c := range cs {
		msg += fmt.Sprintf("\n  port %d claimed by %v — pin an explicit port on one of them (the relevant *Port configuration field)", c.Port, c.Names)
	}
	return fmt.Errorf("%s", msg)
}
