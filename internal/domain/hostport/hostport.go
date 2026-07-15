// Package hostport allocates host-side ports for published container ports.
//
// The problem it solves (feature-requests.md): when every provider hand-picks
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

import "hash/fnv"

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
	return For(name)
}
