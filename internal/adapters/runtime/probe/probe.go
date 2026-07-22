// Package probe holds the reachability-probe machinery shared by the
// Docker and Kubernetes runtime adapters' ProbeReachable/EnsureReachable
// implementations (docs/planning/08 C10, I5): the pinned probe image, the
// portable TCP-dial script both adapters exec inside an existing in-network
// candidate, the ephemeral probe container/pod's dial command, and the
// ctx-aware direct-dial check EnsureReachable's own preflight uses.
//
// Only what is byte-identical (or identical modulo the technology-specific
// exec/create/list/wait calls each adapter's SDK requires) lives here — the
// two-tier "exec in an existing candidate, else spin up an ephemeral probe"
// control flow itself stays in each adapter, since the Docker Engine API
// and the Kubernetes exec subresource/Pod API have genuinely different
// shapes (docs/planning/08 I5's "keep only the transport adapter-specific").
//
// This is a runtime-adapter-only helper package: internal/adapters/runtime/
// docker and internal/adapters/runtime/kubernetes may import it; ports and
// domain must not (and never need to — nothing here is part of any port).
package probe

import (
	"context"
	"net"
	"time"
)

// Image is the minimal, pinned probe image (scripts/pinned-images.txt) used
// for ProbeReachable's ephemeral in-network probe container/pod — the
// fallback when no existing managed container/pod on the target
// network/namespace can conclusively answer the dial itself (docs/planning/08
// C10).
const Image = "busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"

// TCPDialScript is a portable (BusyBox ash or bash) probe: prefer `nc -z`
// (a true, no-data connect-and-close scan) and fall back to the /dev/tcp
// pseudo-device redirection trick for shells with no nc — host/port arrive
// as $1/$2 (positional args, not string-interpolated) so a target's host
// component can never be interpreted as shell syntax.
const TCPDialScript = `nc -z -w3 "$1" "$2" 2>/dev/null || (exec 3<>/dev/tcp/$1/$2) 2>/dev/null`

// ExecArgs is the argv an adapter execs inside an existing in-network
// candidate container/pod to run TCPDialScript.
func ExecArgs(host, port string) []string {
	return []string{"sh", "-c", TCPDialScript, "sh", host, port}
}

// Command is the ephemeral probe container/pod's own entrypoint command —
// a direct `nc -z` dial, no shell/script needed since the probe image
// (unlike an arbitrary managed workload's image) is known to carry nc.
func Command(host, port string) []string {
	return []string{"nc", "-z", "-w3", host, port}
}

// Dialable reports whether a TCP connection to addr succeeds right now,
// honoring ctx's deadline (bounded to 2s when ctx has none) rather than
// hanging on an address nothing will ever answer.
//
// This is the Docker adapter's original ctx-aware semantics — the keeper
// (docs/planning/08 I5): the Kubernetes adapter's own dialable used to
// ignore ctx entirely and hardcode a 2s dial, a drift this hoist fixes as
// the one deliberate behavior change (docker's own callers see zero
// change).
func Dialable(ctx context.Context, addr string) bool {
	timeout := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		return false
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
