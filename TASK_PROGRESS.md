# D5: WireGuard tunnel provider — task progress

Doc 08 §6 D5. Size L. Protocol: doc 08 §2.1 (step 0 = this file). This file
was previously used by a D3/D4 session on this worktree; that work is
already merged (commit 80b5bf6) — this replaces it for D5.

## Step plan

1. [done] git merge main --no-edit (already up to date).
2. [done] Read: CLAUDE.md, D5 entry, ADR 002 (+addendum), ADR 018, ADR 022
   §boundaries, proxy provider (full, read-only reference), Connection
   domain/schema, doc 03 §8.2, doc 04 §12, reconciler.go, runtime.go,
   providerkit, postgres.go's bootstrap-credential file-mount pattern,
   scripts/pinned-images.txt, scripts/test-impact.sh, main.go wiring.
3. [done] Design spike (live Docker, not committed): validated the whole
   mechanism by hand before writing Go —
   - kernel WireGuard interface creation works in a container with just
     `--cap-add NET_ADMIN` (no `/dev/net/tun` needed) on this host.
   - `net.ipv4.ip_forward=1` must be set at container-CREATE time via
     `--sysctl` (writing `/proc/sys/net/ipv4/ip_forward` from inside a
     running unprivileged container fails: read-only). This means
     `runtime.ContainerSpec` needs a new `Sysctls` field — genuinely
     required, not optional; see step 5.
   - raw `wg set` does NOT install AllowedIPs routes (that's `wg-quick`'s
     job) — using `wg-quick` avoids a manual `ip route add` step.
   - End-to-end proven live: initiator (transit-net only) dials target
     (vpc-net only, no shared network) through responder (vpc-net +
     transit-net) via WireGuard + iptables MASQUERADE/FORWARD; a second
     container dialing the initiator's transit-net IP:port hits an
     iptables PREROUTING DNAT rule that relays through wg0 to the target —
     this DNAT rule *is* "the existing proxy/forwarder chaining through
     it," implemented with the tunnel container's own iptables (already
     required for wg-quick/routing) instead of a second tool (socat) the
     pinned image doesn't ship. Recorded as ADR 023 Decision 4.
4. [in-progress] ADR 023 (docs/adr/023-wireguard-tunnel.md) — decisions
   left open by the task: image + pin, key lifecycle, handshake-recency
   probe thresholds, test-rig design, the DNAT-not-socat call, and the
   `via` scope-vs-file-fence resolution (see "Deviations" below).
5. [next] `internal/ports/runtime`: `ContainerSpec.Sysctls map[string]string`
   (additive); Docker adapter wires it to `HostConfig.Sysctls`; fake
   adapter records it (round-trips via Inspect for tests) but doesn't
   interpret it; Kubernetes adapter leaves it unimplemented (documented —
   K8s pod sysctls need node-level allowlisting; out of scope, doc 08
   status note says so).
6. [next] `internal/ports/reconciler`: `TunnelCapableProvider` interface
   (structural marker for `Connection.spec.via`).
7. [next] Connection schema + domain: additive `spec.via` (nameRef,
   managed-only). doc 03 §8.2 additive edit. compatibility.go: validate
   `via` resolves to a `TunnelCapableProvider`.
8. [next] `internal/adapters/providers/wireguard` (new): Provider(type:
   wireguard) is the tunnel initiator; `reconcileInstance` scans
   `req.Resources` for every Connection naming it via `providerRef`,
   builds one wg-quick conf (private key file-mounted, never env) + one
   DNAT/MASQUERADE rule per such Connection into one boot script
   (spec-hashed — a Connection add/remove/key-rotation recreates the
   container, the same trade-off ADR 018 documents for `prometheus`'s
   scrape config). `reconcileConnection` is a thin status/endpoint
   publisher (the container already carries its rule). Probe: handshake
   recency (`wg show ... latest-handshakes`) + upstream dial through the
   forwarder (`runtime.WithReachable`).
9. [next] main.go: register `wireguard` provider + `TunnelProvider` gate
   (Alpha, disabled). scripts/pinned-images.txt: add the pinned image.
   doc 04 §12: append the `TunnelProvider` row.
10. [next] Integration test + testdata: raw (unmanaged) Docker VPC network
    + Postgres + WireGuard responder fixture; managed wireguard Provider +
    Connection + external Source + CDC Binding. Negative reachability via
    `runtime.ProbeReachable` before the tunnel is configured. Key rotation
    via a second `apply` with a new SecretReference value. Destroy leaves
    no container/network artifacts.
11. [next] scripts/test-impact.sh: new `wireguard` suite row.
12. [next] Verify: gofmt, build, vet, go test ./..., test-impact.sh, live
    rig. Recorded below.
13. [next] Commit.

## Verification log

(filled in as steps complete)

## Deviations (recorded, not silently worked around)

- **`Connection.spec.via` is schema-complete but not wired into `proxy`'s
  own forwarder in this task.** The task's file fence marks
  `internal/adapters/providers/proxy` "read-only reference," and ADR 002's
  addendum's literal design ("each proxy route accepts an optional `via`
  field") requires editing `proxy.go`'s `reconcileConnection` to chain a
  route's own forwarder through the named tunnel — which the fence
  forbids. Exercising the task's explicit "or the equivalent the addendum
  sketched" latitude: this task ships the actually-useful, fully working
  half (a tunnel-mediated Connection realized *directly* by the
  `wireguard` provider, which implements `ConnectionCapableProvider`
  itself — no `proxy` involvement needed for the D5 Accept scenario) and
  replants the same "schema carries the seam, wiring waits" discipline
  the original addendum used, one link further down the chain: `via` is
  schema-accepted and validate-time capability-checked (must resolve to a
  `TunnelCapableProvider`) but has no consumer that changes `proxy`'s or
  `ingress`'s own egress yet. Full detail + reasoning in ADR 023's
  "Scope" section.
- `ContainerSpec.Sysctls` is a new runtime-port field this task's spike
  found was genuinely required (not a nice-to-have) for `ip_forward` —
  ADR 023 Decision 5 records why, and why Kubernetes leaves it a documented
  gap rather than a half-implementation.
