# Design note 023 — WireGuard tunnel provider on the Connection seam

**Status:** accepted; implemented (docs/planning/08 Stage D, task D5).
**Prompted by:** docs/planning/08-production-readiness-plan.md §6 D5 —
docs/adr/002's addendum designated `Connection` as the seam a future
`tunnel`-typed provider chains through for VPC/VPN reach; D5 is that
provider. The task text and the addendum leave four things genuinely open:
which image (and how it's pinned), what "key lifecycle" means when the key
is file-mounted only, what a handshake-recency probe threshold should be,
and how a Docker-only test rig proves a negative ("unreachable without the
tunnel") for real. This note decides those four, plus a scope question the
task's own file fence forces (see "Scope").

## Decision 1 — image: `linuxserver/wireguard`, pinned by digest

### Options considered (research, 2026-07)

1. **`linuxserver/wireguard`** (chosen). Alpine-based (`ghcr.io/linuxserver/
   baseimage-alpine:3.24`), actively released (weekly-cadence tags tracking
   upstream `wireguard-tools`), ships `wireguard-tools` (`wg`/`wg-quick`)
   and `iptables`/`iproute2` — verified live (`which wg wg-quick iptables
   ip` all resolve; `wg --version` reports the pinned release). Supports a
   "bring your own `wg-quick` config" mode (drop a complete `.conf` into
   `/config/wg_confs/` and it just runs it) alongside its auto-generating
   "server" mode — this task uses only the former, driving `wg-quick`
   directly via a custom entrypoint rather than the image's own
   PEERS/SERVERURL auto-config path (more control, easier to reason about
   and test).
2. **`qdm12/wireguard-docker` / `procustodibus/wireguard`**. Smaller
   (~13MB), also Alpine+wireguard-tools+iptables, also maintained. Rejected
   only on the margin: lower download/update cadence and community size
   than linuxserver's (a multi-project org whose images this codebase
   already trusts implicitly — Nessie/Marquez-adjacent tooling choices
   elsewhere follow the same "prefer a widely-used maintained publisher"
   bar `docs/planning/07` states for pinned images generally). Either would
   have worked technically; this is a maintainer-trust tiebreak, not a
   capability difference.
3. **A custom-built image** (Alpine + `wireguard-tools` + a second tool).
   Rejected: this codebase has no image-build pipeline in `apply` — every
   pinned image is pulled, never built (`scripts/pinned-images.txt`'s own
   header: "Release-tested default images"). Building one would be new
   infrastructure this single task shouldn't introduce.

### The pin

`linuxserver/wireguard:1.0.20260223@sha256:2868ae5e3dd9065ea3b1e44b4214b33b02b7ce5ebcb9e4f33e1132b75007f39c`
— resolved live via `docker pull`/`docker manifest inspect` against the
current tag at task time, recorded in `scripts/pinned-images.txt` (the
`refresh-digests.sh` source-of-truth list) and
`internal/adapters/providers/wireguard/wireguard.go`'s `defaultImage`,
matching every other provider's pin (`postgres`, `alpine/socat`, `caddy`,
...).

## Decision 2 — NET_ADMIN, documented plainly

The tunnel container runs with `SecurityContext.CapAdd: ["NET_ADMIN"]` —
required to create the `wg0` interface (`ip link add ... type wireguard`),
assign it an address, bring it up, and (Decision 4) install `iptables`
NAT/forward rules. This is a real, broad capability grant (NET_ADMIN can do
far more than WireGuard alone needs — reconfigure any interface, routing
table, or firewall rule in the container's own network namespace) — no
narrower POSIX capability covers "create a WireGuard interface," and Linux
does not expose one. Scoped by the ordinary boundary every other capability
grant in this codebase already relies on: a Linux network namespace, one
per container, so NET_ADMIN here reaches only this container's own
interfaces/routes/rules, never the host's or any other container's. Verified
live: the smoke spike (this note's Decision 4) never touched anything
outside the tunnel container's own netns. Documented here, in the
provider's package doc comment, and in doc 03 §8.2's `via`/wireguard
example — the task's explicit "NET_ADMIN documented in the provider docs +
ADR" requirement.

## Decision 3 — key lifecycle: file-mounted, rotation is a container recreate

**Where the key lives:** the WireGuard private key is resolved from
`SecretReference` (via `spec.secretRefs` + `configuration.privateKeySecretRef`,
the same `providerkit.ResolveCredential` mechanism `postgres`'s bootstrap
password uses) and placed *only* in the `wg-quick` config file mounted via
`runtime.FileMount` — never `Env` (readable by `docker inspect`), never
`status.ProviderState` (readable by `platformctl state inspect`), never
logged. This mirrors `postgres.go`'s `superuserPasswordPath` discipline
exactly (`docs/planning/07` Gate 1 checkbox 4): a `*_FILE`-style secret
channel, not a `*_FILE`-suffixed env var pointing at a file this provider
also writes with the same content, since `wg-quick` has no separate
key-file-path option — the private key is a config-file directive
(`[Interface] PrivateKey = ...`), so the config file *is* the secret file.

**Rotation:** unlike `postgres`'s credential rotation (a live database
session that must keep authenticating through a password change, needing
`providerkit.CredentialRotation`'s try-desired/try-previous/rotate-live
state machine), a WireGuard tunnel has no live authenticated session to
preserve — the protocol's own handshake is cheap and stateless across
restarts. So key rotation here is deliberately *not* a live in-place `wg
set wg0 private-key ...` call: the resolved private key is part of the
`wg0.conf` content in `ContainerSpec.Files`, which already participates in
the container's spec hash (`docs/adr/018`'s "Content participates in the
spec hash (one-way), so changing it replaces the container like any other
field" — the exact mechanism `EnsureContainer` already implements and
tests, needing no new code here). A new `SecretReference` value on the next
`apply` changes the file content, changes the hash, and `EnsureContainer`
recreates the container with the new key — which re-establishes the tunnel
from scratch (a fresh `wg-quick up`), satisfying the Accept criterion "key
rotation via SecretReference re-establishes the tunnel" by construction,
not by new rotation logic.

## Decision 4 — the forwarder: iptables DNAT, not socat

The task's Do-text says the tunnel container joins the peer network "with
the existing proxy/forwarder chaining through it." `proxy` itself is a
read-only reference for this task (docs/planning/08's file-ownership
note) — its `reconcileConnection` is not touched, so `via`-chaining through
an *existing* `proxy`-realized Connection is not wired in this task (see
"Scope," below). What *is* implemented: a Connection realized directly by
`wireguard` gets a forwarder in the same conceptual sense `proxy`'s
does — a stable local listen port relaying byte-for-byte to the real
upstream — reusing the concept, not the tool. `alpine/socat` (proxy's tool)
is not in the pinned wireguard image, and installing a second tool into it
at `apply` time (`apk add`) would make the image's content
network-dependent and non-reproducible, against the whole point of pinning
(`scripts/pinned-images.txt`). The tunnel container already needs
`iptables` for WireGuard's own routing (any container acting as a gateway
between a tunnel interface and a "real" interface needs
`net.ipv4.ip_forward=1` plus a `POSTROUTING -j MASQUERADE` rule so return
traffic routes back correctly — Decision 5) — so one more `iptables -t nat`
rule per Connection (`PREROUTING -p tcp --dport <conn.port> -j DNAT
--to-destination <conn.target>`) is not a new tool, just one more rule of a
kind the container already carries. Verified live (this note's spike): a
second container, attached only to the same "transit" network as the
tunnel container (not the isolated network the real target lives on),
dialing the tunnel container's transit-network address on the forwarder
port, was relayed through `wg0` to the target — full round trip, byte
counters incrementing on both WireGuard peers. Kernel NAT/conntrack is also
more robust under concurrent and long-lived connections than a userspace
relay would be, which matters for the Accept scenario's sustained CDC
replication connection.

**One tunnel container per Connection, not one shared container per
Provider.** `reconcileConnection` (the Connection-kind call) runs its own
`EnsureContainer`, named `naming.RuntimeObjectName(res)` — the Connection's
own name — carrying that one Connection's `wg-quick` config and its one
DNAT/MASQUERADE rule. `reconcileInstance` (the Provider-kind call) only
anchors the shared platform network and `configuration.peerNetwork`,
mirroring `proxy`'s own Provider-kind reconcile exactly (no central
container there either). This is the same shape every existing
`ConnectionCapableProvider` in this codebase uses (`proxy`: "one socat
forwarder container per route"; `ingress`: one shared Caddy container is
the *exception*, justified by Caddy's own admin API — Decision 3 of
`docs/adr/018` — which this provider's image has no equivalent of).

A first draft of this provider tried the opposite shape — one shared
container per Provider, with `reconcileInstance` scanning `req.Resources`
for every Connection naming it and baking all of their DNAT rules into one
boot script (motivated by `ContainerRuntime` having no "run a command
inside an already-running container" port method, so a *shared* container's
rules would need to be fully regenerated and the container recreated on any
Connection change — the same trade-off `docs/adr/018` documents for
`prometheus`'s scrape config). **Found wrong before implementation
finished, by reading `debezium.go`'s Connection resolution
(`internal/adapters/providers/debezium`, read-only reference) closely**:
`buildDesiredConnector` resolves a managed Connection's dial address as
`conn.Endpoint(naming.RuntimeObjectName(connEnv))` and its preflight check
calls `rt.EnsureReachable(ctx, d.preflightConnectionName, d.preflightPort)`
with that same Connection-derived name — i.e. every existing consumer of a
managed Connection expects a *runtime object literally named after the
Connection* to exist and answer. A shared container named after the
Provider instead would leave that name unresolvable — not a subtle
behavioral difference, a hard "container not found" failure the first time
any real consumer (starting with `debezium`) tried to dial through it. Since
`ContainerRuntime.Inspect`/`EnsureReachable` look up a container by its
literal name (not a Docker network alias — `ContainerSpec.Aliases` was
briefly considered as a fix and rejected: aliases resolve DNS for
container-to-container traffic, but `Inspect`/`EnsureReachable` do a literal
name lookup, not a DNS resolution, so an alias would fix data-plane dialing
while leaving the Ready-gating preflight/probe calls broken), the only
correct fix was the one-container-per-Connection shape above. Recorded here
rather than silently fixed because it is exactly the kind of cross-provider
naming-contract check `docs/planning/08 §2.1` step 2 ("map the task to the
interfaces it touches... read the doc comments") exists to catch, and is
worth a future provider author reading before assuming a shared-container
shape is free to choose.

A real cost of the corrected shape: multiple Connections naming the same
wireguard Provider each get an *independent* WireGuard session dialing the
same peer with the *same* static keypair (the Provider-level
`configuration.privateKeySecretRef`) — WireGuard's own roaming behavior
means the peer tracks one active endpoint per public key, so two
simultaneous sessions from the same identity can contend for which one the
peer routes return traffic to. Not exercised by this task's Accept scenario
(exactly one Connection); recorded as a follow-up, not fixed here.

## Decision 5 — a new runtime-port field: `ContainerSpec.Sysctls`

Found live, not anticipated from reading the task alone: `net.ipv4.ip_
forward=1` must be set at container-*create* time. Writing
`/proc/sys/net/ipv4/ip_forward` from inside an already-running container —
even with `NET_ADMIN` — fails (`Read-only file system`); Docker only makes
a per-namespace sysctl writable when it was named in the container's own
`--sysctl`/`HostConfig.Sysctls` at creation. No existing `ContainerSpec`
field carries this (`SecurityContext.CapAdd`/`CapDrop`/`SecurityOpt` are
capabilities and Docker-only escape hatches, not sysctls). Added
`ContainerSpec.Sysctls map[string]string` (additive; zero value is today's
behavior, byte-for-byte, for every existing provider) — Docker adapter maps
it to `container.HostConfig.Sysctls`; the fake adapter records it
(round-trips through `Inspect` for tests) without interpreting it;
Kubernetes is left **not implemented** (a pod's `securityContext.sysctls`
entry needs the field to be in the cluster's "safe" sysctls allowlist or
the node's kubelet to opt into unsafe sysctls — a cluster-operator
decision this codebase has no way to make on the operator's behalf, unlike
`NET_ADMIN` which K8s grants per-pod unconditionally via
`securityContext.capabilities.add`). Recorded in doc 08's D5 status note as
an explicit scope line: **this task is Docker-only; NET_ADMIN + the
`ip_forward` sysctl on Kubernetes is future work**, not a silent gap.

## Decision 6 — probe: handshake recency + upstream dial

`Probe` on the Connection reads `wg show wg0 latest-handshakes` (via
container-log/inspect-adjacent access to the already-identified tunnel
container, the same "ask the thing itself, never guess" discipline
`docs/adr/015` requires) and treats a handshake older than **3x the peer's
`PersistentKeepalive` interval** (default keepalive 25s -> 75s threshold)
as stale — WireGuard's own documented behavior is a rekey roughly every 2
minutes under active traffic and a keepalive-driven handshake at the
configured interval when idle, so 3x keepalive is comfortably inside a
healthy interval while still catching a peer that has gone silent for
several keepalive cycles. A zero/never handshake (interface freshly up,
first handshake not yet completed) is treated as `Progressing`, not
`Ready: false` with a hard failure, for a bounded startup window (the same
"still coming up vs. actually broken" distinction `WaitHealthy`/`WithReachable`
already draw elsewhere) — then a real dial through the forwarder
(`runtime.WithReachable` against the tunnel container's forwarder port) is
the second, stronger half of the probe: a stale handshake with the
upstream still dialable successfully is logged as drift but not failed
outright (WireGuard's own roaming/NAT-traversal behavior can show a stale
`latest-handshakes` while the tunnel is still functionally up); a dial
failure through a *fresh* handshake is the clear failure case. Mirrors
`proxy.probeThroughForwarder`'s "forwarder up but upstream unreachable"
distinction conceptually (read-only reference, not shared code).

## Decision 7 — test rig: raw Docker fixtures, not platformctl-managed ones

The Accept scenario needs a "database UNREACHABLE without the tunnel"
negative proof to be *real*, not structural — and the shipped `postgres`
provider (off-limits to edit for this task) always publishes port 5432 to
the Docker host, which the test process itself could dial directly
regardless of any Docker network topology, trivially defeating a
host-side negative proof. Two fixes, both already-idiomatic:

1. **The negative proof uses `runtime.ProbeReachable`**, not a host-side
   dial — it answers "can a container *on network X* reach *target*,"
   which is the actually-relevant claim (Debezium, a container on the
   shared platform network, cannot reach the database without the tunnel)
   and is unaffected by whether the database also happens to have an
   (irrelevant, unrelated) host-published port.
2. **The database itself is an unmanaged (`external: true`) Source**,
   stood up by the test as a plain `postgres:16` container via the same
   `runtime.ContainerRuntime` primitives providers use — attached *only*
   to the isolated "VPC" network, `Audience: internal` (no host publish at
   all) — with the replication role, publication, and `wal_level=logical`
   bootstrapped by the test directly over SQL (the same bootstrap
   `postgres.go`'s `reconcileSource` would do for a managed Source; not
   reusable here since `external: true` never invokes it, and imitating it
   is legitimate test-fixture code, not an edit to the forbidden package).
   This is exactly the existing `external: true` + `connectionRef`
   mechanism (`docs/adr/002`'s original design, shipped since v1.0.0) —
   the wireguard-realized `Connection` gives Debezium (on the shared
   network, no knowledge of the VPC network or peer network at all) a
   `<connection-name>:<port>` address to dial; the standard resolution
   engine wires the credential and endpoint the same way it would for any
   other external database behind a Connection.

The **WireGuard responder** (the VPC's own gateway, playing the role of
"a corporate VPN concentrator platformctl doesn't own or provision") is
likewise raw test fixture, using the same pinned `linuxserver/wireguard`
image configured as a plain `wg-quick` server via a hand-built config —
not a second provider. The provider this task ships is the **initiator**
role only, matching real-world usage: an organization dials *into* an
existing VPN gateway it doesn't ask platformctl to also stand up.

## Scope: `Connection.spec.via` is schema-complete, not `proxy`-wired

ADR 002's addendum literally designed `via` as a field *on `proxy`'s own
Connection route*, chaining that route's egress through a named tunnel
provider — which requires editing `proxy.go`'s `reconcileConnection`. This
task's file fence marks `internal/adapters/providers/proxy` **read-only
reference**, the same posture `docs/adr/018` held toward it while adding
`ingress` as a second, independent `ConnectionCapableProvider` realization
without touching `proxy.go` at all. D5's own task text grants an explicit
escape hatch here — "implement `Connection.spec.via` **or the equivalent
the addendum sketched**" — exercised as follows:

- **What ships and works today:** a tunnel-mediated Connection realized
  *directly* by `wireguard`, which implements `ConnectionCapableProvider`
  itself (scheme `tcp`) — no `proxy` involvement needed. This is a complete,
  independently useful realization of "a managed Connection whose upstream
  is only reachable through a WireGuard peer," the task's own framing of
  what D5 delivers, and is what the Accept scenario (CDC through the
  tunnel) exercises end to end.
- **What `Connection.spec.via` means today:** schema-accepted (additive,
  `nameRef`, managed-Connections-only) and validate-time capability-checked
  — the named Provider must implement the new `reconciler.
  TunnelCapableProvider` marker interface (`wireguard` does) — but has no
  consumer that changes `proxy`'s or `ingress`'s own egress yet. This
  replants the exact discipline the original (pre-remodel) `proxy` design
  used for the *first* link of this chain ("schema-accepted, validation
  rejects it as not-yet-supported" — `docs/adr/002`) for the *next* link
  (an existing forwarder chaining through an existing tunnel), now that the
  first link (a tunnel provider existing at all) is real. Wiring `proxy`/
  `ingress` to consume `via` is unstarted work, not a design gap — the
  seam (`TunnelCapableProvider`, and `Connection.spec.via` naming it) is
  exactly what a future task needs to land it without a schema change,
  matching this whole ADR chain's running discipline.

This is recorded as a **deviation** in the D5 commit and `TASK_PROGRESS.md`
(doc 08 §2.1's "a deviation you cannot avoid is a finding, not a judgment
call"), not a silent scope cut — the maintainer may prefer the literal
addendum wiring once `proxy.go` is back in scope for a future task.

## Relationship to ADR 022 (identity-aware mediation / OpenZiti)

ADR 022 §"Explicit boundaries" already draws this line in the abstract;
this note makes it concrete now that both a network-layer tunnel (this
ADR) and a planned identity-layer mesh (ADR 022 Ring 2, `MediatedConnection`
via OpenZiti) exist as named things:

- **WireGuard (this ADR) is network-layer reach**: it answers "how does a
  packet destined for a private VPC subnet get there at all" — no identity
  beyond the two static keypairs configured into the tunnel, no per-service
  policy, no dial/bind authorization finer than "anyone who can reach the
  forwarder's listen port reaches whatever `AllowedIPs` routes to." It is
  the right tool for exactly one thing: crossing a network boundary
  platformctl doesn't otherwise have a path across.
- **OpenZiti (ADR 022 Ring 2) is identity-layer mediation**: per-workload
  cryptographic identity, dial/bind policy compiled from ADR 021 rules, and
  (per ADR 022's dark-services posture) no listening port on any shared
  network at all — a fundamentally stronger posture than a WireGuard
  forwarder's "reachable to anyone on the transit network" default.
- **They compose; neither replaces the other.** A WireGuard tunnel can be
  the network-layer path a mesh's own router dials across to reach a
  workload that lives behind a VPC boundary the mesh has no other way to
  cross — exactly the relationship a real-world Ziti or Consul deployment
  already has with an underlying VPN when the two networks aren't otherwise
  routed. Nothing in this design boxes that out: `wireguard` is one more
  `ConnectionCapableProvider`, the same seam `MediatedConnection` will
  realize against.

## Feature gate

`TunnelProvider` — Alpha, disabled by default (`docs/planning/04-roadmap-
and-feature-gates.md` §12, already named as a planned gate before this
task). Matches the `IngressProvider`/`TrinoProvider`/`JDBCSinkProvider`
posture, not the Phase 6.5 enabled-Alpha precedent: a new provider granting
`NET_ADMIN` and opening a routed path into a private network is a
meaningfully different risk profile from `NessieProvider`/`OpenLineageProvider`'s
"one more REST endpoint behind the platform network."

## Follow-ups (non-blocking)

- Wire `Connection.spec.via` into `proxy`'s (and/or `ingress`'s) own
  `reconcileConnection` once those packages are back in scope for a task —
  the seam (`TunnelCapableProvider`) is ready for it, no schema change
  needed (see "Scope," above).
- `ContainerSpec.Sysctls` on Kubernetes — needs a documented
  cluster-operator opt-in story (allowlisted safe sysctls, or an
  unsafe-sysctls kubelet flag), not something this task can decide
  unilaterally on the operator's behalf.
- A `TunnelCapableProvider.SupportsTunnelChaining()`-driven `ingress`/
  `proxy` chaining test once the follow-up above lands.
- OpenZiti-over-WireGuard composition (ADR 022 Ring 2 reaching a
  WireGuard-only-reachable workload) — no design work done here beyond
  naming the relationship; revisit once `MediatedConnection` exists.
- Multiple Connections against one wireguard Provider each dial the peer
  independently with the same static keypair (Decision 4's "found wrong
  before implementation finished" note) — fine for one Connection per
  Provider (this task's Accept scenario), a real limitation for more than
  one. A per-Connection derived identity (e.g. a deterministic sub-key, or
  requiring a distinct `configuration.privateKeySecretRef` per Provider
  instance and one Provider per Connection) is the natural fix, not
  designed here.

## Cross-references

- `docs/adr/002` (Connection's origin as the seam; the addendum's original
  `via` sketch) and its addendum.
- `docs/adr/018` (Connection's second realization, `ingress`; the
  spec-hash-recreate trade-off this note reuses for Decision 4/7; the
  `proxy`-as-read-only-reference precedent this note's "Scope" section
  follows).
- `docs/adr/022` (identity-aware mediation; the network-layer/identity-layer
  relationship this note's own section above makes concrete).
- `internal/adapters/providers/proxy` — read-only reference (forwarder
  concept, managed/external Connection lifecycle split), not imported or
  edited.
- `internal/adapters/providers/postgres` — read-only reference (file-mounted
  bootstrap credential discipline), not edited.
- `docs/planning/03-resource-model-reference.md` §8.2 (`Connection`,
  additive `via` + wireguard example) and §12's `TunnelProvider` gate row.
- `docs/planning/08-production-readiness-plan.md` §6 D5 (this task).
