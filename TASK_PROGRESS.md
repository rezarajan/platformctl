# I1: Consume Connection.spec.via — task progress

## Design (locked in after reading ADR023, doc02 §4.1/4.2, doc08 §7.8 I1)

`proxy` realizes a Connection whose `spec.via` names a tunnel-capable
Provider (wireguard). Packet path requires a real container with `wg0` for
proxy's socat forwarder to dial into (Docker bridge networking gives no L3
route through another container's netns without DNAT — verified against
ADR023 Decision 4's own reasoning). Chosen shape:

- `wireguard`'s Provider-kind `reconcileInstance` scans `req.Resources` for
  managed Connections whose `spec.via` names it, and for each, ensures a
  "via tunnel" container (same wg-quick/DNAT machinery as its own
  `reconcileConnection`, reused), named `<conn>-via-tunnel` (never
  `naming.RuntimeObjectName` — that identity is reserved for the object a
  third party independently re-derives; here proxy learns the name only via
  a published engine fact, ADR 015), attached ONLY to the transit network
  (`configuration.peerNetwork`). Publishes an endpoint fact named
  `connection.ViaFactName(ns,name)` with the tunnel's dial address.
- `reconciler.Request.TunnelFacts` (new field, mirrors CatalogFacts/
  WarehouseFacts): `TransitNetwork` (read directly from the via Provider's
  static `spec.configuration.peerNetwork` — no state dependency) and
  `Internal` (the published per-Connection fact above — state dependency,
  ordered by a new `via` graph edge, graph.go refFields).
- `proxy`'s `reconcileConnection`, when `conn.Via != nil`: joins the
  forwarder container to `[platform, TunnelFacts.TransitNetwork]` and
  dials `TunnelFacts.Internal` instead of `conn.Target` directly. Probe's
  existing dial-the-forwarder settle check is unchanged (transparent to
  via) since the socat target is baked in at container-create time.
- `compatibility.go`: deletes the "not consumed yet" refusal; adds a
  pairing check — Connection's OWN realizing provider must implement a new
  `reconciler.ViaConsumingProvider` marker (implemented by `proxy`, NOT by
  `wireguard` itself — wireguard realizes tunnels directly, doesn't chain
  through a second one).

**Deviation (recorded, not silently cut):** "excess network attachment is
drift" (task's Do-text) is NOT implemented as a live Probe check —
`runtime.ContainerState` has no attached-networks field today, and adding
one is a real port-wide change (docker/fake/k8s adapters + conformance
suite) out of proportion to this task's M size. Achieved by construction
instead (reconcile never attaches more than [platform, transit]); noted in
ADR023's closure note and doc08's I1 Done-note.

## Steps

1. [x] internal/ports/reconciler/reconciler.go — TunnelFacts + Request field,
   ViaConsumingProvider interface
2. [x] internal/domain/connection/connection.go — ViaFactName helper
3. [x] internal/domain/graph/graph.go — "via" refFields edge
4. [x] internal/adapters/providers/wireguard/wireguard.go — reconcileViaTunnels,
   wire into reconcileInstance + Destroy
5. [x] internal/adapters/providers/proxy/proxy.go — consume TunnelFacts,
   implement ViaConsumingProvider
6. [x] internal/application/engine/engine.go — resolveTunnelFacts wired into
   resolveRequest
7. [x] internal/application/compatibility/compatibility.go — delete refusal,
   add pairing check
8. [x] internal/application/compatibility/compatibility_test.go — replace
   TestConnectionViaNotConsumedRefused
9. [x] docs: doc03 §8.2.4 item 3 additive note; ADR023 closure note; doc08 I1
   Done-note — all applied cleanly (additive appends, no guard-hook issue)
9b. [x] unit tests added: proxy_test.go (via joins transit net + dials
   TunnelFacts.Internal; nil-facts honest error) and wireguard_test.go
   (reconcileViaTunnels creates+publishes fact; honest failure when
   unreachable) — all passing, gofmt/vet/go build clean (both tag sets)
10. [x] cmd/platformctl/wireguard_integration_test.go + testdata — extended:
    Connection now realized by new `wg-edge` (proxy) Provider with
    `via: {name: wg-tunnel}`; forwarder=wg-orders-db-conn (unchanged name),
    via-tunnel=wg-orders-db-conn-via-tunnel (new, created by wireguard's
    reconcileInstance); negative proof probeVPCFromTransitFails added
    (raw socat container, not rt.ProbeReachable — see its doc comment for
    why); idempotency/key-rotation assertions split across both
    containers (forwarder must NOT recreate on tunnel key rotation —
    isolation proof); destroy assertions extended to the via-tunnel
    container. `platformctl validate`/`lint` pass clean against the
    updated manifest (10 resources, 1 pre-existing unrelated info finding).
11. [x] gofmt/build/vet clean (both tag sets); `go test ./...
    ; echo true-exit=$?` = 0 (unfiltered, confirmed)
12. [x] test-impact.sh sweep — 18 suites selected (broad: Request struct is
    a core cross-cutting port type, per doc06 §10 point 3 this is the
    expected "broad sweep" trigger, not a bug in suite selection). Launched
    in background (nohup, pid noted at launch), KUBECONFIG set to
    /tmp/claude-1000/platformctl-rbac/platformctl.kubeconfig. LOG:
    /tmp/claude-1000/-home-cascadura-git-platformctl/3ff96d5f-6a0c-4676-8628-0810b1d9fe68/scratchpad/i1-test-impact-sweep.log
    — not polled per orchestration rules; check this log (or re-run
    `bash scripts/test-impact.sh --base main --print` for ledger status)
    on resume.
13. [x] final squashed commit — GPG signing timed out on 3 attempts (WIP,
    full heredoc, `git commit -F COMMIT_MSG.txt` under `timeout 20`, all
    hung on pinentry / gpg-agent). Per task instructions: fell back to
    staged + COMMIT_MSG.txt at repo root (`git add -A` already run; every
    file above is staged; COMMIT_MSG.txt holds the exact intended
    message, subject `feat(connection): consume spec.via — tunnel-routed
    managed Connections (I1)`). Once GPG unlocks, run from repo root:
    `git commit -F COMMIT_MSG.txt && rm COMMIT_MSG.txt`.

ALL STEPS DONE except the literal commit object (GPG-blocked, staged) and
the integration sweep result (running in background, queued behind
another agent's sweep — do not poll; read the log or re-run
`bash scripts/test-impact.sh --base main --print` for ledger status on
resume).

## Notes / open questions

- graph.go via-edge namespace resolution mirrors providerRef exactly.
- wireguard's own directly-realized Connections (D5, unchanged) are
  untouched by this — reconcileConnection/Destroy for Kind=="Connection"
  in wireguard.go is not modified.
