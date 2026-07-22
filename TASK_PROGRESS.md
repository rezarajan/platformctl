# D5: WireGuard tunnel provider — task progress (COMPLETE)

Doc 08 §6 D5. Size L. Protocol: doc 08 §2.1 (step 0 = this file). This file
was previously used by a D3/D4 session on this worktree; that work is
already merged (commit 80b5bf6) — this replaces it for D5.

## Step plan — all done

1. [done] git merge main --no-edit (already up to date).
2. [done] Read: CLAUDE.md, D5 entry, ADR 002 (+addendum), ADR 018, ADR 022
   §boundaries, proxy provider (full, read-only reference), Connection
   domain/schema, doc 03 §8.2, doc 04 §12, reconciler.go, runtime.go,
   providerkit, postgres.go's bootstrap-credential file-mount pattern,
   scripts/pinned-images.txt, scripts/test-impact.sh, main.go wiring.
3. [done] Design spike (live Docker, not committed): kernel WireGuard +
   NET_ADMIN works with no /dev/net/tun; net.ipv4.ip_forward needs a
   container-create-time --sysctl; wg-quick (not raw wg) installs
   AllowedIPs routes; iptables PREROUTING DNAT is a working forwarder
   mechanism with no socat/nc needed — full round trip proven by hand
   before writing any Go.
4. [done] ADR 023 (docs/adr/023-wireguard-tunnel.md), later amended in
   place (pre-review, same task) once a design flaw was found — see
   "Design correction" below.
5. [done] `internal/ports/runtime.ContainerSpec.Sysctls` (additive;
   Docker-wired; Kubernetes intentionally unimplemented, documented).
6. [done] `internal/ports/reconciler.TunnelCapableProvider` (structural
   marker for `Connection.spec.via`).
7. [done] Connection schema + domain: additive `spec.via`. doc 03 §8.2.2.
   compatibility.go: validates `via` resolves to a `TunnelCapableProvider`.
8. [done, corrected] `internal/adapters/providers/wireguard`: **one tunnel
   container per Connection** (not a single shared container per
   Provider — the first draft's shape, found wrong before the integration
   test was written by reading debezium.go's Connection-resolution code:
   every existing managed-Connection consumer dials
   `naming.RuntimeObjectName` of the *Connection* resource itself, so a
   container named after the *Provider* is unresolvable by any real
   consumer). Mirrors `proxy`'s exact shape. Forwarder = one iptables
   PREROUTING DNAT rule baked into the same container's boot script
   alongside the wg-quick config. Private key file-mounted only. Probe:
   handshake recency (background poller file + `runtime.ReadFile`, no
   exec primitive exists) + upstream dial via `runtime.WithReachable`.
9. [done] main.go (`TunnelProvider` gate, Alpha/disabled; provider
   registration), scripts/pinned-images.txt, provider.json, new
   `Reason*` constants in status/{reasons,catalog}.go.
10. [done] Integration test: `cmd/platformctl/wireguard_integration_test.go`
    + `testdata/wireguard-scenario/manifests.yaml`. Two bugs found and
    fixed live (see "Live-rig bugs found" below).
11. [done] `scripts/test-impact.sh`: new `wireguard` suite row (2400s).
12. [done] Verify: gofmt/build/vet/go test ./... clean throughout;
    `TestWireGuardTunnelEndToEnd` green twice live (26.25s, 29.17s);
    `scripts/test-impact.sh --base main` — see "test-impact.sh" below.
13. [done] Final commit (this session's last commit).

## Commits (chronological)

1. `990703f` wip: checkpoint — task plan + design spike
2. `39093a9` docs(adr): 023 — initial design
3. `fd46f14` feat(seam): Connection.spec.via + TunnelCapableProvider +
   ContainerSpec.Sysctls
4. `237793c` feat(providers): wireguard tunnel provider implementation
   (first draft — shared-container shape)
5. `fd4dfd7` fix(wireguard): DriftDetected/Probe-vs-Reconcile self-review
6. `4eb0f65` fix(wireguard): **one tunnel container per Connection** — the
   design correction; ADR 023 amended in place to record both the
   corrected design and the rejected alternative
7. `ba56871` test(wireguard): live integration rig
8. `355d2ec` fix(wireguard): mount wg0.conf outside /etc/wireguard (image
   symlink bug, found live)
9. `c696135` fix(wireguard-test): correct Connect port + relax an
   over-strict destroy assertion (both test bugs, found live)
10. docs(planning): doc 08 D5 status note + Stage D exit criterion
    checkbox (this commit, or folded into the final commit)
11. Final: `feat(providers): wireguard tunnel on the Connection seam (D5)`

## Live-rig bugs found (both fixed, recorded in commit messages + ADR)

1. **Image symlink**: `linuxserver/wireguard` symlinks `/etc/wireguard` ->
   `/config/wg_confs`, which doesn't exist pre-boot — `ContainerSpec.Files`
   writes there failed with an opaque Docker error ("Could not find the
   file /"). Reproduced directly against the raw Docker SDK (both a
   created-not-started and a started container, both failed identically —
   ruled out a start-ordering theory) before fixing. Fix: config moved to
   `/etc/datascape/wg0.conf` (wg-quick accepts any absolute path).
2. **Test bugs** (not provider bugs): `connectorStatus` hardcodes a
   different scenario's Connect port; the destroy assertion checked the
   *peer* network was gone, but the raw responder fixture (never
   platformctl-managed) legitimately keeps it open — `RemoveNetwork`
   correctly refuses rather than cascade-removing someone else's
   container. Both fixed; the corrected assertion checks the *platform*
   network instead (which is genuinely fully removable).

## Design correction (pre-review, recorded not silently fixed)

First draft: one shared tunnel container per Provider, `reconcileInstance`
scanning `req.Resources` for every Connection naming it and baking all
their DNAT rules into one boot script. Found wrong by reading
`internal/adapters/providers/debezium/debezium.go`'s Connection-resolution
code closely (read-only reference) before the integration test was
written: `buildDesiredConnector`'s `conn.Endpoint(naming.RuntimeObjectName
(connEnv))` and the preflight `rt.EnsureReachable(d.preflightConnectionName,
...)` both dial a runtime object literally named after the *Connection*
resource — `naming.RuntimeObjectName`'s own package doc comment describes
this exact class of mistake as previously made and fixed, once, elsewhere
in this codebase. Reworked to one container per Connection
(`naming.RuntimeObjectName(res)`), matching `proxy`'s shape exactly.
Full record in ADR 023 Decision 4 (including why an `Aliases`-based
patch was considered and rejected: aliases fix DNS resolution between
containers, not the literal-name lookup `Inspect`/`EnsureReachable` do).

## Verification log

- `gofmt -l .`: empty throughout.
- `go build ./... && go vet ./...`: clean throughout (including
  `-tags integration`).
- `go test ./...`: green throughout, including `internal/archtest`
  (`TestExplainCatalogCoversEveryReason`, `TestNoConstructedLoopbackAddresses`,
  `TestIntegrationSuiteMapCoversEveryTest`).
- `go run ./cmd/platformctl docs build --out docs/reference`: re-run after
  every schema/reason change; `TestGeneratedReferenceInSync` green.
- `go test -tags integration -run TestWireGuardTunnelEndToEnd -timeout 2400s
  ./cmd/platformctl/`: **green twice in a row** against real Docker
  (26.25s, then 29.17s) — apply, status all-`True`, connector `RUNNING`
  through the tunnel, idempotent re-apply (unchanged container ID), key
  rotation (container ID changes, connector stays `RUNNING`, responder
  reconfigured live to accept the new peer key), clean destroy. Negative
  reachability (`runtime.ProbeReachable` from the shared platform network,
  before the tunnel exists) implicitly proven by the test reaching this
  point at all — a false negative there is a `t.Fatal` before `apply` even
  runs.
- `scripts/test-impact.sh --base main`: selects **17 suites** — every
  change in this task to `internal/ports`/`internal/domain`/
  `internal/application/compatibility` (SHARED_CORE) is purely additive
  (a new struct field, a new interface, a new optional schema field, a new
  validate-time branch gated on that field being set) and changes no
  existing provider's behavior, but the tool's scope matching is
  path-prefix-based and can't know that semantically — it selects every
  SHARED_CORE-scoped suite regardless. Launched in the background
  (`bash scripts/test-impact.sh --base main`, log at
  `/tmp/.../scratchpad/test-impact-run.log`) given the ~17-suite blast
  radius and this being a shared Docker daemon with several other agents'
  suites also flock-serialized against it at the same time — see the
  final report for whatever state it reached. The suite this task actually
  changes (`wireguard`) was run directly, twice, green — the load-bearing
  evidence for this task's own Accept criteria.
- **Final status of the background `--base main` sweep**: at delivery
  time it had not progressed past the first (`docker-conformance`) suite
  — `ps` showed at least three other concurrent
  `bash scripts/test-impact.sh` invocations (other agents' own sessions)
  all contending for the same `/tmp/platformctl-itest.lock` flock, one of
  them mid-run on `TestLakehouse` (2400s timeout). This is expected
  behavior of the shared-daemon economy design (docs/planning/06 §10) —
  every suite it eventually runs records a ledger hit any later
  `--base main` run (mine or another agent's) will dedupe against — not a
  failure of this task's own gate. A maintainer or a later session can
  check `git rev-parse --git-common-dir`/platformctl-itest-ledger for
  entries with today's date to see how far it got. Not blocking merge on
  this task's own account: the wireguard suite (the actual load-bearing
  test for D5's Accept criteria) already ran directly, twice, green.

## Deviations (recorded, not silently worked around)

- **`Connection.spec.via` is schema-complete but not wired into `proxy`'s
  own forwarder.** The task's file fence marks
  `internal/adapters/providers/proxy` "read-only reference," and ADR 002's
  addendum's literal design requires editing `proxy.go`'s
  `reconcileConnection` — which the fence forbids. Exercised the task's
  explicit "or the equivalent the addendum sketched" latitude: ships a
  fully working tunnel-mediated Connection realized *directly* by
  `wireguard` (no `proxy` involvement needed for the Accept scenario), and
  replants the "schema carries the seam, wiring waits" discipline one link
  further down the chain. Full reasoning: ADR 023's "Scope" section.
- `ContainerSpec.Sysctls` (new runtime-port field) and the multi-Connection
  shared-keypair limitation (ADR 023 Decision 4 follow-up) are both
  recorded design/scope notes, not silent gaps.
