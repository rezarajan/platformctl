# Fix: OpenZiti mediated-connection Kubernetes bind path — task progress

Worktree: agent-a62b79fe9269f0337. Branch: worktree-agent-a62b79fe9269f0337.
Started `git merge main --no-edit` (fast-forwarded abbbd1b -> d5045c1, pulling
in the H6 K8s test file `cmd/platformctl/openziti_kubernetes_integration_test.go`
and the rest of the H6/H8/H5 merge landed on main).

Goal: fix `TestOpenZitiMediatedConnectionOnKubernetesEndToEnd` — the last red
suite — green twice live, Docker leg green once (regression), unfiltered
`go test ./...` = 0, lint clean, additive Done-note, one squashed commit.

## Reading done

- [x] docs/adr/027, 022 (+ 2026-07-23 domain-of-record addendum), the H6 spec
  + Done-note in docs/planning/08 §7.7, internal/adapters/providers/openziti
  fully (openziti.go, instance.go, connection.go, client.go, identity.go),
  internal/application/engine/domainruntime.go (H5 decorator — holes/
  buildCrossDomainIngressPolicy mechanism), internal/adapters/runtime/
  kubernetes/{network,convert,container}.go (B7 walls, Service creation
  rules), the failing test itself.

## Root-cause verification (orchestrator's hypothesis checked, corrected)

- [x] **Hypothesis as given** (namespace-qualified terminator FQDN + ingress
  hole for the bind side) — checked against the actual accept scenario
  (`testdata/openziti-k8s-scenario`): every Provider pins the SAME
  `runtime.network: datascape-zk8s`, i.e. one K8s namespace, zero domain
  crossing. Live `kubectl exec ... getent hosts zk8s-pg` from inside the
  router pod resolved correctly. **Hypothesis does not explain the observed
  failure for this scenario** — recorded as corrected, not silently
  discarded (full account in doc 08 H6's new addendum note, same commit).
- [x] **Actual root cause #1**, found live (router pod logs): the dial-side
  tunneler's SDK failed with `dial tcp: lookup zk8s-mesh-router ...: no such
  host` / `no edge routers connected in time` — before any bind-side dial
  was ever attempted. `instance.go`'s router `ContainerSpec` declared no
  `Ports`, so Kubernetes never created its Service (`ensureOneService`
  skips Service creation when `len(Ports)==0`) — no DNS record for
  `ZITI_ROUTER_ADVERTISED_ADDRESS`. Docker never needed this (embedded DNS
  resolves any container name regardless of published ports).
- [x] **Actual root cause #2**, found live only after #1 was fixed and the
  test progressed far enough to reach a second reconcile: router's and
  dial-side tunneler's `ContainerSpec.Env` conditionally carried a one-time
  enrollment JWT keyed to a LIVE, async fact (`isVerified` / identity
  existence) re-queried fresh every reconcile — a `CLAUDE.md` idempotency
  violation. `drift` immediately after `apply` recomputed a different spec,
  forcing an unwanted Deployment rollout, restarting pods mid-test.

## Fix shipped (all within internal/adapters/providers/openziti — zero edits
outside the adapter; decoupling contract / archtests untouched)

- [x] `instance.go`: router `ContainerSpec.Ports` now declares
  `{ContainerPort: ic.RouterPort, Audience: AudienceInternal}` (fix #1).
- [x] `instance.go`: `waitEdgeRouterVerified` + a settle-and-reconverge
  second `EnsureContainer` call (token stripped) once the router's real
  enrollment completes, before Reconcile returns (fix #2, router side).
- [x] `connection.go`: settle-and-reconverge second `EnsureContainer` call
  (token stripped) for the dial-side tunneler, no wait needed —
  `upsertIdentity`'s "already exists" branch is a synchronous idempotency
  check (fix #2, dial-side).
- [x] Docker leg: both fixes are inert there (`AudienceInternal` only
  affects `ExposedPorts` metadata on Docker; the settle logic just adds one
  extra idempotent round-trip) — verified live, not just reasoned.

## Verification (all live, this session)

- [x] `gofmt -l .` — clean
- [x] `go build ./...` — clean
- [x] `go vet ./...` and `go vet -tags integration ./...` — clean
- [x] `go test ./...` unfiltered — true-exit=0
- [x] `golangci-lint run` (pinned v2.12.2, matches ci.yml) — 0 issues
- [x] `TestOpenZitiMediatedConnectionOnKubernetesEndToEnd` — green TWICE,
  live, back-to-back, each a fresh `-count=1` compile against the shared
  minikube (minted minimal-RBAC kubeconfig, fresh token minted this
  session): 77.34s and 76.41s. All three proofs passed both times
  (apply→Ready, CDC RUNNING through the mediated Connection, wrong-identity
  dial refused).
- [x] `TestOpenZitiMediatedConnectionEndToEnd` (Docker leg, regression) —
  green, 27.84s.
- [x] Manual live confirmation that the churn (fix #2) is actually gone:
  fresh apply + 3x repeated `drift` showed 0 pod restarts (pre-fix: 2/2
  repro attempts showed a router+tunneler Deployment rollout on the very
  first post-apply probe).

## Docs

- [x] Additive Done-note appended under H6's existing Done-note in
  docs/planning/08 §7.7 (`#### Addendum (2026-07-23): Kubernetes bind-path
  fixed`) — every existing line preserved verbatim, guard-hook-additive.

## Commit

- [ ] One final squashed commit:
  `fix(openziti): Kubernetes bind path — FQDN terminator + compiled ingress
  hole (H6 substrate parity)` (message text pinned by the task prompt; body
  corrects the hypothesis and states the actual mechanism shipped, per
  report).
