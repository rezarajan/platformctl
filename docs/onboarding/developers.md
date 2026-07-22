# Developer onboarding

You're about to write Go code for platformctl, or review a change to it. This page is the fast
path through the planning package and the codebase's own conventions — it links to the
authoritative docs rather than restating them, so it stays true as they evolve.

## Reading order

Mirrors [docs/README.md](../README.md)'s contributor row, with the *why* spelled out:

1. **`CLAUDE.md`** (repo root) — the one invariant (domain/ports never import an adapter) and
   the pre-coding checklist. Read this first every session; it's short on purpose.
2. **[docs/planning/01-product-requirements.md](../planning/01-product-requirements.md)** — what
   Datascape is and deliberately isn't (goals G1–G8, non-goals NG1–NG7). Skipping this is how
   you end up building a general-purpose orchestrator (NG2) by accident.
3. **[docs/planning/02-architecture.md](../planning/02-architecture.md)** — layering, module
   layout, the `reconciler.Provider` contract, capability interfaces, the exact validate-error
   shapes, CLI surface, testing strategy. This is the file you re-open mid-task, not just once.
4. **The sections of [docs/planning/03-resource-model-reference.md](../planning/03-resource-model-reference.md)
   your task touches** — every Kind, field by field. If your task changes a schema, this file
   changes in the same commit (see Docs rules below).
5. **[docs/planning/06-agentic-execution-guide.md](../planning/06-agentic-execution-guide.md)
   §3's pre-coding checklist** — the concrete "did you read the interface doc comment, not
   remember it" step before you write a line of code.
6. **[docs/adr/](../adr/README.md)** — decisions are settled; a task that needs to revisit one
   starts with a new ADR, not a re-litigation mid-diff. The index tells you which ADR covers
   which area (ports/providers → 008/009/015/016; destructive surfaces → 013; gates → 014;
   validation → 011).

## The one invariant

> If you're about to import an adapter package from `domain` or `ports`, stop.

A 10-line tour with real package names (docs/planning/02-architecture.md §2):

```text
internal/domain/...          resource, source, eventstream, binding, dataset, provider,
                              catalog, connection, secret, status, lineage, graph, endpoint
                              — imports nothing else in this repo.
internal/ports/...           runtime, reconciler, state, secretstore, clock
                              — interfaces + conformance suites. Imports domain only.
internal/adapters/...        runtime/docker, runtime/kubernetes, providers/redpanda,
                              providers/postgres, providers/nessie, state/localfile,
                              secrets/env, ... — implement ports, may import third-party SDKs.
internal/application/...     manifest, compatibility, plan, engine, registry, featuregate
                              — orchestrates ports; registry + cmd/platformctl are the ONLY
                              places allowed to import concrete adapters.
cmd/platformctl               wiring/DI only (main.go), CLI surface (root.go and friends).
```

`internal/application`'s own tests may import the `fake` runtime, `localfile` state, `env`
secrets, and `noop` provider as test doubles; importing a real technology adapter (postgres,
redpanda, ...) from an application test is not allowed — write a local stub of the port/
capability interface instead. `internal/application/compatibility/compatibility_test.go`'s
`versionedStub` is the pattern: a local double for `reconciler.VersionedProvider` that exercises
`compatibility`'s use of `VersionCatalog()` without importing a concrete technology adapter.

## How work happens here

[docs/planning/08-production-readiness-plan.md](../planning/08-production-readiness-plan.md)
(doc 08) is **the live backlog** — stage-gated (A–G), every task self-contained with Context/Do/
Accept. §2.1 is the literal execution protocol every task follows, in order — summarized:

0. **Checkpoint continuously** (M/L tasks): create `TASK_PROGRESS.md` at the working-tree root
   before any other work — the step plan, one line per step with status, and commit after every
   completed increment (WIP commits are fine). A session that dies mid-task must be resumable by
   a different session from `TASK_PROGRESS.md` + `git log` alone.
1. **Read** `CLAUDE.md`, the task's full entry, every doc section it names, and the relevant ADR
   index entries — before writing code.
2. **Map the task** to the interfaces it touches — open the actual port file and read the doc
   comments; never re-derive a signature from memory.
3. **Size L and no design note yet? Write the ADR first** (docs/adr/, house style: 001/003/005/
   006 — question, options, decision, why nothing's boxed out, follow-ups). This is the
   **ADR-015 tripwire** in spirit: new dial/wait logic must use `runtime.WithReachable` — typing
   an IP, a port number into a URL, or `time.Sleep` in a provider's retry loop is the violation
   the arch test rejects (docs/adr/015-connectivity-plane.md).
4. **Implement** under the standing rules: tests, idempotency, schema→doc-03 sync, feature gate,
   machine output, no secret values, deterministic plans.
5. **Verify, in order:** `gofmt -l .` empty, `go build ./... && go vet ./...`, `go test ./...`,
   the task's own Accept list run literally (not reasoned about), `just test-integration` when
   adapters/engine changed — prefer `scripts/test-impact.sh --base main` (**test-impact
   gating**, §10 below) and record suite ids + timings in `TASK_PROGRESS.md`.
6. **Doc sync** — tick nothing unverified; append facts additively (the guard hook allows
   checkbox toggles and pure insertions into `docs/planning/*.md`; modifying existing text is
   blocked — stop and report rather than working around it).
7. **Commit** with a conventional-commit subject naming the task ID, a body stating what was
   verified, and any protocol deviation called out explicitly (a deviation is a *finding*, not a
   judgment call — stop at the smallest consistent state and report it).

## Your first contribution: adding a provider

The full author guide + conformance suite is doc 08's task E6 (not yet landed) — until then,
this is the recipe (docs/planning/02-architecture.md §11):

1. **Implement `reconciler.Provider`** (`Type`, `Reconcile`, `Destroy`, `Probe`) plus whichever
   capability interfaces you support (`CDCCapableProvider`, `SinkCapableProvider`,
   `CatalogCapableProvider`, `ConnectionCapableProvider`, `LineageAware`, ...) — see
   `internal/ports/reconciler/reconciler.go`'s doc comments for the exact signatures, and use
   **`internal/adapters/providers/providerkit`** (`instance.go`, `credential.go`, `rotation.go`)
   for the cross-cutting mechanics every provider needs (container naming, credential
   rotation) rather than reimplementing them per provider.
2. **Use `internal/adapters/providers/nessie`** (`nessie.go`, ~430 lines) as the template for a
   small, complete provider with an external-lifecycle path and a capability interface
   (`CatalogCapableProvider`) — `internal/adapters/providers/redpanda` is the larger reference
   if your provider needs the ordinal/StableIdentity multi-instance shape (`brokers`/`nodes`/
   `workers`, docs/adr/017-redpanda-multibroker-and-replica-state.md).
3. **Register it** in `application/registry` (`cmd/platformctl/main.go`'s `defaultWiring`) —
   provider type string, constructor, and the feature gate name guarding it.
4. **Add a JSON Schema** under `schemas/v1alpha1/` for its `Provider.spec.configuration` shape.
5. **Add a feature gate entry**, defaulting to **Alpha/disabled**
   (docs/planning/03-resource-model-reference.md §1, docs/adr/014-feature-gate-strategy.md) —
   `gates.Register("YourProvider", featuregate.Alpha, false)`.
6. **Update the impact map** — add your provider's package(s) to `scripts/test-impact.sh`'s
   suite↔scope table in the same commit as your first integration test, so `test-affected`
   actually selects it.
7. **Cover it with an integration test** stood up against real Docker (and, if it touches the
   Kubernetes runtime path, under the minimal-RBAC kubeconfig — see Testing below).

## Testing

- **Unit** (`go test ./...`, no Docker) — domain logic, application-layer orchestration against
  the `fake` runtime/test doubles, schema validation.
- **Conformance** — every port with a conformance suite (`runtime.ContainerRuntime`,
  `reconciler.Provider` families) must have every adapter pass it; this is what proves a second
  runtime adapter (Kubernetes) is a drop-in, not a special case.
- **Integration** (`-tags integration`, real Docker/Kubernetes) — `just test-integration` runs
  the full sweep (budget ~30-60 min); prefer `just test-affected` (`scripts/test-impact.sh
  --base main`) day to day — it impact-maps your diff to the affected suites and dedups against
  a shared ledger keyed by content-state, so the same green isn't re-earned redundantly across
  sessions and agents (docs/planning/06 §10).
- **Kubernetes/minimal-RBAC rule**: Kubernetes verification must run under the minimal-RBAC
  kubeconfig minted for the `platformctl` ServiceAccount (exactly as CI's K8s job does), never
  ambient admin credentials — a new Kubernetes API call that passes under admin creds but isn't
  in the minimal Role is a real gap CI's minimal-RBAC leg exists to catch. The Role
  (`deploy/kubernetes/rbac/role.yaml`), `internal/adapters/runtime/kubernetes/preflight.go`'s
  check list, and `deploy/kubernetes/rbac/README.md` must all gain a new verb in the same commit
  (docs/planning/06 §8, rule 4).

## Docs rules

- **Schema → doc 03, same commit.** A change under `schemas/` requires a matching update to
  `docs/planning/03-resource-model-reference.md` in the same commit — not a follow-up.
- **`docs/reference/` is generated**, never hand-edited — `platformctl docs build` regenerates
  it from `schemas/`; `TestGeneratedReferenceInSync` fails CI if it drifts.
- **The planning-doc guard.** `docs/planning/*.md` edits go through
  `scripts/hooks/guard-planning-docs.sh`. Three shapes pass automatically: a checklist toggle
  (`- [ ]` ↔ `- [x]`) with no other change; a purely additive edit (every original line survives
  verbatim and in order, only new lines inserted); a brand-new file under `docs/planning/`.
  Modifying or deleting existing text is blocked outright — no retry-with-justification path; it
  needs a human directly, or the documented maintenance-unlock marker for an authorized
  documentation pass.
- **ADR practice** ([docs/adr/README.md](../adr/README.md)): numbering is monotonic and never
  reused; one decision per ADR; shape is `NNN-kebab-title.md` with a `**Status:**` line
  (proposed | accepted | superseded-by-NNN); when an ADR and a contract doc (docs/planning/01–03)
  disagree, the contract doc wins — update it in the same commit as the ADR that changes it.
- **History docs are append-only.** `docs/history/`, `docs/planning/10-project-history-and-evolution.md`,
  and `docs/remediation/` are records — append facts, never revise their meaning.
