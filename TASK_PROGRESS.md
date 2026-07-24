# TASK_PROGRESS — M2: Auto-provisioned Connection ports

Status: DONE (pending final commit).

## What
- `Connection.spec.port` is now optional for managed (non-external)
  connections. Omitted, `internal/domain/connection.FromEnvelope` auto-
  allocates it deterministically via `internal/domain/hostport.Resolve`,
  keyed on the Connection's own runtime object name
  (`internal/domain/naming.RuntimeObjectName`) — the identical allocator
  and identical shared collision table a Provider's own omitted host port
  already resolves through (`providerkit.HostPort`).
- An explicit pin (`spec.port` set) passes through `hostport.Resolve`
  unchanged — byte-identical to pre-M2 behavior.
- External connections are unaffected: `spec.port` stays required there
  (no entrypoint to auto-allocate a port for; it's the external system's
  own literal port).
- No provider adapter code changed (proxy, wireguard, openziti connection
  realizers) — they all read the already-resolved `conn.Port` from
  `connection.FromEnvelope`, so the auto-allocated value flows through
  unmodified to the container's listen port and the published endpoint
  fact.

## Files
- `schemas/v1alpha1/connection.json` — `port` removed from top-level
  `required`; `allOf`'s external branch now requires `["host", "port"]`;
  description updated.
- `internal/domain/connection/connection.go` — `FromEnvelope` auto-
  allocates for managed connections via `hostport.Resolve`; `validate`'s
  port-required check narrowed to `External && Port <= 0`.
- `docs/planning/03-resource-model-reference.md` — Connection field notes
  gained a bullet on port optionality (additive); §8.2.1's ingress example
  gained an additive "Update" note (the guard hook blocks editing existing
  text in docs/planning, so the stale inline YAML comment there was left
  as-is and clarified via the new paragraph instead).
- `docs/reference/connection.md` — regenerated via
  `go run ./cmd/platformctl docs build --out docs/reference`.
- `internal/domain/connection/port_test.go` (new) — unit tests: omitted
  port auto-allocates + is stable across repeated `FromEnvelope` calls;
  pinned port unchanged; external connection still requires a port
  (omitted and pinned).
- `internal/application/engine/connection_port_test.go` (new) —
  `TestConnectionAutoAllocatedPortPublishedAndResolved`: fast-tier,
  full-pipeline proof (`graph.Build` -> `plan.Compute` -> `Engine.Apply`,
  a `fakeForwarderProvider` mimicking `proxy`'s own Connection
  realization) that the auto-allocated port is (a) what the provider's
  container listens on, (b) what gets published as the endpoint fact's
  `ContainerPort`, and (c) what a consumer resolves through
  `Engine.connectionDialAddress` — never a literal.

## Verification
- `gofmt -l` — clean.
- `go build ./...` — clean.
- `go vet ./...` and `go vet -tags integration ./...` — clean.
- `go test ./...` — full unfiltered run, exit 0 (log:
  `/tmp/claude-1000/-home-cascadura-git-platformctl/3ff96d5f-6a0c-4676-8628-0810b1d9fe68/scratchpad/m2_test_log.txt`).
  Included `TestGeneratedReferenceInSync` (failed once before the
  `docs reference build` regen, green after).
- `golangci-lint run` (v2.12.2, whole repo) — 0 issues.
- `internal/archtest/...` — all pass (layering/naming-authority checks
  unaffected by the new `domain/connection` -> `domain/hostport` +
  `domain/naming` imports; both are leaf `internal/domain/*` packages,
  same precedent as `internal/domain/graph` already importing `naming`).

## Open items for the orchestrator
- No live (Docker/Kubernetes) integration check was run per instructions
  (fast-tier only). Recommend a live smoke of `examples/*` with a
  Connection that omits `spec.port`, once M2 lands alongside M5/M4
  (zero-trust default-on) — an auto-allocated port is exercised implicitly
  by every *pinned* Connection scenario today, but no existing example
  manifest omits `spec.port`, so no example currently exercises the new
  path end-to-end on a real runtime.
- `cmd/platformctl/expose.go`'s `--port` CLI flag (the interactive
  `expose` composition command) still requires `--port` explicitly and was
  deliberately left untouched — out of M2's Do list (schema/domain only);
  worth a follow-up UX pass under the same ADR 035 "just works" theme if
  the owner wants `expose` to default the flag too.
