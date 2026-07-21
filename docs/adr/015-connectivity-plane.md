# ADR 015 — The connectivity/discovery plane

**Status:** accepted (Stage F, 2026-07-20; this record consolidates
docs/planning/09 §2–4 and doc 08 F1–F4/F6, all shipped on main).

## Context

Seventeen live-caught defects (thirteen on Kubernetes, four earlier on
Docker) reduced to five recurring classes, ten of them living where
providers, the engine, and even the domain layer each answered "how do I
reach this container?" locally — constructing `127.0.0.1:port` from
configuration, correct on Docker only by coincidence. The
connectivity/discovery plane had never been named as a layer, so its logic
precipitated into whichever provider needed it that day (doc 09 §4).

## Decision

Promote connectivity/discovery to a first-class internal plane with one
rule: **providers publish endpoints and request reachability; only the
runtime layer answers; nothing else may.** Concretely:

1. **Addresses are unconstructible** (F1): no loopback/localhost literal in
   `internal/domain` or `internal/adapters/providers` (CI arch test;
   allowlist: in-container healthchecks, runtime adapters). Managed
   reachability requires a runtime, so the domain offers no managed-address
   getter (`ExternalAddress()` exists only for external Connections). All
   dialing goes through `runtime.WithReachable`, which resolves via
   `EnsureReachable`, retries, and **re-resolves a fresh address per
   attempt**.
2. **Specs state their full intent** (F2): `PortBinding.Audience:
   host | internal` — every listener a dependent may dial is declared. The
   **fake runtime is the strict interpreter**, refusing resolution of
   undeclared ports, so under-declaration fails in `go test ./...` before
   any cluster sees it. The most permissive runtime must never define the
   contract.
3. **Ready means serving** (F3): an `EnsureReachable` address is currently
   dialable; adapters absorb asynchronous programming races (NodePort
   iptables, port-forward listen, initdb socket-only windows).
4. **Identity by handle, not convention** (F4): `internal/domain/naming` is
   the single resource→runtime-object naming authority; cross-resource
   dial sites resolve **published endpoint facts**
   (`internal/domain/endpoint`: runtime name, container port, audience)
   instead of re-deriving names.
5. **The conformance ratchet** (F6): a live-caught bug lands with a
   contract-level reproduction in the same commit, or a documented
   per-runtime difference in doc 07 when the semantic is outside the port
   (doc 06 §8 is the policy text).

## Consequences

- A provider author *cannot* reintroduce the bug classes: the compiler,
  the arch test, or the strict fake refuses (doc 09 §5's segregation bar).
- Every future runtime adapter inherits the encoded lessons: conformance
  green is necessary; unmodified example pipelines reaching Ready is the
  acceptance bar.
- New cross-resource communication features (ingress C7/C8, tunnels D5,
  in-network probes C10) build **on** this plane; any design that hands a
  provider a constructed address is wrong by definition — the C6 branch
  review rejected exactly that shape.

## References

docs/planning/09 (the analysis); doc 08 §7.5 (tasks and acceptance);
doc 06 §8 (ratchet policy); doc 07 Cross-Runtime (per-runtime differences
ledger).
