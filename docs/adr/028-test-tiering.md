# ADR 028 — Test tiering: fast local signal, deep explicit, CI as arbiter

**Status:** accepted (2026-07-23). **Prompted by:** the project owner:
local tests MUST complete in ≤1 minute (per-test ≤60s) unless longer
runs are explicitly requested; every test parallelizable; CI is where
stress happens. The observed pathology: 20-minute local sweeps followed
by 20-minute CI runs, with a recurring pass-local/fail-CI pattern.

## Diagnosis

The pyramid is inverted: end-to-end integration suites (real
containers, real clusters) are the PRIMARY evidence for provider
behavior, so the default development loop pays e2e cost. The
pass-local/fail-CI pattern is environment-timing divergence — local
warm caches and idle daemons hide races that CI's cold, slow runners
expose (every timing bug this project fixed was found that way). Making
local runs longer does not fix divergence; it doubles the price of it.

## Decision: three tiers, three jobs

| Tier | Invocation | Contents | Budget | Role |
|---|---|---|---|---|
| **Fast** | `just test` (default; every save) | unit + contract tests vs the fake runtime and fake technology APIs; `t.Parallel()` throughout | ≤60s per test, ~1 min total, enforced by a budget guard | The TDD loop. The ONLY thing a developer waits for |
| **Deep** | `just test-deep [suite…]` (explicit) | the existing integration suites + impact map/ledger | minutes; developer opted in | Pre-push confidence on what the diff touches |
| **Stress** | CI (every PR) | per-suite matrix on isolated runners; Calico-enforced K8s; race detector; full sweep on main | the slowest suite, parallelized | **The arbiter.** Local green is a signal; CI green is the verdict |

Consequences of "CI is the arbiter":

1. **The provider conformance suite (E6) is the pyramid's missing
   middle** — a contract suite driving any `reconciler.Provider`
   through lifecycle/idempotency/drift semantics against fakes, in
   milliseconds. Providers get their fast-tier evidence there, not from
   e2e. E6 is therefore re-scoped as the fast-tier cornerstone.
2. **Fakes must be honest.** The fake runtime already passes the same
   runtime conformance suite as Docker/Kubernetes — that discipline
   extends to technology fakes (fake Connect REST, fake Kafka admin):
   each fake's behavior is pinned against the real system's observed
   semantics in the deep tier, so fast-tier green is meaningful.
3. **The budget is enforced, not aspirational**: a guard parses
   `go test -json` in CI and fails on any fast-tier test exceeding 60s
   (and on the tier exceeding its total). A slow test is moved to deep
   or made fast — never waited on.
4. **Timing discipline stays** (doc 02 §4.1, ScaledWait): deep/stress
   tests remain condition-polled with generous failure bounds; the fast
   tier contains no timing at all (fakes are synchronous).
5. The impact map/ledger continue to govern the deep tier unchanged.

## Out of scope

Local parallel execution of multiple INTEGRATION suites against one
shared daemon (the flock exists because contention produced flaky
timeouts on developer-class machines) — deep-tier wall-clock is solved
by running it less often, not by racing one daemon. CI already runs
suites concurrently on isolated runners.

## References

Doc 11 (the timed-poll census; the pass-local/fail-CI incident log),
E6 (the provider conformance suite this ADR re-scopes), doc 06 §10
(impact economy — deep tier), ADR 027 (CI Calico — the stress
environment's enforcement honesty).
