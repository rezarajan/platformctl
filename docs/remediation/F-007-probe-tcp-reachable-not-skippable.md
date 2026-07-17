# F-007: `TestProbeTCPReachable` still hard-fails in loopback-restricted runners

**Severity:** Low (unit suite fails in sandboxed environments; the gap doc's
own validation-signal section has requested this since its first revision).
**Status:** RESOLVED (2026-07-17). Verified the test still runs and passes
normally in an unrestricted environment (the skip path is unreachable
there).

## Claim audited

`docs/planning/07`, "Validation signal during review": "the test should
still be made clearer or skippable in restricted runners"; §3.2 open item
"Make `TestProbeTCPReachable` skip or self-describe when loopback listen is
blocked by a restricted runner." Open in the doc — consistent — but the
same policy was already implemented for the Kubernetes conformance test
(`kubernetes_integration_test.go`, commit `7865b93`), so the pattern to
copy exists in-repo.

## Evidence

`internal/application/engine/engine_test.go` `TestProbeTCPReachable`
(line ~585): `net.Listen("tcp", "127.0.0.1:0")` followed by `t.Fatal(err)`
— a sandbox that denies loopback listen fails the whole unit suite.

## Required behavior

When the initial `net.Listen("tcp", "127.0.0.1:0")` fails, skip with a
self-describing message instead of failing:

```go
live, err := net.Listen("tcp", "127.0.0.1:0")
if err != nil {
    t.Skipf("loopback listen blocked by this environment; skipping (see docs/planning/07 §3.2): %v", err)
}
```

Only the *first* listen is the environment probe; subsequent errors in the
test body remain fatal (they indicate real bugs, not sandbox limits).

## Exact files and symbols

- `internal/application/engine/engine_test.go`: `TestProbeTCPReachable`.

## Implementation constraints

- Test-file-only change. Do not add env-var opt-outs (the k8s test's
  `PLATFORMCTL_REQUIRE_K8S` exists because a *cluster* is heavyweight;
  loopback either works or the runner is restricted — no enforcement knob
  needed).

## Tests / validation commands

```
go test ./internal/application/engine/ -run TestProbeTCPReachable -v
```

(In a normal environment it must still RUN and PASS, not skip.)

## Dependencies / ordering

None.

## Risk

Minimal. The skip predicate is listen failure only — cannot mask probe
logic regressions in normal environments.

## Escalation conditions

None.

## Doc correction required

Tick the §3.2 checkbox when landed.
