# ADR 031 — Provider diagnostics: a warnings channel on Request, no process-global streams in adapters

**Status:** accepted (2026-07-23). **Prompted by:** the codebase
structural review (doc 11, 2026-07-23): three provider call sites write
warnings directly to `os.Stderr` (postgres/mysql restore's "promoted
successfully, but dropping the pre-restore database failed — harmless
leftover"). The messages are correct and must not fail the operation;
the *channel* is a layering leak.

## Why this is structural, not cosmetic

An adapter writing to a process-global stream bypasses every layer
above it:

- The CLI owns presentation. It already learned this lesson once at its
  own layer (H8: isolation notes moved off stdout because stdout is the
  machine-parsed output contract); an adapter printing directly can
  re-break that contract from below, invisible to the CLI's placement
  rules.
- A future non-CLI host (server mode, an operator, tests asserting on
  warnings) has no way to capture, route, or count them.
- It is untestable through the port: nothing in the provider contract
  says the warning happened.

The engine already brokers every other provider-to-user signal (status,
conditions, errors). Warnings are the one signal with no seam.

## Decision

1. `reconciler.Request` gains one structural field:
   `Warn func(format string, args ...any)` — plus a nil-safe
   `Request.Warnf` method providers call. This is a *channel*, not a
   fact: it joins Resource/Runtime/Provider in the frozen-fields
   guard's structural set (`internal/archtest/request_facts_frozen_test.go`),
   documented there in the same commit, per that guard's own protocol.
2. The engine populates `Warn` at Request construction, writing to an
   engine-level `Warnings io.Writer` the CLI wires to stderr (the
   established placement: stdout is the output contract, notes go to
   stderr). Hosts that want structured warnings swap the writer; when
   unwired, `Warnf` is a no-op rather than a nil deref.
3. **Adapters may not touch `os.Stderr`/`os.Stdout`/`fmt.Print*`.**
   Enforced by an archtest scan over `internal/adapters` and
   `internal/application` (same grep-scan shape as the loopback and
   charm-confinement guards). The three existing sites migrate to
   `req.Warnf` in this ADR's commit, so the guard lands green on a
   clean tree.

## Consequences

- Best-effort-failure warnings become visible to every host and
  assertable in tests (`Warn` is injectable).
- The severity ladder is now explicit at the contract level: fail the
  operation (return error) / degrade with a recorded condition
  (status) / inform without failing (`Warnf`). Providers no longer
  invent a fourth channel.
- Backup/Restore, which return no status, get their first legitimate
  non-fatal signal path — the exact case that produced the stderr
  writes.

## References

Doc 11 (2026-07-23 structural review), H8 (stdout/stderr placement
rule), docs/planning/08 I9 (the frozen-Request protocol this addition
follows), ADR 016 (provider contract).
