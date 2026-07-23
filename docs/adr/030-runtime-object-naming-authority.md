# ADR 030 — Runtime object names are minted by the naming authority, not concatenated

**Status:** accepted (2026-07-23). **Prompted by:** the I15 live
round-trip failing on
`bkpk8s-pg-src-backup-20260723T071734Z-producer-files` — an invalid
Kubernetes object name (uppercase), constructed by string concatenation
in a provider, three call-sites away from the adapter that finally
rejected it.

## Diagnosis

`internal/domain/naming` exists precisely so that "a future convention
change touches this one file" (its own package comment). That promise
is already broken: seven call sites across four providers build runtime
object names by concatenating onto `RuntimeObjectName(...)` —
`+ "-backup-" + started.Format(...)`, `+ "-coordinator"`,
`+ "-via-tunnel"` — and the *constraints* on those names live in
nobody's code at all. They were discovered by live failure:

- Kubernetes object names must be lowercase RFC 1123 subdomains — the
  timestamp bug above.
- Most Kubernetes names cap at 63 characters — currently unguarded;
  dbjob derives `<job>-producer-files` and `<job>-manifest-write`
  suffixes from provider-built bases, so a long resource name walks
  straight into the limit.
- Docker is more permissive, which is exactly the trap: a name scheme
  proven on Docker fails on Kubernetes only, at the *n*-th derived
  object.

This is the same shape ADR 018/007 fixed for capability dispatch: a
cross-cutting rule enforced nowhere accretes per-site violations until
one fails live.

## Decision

1. **`naming.Derived(base, parts...)` is the only way to build a
   derived runtime object name.** It joins with `-`, lowercases,
   validates the RFC 1123 charset, and — when the result would exceed
   63 characters — truncates the base and appends a short content hash
   so derived names stay unique, deterministic, and legal on every
   runtime. Idempotency is preserved: same inputs, same name, always.
2. **Timestamps in names come from `naming.Timestamp(t)`** (the
   lowercase `20060102t150405z` form). The format string exists in one
   file; the class of "a provider picks a format the runtime rejects"
   is closed.
3. **The runtime-type vocabulary is constants, not literals**:
   `provider.RuntimeTypeDocker` / `RuntimeTypeKubernetes` /
   `RuntimeTypeFake` in `internal/domain/provider`, replacing scattered
   `"kubernetes"` literals in dispatch code (dbjob, ingress, engine's
   domainRuntime, gc). ADR 007's amendment made `RuntimeType` the
   dispatch fact; a dispatch fact deserves a greppable, typo-proof
   spelling.
4. **An archtest enforces rule 1 and 2** the same way the loopback and
   charm-confinement scans work: adapter/application code may not
   concatenate onto `RuntimeObjectName(...)` nor use the timestamp
   format string outside `internal/domain/naming`.

## Out of scope

Renaming any existing object. `Derived` reproduces today's names
byte-for-byte for every current call site (all are short and already
lowercase except the timestamps, fixed at the I15 gate); the hash
truncation path activates only for names that would previously have
failed outright.

## References

ADR 007 addendum 3 + amendment (the failing name; the dispatch rule),
ADR 018 (capability layering — the analogous "rule without a guard"
lesson), docs/planning/08 I15, `internal/domain/naming`'s package
comment (the promise this ADR restores).
