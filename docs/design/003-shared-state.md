# Design note 003 — Shared/remote state backend

**Status:** accepted, implemented (`docs/planning/08` Stage A, task A4).
**Prompted by:** `docs/planning/07` §1.4's open item — `ports/state.StateStore`
has one implementation (`adapters/state/localfile`), so two operators (or a
laptop and CI) each hold their own copy of the platform's record with no
coordination. A lost laptop loses the record; two concurrent `apply` runs can
interleave writes.

## The question

Which remote backend should `platformctl` support first, and how should
locking work without a database or a coordination service already running?

## Options considered

1. **Postgres** (a row + `SELECT ... FOR UPDATE` for locking): the most
   familiar transactional model, but it adds a *new* infrastructure
   dependency for teams whose platform doesn't otherwise run Postgres —
   locking is trivial, storage is not free.
2. **etcd/Consul/ZooKeeper**: purpose-built for distributed locks with lease
   semantics, but a genuinely new dependency class this project has never
   needed anywhere else — disproportionate for one JSON file plus a lock.
3. **S3-compatible object storage** (chosen): every example manifest in this
   project already provisions one (minio/s3 provider), so a team adopting a
   shared backend needs zero new technology — point it at the same class of
   store the platform itself uses for Datasets. Locking is the open question
   object storage doesn't answer natively.

## The decision

- First remote backend: **S3-compatible object storage**
  (`internal/adapters/state/s3`), reusing `github.com/minio/minio-go/v7`
  (already a dependency via the `s3`/`minio` provider — no new SDK).
- State lives at one object key (`<prefix>state.json`) — same JSON shape
  `localfile` writes, so `state inspect/doctor/repair` and the migration
  chain (`internal/ports/state/state.go`) work unchanged against either
  backend; the backend is purely a `StateStore` implementation swap.
- **Locking**: a second object (`<prefix>state.lock`) holding a JSON lease
  (`{holder, acquiredAt, expiresAt}`). Acquire is a **create-only-if-absent**
  conditional PUT (`PutObjectOptions.SetMatchETagExcept("*")`, a MinIO
  optimistic-concurrency extension — this is why MinIO, not raw AWS S3
  semantics, is the tested target; AWS S3 gained equivalent
  `If-None-Match: *` support in 2024 and should work unverified). A lock
  request against an existing, unexpired lease fails immediately, naming
  the holder — never a silent wait. An existing but **expired** lease is
  reclaimable via a conditional PUT matching the stale lease's ETag (so two
  clients racing to reclaim the same expired lease can't both succeed).
  Release deletes the lock object only after confirming it still holds our
  own identity — a lease we already lost to expiry is never deleted out
  from under whoever reclaimed it.
- `platformctl state unlock` is the escape hatch for a lease whose holder
  process died before releasing it and where the TTL hasn't lapsed yet
  (matches `localfile`'s existing "remove the `.lock` file" recovery
  instruction, just against the remote object instead of a local file).
- Selected via flags (`--state-backend s3 --state-bucket ... --state-prefix
  ... --state-endpoint ... --state-secret-ref ...`), not a new config-file
  format — this project has no config file today, and CLI flags keep the
  seam consistent with every other backend choice (`--feature-gates`,
  `--env-file`). Credentials resolve through the existing env-backend
  `SecretReference` convention (`DATASCAPE_SECRET_<NAME>_{ACCESSKEY,SECRETKEY}`),
  independent of any manifest — state operations (`gc`, `state doctor`) have
  no manifest path to resolve a SecretReference *from*.
- Gated `SharedStateBackend` (Alpha, disabled) — `--state-backend s3`
  without the gate enabled fails fast with the standard gate error, matching
  every other Alpha capability in this project.

## Known simplification (documented, not hidden)

- **No lease renewal/heartbeat.** The lease TTL (default 15 minutes,
  `--state-lock-ttl`) must outlast the longest `apply`/`destroy` in
  practice, or a second operator can reclaim the lock mid-run. This is an
  intentional v1 simplification, not an oversight — heartbeat renewal is
  natural follow-up work once real usage shows the fixed-TTL bar is too
  low for some deployments' apply durations.

## Accept criteria (verified)

- `internal/ports/state/conformance.Run` (the same suite `localfile`
  passes) is green against a real MinIO instance
  (`internal/adapters/state/s3/s3_integration_test.go`).
- Two concurrent `apply` processes against the same S3-backed state: one
  proceeds, the other fails fast with an error naming the first's holder
  identity — no interleaved writes (`TestConcurrentApplyOneBlocks`,
  `cmd/platformctl`).
- `state doctor`/`state inspect`/`state repair` work unchanged against the
  S3 backend (same `StateStore` interface, no command-level special-casing).

## Follow-ups (non-blocking)

- A Postgres backend can implement the same port later for teams that
  prefer row-level locking over conditional-put leases — nothing here
  precludes it.
- Lease renewal/heartbeat for `apply` runs that legitimately exceed the
  configured TTL.
- A config-file format, if CLI flags for backend selection prove unwieldy
  in practice — out of scope for this note; revisit on evidence.
