# Design note 007 — Backup and restore capability

**Status:** accepted, implemented (`docs/planning/08` Stage C, task C6).
**Note on location:** this file lives under `docs/design/` because that is
where the numbering series lives on this branch; `main` has since migrated
`docs/design/` → `docs/adr/`. At merge this file moves to
`docs/adr/007-backup-restore.md` unchanged — every code comment in this
branch that references it already points at the post-move path.
**Prompted by:** `docs/planning/08` C6 — drift-healing rebuilds
infrastructure; it cannot rebuild data. No recovery story existed for
data-bearing resources (managed Postgres/MySQL, object-store Datasets).

## The question

How does a data-bearing resource's content get streamed to and from an
object-store destination, without ever landing as a whole file in the CLI
process, and without a technology's private conventions (port numbers,
credential shapes, network names) leaking into the engine layer that
dispatches the call?

## Options considered

1. **`exec` into the running database container** and pipe `pg_dump`'s
   stdout back through the CLI process. Simplest to reason about, but
   `ContainerRuntime` has no attach/exec primitive (`docs/planning/02`'s
   port intentionally stays thin — see doc 07 §1.1), and routing a
   multi-gigabyte dump through this process's own stdin/stdout defeats the
   point of *not* buffering it. Rejected.
2. **Provider-native dump/replication tooling** (e.g. `pg_basebackup`
   streaming replication, MySQL binlog shipping) for a more complete,
   engine-aware backup. More powerful, but each engine needs its own
   bespoke wiring, and the managed-database posture
   (`docs/design/005-database-ha-posture.md`) already positions managed
   Postgres/MySQL as **single-node + backup/restore + fast drift-heal** —
   full replication tooling belongs to the `external: true` HA path, not
   here. Deferred.
3. **A short-lived job-container pipeline** (chosen): two throwaway
   containers on the runtime — a producer (the database's own dump/restore
   tool) and a consumer (`mc`, the MinIO client, speaking every S3-compatible
   endpoint this platform targets) — joined by a POSIX FIFO on a shared
   ephemeral volume. The dump streams straight from the producer's stdout to
   the consumer's stdin (or back, for restore) without ever landing as a
   whole file anywhere, backed by the pipe's own kernel buffer for
   backpressure. `internal/adapters/providers/dbjob` implements this once;
   postgres and mysql both use it unchanged.

s3/minio's own `Backup`/`Restore` need none of this: the provider already
speaks the S3 API in-process (`internal/adapters/providers/s3/bucket.go`),
so a Dataset-to-Dataset or Dataset-to-URL backup is a direct bucket/prefix
sync, no job container involved.

## The decision

- New capability interface `reconciler.BackupCapableProvider`
  (`internal/ports/reconciler/reconciler.go`): `Backup(ctx, req, dest)
  (Manifest, error)` / `Restore(ctx, req, src) error`. `dest`/`src` are
  already-resolved `backup.Location` values (endpoint, bucket, prefix,
  credentials) — mirroring how every other capability method takes only
  already-resolved inputs via `reconciler.Request` (`docs/planning/08` F5);
  the engine resolves a `Dataset` or a raw URL + `SecretReference` into one
  before calling either method. Implemented by `postgres` and `mysql` (the
  job-container pipeline above) and `s3` (direct bucket sync).
- CLI: `platformctl backup <Kind/name> --to <Dataset|url>` and `platformctl
  restore <Kind/name> --from <Dataset|url>`, gated `BackupRestore` (Alpha).
  Scheduling stays external (cron/CI) — this is the primitive, not a
  scheduler.
- `Restore` is unconditionally destructive (it always overwrites whatever
  data already exists) and refuses outright — before touching any
  infrastructure, state, or secret store — unless
  `--yes-i-understand-this-overwrites-existing-data` was passed
  (`Engine.AllowOverwrite`, the NFR-3-style pattern `destroy` already uses).

### The Location / endpoint-fact resolution design

The first implementation resolved a Dataset's backing store address by
re-deriving s3's own private conventions inline in the engine
(`internal/application/engine/backup.go`): a hardcoded API port (`9000`),
a hardcoded `http://` scheme, a hardcoded default network name
(`"datascape"`), an explicit `cfg.Type != "s3" && cfg.Type != "minio"`
check, and the in-network address (`http://<container>:9000`) handed
straight to the s3 provider's own in-process `minio-go` client — which
dials from *this* CLI process, not from inside the runtime's network. That
address only resolves from a container on the same Docker network; from the
CLI host it fails with "no such host" the moment the destination isn't also
running as a container in the same process's own network namespace (which
it never is). Both problems are the same root cause: the engine reaching
for technology-private knowledge instead of consuming what the realizing
provider itself publishes.

The fix has two parts:

1. **`backup.Location` carries F4 runtime facts, not just an address.**
   Alongside `Endpoint` (the in-network address, unchanged — this is what a
   *job container* on the shared network needs, since dbjob's producer/
   consumer run inside the runtime and can resolve `Endpoint` by its DNS
   name directly) it now carries `RuntimeName`/`ContainerPort`: the exact
   `(runtime object name, container port)` the realizing provider passed to
   `ContainerRuntime.EnsureContainer`. A caller dialing from *outside* the
   runtime (s3's own `Backup`/`Restore`, which run in-process) resolves a
   currently-dialable address from these via
   `ContainerRuntime.EnsureReachable` — the identical pattern the s3
   provider's own admin calls already use
   (`internal/adapters/providers/s3/s3.go`'s `reachableAddr`) — instead of
   dialing `Endpoint` directly. `RuntimeName` is empty for a raw-URL
   Location (real AWS S3, or any other externally routable endpoint): no
   runtime resolution is needed or possible there.
2. **The engine consumes published endpoint facts, never re-derives
   conventions.** `endpoint.Endpoint` (`internal/domain/endpoint`) gained a
   `Network` field alongside the existing `RuntimeName`/`ContainerPort`/
   `Audience` facts (docs/planning/08 F4). The s3 provider's own `"s3"`
   endpoint entry now publishes all of them —
   `RuntimeName`/`ContainerPort`/`Audience`/`Network` — plus `Internal` as a
   bare `host:port` (matching every other provider's convention; it
   previously embedded a hardcoded `http://` scheme). `resolveDatasetLocation`
   reads this fact (matched by the logical name `"s3"`, not by re-checking
   `cfg.Type` — a future provider that speaks the S3 API under a different
   `spec.type` publishes the same fact and works unchanged) and builds
   `Location` from it: no literal port, scheme, or network name lives in
   the engine anymore. A Provider that never published the fact (never
   applied, or applied before F4 landed) fails with a clear, named
   prerequisite instead of guessing.
3. **The fact comes from persisted state, not the manifest envelope.**
   `backup`/`restore` run as a separate CLI invocation from `apply`, and a
   manifest envelope's `Status` is always empty — `manifest.Load` refuses a
   hand-authored `status:` block outright, since status is Datascape-written
   only. `resolveDatasetLocation` therefore loads `e.StateStore` directly
   and reads `state.ResourceState.Status.ProviderState["endpoints"]` for the
   Dataset's realizing Provider — the same field `reconcileOne` itself
   writes after a successful `apply`. This is why the s3 Dataset round-trip
   integration test seeds its scenario with a real `apply` first
   (`setupBackupScenario` in `cmd/platformctl/backup_integration_test.go`):
   the endpoint fact genuinely has to exist in state before `backup` can
   read it.

What remains a deliberate, minimal, explicit coupling: resolving the
Provider's own root-credential `SecretReference` uses the `username`/
`password` key convention — the same shape `postgres`'s superuser and
`mysql`'s root password already use platform-wide, not an s3-only guess.
There is no fact-based equivalent for a credential *shape* the way there is
for a network address; `endpoint.Endpoint` deliberately describes network
identity only, never credentials.

### Secrets handling

- Every credential a job container needs (a database superuser password, an
  `mc` alias's access/secret key) rides a `runtime.FileMount` — a `0600`
  file inside the container — never an environment variable (`docker
  inspect` reveals env) and never a command-line argument (visible in
  `docker top`/process listings). `dbjob.MCConfig` renders `mc`'s
  `config.json` for exactly this; postgres/mysql mount a `.pgpass` /
  `--defaults-extra-file` the same way their own server containers do.
- `backup.Location.AccessKey`/`SecretKey` live only in memory for the
  duration of one `Backup`/`Restore` call — exactly like
  `reconciler.Request.Secrets`. `backup.Ref` (what a `Manifest` records) has
  no field capable of holding a secret at all, mechanically enforced by
  `TestManifestNeverEmbedsPlaintextCredentials`
  (`internal/domain/backup/backup_test.go`), which asserts the exact field
  set a `Ref` may ever carry.
- Nothing above appears in `state` either: state persists `Manifest`/`Ref`
  shapes (via the CLI's own JSON output plumbing), never a `Location`.

## Known limitations

- **(a) Protect vs. restore.** `Engine.Restore` REFUSES a target whose
  `metadata.protect: true` was set, even when the caller passed
  `--yes-i-understand-this-overwrites-existing-data` — `protect` is not
  something a single flag can waive. This mirrors the safe default `destroy`
  already gives a protected resource (`internal/application/plan`'s
  `isProtected`): protect exists to make "this resource's data must not be
  destroyed" true regardless of which destructive verb is used. Covered by
  `TestRestoreRefusesForProtectedResource`
  (`internal/application/engine/backup_test.go`).
- **(b) Restore's JSON output previously duplicated key and prefix.**
  `backup.RefOf` built a restore's `Ref` with `Prefix` set to the same full
  object key as `Key` (both derived from the same `src.Prefix`, since
  Restore's Location carries the exact object to read back, not a
  directory). Fixed: `RefOf` now derives `Prefix` from `key`'s directory
  portion (the substring before the final `/`) instead of copying
  `loc.Prefix` verbatim whenever a specific `key` is given — `key == ""`
  (s3's own whole-prefix-sync backup) still reports `Prefix` as the synced
  tree, unchanged.
- **(c) Docker-only mechanism.** `backup`/`restore` refuse outright — before
  resolving a provider, before any infrastructure call — when the target
  resource's realizing Provider resolves to any runtime other than Docker
  (`Engine.backupCapable` checks `spec.runtime.type`). The job-container-
  plus-FIFO-volume mechanism (`dbjob`) and s3's own read-after-exit
  sentinel-file protocol both assume a container that can be inspected for
  "still running vs. exited with code N" *after* it stops — a Docker
  container's terminal state. A Kubernetes Deployment (what every other
  provider realizes there today) has no such primitive: a Pod under a
  Deployment is expected to keep running, restarting on exit rather than
  reporting a terminal exit code, and there is no notion of "read a file
  back from a Pod that has already terminated." A Kubernetes `Job` (which
  *does* model run-to-completion with an observable exit code) would be a
  different realization entirely, not a mechanical port of the Docker one —
  left for a follow-up, not attempted here. The error names the resolved
  runtime type so this reads as an explicit limitation, not a mysterious
  failure.

## Accept criteria (verified)

- Integration: seed rows → backup → destroy → apply fresh → restore → rows
  present, for both postgres and mysql
  (`TestBackupRestorePostgresRoundTrip`, `TestBackupRestoreMySQLRoundTrip`,
  `cmd/platformctl/backup_integration_test.go`).
- Integration: an s3 Dataset → Dataset round trip
  (`TestBackupRestoreS3DatasetRoundTrip`), including the in-process
  `EnsureReachable`-based dial fix above.
- Backups never embed plaintext credentials
  (`TestManifestNeverEmbedsPlaintextCredentials`; the integration tests also
  grep their own `-o json` output for the seeded passwords).
- Restore onto live data without
  `--yes-i-understand-this-overwrites-existing-data` refuses
  (`TestRestoreRefusesWithoutAllowOverwrite`), and a `protect: true` target
  refuses even with it set (`TestRestoreRefusesForProtectedResource`).
- `internal/ports/runtime/conformance`'s
  `EntrypointFaithfulness_EntrypointReplaces` subtest, green against Docker
  and Kubernetes: `ContainerSpec.Entrypoint` replaces the image's own
  `ENTRYPOINT` while `Cmd` still appends after it — the contract-level
  reproduction of the dbjob entrypoint bug this note's job-container design
  depends on (docs/planning/08 F6's ratchet: a live-found bug ships with a
  conformance reproduction in the same commit).
