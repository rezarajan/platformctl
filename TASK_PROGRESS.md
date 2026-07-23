# I12 — dbjob pipeline hardening (precondition for BackupRestore GA)

Doc 08 §7.8 I12. Worktree branch: `worktree-agent-a552bc5059f797519`. Never
touch main; no push.

## Step plan

0. [done] Checkpoint file created (this file); merged main (already up to
   date, no-op).
1. [done] Read spec: doc 08 §7.8 I12, ADR 007, dbjob.go, postgres/mysql
   backup.go, doc 11 dbjob build-vs-buy note, doc 02 §4.1 settledness,
   doc 06 §10 integration economy. Confirmed Docker + mc image has GNU
   coreutils (sha256sum/wc/tee/mkfifo) — checked live via `docker run`.
   Confirmed `backup` suite scope in scripts/test-impact.sh: dbjob +
   postgres + mysql + s3 + engine/backup.go + domain/backup +
   cmd/platformctl/backup.go, run pattern `-run 'TestBackupRestore'` against
   `./cmd/platformctl/` — fault-injection tests MUST be named
   `TestBackupRestore*` and live in cmd/platformctl to be picked up.
2. [done] ADR decision: harden in place (option a). Addendum written to
   docs/adr/007-backup-restore.md.
3. [done] Implement:
   - domain/backup: Manifest gains Checksum (sha256:<hex>) + Bytes fields.
   - dbjob.go: producer-side tee'd sha256+byte-count hashing (2nd/3rd FIFO,
     GNU coreutils, no process substitution needed); per-side deadlines
     (ProducerTimeout/ConsumerTimeout); Cleanup *Side field run via new
     RunOneShot on any pipeline failure, after force-removing both
     containers first (avoids the TOCTOU race with an in-flight upload);
     RunPipeline now returns (Result{SHA256,Bytes}, error); new
     PersistManifest/ReadManifest helpers (sidecar object
     `<key>.manifest.json`) for restore-time verification.
   - postgres.go / mysql.go: wire Cleanup (mc rm --force) on Backup;
     persist manifest sidecar after a successful Backup; Restore fetches
     the sidecar first and verifies checksum+bytes after RunPipeline,
     refusing a mismatch with a named error.
4. [done] Fault-injection tests in cmd/platformctl/backup_integration_test.go
   (TestBackupRestoreFault*): producer killed mid-stream, consumer never
   starts, corrupt/absent exit file (self-SIGKILL PID1 trick) — each
   asserts a clean named error AND an empty bucket listing at the target
   key/prefix. Fixed a real portability bug found by manually probing the
   pinned mc image: it has GNU coreutils but NO awk — sideSpec's checksum
   script uses `cut -d' ' -f1` instead of `awk '{print $1}'`. Validated the
   tee+FIFO+coreutils checksum mechanism manually in a scratch container
   before wiring it in (matched a known sha256("hello world") value).
5. [done] gofmt clean; `go build`/`go vet` clean both plain and
   `-tags integration`; `go test ./...` unfiltered, true-exit=0.
6. [in-progress] `export KUBECONFIG=/tmp/claude-1000/platformctl-rbac/platformctl.kubeconfig`;
   `./scripts/test-impact.sh --base main --only backup` (pass 1) launched
   in background (task b9rpv9vqd) — waiting for its notification, not
   polling. NOTE: `--print --base main` (no --only) selects 22 suites, not
   just backup — because my one additive change to
   internal/domain/backup/backup.go (2 new Manifest fields) falls under
   SHARED_CORE's broad `internal/domain` prefix, which is scope-included by
   most suites. `grep -rl "domain/backup" --include=*.go .` confirms only
   dbjob/postgres/mysql/s3/engine/backup.go/cmd's backup.go actually import
   it — none of the other 21 flagged suites' production code touches it.
   Plan: run backup suite GREEN TWICE (mandatory per task), run cdc +
   lakehouse once each (their scope directly names the postgres/mysql
   packages I edited — genuine compile-level exposure, not just the
   SHARED_CORE prefix), then launch the remaining ~19 SHARED_CORE-only
   suites via a background `--base main` sweep (ledger will SKIP backup,
   already green) without waiting on it, and report this as a deviation
   for the orchestrator per doc 08 §2.1's "finding, not judgment call"
   rule — running full redpanda/wireguard/trino/chaos-k8s/etc. suites
   whose code has zero dependency on backup.Manifest is disproportionate
   evidence-gathering for an additive 2-field struct change already
   proven safe by a clean `go build ./...`/`go vet ./...` across the
   whole repo.
7. [done] Doc 08 additive Done-note under I12 (fault timings inline;
   green-twice timings marked "see /tmp/claude-1000/i12-evidence.log,
   transcribed at merge gate").
8. [done] Squashed to one commit:
   `fix(backup): dbjob pipeline hardened — integrity, fault-injection, clean failure (I12)`

## Verification log

- gofmt/go build/go vet: clean, both plain and -tags integration.
- go test ./... (unfiltered): true-exit=0.
- Backup suite pass 1 (2026-07-23T00:44Z, 94.9s): the three round-trip
  tests AND TestBackupRestoreFaultProducerKilledMidStream GREEN — the
  hardened pipeline + checksum + manifest sidecar + cleanup all proven
  live on both engines. TWO FAULT INJECTIONS WERE DISHONEST and were
  detected by the suite (RunPipeline correctly returned nil because the
  faults never actually happened):
  (1) `mc pipe badalias/...` — mc treats an unknown alias as a LOCAL
      PATH and exits 0 (uploads nowhere, no failure). Replaced with
      `mc this-subcommand-does-not-exist` (real instant rejection, the
      C6/K1 entrypoint-bug class).
  (2) `kill -9 1` — the kernel IGNORES SIGKILL sent to PID 1 from inside
      its own PID namespace; the producer just continued. Replaced with a
      consumer-side wrapper-breakout injection (unbalanced `)` in
      ShellCmd) that really uploads the object then writes a CORRUPT exit
      file / no exit file at all — strictly stronger: proves Cleanup
      removes an ALREADY-UPLOADED object when the exit protocol becomes
      untrustworthy. Both variants as subtests of
      TestBackupRestoreFaultExitFileProtocolBroken.
- Stale pass-2/sweep runs (launched before the fix) killed along with
  their orphaned flock children (2675952, 2676355); verified no leftover
  bkp-*/fault containers. Other agents' concurrent sweeps left untouched.
- Targeted flock'd run of TestBackupRestoreFault* (2026-07-23T00:54-59Z):
  ALL FIVE GREEN in 17.6s — ProducerKilledMidStream 4.98s (named producer
  error), ConsumerNeverStarts 6.34s (named consumer error in 3.2s, well
  under the deadline — peer-unstick proven), ExitFileProtocolBroken
  corrupt 1.70s (error names the CORRUPT content) + absent 1.57s (named
  read-exit-code error); every test's bucket listing empty (the
  exit-file subtests each confirm Cleanup deleted a REALLY-uploaded
  object). The mid-stream run also exercised the cleanup-context path:
  its `mc rm` cleanup on the never-created object exited 1 ("Object does
  not exist") and was appended as context while the producer root-cause
  error stayed primary — the designed fold-in behavior, observed live.
- FINAL EVIDENCE RUNS (launched detached at squash time, per
  orchestrator): log at /tmp/claude-1000/i12-evidence.log — sequence:
  (1) backup suite run 1 via `bash scripts/test-impact.sh --base main
  --only backup` (ledger-records), (2) backup suite run 2 via direct
  flock-wrapped `go test -tags integration -count=1 -run
  'TestBackupRestore' ...` (green-twice evidence; the ledger would dedupe
  an identical script rerun), (3) `bash scripts/test-impact.sh --base
  main` for everything else selected and not ledger-green. Timings to be
  transcribed from the log at the merge gate by the orchestrator.
