# E8 (release engineering and CI matrix) — task progress

Doc 08 §2.1 protocol. Step 0 checkpoint file (overwrites the previous
task's D3/D4 file per convention — that work is already committed on
main, `git log` has it, commit 2fca03e).

## Plan

1. [done] `git merge main --no-edit` — fast-forwarded 80b5bf6..c497530
   (docs(06) §10.7 addition only, unrelated to E8). NOTE: an earlier
   attempt was run with an explicit `cd` into the *shared* checkout path
   (`/home/cascadura/git/platformctl`, not this worktree) and reported
   "already up to date" against a stale ref before another agent
   advanced `main`; corrected by re-running the merge from the actual
   worktree cwd. All further shell commands in this task use this
   worktree's cwd, never the shared checkout path.
2. [done] Read: CLAUDE.md; doc 08 §2.1 protocol + full E8 entry; §10
   execution order (confirms Step 0's C1/D1/C6 branch-landing items are
   other agents' worktrees, out of scope here — file fence says work
   only in this worktree); ADR README index (no new ADR needed — E8 is
   Size M, not L, and B8/A10 its named deps are both already shipped);
   `.github/workflows/ci.yml` (unit / integration / integration-k8s
   jobs, G7 test-impact wiring) + `refresh-digests.yml` (existing weekly
   cron pattern: `0 6 * * 1`); `cmd/platformctl/main.go`'s `version` var
   (`-ldflags -X main.version=...`) and `root.go`'s cobra wiring;
   `docs/history/checkpoint.md` "Release mechanics" section;
   `scripts/pinned-images.txt`; `docs/README.md`; `docs/adr/README.md`.
3. [done] Confirmed already-existing coverage (do not duplicate):
   - example-validate: unit job's "Validate examples" step
     (cdc-attendance + lakehouse) + `TestInitBlueprintValidatesWithNoEdits`
     (cmd/platformctl/init_test.go, no build tag, runs under
     `go test ./...` in the unit job) covers every blueprint (currently
     just cdc-to-lake and lakehouse per internal/application/blueprint's
     catalog).
   - machine-output harness (A7):
     `cmd/platformctl/output_contract_harness_test.go`, no build tag,
     already runs under `go test ./...`.
   - Kubernetes leg (B8): `integration-k8s` job, minimal-RBAC, already
     exists.
   - Digest refresh (A10): `refresh-digests.yml`, already exists.
4. [done] Timed `go test -race -count=1 ./...` locally: ~41s wall
   (vs ~5.5s plain `go test -count=1 ./...`) — cheap enough for a CI leg
   scoped to unit tests (no `-tags integration`, so it never touches
   adapters needing Docker/K8s).
5. [done] `go run github.com/goreleaser/goreleaser/v2@latest --version`
   resolved v2.17.0 (network install available).
6. [done] `.goreleaser.yaml` — linux/darwin x amd64/arm64,
   CGO_ENABLED=0 -trimpath, version stamped via `-ldflags -X
   main.version={{.Tag}}` (keeps the "vX.Y.Z" shape `main.go` already
   uses — goreleaser's `.Version` strips the leading "v", `.Tag` does
   not), archives + checksums. Validated: `goreleaser check` clean;
   `goreleaser release --snapshot --clean` built all 4 targets, archived,
   checksummed (64-hex sha256 verified). Found+removed a
   `before.hooks: go mod tidy` step from the first draft — it silently
   rewrote go.mod/go.sum (pulled in other in-flight branches' indirect
   test deps as direct requires); reverted with `git checkout -- go.mod
   go.sum` and re-validated clean without the hook.
7. [done] `.github/workflows/release.yml` — triggers on `v*.*.*` tags,
   `verify` job re-runs the unit job's checks against the tagged commit,
   then `goreleaser` job (needs verify) runs `goreleaser-action@v6`.
8. [done] ci.yml additive edits: `Race detector` step in the `unit` job
   (`go test -race ./...`, ~41s measured vs ~5.5s plain), and a
   `schedule: cron: "0 7 * * 1"` trigger added to `on:` (offset from
   refresh-digests.yml's `0 6 * * 1`) — the existing `integration` job's
   `if event_name == pull_request` branch already falls through to
   `--full` for any non-pull_request event including `schedule`, and
   `integration-k8s` has no event gating at all, so adding the schedule
   trigger alone reproduces "test-impact --full + the K8s job" weekly
   with zero restructuring of G7's wiring.
9. [done] `docs/releasing.md` — preconditions, tag command, what the
   workflow does, post-release verification, doc 08 §10 milestone-tagging
   convention. Additive link from docs/README.md's Process section.
10. [done] Local end-to-end version-stamp proof:
    `go build -ldflags "-X main.version=v9.9.9-test"` → `--version`
    printed "platformctl version v9.9.9-test" exactly.
11. [done] Verify: gofmt clean, build/vet/`go test ./...` green (sanity
    only, no Go source touched); `go run
    github.com/rhysd/actionlint/cmd/actionlint@latest` (network install
    available, v1.7.12) against all three workflow files — zero findings.
12. [done] doc 08 additive E8 status note (pure insertion, hook-verified
    additive via `git diff`); final commit.

## Resume point if this session dies

Read this file + `git log --oneline -10` first. Files touched:
`.goreleaser.yaml`, `.github/workflows/release.yml`,
`.github/workflows/ci.yml` (additive only), `docs/releasing.md`,
`docs/README.md` (additive line), this file, and an additive doc 08 E8
status note. No `internal/**` changes (none expected/needed for E8).

## Notes / open questions

- Doc 08 §10's "Step 0" (merge C1/D1/C6 branches) is a maintainer action
  on *other* agents' worktrees — out of scope here per the file-fence
  instruction to work only in this worktree.
