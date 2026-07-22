# Releasing platformctl

Mechanical checklist for cutting a `platformctl` release. Written to be
executed by an agent session with no additional context beyond this file,
`docs/planning/08-production-readiness-plan.md` §10 (execution order / the
milestone-tagging convention), and `docs/adr/014-feature-gate-strategy.md`.

## 1. Preconditions (check every item before tagging)

1. **`main` is green.** `git fetch && git log origin/main -1` — the latest
   commit's CI run (`.github/workflows/ci.yml`, all three jobs: `unit`,
   `integration`, `integration-k8s`) is green. Do not tag a commit whose CI
   run is still in flight, red, or was cancelled.
2. **Gates table review (ADR 014).** Open
   `docs/planning/04-roadmap-and-feature-gates.md` §12's feature-gate
   master table. For every gate that graduated (Alpha → Beta/GA, or was
   enabled by default) since the last release, confirm the table already
   reflects it — ADR 014's graduation convention requires the master table
   to be the single source of truth for gate maturity, and it must never
   lag what's actually enabled in `cmd/platformctl/main.go`'s
   registrations.
3. **Upgrade-notes check.** Open `docs/upgrade-notes.md`. If any commit
   since the last tag introduced a behavioral migration (a state-shape
   change, a default flip, anything an operator upgrading in place could
   mistake for a regression), confirm it has an entry. If it doesn't,
   stop and add one before tagging — this is not optional for a
   migration-carrying release.
4. **Docs/reference in sync.** `go test ./internal/application/docsgen/...`
   — `TestGeneratedReferenceInSync` guards that `docs/reference/` (the
   generated Kind reference) matches the current `schemas/` tree. A schema
   change without a regenerated reference fails this test; run
   `go run ./cmd/platformctl docs build` and commit the diff if it does.
5. **Working tree clean, on `main`, up to date with `origin/main`.**
   `git status` reports nothing to commit; `git log HEAD..origin/main`
   and `git log origin/main..HEAD` are both empty.
6. **Full verification, not a re-read of stale CI output:**
   ```bash
   gofmt -l .                                   # empty output
   go build ./... && go vet ./...
   go test ./...
   just test-integration                        # requires Docker; full sweep
   go run ./cmd/platformctl validate examples/cdc-attendance/
   go run ./cmd/platformctl validate examples/lakehouse/
   ```

## 2. Decide the version and tag

- Follow doc 08 §10's **milestone-tagging convention**: a tag marks a
  *stage* closing, not every merged task. Current mapping (update this
  list, additively, as stages close):
  - `v1.1` = Stage A (operational hardening) closed.
  - `v1.2` = Stage B (Kubernetes Beta) closed.
  - `v1.3` = Stages C+D (HA/routing/TLS/monitoring/backup +
    pipeline-infrastructure completeness) closed.
  - `v2.0` = Stages E+F (DX/contribution readiness + segregation
    readiness) closed — the "production data-pipeline platform,
    contribution-ready" declaration point.
  - Tags may collapse multiple newly-closed stages into one release (as
    `v1.2.0` did for Stages A+B+F plus the C1/D1 merges) when they land
    close together — the tag documents *what actually closed*, not a
    rigid 1:1 stage↔tag mapping.
- Bump `cmd/platformctl/main.go`'s `version` var to match the tag you are
  about to cut (`var version = "vX.Y.Z"`) in its own commit on `main`
  *before* tagging — the release workflow stamps `main.version` from the
  git tag via `-ldflags`, but the checked-in default is what every
  `go build`/`go install` outside the release workflow reports, and it
  must not lag the tag.
- Tag command (run from a clean, up-to-date `main`):
  ```bash
  git tag -a vX.Y.Z -m "vX.Y.Z: <one-line summary of what closed>"
  git push origin vX.Y.Z
  ```
  An annotated tag (`-a`), not a lightweight one — goreleaser's changelog
  and `{{ .Tag }}`/`{{ .PreviousTag }}` templating expect tag metadata.

## 3. What the release workflow does

Pushing a `v*.*.*` tag triggers `.github/workflows/release.yml`:

1. **`verify`** — re-runs `ci.yml`'s `unit` job checks (gofmt, build,
   `go vet` incl. `-tags integration`, `go test ./...`, example validate)
   against the tagged commit itself. This is a real re-verification, not a
   read of `ci.yml`'s prior result on some earlier commit on `main` — the
   tag might not even point at `main`'s tip.
2. **`goreleaser`** (needs `verify`) — checks out full history (tags need
   `fetch-depth: 0`), then runs `goreleaser release --clean` per
   `.goreleaser.yaml`:
   - Builds `platformctl` for `linux/darwin` × `amd64/arm64`
     (`CGO_ENABLED=0`, `-trimpath`, stripped, `-X main.version={{ .Tag }}`
     — the full `vX.Y.Z` tag, matching `main.go`'s own doc comment for how
     `main.version` is meant to be overridden).
   - Archives each binary as `platformctl_<tag>_<os>_<arch>.tar.gz`
     (with `README.md` alongside the binary) and writes a `checksums.txt`.
   - Publishes all archives + `checksums.txt` to the GitHub Release for
     that tag (creating it if it doesn't already exist).

Nothing in this workflow touches `scripts/pinned-images.txt` or the image
digests — that is `refresh-digests.yml`'s independent weekly job
(docs/planning/08 A10); a release does not imply the pinned images moved.

## 4. Post-release verification

1. Confirm the GitHub Release for the tag has exactly four archive assets
   (`linux_amd64`, `linux_arm64`, `darwin_amd64`, `darwin_arm64`) plus
   `checksums.txt`.
2. Download one archive for the current platform, extract, and check the
   version stamp matches the tag exactly:
   ```bash
   curl -sL -o platformctl.tar.gz \
     https://github.com/rezarajan/platformctl/releases/download/vX.Y.Z/platformctl_vX.Y.Z_linux_amd64.tar.gz
   tar -xzf platformctl.tar.gz platformctl
   ./platformctl --version   # must print "platformctl version vX.Y.Z"
   ```
3. Verify the checksum:
   ```bash
   sha256sum -c <(grep platformctl_vX.Y.Z_linux_amd64.tar.gz checksums.txt)
   ```
4. Record the release in `docs/planning/08-production-readiness-plan.md`'s
   §10 milestone-tagging note (additive edit only — the planning-doc guard
   hook blocks anything else) with the date and what closed, mirroring the
   `v1.2.0` entry's shape.

## 5. Local dry run (no tag push, no GitHub Release)

Before trusting a change to `.goreleaser.yaml` or `release.yml`, validate
without publishing anything:

```bash
go run github.com/goreleaser/goreleaser/v2@latest check
go run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean
./dist/platformctl_linux_amd64_v1/platformctl --version
rm -rf dist
```

`--snapshot` skips the GitHub publish step entirely and stamps a
`<version>-SNAPSHOT-<commit>`-style version instead of a real tag, so it is
always safe to run locally or in a non-release CI job.
