# E9 (interactive composition — add/wire/expose) — task progress

Doc 08 §2.1 protocol. Step 0 checkpoint file (overwrites the previous task's
E8 file per convention — that work is already committed on main).

Task: docs/planning/08-production-readiness-plan.md §7 E9. Design in
docs/adr/024-interactive-composition.md (read in full; implement literally).
**The one rule:** composition compiles to manifest patches only. Never
applies, never bypasses files.

## Resumption note

If resuming mid-task: read this file + `git log --oneline -20` first. Do not
re-read the full context; continue from the first step below marked
`[ ]`/`next`.

## Plan

0. [done] `git merge main --no-edit` (fast-forward, brought in ADR 024,
   goreleaser/CI files — no conflicts). Read CLAUDE.md, doc 08 §2.1 protocol
   in full, doc 08 E9 entry, ADR 024 in full, internal/application/blueprint
   (template stock — the "validates with zero edits" bar and exact YAML
   shapes for source/provider/eventstream/binding/dataset/secret/connection),
   cmd/platformctl/root.go's `loadAndValidate`, internal/domain/resource,
   internal/domain/graph, internal/application/manifest, internal/application/
   compatibility (Check + ResolveKafkaBootstrapAddress — the graph-inferable
   bootstrapServers pattern compose's generated Providers reuse), ADR
   README index for the Connection-realization ADRs (018 = ingress routing;
   confirmed **https is NOT yet supported**: internal/adapters/providers/
   ingress/ingress.go `SupportedConnectionSchemes() []string { return
   []string{"http"} }` — C8/TLS has not merged into this tree, so `expose
   --scheme https` must degrade with a clear message, never crash).
   Read cmd/platformctl/output_contract_harness_test.go in full: every new
   leaf cobra command needs a `commandScenarios` entry or
   `TestOutputContractHarnessCoversEveryCommand` fails.
1. [done] `go get charm.land/huh/v2@v2.0.3` — resolved cleanly from
   proxy.golang.org. Pins recorded below. `go mod tidy` deferred to just
   before the final verification pass (after all code is in) so the diff
   reviewed for "no other in-flight branch's deps leak in" is the real one.
2. [done] internal/application/compose (headless engine — see file list
   below for what landed).
3. [done] cmd/platformctl: add.go, wire.go, expose.go, compose_shared.go
   (shared --dry-run/-o json contract + reuse-first/prompt glue) + root.go
   registration + commandScenarios entries (8 new leaf paths: add
   source|pipeline|sink|catalog|monitoring, wire, expose).
4. [done] internal/cliutil: huh-based prompt helpers (prompt.go).
5. [done] internal/archtest: charm confinement test
   (charm_confinement_test.go) + fixture-violation proof (described in
   commit/report, not committed as a real violation).
6. [done] gofmt/build/vet/go test ./... green (unit + -tags integration
   build).
7. [done] Live Docker owner-scenario integration test
   (cmd/platformctl/compose_integration_test.go, `//go:build integration`,
   `TestComposeOwnerScenario`) — ran live, PASS (65.5s): init -> engine-
   level candidate assertion (broker+raw-lake listed) -> add pipeline
   (flags, reuse + --sink-prefix other/) -> expose Source/app-db --scheme
   tcp -> validate green -> apply to Ready (24 resources) -> plan
   zero-drift -> idempotent add pipeline rerun (changed=false via -o json,
   "no changes" human) -> destroy clean (0 labeled leftovers). Also
   manually smoke-tested the CLI binary directly (dry-run diffs, non-TTY
   hard error listing exact missing flags, https degrade both with and
   without the IngressProvider gate enabled).
8. [done] scripts/test-impact.sh --base main — added ONE new "compose" row
   (existing rows untouched); TestIntegrationSuiteMapCoversEveryTest green.
9. [done] go.mod tidy pass: reviewed the diff — only charm.land/huh/v2 and
   golang.org/x/term (already-indirect, now directly imported) flipped to
   direct requires; go.sum gained hash-completion entries for modules
   already in the build list (huh's own transitive graph, plus
   already-present indirects like twpayne/go-geom's own test deps) — no
   new production dependency beyond the charm.land/huh/v2 tree. Manually
   reverted `go mod tidy`'s incidental reclassification of
   github.com/parquet-go/parquet-go (indirect -> direct): it's only
   imported by an unrelated pre-existing integration-tagged test file, not
   by anything this task touched, so it was put back under `// indirect`
   to keep the go.mod/go.sum diff scoped to charm deps only, per the file
   fence.
10. [done] Final commit.

## Dependency pins (`go get charm.land/huh/v2@v2.0.3`)

- charm.land/huh/v2 v2.0.3
- charm.land/bubbletea/v2 v2.0.2
- charm.land/bubbles/v2 v2.0.0
- charm.land/lipgloss/v2 v2.0.3
- github.com/charmbracelet/ultraviolet bumped to
  v0.0.0-20260205113103-524a6607adb8 (was already an indirect dep at an
  older pseudo-version)
- new indirects: github.com/atotto/clipboard v0.1.4,
  github.com/catppuccin/go v0.2.0, github.com/mitchellh/hashstructure/v2
  v2.0.2, github.com/charmbracelet/x/exp/{strings,ordered}
- golang.org/x/term v0.43.0 was already indirect (k8s client-go) — reused
  for non-TTY detection, no new dependency class introduced for that.

## File map

- internal/application/compose/{compose,patch,candidates,templates,source,
  pipeline,sink,catalog,monitoring,wire,expose}.go + _test.go siblings.
- cmd/platformctl/{add,wire,expose}.go
- internal/cliutil/prompt.go
- internal/archtest/charm_confinement_test.go
- cmd/platformctl/compose_integration_test.go (owner scenario, live Docker)
- scripts/test-impact.sh: one new row for the compose integration suite.

## Open findings / deviations

- H1 (lint, internal/application/lint) has not merged into this tree as of
  this session — confirmed absent. The Accept criterion's "if H1 has
  merged, assert lint-clean" is therefore deferred-pending-H1; recorded in
  the final report and as a doc-08 status note.
