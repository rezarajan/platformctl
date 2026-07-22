# Datascape — Agentic Execution Guide

This document is about *process*, not design: how to actually build the phases in
[04-roadmap-and-feature-gates.md](04-roadmap-and-feature-gates.md) using Claude Code (or a
similar coding agent) without the codebase drifting from this planning package, and without
burning through usage limits faster than necessary. It assumes the reader has Claude Code
installed and is working from this repo.

## 1. Repo structure for agentic work

Add these on top of the module layout in `02-architecture.md` — none of this is Go code, all of
it is agent configuration:

```text
.
├── CLAUDE.md                     # project-wide instructions, kept under 200 lines
├── docs/
│   └── planning/                 # this planning package (00–06), read-only source of truth
├── .claude/
│   ├── rules/
│   │   ├── layering.md            # "domain never imports adapters" etc. — paths: internal/**
│   │   ├── go-style.md            # formatting/lint conventions — paths: **/*.go
│   │   └── schema-changes.md      # rule: a schema change needs a resource-model-reference.md update — paths: schemas/**
│   ├── agents/
│   │   ├── provider-implementer.md
│   │   ├── compatibility-reviewer.md
│   │   ├── integration-test-runner.md
│   │   ├── docker-verifier.md
│   │   └── schema-doc-sync.md
│   └── settings.json              # hooks, model defaults, permission rules — see §3, §5
├── internal/ ...                  # as defined in 02-architecture.md
└── examples/cdc-attendance/       # the acceptance scenario from 05-v1-first-version-spec.md
```

**Why `docs/planning/` stays read-only in practice:** treat this package the way the codebase
treats `schemas/` — a contract other things are checked against, not a scratchpad. If a phase's
implementation reveals the plan was wrong (it will, at least once), that's a deliberate edit to
one of these six files with a reason, not an incidental drift while heads-down on code. Say so
explicitly in the commit message when it happens.

### `CLAUDE.md` — what actually goes in it

Keep this under 200 lines (longer files measurably reduce how reliably Claude follows them).
Put only what's true in *every* session:

```markdown
# Datascape (platformctl)

Go 1.22+. Build: `CGO_ENABLED=0 go build -trimpath -buildvcs=false ./cmd/platformctl`.
Test: `GOCACHE=/tmp/datascape-go-cache go test ./...`. Integration: `just test-integration`
(requires Docker; tagged `integration`, skipped by default `go test`).

## Layering (see docs/planning/02-architecture.md §1-2)
- `internal/domain` imports nothing else in this repo. `internal/ports` imports only `domain`.
  `internal/adapters` implement ports and may import third-party SDKs.
- Only `cmd/platformctl` and `internal/application/registry` import concrete adapters.
- If you're about to import an adapter package from `domain` or `ports`, stop — that's the one
  invariant this whole design depends on.

## Before implementing anything
1. Read the relevant phase's exit criteria in docs/planning/04-roadmap-and-feature-gates.md.
2. Read the Kind/interface definitions you're touching in docs/planning/02-architecture.md and
   docs/planning/03-resource-model-reference.md.
3. Check docs/planning/05-v1-first-version-spec.md if the change touches the acceptance scenario.

## Conventions
- New provider → implement `reconciler.Provider`, register in `application/registry`, add a
  JSON Schema, add a feature gate entry defaulting to Alpha/disabled (docs/planning/02 §11).
- Every `Ensure*` runtime method must be idempotent — a second call with the same spec makes zero
  API calls to Docker. This is tested by the conformance suite; don't special-case it.
- A schema change under `schemas/` requires a matching update to
  docs/planning/03-resource-model-reference.md in the same commit.

## Compact instructions
When compacting, preserve: which phase/exit-criteria item is in progress, test output, and any
open design question raised during this session. Discard exploratory file-reading history.
```

Everything more detailed than this — e.g., "how to write a new provider step by step," "how the
Debezium connector REST API works," "how golden-file tests are structured" — belongs in a
**skill** (`.claude/skills/`), not here. Skills load on demand; CLAUDE.md loads every session
whether it's relevant or not.

### `.claude/rules/` — path-scoped, not always-loaded

Rules with a `paths:` frontmatter field only enter context when Claude touches a matching file,
which keeps the "always in context" budget (CLAUDE.md) small. Use this for anything that's
detailed but only matters in a subset of the repo:

```markdown
---
paths:
  - "internal/domain/**/*.go"
  - "internal/ports/**/*.go"
---

# Domain/ports layering

Files here must not import anything under internal/adapters. If you need a concrete
implementation, define an interface here and let internal/application wire the adapter in.
```

```markdown
---
paths:
  - "schemas/**"
---

# Schema changes

Every file here corresponds to a section in docs/planning/03-resource-model-reference.md.
Adding or changing a field here without updating that doc in the same commit is incomplete work.
```

## 2. Standing bookkeeping tasks

These are the things that must happen on effectively every task, regardless of which phase or
which agent is doing the work. Two ways to enforce them: **hooks** (mechanical, always fire,
can't be talked out of) and a **checklist a subagent runs** (judgment-based, for things a shell
script can't verify). Use hooks wherever the check is mechanical; reserve judgment calls for the
subagent/checklist path.

| Bookkeeping task | Mechanism | Why this mechanism |
|---|---|---|
| Format and lint Go code after every edit | `PostToolUse` hook on `Edit`/`Write`, matcher `\.go$`, runs `gofmt -w` + `golangci-lint run --fix` on the changed file | Mechanical, deterministic, doesn't need judgment. |
| Block edits to `docs/planning/*.md` without an explicit reason in the prompt | `PreToolUse` hook on `Edit`/`Write` matching `docs/planning/` | These files are the contract; an accidental edit while "just fixing a typo elsewhere" shouldn't slip through. |
| Run the affected package's tests after every non-trivial edit | `PostToolUse` hook on `Edit`/`Write` matching `internal/**/*.go`, runs `go test ./<changed-package>/...`, filtered to failures only | Keeps the loop tight without flooding context with passing-test output (see §5 on context cost). |
| Verify a new/changed schema has a matching doc update | `PreToolUse` hook (or a `PostToolUse` check) on `schemas/**` that greps `docs/planning/03-resource-model-reference.md` for the Kind name and warns if absent | Catches the single most likely place implementation and plan silently diverge. |
| Update the state-format version note if `StateStore`'s `State` struct changes shape | Left as a subagent/checklist item, not a hook — judgment on whether the change is actually breaking | A hook can detect the struct changed; it can't judge whether the change needs a migrator. |
| Re-run the full acceptance scenario before marking a phase's exit criteria complete | `just test-integration` run explicitly, not on every edit (too slow/expensive to run per-file) | Reserve for phase-boundary checkpoints, not every commit. |
| Keep the feature-gate table (`04-roadmap-and-feature-gates.md` §12) in sync with `application/featuregate`'s registered gates | Subagent checklist item at the end of any task that adds/changes a gate | Mechanical existence-check is easy; whether the *stage* (Alpha/Beta/GA) is still accurate is judgment. |

Example hook config (`.claude/settings.json`):

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Edit|Write",
        "hooks": [
          { "type": "command", "command": "./scripts/hooks/fmt-and-lint.sh" }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "Edit|Write",
        "hooks": [
          { "type": "command", "command": "./scripts/hooks/guard-planning-docs.sh" }
        ]
      }
    ]
  }
}
```

`fmt-and-lint.sh` reads the tool input JSON from stdin, checks the file path matches `\.go$`, and
runs formatting/linting only on that file — same pattern as any `PreToolUse`/`PostToolUse` hook:
read JSON from stdin, act, return a JSON decision. Keep these scripts fast; they run on every
matching tool call.

## 3. What agents should review before writing any code

This is the single most important habit for keeping a multi-session, multi-agent build coherent
with a five-document plan nobody can hold entirely in their head. Before starting *any* task,
whether it's a fresh session or a delegated subagent:

1. **Which phase and which exit-criteria line** (`04-roadmap-and-feature-gates.md`) does this
   task correspond to? If it doesn't map to one, that's worth flagging before writing code —
   either the roadmap is missing something or the task is scope creep.
2. **Which Kind(s) or interface(s) does this touch**, and what does
   `03-resource-model-reference.md` / `02-architecture.md` say their final shape is? Don't
   re-derive the schema from first principles mid-task; it's already been decided.
3. **Does this touch a capability interface** (`CDCCapableProvider`, `SinkCapableProvider`,
   `LineageAware`)? If so, re-read `02-architecture.md` §4.2 and §5.2 — the compatibility-check
   error format is specified exactly and should match on the character, not just in spirit.
4. **Does the acceptance scenario in `05-v1-first-version-spec.md` reference this resource or
   provider?** If yes, the acceptance manifests are the integration test target — don't invent a
   different example.
5. **Is there an existing contract test suite for the port being touched?** (`02-architecture.md`
   §9). A new adapter must pass it, not merely "seem to work."

In practice, this is what **plan mode** is for: press Shift+Tab before implementation on
anything larger than a one-file fix, let Claude read the relevant planning-doc sections and the
existing code, and approve the plan before it writes anything. For a task big enough to delegate
to a subagent, put this checklist directly in the subagent's system prompt (see the
`provider-implementer` example in §6) so it happens automatically rather than depending on the
person remembering to ask for it.

## 4. Model selection

Claude Code's model aliases as of this writing: `sonnet`, `opus`, `haiku`, `fable`, `best`
(resolves to Fable 5 where available, otherwise the latest Opus), and `opusplan` (Opus during
plan mode, Sonnet for execution). Effort levels (`low`/`medium`/`high`/`xhigh`/`max`) are a
second, independent dial on top of model choice. Below is which combination fits which part of
this project — check `docs.claude.com/en/docs/claude-code/model-config` for anything that may
have changed since.

| Task type | Model | Effort | Why |
|---|---|---|---|
| Scaffolding a phase's domain/ports packages from this plan (Phase 0 especially) | `sonnet` | `high` (default) | Sonnet handles well-specified, mechanical translation-from-spec work at a fraction of Opus's cost. This plan is detailed enough that the hard thinking has already happened. |
| Writing a new provider adapter (Redpanda, Postgres, Debezium, S3, S3 sink) against the interfaces in `02-architecture.md` | `sonnet` | `high` | Same reasoning — the interface is fixed, the technology's API is documented, this is implementation work. |
| Deciding *whether* the compatibility-check design, the lineage mechanism, or the sink `Binding` mode need to change based on something discovered mid-implementation | `opus` or `opusplan` | `high`/`xhigh` | This is the kind of judgment call that produced the `Source`/lineage design revisions earlier in this package's history — genuine architectural reasoning, not mechanical execution. Use plan mode (`opusplan`) so the reasoning happens before code is written, not interleaved with it. |
| Root-causing a flaky Debezium/Kafka Connect integration test, or a non-deterministic `plan` diff | `opus`, escalate to `fable` if it spans multiple sessions or the root cause isn't obvious after one focused pass | `high`/`xhigh` | Exactly the kind of ambiguous, multi-hypothesis investigation Fable 5 is suited to — but it isn't the default, so opt in deliberately (`/model fable`) rather than leaving it on for routine work. |
| A long, autonomous run to take one full phase (e.g., Phase 4: Object Storage Sink) from "exit criteria written" to "all boxes checked, tests green" without babysitting | `fable`, paired with [`/goal`](https://docs.claude.com/en/docs/claude-code/goal) set to the phase's exit criteria as the stop condition | model default (`high`) | This is the explicit case Fable 5 is built for: hand it an outcome, let it plan the path, and it verifies its own work more than smaller models do. Reserve this for genuinely large, well-bounded units (a whole phase), not small edits — the cost difference is real. |
| High-volume, low-judgment operations: running the full test suite and reporting only failures, grepping integration test logs, exploring an unfamiliar part of the codebase before a task | `haiku`, via a subagent (see `docker-verifier`, `integration-test-runner` in §6) | `low`/`medium` | These tasks produce output nobody needs to read in full. Delegating to a Haiku subagent keeps that volume out of the main conversation's context entirely, at the lowest per-token cost. |
| Generating or updating reference docs from `schemas/` (`platformctl docs build` output, not this planning package) | `sonnet`, or `haiku` for simple formatting-only passes | `medium` | Mechanical, well-defined transformation. |
| Reviewing a PR against `docs/planning/02-architecture.md`'s layering rule and the compatibility-check error format | `sonnet` subagent (`compatibility-reviewer`, §6) | `high` | Needs to actually read and compare against spec, not just grep — worth a capable model, but not Opus-tier judgment. |

A few things worth calling out explicitly:

- **`fable` is not the default model and should stay that way for this project.** Model-config
  guidance is direct about this: describe the outcome and let it plan, skip routine verification
  reminders, and size up the task to something that would otherwise be broken into pieces. Using
  it for routine provider-implementation work wastes exactly the capability it's good at.
- **Fable 5 runs safety classifiers for cybersecurity and biology content** and will
  automatically fall back to Opus if a request trips one. This project's secret-handling,
  sandboxing, and Docker-socket-adjacent code is a plausible (if unlikely) place to see that
  fallback fire — it's expected behavior, not a bug, if it happens while working on
  `adapters/secrets/` or anything touching credential material.
- **`opusplan` is a good default posture for anything phase-boundary-shaped**: plan on Opus,
  execute on Sonnet, without manually switching models mid-task.
- **Switching models mid-session invalidates the prompt cache**, so pick deliberately at the
  start of a task rather than bouncing between models within one conversation.

## 5. Avoiding excessive usage limits

This project's plan is long and detailed on purpose, which means the failure mode to guard
against isn't "the agent doesn't know what to build" — it's "the agent burns budget re-reading
and re-deriving things that are already decided." Concretely, for this codebase:

- **Default to Sonnet, not Opus or Fable, for the bulk of the work.** Per the table above, most
  of Phases 0–5 is well-specified implementation against fixed interfaces — exactly what Sonnet
  is for. Reserve the pricier tiers for the specific junctures called out above.
- **Delegate verbose operations to Haiku subagents.** Running `go test ./...`,
  `just test-integration`, or grepping Docker/Kafka Connect logs produces output that's mostly
  noise. A subagent (ideally `haiku`) absorbs that into its own context and returns only a
  summary — see `integration-test-runner` in §6. This is the single highest-leverage habit for
  this specific project, since integration tests against real Docker/Debezium/Kafka Connect are
  exactly the kind of high-volume, low-signal output this guards against.
- **Keep CLAUDE.md under 200 lines** (§1) — it loads every session regardless of relevance.
  Anything phase-specific or provider-specific belongs in a skill or a path-scoped rule instead.
- **Use path-scoped `.claude/rules/`** rather than one large always-loaded rules file, so the
  Debezium-connector-registration details only load when someone's actually touching
  `adapters/providers/debezium/`.
- **Clear context between unrelated phases.** Don't carry a stale Phase 2 (Redpanda) conversation
  into Phase 4 (Object Storage) work — `/clear` (and `/rename` first if you'll want to find it
  again) rather than letting unrelated context accumulate and get re-processed on every turn.
- **Use plan mode before large changes**, not after a wrong-direction implementation attempt —
  re-work from a bad first guess costs far more than the planning pass would have.
- **Prefer CLI tools over MCP servers where both exist** (e.g., `docker`, `gh`) — MCP tool
  listings cost context even before they're used; only connect MCP servers this project actually
  needs continuously (if any), and disable ones that aren't in active use.
- **If working with agent teams or many parallel subagents, keep them few and short-lived.**
  Each teammate is a fully separate context window; a large team investigating Phase 3's CDC flow
  in parallel costs meaningfully more than one focused session, so reserve that pattern for
  genuinely parallel, independent work (e.g., "one agent on the Redpanda provider, another on the
  Postgres provider, at the same time" — not "five agents all looking at the same bug").
- **Solo/small-team usage limits**: if usage limits become a binding constraint rather than a
  cost-optimization concern, current per-user rate-limit guidance for small teams (1–5 users) is
  in the 200k–300k TPM / 5–7 RPM range — useful as a sanity check for whether a given session's
  pace is normal or unusually heavy. Check `/usage` for actual per-session spend before assuming
  a specific task is the culprit.

## 6. Suggested subagents for this project

Concrete starting points — adjust tool lists and models as the codebase grows. Save these under
`.claude/agents/` and check them into version control so the whole team (and future agent
sessions) get them for free.

```markdown
---
name: provider-implementer
description: Implements a new Provider adapter (reconciler.Provider) against an existing technology's API, following the interfaces in docs/planning/02-architecture.md. Use when adding or modifying a provider under internal/adapters/providers/.
tools: Read, Grep, Glob, Edit, Write, Bash
model: sonnet
---

Before writing any code:
1. Read docs/planning/02-architecture.md §4.2 (Provider interface and capability interfaces).
2. Read docs/planning/03-resource-model-reference.md for the Kind(s) this provider reconciles.
3. Read docs/planning/04-roadmap-and-feature-gates.md for this provider's phase and exit criteria.
4. Check internal/ports/reconciler for the exact interface signatures — do not re-derive them.

Implement against the existing runtime.ContainerRuntime port; never import a concrete runtime
adapter directly. Every Ensure*-style operation you call must already be idempotent by contract —
if it isn't, that's a bug in the runtime adapter, not something to work around here.

When done, verify the provider passes the shared conformance suite for its port, and add a
feature gate entry (default: Alpha, disabled) per docs/planning/02-architecture.md §11.
```

```markdown
---
name: compatibility-reviewer
description: Reviews Binding-related changes against the mode/Kind-pairing rules and capability-interface contract in docs/planning/02-architecture.md §5.2 and docs/planning/03-resource-model-reference.md §7. Use after modifying internal/domain/binding, internal/application/compatibility, or any provider's capability methods.
tools: Read, Grep, Glob, Bash
model: sonnet
---

Check that:
- Every Binding mode has an entry in the mode->Kind pairing table and the code matches it exactly.
- CDCCapableProvider.SupportedSourceEngines() and SinkCapableProvider.SupportedSinkFormats()
  are checked at validate/plan time, not deferred to apply.
- The validate-time error message names the Binding, the Provider, its type, and what it
  actually supports, matching the format shown in docs/planning/02-architecture.md §5.2.
Report deviations; do not fix them yourself unless asked.
```

```markdown
---
name: integration-test-runner
description: Runs `just test-integration` or `go test ./...` and reports only failures with enough context to act on them. Use proactively after any change to internal/adapters or internal/application.
tools: Bash, Read, Grep
model: haiku
---

Run the requested test command. Filter output to failing tests only: test name, assertion
failure, and up to 20 lines of surrounding output per failure. Do not report passing test counts
in detail — a one-line summary ("142 passed, 2 failed") is enough. If everything passes, say so
in one line and stop.
```

```markdown
---
name: docker-verifier
description: Inspects the real Docker daemon state (containers, networks, volumes, labels) to verify a runtime adapter change did what it claims, without polluting the main conversation with raw docker inspect output. Use after changes to internal/adapters/runtime/docker.
tools: Bash, Read
model: haiku
---

Use `docker ps`, `docker network ls`, `docker volume ls`, and `docker inspect` filtered to
Datascape-labeled objects (io.datascape.managed-by) to confirm expected state. Report a concise
diff against what was expected, not raw command output.
```

```markdown
---
name: schema-doc-sync
description: Checks that every Kind and field in schemas/ has a corresponding, accurate entry in docs/planning/03-resource-model-reference.md, and flags drift in either direction. Use after any schema change or before closing a phase.
tools: Read, Grep, Glob
model: sonnet
---

Compare schemas/*.json against docs/planning/03-resource-model-reference.md kind by kind.
Report any field present in one but not the other. Do not edit either file — report only.
```

## 7. Putting it together: a per-phase workflow

1. **Start of phase**: open a fresh session (`/clear` if continuing in the same terminal), read
   the phase's section in `04-roadmap-and-feature-gates.md` aloud to Claude (or reference it
   directly — it's already in the repo), and use `opusplan` or explicit plan mode to produce an
   implementation plan before any code is written.
2. **Implementation**: switch to `sonnet` for the bulk of the work, delegating to
   `provider-implementer` or working directly for domain/ports/application changes. Let the
   `PostToolUse` hooks (§2) handle formatting, linting, and package-level test runs automatically.
3. **Verification**: delegate to `integration-test-runner` and `docker-verifier` (both `haiku`)
   for the noisy parts; review their summaries rather than raw output.
4. **Compatibility/schema review**: run `compatibility-reviewer` and `schema-doc-sync` before
   considering the phase's exit criteria satisfied.
5. **Close of phase**: check every box in the phase's exit-criteria list against the actual
   running binary — not from memory, actually run the commands — and only then consider the
   phase done. If anything in this planning package turned out to be wrong along the way, edit
   the relevant document with a clear reason, the same way you'd amend a design doc after a real
   design review.
6. **Escalate to `fable`** only when a task in this phase turns out to be a genuine
   root-cause-unknown investigation or a large, well-bounded unit of work you want to run
   autonomously to completion — not as a default starting point.

## 8. The conformance ratchet (standing policy — docs/planning/08 F6, docs/planning/09 §3-F6)

This policy applies to every bug found only by live testing (a real Docker
daemon, a real cluster) that unit tests, the conformance suite, and the
integration suite all missed:

1. **The fix lands with a contract-level reproduction in the same commit** —
   a conformance-suite subtest (preferred, it runs against every adapter) or
   a port-contract test. The same discipline the repo already applies to
   schema↔doc sync. Examples: `RemoveNetwork_refuses_while_container_attached`
   (the shared-namespace destroy bug), the entrypoint-faithfulness and
   delayed-listen readiness subtests.
2. **If the class cannot be expressed at the contract level, that is itself
   a finding**: the semantic lives outside the port, and it must be recorded
   in doc 07's Cross-Runtime per-runtime differences ledger instead — the
   NetworkPolicy/external-access-mode interaction (K13/B7) is the model.
3. **Translation-fidelity gate for future runtime adapters**: conformance
   green is necessary but not sufficient; the runtime-parameterized real
   example suites (`cmd/platformctl/kubernetes_examples_integration_test.go`
   pattern) reaching Ready with unmodified providers is the acceptance bar.
   A synthetic conformance suite proves the *port contract*; only real
   providers against real infrastructure prove the *translation*.
4. **Kubernetes verification must run under the minimal RBAC kubeconfig**
   (mint a token-scoped kubeconfig for the `platformctl` ServiceAccount,
   exactly as CI's K8s job does), never ambient admin credentials — a
   change that adds a Kubernetes API call passes admin-credential runs
   while violating the B5 posture, and the role +
   `preflight.go`'s check list + `deploy/kubernetes/rbac/README.md` must
   all gain the new verb in the same commit (lesson: C1's StatefulSet
   shape-guard reads, caught only by CI's minimal-RBAC leg, 2026-07-21).

## 9. Shared integration-test harness (docs/planning/08 G6)

`cmd/platformctl/integration_harness_test.go` (same package, `//go:build
integration`) holds the setup/cleanup shapes that recur across
`cmd/platformctl/*_integration_test.go`'s Docker-backed suites:
`requireDocker(t)` (connect to the Docker daemon, `t.Fatalf` on error) and
`registerDockerCleanup(t, rt, containers, volumes, network)` (best-effort
removal in containers → volumes → network order, registered via
`t.Cleanup`, with the func also returned for suites that additionally run
it once up front). New Docker-backed integration tests should use these
instead of re-implementing them. Migration of existing suites is
opportunistic, never a big-bang rewrite — bespoke setups (Kubernetes
cluster guards, chaos's mid-apply kill, shared-state's raw MinIO container)
stay local to their file.

## 10. Integration-test economy (docs/planning/08 G7)

The integration suites are the project's ground truth, but a full sweep
costs ~30+ minutes and this repo's agent workflow was re-running
overlapping suites at branch gates, merge gates, and after flakes. The
standing method — **run each affected suite exactly once per
content-state, across all sessions**:

1. **Select by impact, not by habit:** `scripts/test-impact.sh --base
   main` diffs your change, maps it through the suite↔scope table inside
   the script, and runs only the affected suites. `--print` previews;
   `--full` selects everything (release gates). The map is a contract:
   adding a suite or moving files updates it in the same commit.
2. **Dedup by scope-hash, not by memory:** every pass is recorded in the
   shared git common dir keyed by (suite, hash of the suite's scoped
   content, including uncommitted changes). An identical content-state is
   SKIPped with the prior evidence cited — across branches, worktrees,
   agents, and sessions. A change outside a suite's scope cannot
   invalidate its green, so a merge gate re-runs only the suites whose
   scope the merge actually touched. `--force` overrides when you have
   reason to distrust a recorded pass.
3. **Two tiers:** an agent's branch gate runs its affected suites once
   and records the evidence in TASK_PROGRESS.md (suite ids + timings).
   The merge gate runs the union of affected suites once on the merged
   tree — which the ledger reduces to only the suites whose scope changed
   in the merge. The broad sweep (`--full` or `just test-integration`)
   is reserved for: runtime-port contract changes, provider-wide
   refactors (the providerkit class), and release tags.
4. **Serialize on the shared daemon:** the script wraps every suite in a
   single flock — two example-scenario suites racing one Docker daemon
   produce flaky timeouts whose retries cost more than serialization
   saves. Agents must not run integration suites outside the wrapper
   while other agents are active.
5. **Environment hygiene before any long suite** — a red run caused by
   the environment costs a full re-run: pre-pull pinned images on a fresh
   node (`scripts/pinned-images.txt`), re-mint the minimal-RBAC token if
   older than its duration (§8 rule 4), and clear leftover
   `datascape-*` namespaces/containers from aborted runs first. The two
   historical wasted K8s runs were cold image pulls and an expired token,
   not code.
6. **Kubernetes legs** run only when the k8s-adapter scope (or a
   provider's K8s-relevant behavior) changed — and always under the
   minted minimal-RBAC kubeconfig (§8 rule 4), via
   `PLATFORMCTL_KUBECONFIG`/`KUBECONFIG` env ahead of the script.
7. **Never remove a worktree with live test processes rooted in it.**
   Before `git worktree remove` on a merged agent branch, check
   `pgrep -af <worktree-path>` — a suite still running from a vanished
   directory produces spurious failures that look like code regressions
   (observed live, 2026-07-22: a green-twice suite "failed" only because
   the orchestrator deleted its cwd mid-run). Wait for the flock queue
   to drain or kill the specific run deliberately, then remove.
7. **Completeness is enforced, not just documented** (docs/planning/08
   G7): `internal/archtest/test_impact_completeness_test.go` parses the
   suite map straight out of `scripts/test-impact.sh` (never a duplicated
   copy) and fails naming any `Test*` in `cmd/platformctl/
   *_integration_test.go` or any other `//go:build integration`-tagged
   package that no suite's `-run` pattern would actually execute — unless
   it's named on that test file's `integrationTestExemptions` map with a
   reason. Adding a suite or widening a `-run` pattern in the same commit
   that adds the test keeps this green; an unavoidable gap goes on the
   exemption list instead of being silently unmapped.
8. **Ledger pruning:** `scripts/test-impact.sh --prune <days>` deletes
   ledger entries older than `<days>` days (by file mtime) and exits —
   a standalone maintenance action, run independently of a normal
   selection/execution invocation. The ledger has no automatic expiry
   otherwise; a maintainer runs `--prune` periodically (or wires it into
   a scheduled job) to keep the shared git common dir from accumulating
   scope-hash keys for content-states that no longer exist on any branch.
9. **CI adoption:** `.github/workflows/ci.yml`'s `integration` job runs
   `scripts/test-impact.sh --base origin/main` on pull requests (impact
   selection) and `scripts/test-impact.sh --full` on pushes to `main`
   (the full sweep) — the always-full PR sweep this section originally
   described is retired in favor of that split. `integration-k8s` is
   unaffected (it is not suite-map-driven; see §8).
