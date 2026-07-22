# Datascape documentation map

Everything under `docs/` in one page: what each piece is, whether it is a
**contract** (code is checked against it), a **plan** (work is drawn from
it), or a **record** (history — never edit to change meaning, only to
append facts).

## Start here

| Audience | Reading order |
|---|---|
| New contributor / agent | [onboarding/developers.md](onboarding/developers.md) → `CLAUDE.md` (the invariant) → [planning/01](planning/01-product-requirements.md) → [planning/02](planning/02-architecture.md) → the sections of [planning/03](planning/03-resource-model-reference.md) your task touches → [planning/06](planning/06-agentic-execution-guide.md) §3's pre-coding checklist |
| "What should I work on?" | [planning/08](planning/08-production-readiness-plan.md) — the live stage-gated backlog; §10 is the sequencing |
| "Why is it like this?" | [planning/10](planning/10-project-history-and-evolution.md) — the consolidated history with reasoning, then the specific [ADR](adr/) |
| Operator / user | [onboarding/users.md](onboarding/users.md) → the repo [README](../README.md) → generated kind reference in [reference/](reference/index.md) → [upgrade-notes.md](upgrade-notes.md) |

## Onboarding

- [onboarding/users.md](onboarding/users.md) — operating platformctl: the
  mental model (kinds, lifecycles, Bindings), the daily
  validate/plan/apply/status/drift workflow with exit codes, secrets,
  runtimes, feature gates, and the most likely failures with their actual
  error text.
- [onboarding/developers.md](onboarding/developers.md) — contributing to
  platformctl: reading order, the one invariant with real package names,
  how doc 08's task protocol works, a first-contribution provider
  walkthrough, testing (unit/conformance/integration, minimal-RBAC), and
  the docs rules (schema sync, generated reference, planning-doc guard,
  ADR practice).


## Find your answer ("I want to…")

The fastest route from question to answer, for humans and agents alike.
Naming rule used everywhere (docs/adr/019): **Datascape** = the product,
**`platformctl`** = the binary/commands, **`datascape`** = wire/disk/env
identifiers (frozen contracts), **d7s** = the short brand alias (prose
only, never identifiers).

| I want to… | Go to |
|---|---|
| Run my first pipeline | [onboarding/users.md](onboarding/users.md) → README quickstart (`platformctl init cdc-to-lake`) |
| Understand a Kind's fields | [reference/](reference/index.md) (generated, always current), depth in [planning/03](planning/03-resource-model-reference.md) |
| Know what a command does / exit codes | [onboarding/users.md](onboarding/users.md) §workflow; `platformctl <cmd> --help` |
| Diagnose a condition/reason or error | `platformctl explain <reason-or-type>` — accepts constants (`WALNotLogical`), pasted dynamic reasons (`"PartitionCountMismatch(3!=5)"`), and case-insensitive prefixes; `-o json`; static catalog in [reference/explain.md](reference/explain.md) |
| Connect an external system (prod DB, cloud bucket) | [planning/03](planning/03-resource-model-reference.md) §3 lifecycles + §8.2 Connection; [adr/005](adr/005-database-ha-posture.md) (databases), doc 08's C4 notes (object stores) |
| Wire Spark/Trino/dbt/Dagster/Grafana to the platform | `platformctl inventory --for <tool>`; [onboarding/users.md](onboarding/users.md) |
| Decide platformctl vs Terraform | README's "platformctl and Terraform"; full page: [positioning/terraform.md](positioning/terraform.md) |
| Contribute — first change | [onboarding/developers.md](onboarding/developers.md), then `CLAUDE.md`'s checklist |
| Add a provider | [onboarding/developers.md](onboarding/developers.md) §walkthrough (providerkit + the nessie template; E6's full author guide supersedes later) |
| Pick up a backlog task (agents) | [planning/08](planning/08-production-readiness-plan.md) §2.1 protocol → your task's entry → the ADRs it names |
| Know why a design is the way it is | [adr/README.md](adr/README.md) index → the numbered record; narrative: [planning/10](planning/10-project-history-and-evolution.md) |
| Check what's shipped vs planned | [planning/08](planning/08-production-readiness-plan.md) done-notes + [planning/04](planning/04-roadmap-and-feature-gates.md) §12 gate table |
| Run only the tests my change affects | `just test-affected` ([planning/06](planning/06-agentic-execution-guide.md) §10) |
| Understand an operational migration | [upgrade-notes.md](upgrade-notes.md) |
| Check my wiring / design quality, or enforce org guardrails | [adr/020](adr/020-design-lints.md) (lints) + [adr/021](adr/021-policy-engine-zero-trust.md) (policy/zero-trust) — proposed designs, commands land when scheduled |

## Contracts — the code is checked against these

- [planning/01-product-requirements.md](planning/01-product-requirements.md)
  — vision, goals G1–G8, non-goals NG1–NG7, guiding principles, FRs/NFRs.
- [planning/02-architecture.md](planning/02-architecture.md) — layering,
  module layout, ports (including the `reconciler.Request` provider
  contract), capability interfaces, the exact validate-error shapes, CLI
  surface, testing strategy.
- [planning/03-resource-model-reference.md](planning/03-resource-model-reference.md)
  — every kind, field by field, lifecycle taxonomy, status conditions. A
  schema change under `schemas/` must update this file in the same commit.
- [reference/](reference/index.md) — **generated** from `schemas/` by
  `platformctl docs build`; never hand-edited
  (`TestGeneratedReferenceInSync` enforces).

Edits to `docs/planning/*.md` are guarded by
`scripts/hooks/guard-planning-docs.sh`: checkbox toggles, purely additive
edits, and new documents pass; modifying existing contract text needs a
human (or the documented maintenance unlock).

## Plans — work is drawn from these

- [planning/08-production-readiness-plan.md](planning/08-production-readiness-plan.md)
  — **the live backlog.** Stages A (ops hardening, closed), B (Kubernetes
  Beta, closed), C (HA/routing/TLS/monitoring/backup), D
  (pipeline-infrastructure completeness), E (DX + contribution readiness),
  F (segregation readiness, closed). Every task is self-contained; §9 maps
  the historical gap analysis onto it.
- [planning/04-roadmap-and-feature-gates.md](planning/04-roadmap-and-feature-gates.md)
  — phase framework (0–8) and the **feature-gate master table** (§12),
  kept in sync with `cmd/platformctl/main.go`'s registrations.

## Process

- [planning/06-agentic-execution-guide.md](planning/06-agentic-execution-guide.md)
  — how to build this with coding agents: pre-coding checklist, hooks,
  subagents, model selection, usage discipline, and the F6 conformance
  ratchet (§8).
- [planning/00-README.md](planning/00-README.md) — the planning package's
  own index and the founding design-decision table.

## Records — history; append facts, never revise meaning

- [planning/10-project-history-and-evolution.md](planning/10-project-history-and-evolution.md)
  — the consolidated narrative: phases, stage gates, audits, pivots, and
  the reasoning behind each, with commit anchors.
- [planning/05-v1-first-version-spec.md](planning/05-v1-first-version-spec.md)
  — what v1.0.0 committed to (shipped; the acceptance scenario still runs
  in CI).
- [planning/07-production-grade-docker-runtime-gap-analysis.md](planning/07-production-grade-docker-runtime-gap-analysis.md)
  — the post-v1.0.0 gap analysis, Gates 0–3, and the **per-runtime
  differences ledger** (Cross-Runtime Portability section). Open items all
  map into doc 08.
- [planning/09-systemic-findings-and-segregation-readiness.md](planning/09-systemic-findings-and-segregation-readiness.md)
  — the live-testing bug ledger, the five failure classes, and the
  rationale for Stage F.
- [adr/](adr/README.md) — Architecture Decision Records (formerly
  `docs/design/`): 001–006 the original decision notes, 008+ the standing
  architectural decisions extracted from the planning package. 004
  (replicas-and-identity) lives on C1's unmerged implementation branch;
  007 is reserved for backup/restore (C6's required design note). See the
  ADR index for numbering rules.
- [remediation/](remediation/README.md) — the closed 2026-07-17 doc-audit
  ledger (findings F-001–F-010) and architecture assessment.
- [history/](history/) — archived working ledgers: `checkpoint.md`
  (phase-by-phase evidence through Phase 6.5 and into Stage B), `errors.md` (root-cause
  analyses of live failures), `feature-requests.md` (delivered requests
  with their original asks).
- [upgrade-notes.md](upgrade-notes.md) — behavioral migrations an operator
  would otherwise mistake for regressions.

## Positioning — comparative framing, not a contract/plan/record

- [positioning/terraform.md](positioning/terraform.md) — how platformctl
  compares to Terraform (same declarative/plan/apply/state family, but a
  domain control plane that reconciles past resource creation into
  application-level configuration), when to reach for Terraform instead,
  how the two are used together today through the `Connection` seam, and
  unscheduled future-integration ideas.
