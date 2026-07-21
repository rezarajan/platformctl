# Architecture Decision Records

This directory (formerly `docs/design/`) holds every architectural decision
as a numbered, immutable record. An ADR states the question, the options
considered, the decision, and its consequences — it is a **record**: once
accepted, its meaning is never edited. Superseding a decision means writing
a new ADR that names the one it replaces (see 002's addendum for the
pattern of recording a redirected first cut).

## Rules

- **Numbering is monotonic and never reused.** Claim the next free number.
  Numbers reserved by in-flight branches count as claimed (see the index).
- **One decision per ADR.** If a task changes a shipped shape, adds a
  dependency class, or draws a scope line, it needs an ADR (doc 08's L-size
  tasks say "design note first" — that note is an ADR here).
- **Shape:** `NNN-kebab-title.md` with a `**Status:**` line
  (proposed | accepted | superseded-by-NNN), what prompted it, options
  considered, the decision, why it doesn't box out the future, and
  non-blocking follow-ups. ADRs 001/003/005/006 are the house style.
- ADRs **record** decisions; the **contracts** stay in docs/planning/01–03.
  When an ADR and a contract doc disagree, the contract doc wins — update
  the contract doc in the same commit as the ADR that changes it.

## Index

| # | Title | Status | Decides |
|---|---|---|---|
| [001](001-bindings-are-directed-edges.md) | Bindings are directed edges | accepted, shipped pre-v1.0.0 | `AllowedKindPairs` is a relation; asset kinds are role-neutral |
| [002](002-soak-orchestrator-infrastructure.md) | Orchestrator-ready infrastructure | accepted (first cut superseded by its own addendum) | Phase 6.5 scope; Catalog/Connection remodel; "soak" retired |
| [003](003-shared-state.md) | Shared/remote state backend | accepted, shipped (08 A4) | S3-compatible store + conditional-PUT lease locking |
| [004](004-replicas-and-identity.md) | Replicas and stable identity | accepted, shipped (08 C1) | `ContainerSpec.Replicas`/`StableIdentity`; ordinal naming; StatefulSet mapping |
| [005](005-database-ha-posture.md) | Database HA posture | accepted, decision-only | managed DBs stay single-node; production HA enters via `external: true` + Connection |
| [006](006-compute-engines.md) | Compute-engine infrastructure | accepted | Trino provider first (D10); Flink deferred; engine infra in scope, jobs never |
| [007](007-backup-restore.md) | Backup/restore mechanism | accepted, shipped (08 C6) | job-container pipeline; endpoint-fact Location resolution; protect refusal; Docker-only for now |
| [008](008-hexagonal-layering.md) | Hexagonal layering and the one invariant | accepted, retroactive | domain/ports/adapters; who may import adapters |
| [009](009-capability-interfaces.md) | Compatibility as provider capability | accepted, retroactive | optional capability interfaces checked at validate; the exact error shape |
| [010](010-lineage-observed-not-synthesized.md) | Lineage observed, never synthesized | accepted, retroactive | `observers` forwards connection facts only |
| [011](011-validate-time-completeness.md) | Validate-time completeness | accepted, retroactive | a validating manifest set cannot half-apply mis-wired |
| [012](012-determinism-and-state.md) | Determinism, state, and authoritative apply | accepted, retroactive | plan from manifests+state only; spec hashes; apply-deletes; secret fingerprints |
| [013](013-safety-in-the-engine.md) | Safety enforced in the engine | accepted, retroactive | NFR-3 double flags, `protect`, `deletionPolicy`, ownership labels |
| [014](014-feature-gate-strategy.md) | Feature-gate strategy | accepted, retroactive | Alpha/disabled convention; graduation; master-table sync |
| [015](015-connectivity-plane.md) | The connectivity/discovery plane | accepted (Stage F) | EnsureReachable-only dialing; port audiences; naming authority; endpoint facts; strict fake; F6 ratchet |
| [016](016-provider-request-contract.md) | Provider invocation via Request struct | accepted (Stage F5) | stateless providers; additive request fields; no setter interfaces |
| [017](017-redpanda-multibroker-and-replica-state.md) | Redpanda multi-broker clusters and replica state | accepted (C2) | brokers opts into the ordinal-set shape; StableIdentity at N=1; per-ordinal dialer map; 3→1 refused; aggregate state + published per-ordinal endpoint facts |

"Retroactive" ADRs (008–014) record decisions made in the planning package
(docs 00–06) before this convention existed — written 2026-07-21 so the
decision, its alternatives, and its rationale are findable without
archaeology. Their authoritative statements remain the planning docs they
cite.
