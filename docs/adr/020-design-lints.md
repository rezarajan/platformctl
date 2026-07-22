# ADR 020 — Design lints: deterministic wiring-quality evaluation

**Status:** proposed (2026-07-22) — design accepted by the project owner in
intent ("guardrails a data engineer would expect"); backlog placement
awaits the owner's green light. No sequencing content in doc 08 is
modified by this record.

## Context

`validate` today enforces **legality**: a manifest set that validates
cannot half-apply into a mis-wired platform (ADR 011). It deliberately
says nothing about **design quality**. Legal-but-hazardous or
legal-but-inert topologies pass silently:

- Two `cdc` Bindings capture the same `Source` with overlapping
  `options.tables` — legal, but on Postgres that is two replication slots
  holding WAL, duplicate change streams downstream, and (pre-F-006-style)
  subtle connector interference; a data engineer should at least be told.
- A `Catalog` that nothing references — no `catalogRef`, no
  `warehouseProviderRef` consumer, no Connection routing to it: inert
  infrastructure that will be provisioned, billed, and monitored for
  nothing.
- An `EventStream` no Binding reads or writes; a `SecretReference` nothing
  resolves; a `Connection` with no consumers; two `sink` Bindings writing
  the same `Dataset` bucket+prefix (object-key collisions); `observers`
  naming a provider whose owning chain can never consume it (today
  surfaced only at runtime as `LineageEndpointDeclaredNotConsumed`).

Blueprints avoid these by construction, but the owner's requirement is
explicit: hand-written manifests deserve the same guardrails, **without**
removing the freedom to do unusual things deliberately.

## Can this be done algorithmically and deterministically?

**Yes, entirely.** At validate time the manifest set is a closed, typed,
fully-resolved graph: `graph.Build` has resolved every reference
unambiguously, `compatibility.Check` has resolved every provider
implementation and capability, Binding option blocks are parsed
(`options.tables` etc. are plain data), and endpoint/security facts
(`Insecure`, schemes, audiences) are declared or derivable. Every lint
below is a pure function of `(envelopes, graph, provider implementations)`
— no live infrastructure, no time, no randomness. Determinism is
guaranteed by the same discipline as NFR-1: canonical iteration order,
findings sorted by `(severity, code, resource key)`, byte-identical output
for identical input. Plan-aware lints (below) additionally take recorded
state — still a deterministic input.

## Decision (design)

### 1. Two layers: lints detect, policies enforce

Lints (this ADR) **detect and report**; they never block by default.
Enforcement — "in this repo, lint DL102 is a hard failure" — belongs to
the policy layer (ADR 021), which consumes lint findings as facts. This
preserves the owner's rule: *once things can connect, the developer does
as they please* — but informed.

### 2. Severity and surface

- `platformctl lint [path]` — full report; `-o json` with stable finding
  codes; exit 0 always unless `--strict` (any warning+) or a policy says
  otherwise. `validate` gains a one-line summary ("12 resources valid; 3
  design findings — run `platformctl lint`") so findings are discoverable
  without a new habit.
- Severities: `warning` (probable mistake or operational hazard) and
  `info` (inert/unused/unconventional). No lint is ever `error` — errors
  are validation's and policy's vocabulary.
- Every code registers in the E4 explain catalog (`platformctl explain
  DL102`) with meaning, why-it's-a-hazard, and the remedies **including
  the intentional-use waiver** (below).

### 3. Waivers are first-class and auditable

`metadata.annotations["lint.datascape.io/waive"] = "DL102: reason"` —
per-resource, per-code, reason mandatory (empty reason = the waiver
itself is a warning). Waived findings still appear in `-o json` as
`waived: true`. This is the "do as they please" mechanism made auditable
rather than silent.

### 4. The built-in lint set (initial, all graph-derivable)

| Code | Severity | Finding |
|---|---|---|
| DL001 | warning | Duplicate capture: ≥2 `cdc` Bindings share a `sourceRef` with overlapping effective table sets (unset `tables` = "all", overlaps everything) |
| DL002 | warning | Sink collision: ≥2 Bindings target the same `Dataset` bucket+prefix (or same Source table set for `sink→Source`) |
| DL003 | warning | `observers` names a Provider but the owning resource's provider chain implements no `LineageAware` — the runtime no-op, surfaced at authoring time |
| DL004 | warning | Plaintext boundary: a Connection/endpoint that will serve non-loopback traffic with `Insecure: true` while a TLS-capable realization exists (post-C8) |
| DL010 | info | Orphaned `EventStream`: no Binding reads or writes it |
| DL011 | info | Unreferenced `Catalog`: no `catalogRef`/warehouse consumer and no Connection routes to it |
| DL012 | info | Unused `SecretReference` / `Connection` / `Provider`: nothing resolves it |
| DL013 | info | Dead-end pipeline: a `cdc` Binding whose EventStream has no downstream (no sink/ingest/consumer-facing Connection) — frequently intentional, hence info |
| DL014 | info | Single-replica data path where the HA field exists and the gate is enabled (brokers/workers/nodes = 1 with `HighAvailability` on) |
| DL020 | warning | `deletionPolicy` unset on a data-bearing kind (Dataset/Source) — the default is retain, but explicitness is the best practice |
| DL021 | warning | `protect` unset on a data-bearing kind in a set that also uses authoritative deletes (state has prior entries) — plan-aware |

### 5. Provider-contributed lints (the ADR 009 pattern)

Technology-specific hazards live with the technology: an optional
capability interface (`DesignLinter`, mirroring `SpecValidator`'s shape —
pure, no Request needed at validate) lets debezium contribute "N
connectors against one Postgres = N replication slots; consider one
connector with a wider table list", redpanda contribute "replication 1 on
a 3-broker cluster", s3sink contribute prefix-collision refinements. Same
determinism rules; codes namespaced `DL-<type>-NNN`.

### 6. What lint is NOT

- Not a simulator: it never predicts runtime behavior beyond what specs
  and provider knowledge state; anything requiring live infrastructure
  belongs to `drift`/`Probe`.
- Not style: no YAML formatting/naming-taste opinions.
- Not blocking: enforcement is exclusively ADR 021's.

## Consequences

- Blueprints gain a CI test: every shipped blueprint lints clean —
  keeping the "blueprints are the worked best practice" promise honest.
- The compatibility layer stays the single validate-time seam; lint runs
  beside it, reusing its resolved index (no second resolution pass).
- Adding a lint = one pure function + a catalog entry + tests; the
  explain/catalog completeness guards extend to lint codes.

## References

ADR 009 (capability pattern), ADR 011 (what validation is), ADR 021
(enforcement layer), E4 (explain catalog), doc 03 §7 (pairing relation).
