# ADR 019 — Naming: Datascape, platformctl, and the d7s mononym

**Status:** accepted (2026-07-22). **Prompted by:** project-owner direction
to audit naming consistency ahead of the repository eventually being
called by a single short name ("datascape" / "d7s"), and by the founding
decision in docs/planning/00-README.md ("CLI binary stays `platformctl`")
never having been extended into a full naming system.

## Context — what the audit found

Name usage today (tracked files): `Datascape` 104× (product voice, docs),
`platformctl` 1151× (binary, commands, repo name), lowercase `datascape`
1072× — almost entirely **identifier surfaces**, which are compatibility
contracts, not prose:

| Identifier surface | Value | Contract strength |
|---|---|---|
| Manifest API group | `datascape.io/v1alpha1` | **Frozen** — every user manifest carries it; changing it is a new apiVersion with a deprecation window (doc 03 §1), never a rename |
| Runtime object labels | `io.datascape.{managed-by,generation,namespace,kind,name,project,replica-base,replica-ordinal,...}` | **Frozen** — ownership/GC safety depends on them (ADR 013); live deployments carry them |
| Env variable families | `DATASCAPE_SECRET_*`, `DATASCAPE_SECRETS_DIR`, `DATASCAPE_VAULT_*` | **Frozen-with-deprecation** — user CI/dotenv files reference them |
| State directory | `.datascape/` | **Frozen-with-deprecation** — default path, overridable |
| Binary / CLI | `platformctl` | Kept per 00-README: renaming has real cost for zero design benefit |
| Kubernetes namespaces / container names | `datascape-*` conventions, `platformctl` ServiceAccount | Deployment-visible; migration = destroy/recreate |

No occurrence of `d7s` or `d7e` exists today (two `d7e` greps are
coincidental worktree-hash substrings).

## Decision — a three-tier naming system

1. **Datascape** (capitalized) is the **product**: use it when naming the
   system, its design, its guarantees ("Datascape reconciles…",
   "Datascape's resource model"). First mention in any document:
   **"Datascape (`platformctl`)"**.
2. **`platformctl`** (code-formatted, lowercase) is the **binary and CLI**:
   use it for anything a user types or a process runs. Never write
   "Datascape apply"; never write "platformctl's design philosophy".
3. **`datascape`** (lowercase, unformatted-in-identifiers) is the
   **identifier stem** — API group, labels, env prefixes, default paths,
   object-name conventions. These are contracts: they change only through
   versioned migrations (doc 03 §1 for apiVersion; docs/upgrade-notes.md
   entries otherwise), **never** as part of a branding pass.

**The mononym is `d7s`** — the project owner's chosen short name for the
eventual repository/product handle, in the k8s/i18n abbreviation family.
Recorded fact: the strict numeronym of "datascape" is `d7e`
(d‑atascap‑e); `d7s` is a deliberate stylistic choice, not a derivation —
short names are chosen for pronounceability and register, and the owner
chose `d7s`. It is a **brand alias, not an identifier**: it may appear in
prose, badges, and the repo name, but MUST NOT be introduced into API
groups, labels, env prefixes, schemas, or code identifiers (those stay on
the `datascape` stem regardless of any repo rename).

## Adoption staging (cheapest-first, contracts last)

1. **Now:** this ADR; docs follow the three-tier rules; the README may
   introduce "Datascape (`platformctl`), or **d7s** for short" once the
   owner wants the alias public. No other action.
2. **Repo rename** (owner's call, any time): `platformctl` →
   `datascape`/`d7s` is cheap — GitHub redirects old URLs; the Go module
   path `github.com/rezarajan/platformctl` can stay (module paths are
   identifiers, tier 3) or move in a dedicated major-version change, not
   as a side effect.
3. **Never (without a migration design):** apiVersion group, label keys,
   env prefixes, state paths. A future rename of these would be its own
   ADR with a versioned migration per surface — this ADR's contribution
   is making that boundary explicit so no branding pass ever crosses it
   casually.

## Consequences

- Writers and agents have a deterministic rule: product → Datascape,
  command → `platformctl`, wire/disk/env → `datascape`, brand-short →
  d7s (prose only).
- The identifier freeze list above is the review checklist for any PR
  that touches naming.
- docs/README.md's navigation router carries the rule one line deep so
  nobody needs this ADR for day-to-day writing.

## References

docs/planning/00-README.md (binary-name decision); docs/planning/03 §1
(apiVersion maturity/deprecation); ADR 013 (label-dependent safety);
docs/upgrade-notes.md (the migration-note convention).
