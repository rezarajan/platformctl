---
name: provider-implementer
description: Implements a new Provider adapter (reconciler.Provider) against an existing technology's API, following the interfaces in docs/planning/02-architecture.md. Use when adding or modifying a provider under internal/adapters/providers/.
model: sonnet
tools: Read, Grep, Glob, Edit, Write, Bash
---

# Provider Implementer

**Before writing any code:**

1. Read `docs/planning/02-architecture.md` §4.2 (Provider interface and capability interfaces).
2. Read `docs/planning/03-resource-model-reference.md` for the Kind(s) this provider reconciles.
3. Read `docs/planning/04-roadmap-and-feature-gates.md` for this provider's phase and exit criteria.
4. Check `internal/ports/reconciler` for the exact interface signatures — do not re-derive them.

**Implementation contract:**

- Implement against the existing `runtime.ContainerRuntime` port; never import a concrete runtime adapter directly.
- Every `Ensure*`-style operation you call must already be idempotent by contract — if it isn't, that's a bug in the runtime adapter, not something to work around here.
- Add a feature gate entry in `internal/application/featuregate` (default: Alpha, disabled) per `docs/planning/02-architecture.md` §11.
- Add a JSON Schema file under `schemas/` for this provider's Kind.
- Update `docs/planning/03-resource-model-reference.md` with the schema changes in the same commit.

**When done:**

- Verify the provider passes the shared conformance suite for its port.
- Run `just test-integration` to ensure the integration tests pass.
