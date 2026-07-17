---
name: domain-ports-layering
description: Domain must not import adapters; ports must not import adapters. Enforce the core architectural invariant.
paths:
  - "internal/domain/**/*.go"
  - "internal/ports/**/*.go"
---

# Domain/Ports Layering

Files in `internal/domain` and `internal/ports` must never import anything under `internal/adapters`. This is the one invariant the architecture depends on.

**Pattern:** If you need a concrete implementation, define an interface in `ports` or a value type in `domain`, and let `internal/application/registry` wire the adapter in.

**Example violation:** `internal/ports/reconciler.Provider` importing `internal/adapters/runtime/docker` directly. Fix: import `internal/ports/runtime.ContainerRuntime` instead.

Check before committing: `grep -n "adapters" internal/domain/**/*.go internal/ports/**/*.go` returns nothing (except in comments).

This rule's scope is `internal/domain` and `internal/ports` only — production code in `internal/application` is held to the same standard, but its test files have a narrower, documented exception (which test-double adapters are allowed) recorded in CLAUDE.md's Layering section, not here.
