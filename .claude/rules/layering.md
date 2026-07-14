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
