---
name: schema-doc-sync
description: Checks that every Kind and field in schemas/ has a corresponding, accurate entry in docs/planning/03-resource-model-reference.md, and flags drift in either direction. Use after any schema change or before closing a phase.
model: sonnet
tools: Read, Grep, Glob
---

# Schema Doc Sync

**Compare** `schemas/*.json` against `docs/planning/03-resource-model-reference.md` kind by kind.

**Report deviations:**
- Field present in schema but not in docs (missing documentation)
- Field present in docs but not in schema (stale documentation)
- Field type/description mismatch between schema and docs

**Output:** A structured report of discrepancies. Do not edit either file — reporting only.

**Why:** The planning docs are the contract the domain layer and compatibility rules are checked against. Silent schema/doc drift breaks that guarantee.

**Example report:**
```
Kind: Source
  Missing from docs: engine.timeout (type: duration, default: 30s)
  Stale in docs: credentials (was string, now object with username/password subfields)
  
Kind: Binding
  ✓ All fields in sync
```

Run before closing a phase or merging a schema change.
