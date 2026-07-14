---
name: schema-doc-sync
description: Schema changes require matching updates to the resource model reference.
paths:
  - "schemas/**"
---

# Schema Changes

Every file under `schemas/` corresponds to a Kind section in `docs/planning/03-resource-model-reference.md`. Adding or changing a field here without updating that doc in the same commit is incomplete work.

**Rule:** If you edit `schemas/`, you must also edit `docs/planning/03-resource-model-reference.md` to keep them in sync. Same commit, same PR.

**Why:** The planning docs are the contract other subsystems (domain validation, compatibility rules, provider interfaces) are checked against. Drifting docs break that guarantee.

**Example:** Adding a `--parallelism` field to the S3 provider schema requires updating the S3 section of `03-resource-model-reference.md` with the new field's type and purpose.

Check: `grep -n "schemas/.*\.json"` in the commit message should have a matching reference-doc edit.
