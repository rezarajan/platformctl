# F-005: `provider.json` describes `runtime.network` as docker-specific; the Kubernetes adapter consumes it as the Namespace

**Severity:** Low (documentation contract drift inside the schema itself).
**Status:** Confirmed at `ae99505`.

## Evidence

`schemas/v1alpha1/provider.json`:

```json
"network": { "type": "string", "description": "docker-specific: the shared network name (default: datascape)." }
```

But `internal/adapters/runtime/kubernetes/kubernetes.go` maps
`EnsureNetwork(spec.Name)` → a Kubernetes **Namespace**, and every provider
passes `runtime.network` as that name (`p.network()` helpers). The planning
docs were already corrected (`docs/planning/03-resource-model-reference.md`
says "docker: the shared network name. kubernetes: the Namespace name");
the schema description — the source the reference docs generate from — was
not.

## Root cause

The Phase 7 Kubernetes work updated `runtime.type`'s description in
`provider.json` but missed the sibling `network` description.

## Required behavior

Update the `network` property description in
`schemas/v1alpha1/provider.json` to:

> "The shared addressing/isolation domain the provider's objects join.
> docker: the network name (default: datascape). kubernetes: the Namespace
> name (EnsureNetwork creates it; must not collide with an existing
> unmanaged namespace — see the runtime adapter's ownership policy)."

Then regenerate `docs/reference/` (F-002's task picks this up — coordinate).

## Exact files and symbols

- `schemas/v1alpha1/provider.json`: `properties.spec.properties.runtime.properties.network.description`.
- Same-commit doc rule: `docs/planning/03-resource-model-reference.md`
  already carries the corrected wording — verify no further edit needed
  there (the schema-change rule requires checking, not necessarily
  changing).

## Implementation constraints

- Description-only change: no shape, type, or required-list changes.
- Preserve the file's compact one-line JSON style for this property (match
  surrounding formatting; do not reformat the file).

## Tests / validation commands

```
go test ./internal/application/manifest/   # schema still loads/validates
go run ./cmd/platformctl validate examples/lakehouse/
python3 -c "import json; json.load(open('schemas/v1alpha1/provider.json'))"
```

## Dependencies / ordering

Land before or with F-002 (reference regeneration).

## Risk

Minimal — descriptions are not validated content.

## Escalation conditions

None.
