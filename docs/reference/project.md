# Project

`datascape.io/v1alpha1`

The project root config (docs/adr/035 decision 1, docs/planning/08 M1): a lightweight document, conventionally named datascape.yaml and placed at the manifest path's root, loaded BEFORE the manifest set — not one of the governed resource kinds (it is never returned by a manifest load, never appears in the graph/plan/apply resource list, exactly like Policy is a sibling reference rather than a governed-set kind). Declares the ONE runtime every Provider in the project targets (the Go-module shape): a Provider omitting its own spec.runtime inherits this one; a Provider that DOES declare spec.runtime is an explicit override, validated to match this runtime's type or refused ('a project targets one runtime — put it in its own project folder').

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.runtime` | object | yes | The project's one runtime. Same shape as a Provider's own spec.runtime (docs/planning/03 §4) — type plus runtime-specific fields (network/access/resources/etc.) — copied verbatim onto every Provider in the project that omits its own spec.runtime. |
| `spec.runtime.access` | `port-forward` \| `node-port` \| `load-balancer` \| `in-cluster` | no | kubernetes only; see provider.json's identical field. |
| `spec.runtime.network` | string | no | The shared addressing/isolation domain every Provider in the project joins. docker: the network name (default: datascape). kubernetes: the Namespace name. |
| `spec.runtime.networkPolicy` | `none` | no | kubernetes only; see provider.json's identical field for the full default-deny NetworkPolicy behavior this opts out of. |
| `spec.runtime.resources` | object | no | Optional resource bounds (docs/planning/08 J5) applied to every long-running container across the project's Providers that don't set their own spec.runtime.resources override. |
| `spec.runtime.resources.cpu` | number | no |  |
| `spec.runtime.resources.cpuReservation` | number | no |  |
| `spec.runtime.resources.memory` | string | no |  |
| `spec.runtime.resources.memoryReservation` | string | no |  |
| `spec.runtime.type` | `docker` \| `fake` \| `kubernetes` \| `external` \| `terraform` | yes | Mirrors provider.json's spec.runtime.type enum exactly — docker and fake (testing) are implemented; kubernetes is a real, Beta adapter behind the KubernetesRuntime feature gate; external/terraform are accepted for forward compatibility and rejected at registry construction. |
| `spec.zeroTrust` | boolean | no | docs/adr/035 decision 3. Defaults to true. M1 parses and stores this field only — no engine behavior reads it yet; docs/planning/08 M4 wires the ZeroTrust default-on behavior from it. |
