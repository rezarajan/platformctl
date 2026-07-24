# ADR 036 — Runtime-agnostic storage requests: size and performance tier as a provider need, class as a runtime concern

**Status:** proposed (2026-07-24). **Prompted by:** bringing
`examples/zero-trust-lakehouse` up on Kubernetes (the M7 K8s leg). Every
stateful Provider's persistent volume landed at the cluster's default —
**10Gi on the default StorageClass** — because there is no way to request
otherwise. In production a Redpanda broker (or MinIO node, or Postgres
instance) must be able to ask for a specific *amount* of storage and a
specific *performance tier* (e.g. an SSD-backed class), while the concrete
mechanism for delivering that tier stays outside the Provider. This ADR
records the gap and the intended shape; it is **not yet implemented**.

## The gap today

- `internal/ports/runtime.VolumeSpec` carries `SizeBytes`, but the
  `StableIdentity` path (multi-broker Redpanda, multi-node MinIO, and every
  other ordinal set — ADR 004) never calls `EnsureVolume`. The Kubernetes
  adapter manufactures the StatefulSet `volumeClaimTemplates` itself, sized
  from a hardcoded `defaultVolumeSizeBytes` (10Gi) with **no**
  `storageClassName` — so it always binds the cluster default class.
- There is no field, anywhere, for a **StorageClass** / performance tier.
  `ContainerSpec`/`VolumeMount` carry no per-volume size or class metadata
  (ADR 004 §Follow-ups already flagged this).
- Net effect: `brokers: 3` on a cluster whose default class is slow
  network storage gives three brokers on slow 10Gi volumes, with no
  declarative way to say "each broker wants 200Gi of fast local SSD."

## The intended shape (to be decided/implemented)

Model storage exactly as `resources` (cpu/memory) is modelled — the
pattern the M3 resource defaults and the M7 partial-runtime-override
(ADR 035, docs/planning/08) already established:

1. **The Provider expresses an abstract NEED, not a mechanism.** A stateful
   Provider declares how much durable storage it wants and, optionally, a
   *performance tier* as an abstract hint (e.g. `standard` | `fast` |
   `archive`) — never a concrete `storageClassName`, a Docker volume
   driver, or a CSI parameter. This keeps the Provider runtime-neutral
   (the one architectural invariant): `internal/domain`/provider code must
   not name a Kubernetes StorageClass any more than it names a Docker
   network driver.

2. **Sensible per-provider defaults.** Each stateful Provider type has a
   default storage size (a table like `defaultResourcesForProvider`, M3),
   so the zero-ceremony path still works — omitting storage yields a
   reasonable size, not 10Gi-for-everything-by-accident.

3. **The runtime supplies the mechanism.** The concrete mapping from the
   abstract tier to a real backend is a **runtime** concern, declared on
   the runtime (project-level `spec.runtime`, or a per-Provider partial
   override — ADR 035 / M7), and applied by the runtime adapter:
   - **Kubernetes:** tier → `storageClassName` (a cluster-provided map,
     e.g. `fast` → `local-ssd`), plus `resources.requests.storage` = size,
     on the PVC / `volumeClaimTemplate`. The CSI driver behind that class
     is what actually delivers SSD-vs-HDD, replication, IOPS, etc. — its
     semantics, not ours.
   - **Docker:** size is best-effort (Docker volumes are unsized on most
     drivers); tier maps to a volume driver / driver-opts where one is
     configured, else is a documented no-op.
   - **Fake:** ignored.

   So the plane manifests stay portable (a Provider says `fast`, `200Gi`),
   and only the runtime declaration differs per environment — the same
   split that keeps `resources` portable across Docker and Kubernetes.

4. **Wiring points.** `VolumeSpec` gains a `StorageClass`/tier field and
   the `StableIdentity` volumeClaimTemplate path must consult it instead of
   the hardcoded default; `EnsureVolume` and the by-hand ordinal PVC/volume
   creation converge on the same sizing/tier source; a new schema field
   under `spec.runtime` (Kubernetes) carries the tier→class map, and an
   optional per-Provider storage request lives alongside its `resources`.

## Consequences

- **Not a behavior change until implemented.** Recorded so the 10Gi/default
  -class limitation is a known, documented follow-up rather than a silent
  surprise in production. The M7 Kubernetes example runs on default
  storage and says so.
- **Preserves the layering invariant.** Providers never learn what a
  StorageClass is; the runtime adapter owns the translation, exactly as it
  owns network/PVC/namespace mechanics today.
- **Backup/restore & data-bearing protection** (docs/planning/08 A5/C6,
  ADR 004's retain-on-delete default) are unaffected — this is about
  provisioning size/tier, not reclaim policy.

## Follow-up

Sequence a task in docs/planning to: add the tier field to `VolumeSpec` +
the runtime schema (with a Kubernetes tier→class map), a per-Provider
storage-request field + defaults table, route the `StableIdentity`
volumeClaimTemplate through it, and cover it in the runtime contract test
suite (a requested size/class must reach the PVC). Until then, operators
override by pre-creating PVCs or setting a cluster default StorageClass +
capacity that suits the workload.
