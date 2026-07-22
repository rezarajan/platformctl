# platformctl and Terraform

This page compares platformctl to Terraform honestly — where the comparison is real and
deliberate, where the two tools diverge in kind rather than degree, when you should reach for
Terraform instead, and where the two are meant to be used together, today. It closes with ideas
under consideration for tighter integration; none of them are scheduled work.

## Same family

Declarative desired state, `plan`/`apply`, a state file, drift detection. This isn't a surface
resemblance — platformctl deliberately borrows Terraform's **authoritative-apply** and **state**
conventions (docs/adr/012-determinism-and-state.md): a resource present in state but absent from
manifests plans as `delete` on the next apply, exactly like Terraform; `plan` is computed from
manifests plus recorded state only, never a live probe, so identical inputs yield byte-identical
plans; state persists after every resource so a crash mid-apply leaves it truthful. If you've
used Terraform, the `validate → plan → apply → drift` loop will feel immediately familiar — that
familiarity was a design goal, not an accident.

## The core difference: where reconciliation stops

Terraform provisions resources through cloud/provider APIs and stops at resource CRUD — a VM
exists, a database instance exists, an S3 bucket exists. platformctl is a **domain control
plane**: it continues past creation into application-level reconciliation and operation of the
data infrastructure itself. Concretely, in this codebase today:

- **Creating topics inside a broker it just started** (the `redpanda` provider, `EventStream`
  reconciliation) — not "a Redpanda container exists," but "this specific topic, with this
  partition count and retention, exists inside it."
- **Enabling logical replication and provisioning replication users inside a database** (the
  `postgres`/`mysql` providers) — a database server existing and a database configured for CDC
  are different facts; platformctl reconciles the second.
- **Registering and health-verifying Kafka Connect connectors** (the `debezium`/`s3sink`
  providers) — a running Connect worker and a registered, `RUNNING` connector are different
  facts.
- **Checking that a connector's live config still matches the manifest.** Drift here means
  `wal_level changed` or `connector config drifted`, not just "the VM is gone." `platformctl
  drift` probes the actual running configuration, not merely instance existence.
- **Rotating credentials inside running systems** (`internal/adapters/providers/providerkit`'s
  credential rotation mechanics, shared across providers).
- **Backup/restore of data itself** (`platformctl backup`/`restore`, `BackupCapableProvider`,
  docs/adr/007-backup-restore.md) — streaming a data-bearing resource's actual contents, not a
  snapshot API call against a cloud resource.

Terraform has no native notion of any of this. You'd compose many narrow, unrelated Terraform
providers (one for the VM, a different one — if it exists at all — for the application inside
it) plus custom scripts (`local-exec`, Ansible, a CI step), with no unified health/drift/heal
semantics across the seam between "the container exists" and "the thing inside it is configured
and healthy."

## Second difference: runtime portability

One manifest reconciles to Docker locally and Kubernetes in staging with **zero manifest
changes** — only `spec.runtime.type` differs, and it's a Provider-level field, not a rewrite.
This provider/runtime split is the architecture's central bet (docs/planning/02-architecture.md
§1–2): a `Provider` knows technology semantics (what a topic is, what a replication slot is); a
runtime adapter knows execution mechanics (how to run a container) and nothing about topics or
slots. The bet is proven, not aspirational — the Kubernetes runtime adapter
(`internal/adapters/runtime/kubernetes`) shipped without a single existing provider needing to
change (docs/planning/07-production-grade-docker-runtime-gap-analysis.md's Cross-Runtime
Portability section records the few genuine port-boundary fixes that surfaced, e.g. teaching
`VolumeSpec` about Kubernetes' namespace-scoped PVCs — none of them provider-side).

Terraform's answer to "the same stack, locally, for development" is comparatively weak: cloud
provider blocks don't have a meaningful local-Docker equivalent, and `docker` isn't a first-class
Terraform provisioning target in the way it is platformctl's default. platformctl is
**Docker-first by design** (docs/planning/04-roadmap-and-feature-gates.md §1: "Docker validates
the resource model cheaply before any second runtime is attempted") — the opposite starting
point from Terraform's cloud-API-first posture.

## Third difference: typed domain model with validate-time completeness

A `Binding` to a provider that can't do CDC — say a `cdc`-mode Binding against a provider that
doesn't support your database's engine — fails at `validate`, before any state exists or any
infrastructure is touched, with a precise capability error naming exactly what's missing
(docs/adr/009-capability-interfaces.md, docs/adr/011-validate-time-completeness.md):

```
error: Binding "student-db-to-events": Provider "postgres-cdc" (type: debezium)
does not support source engine "sqlite" (supported: postgres, mysql, mongodb)
```

`validate` is a completeness gate: a manifest set that validates cannot half-apply into a
mis-wired platform. Terraform's equivalent errors — a provider rejecting an unsupported
combination of arguments, a resource type that doesn't support a given attribute — mostly surface
at `apply`, from the provider's own API response, if they surface in a structured way at all;
Terraform's type system doesn't model cross-resource capability compatibility the way
platformctl's `AllowedKindPairs` + capability-interface check does.

## When to use Terraform instead

Be honest about the boundary: platformctl is deliberately **not** a general-purpose provisioner
(docs/planning/01-product-requirements.md NG2 — "does not aim to replace Docker Compose,
Kubernetes, or Nomad for arbitrary workloads") and has no multi-tenant control plane (NG3 — "no
multi-tenant control plane, RBAC, or a hosted service"). Reach for Terraform for:

- **Cloud foundation** — VPCs, IAM, DNS, managed services (RDS, MSK, the S3 *bucket-as-AWS-
  resource*, as opposed to what's inside it).
- **Organization-wide standardization** — shared modules, a private registry, policy-as-code
  across many teams and many kinds of infrastructure, not just data-platform resources.
- **The provider ecosystem** — thousands of narrow providers covering nearly every cloud API;
  platformctl's provider set is intentionally small and data-platform-specific.
- **Multi-team modules and registries** — platformctl is a single-operator/single-CI-pipeline
  CLI in this version (NG3), not a shared, versioned module-registry ecosystem.

## Use them together — today

This isn't a future integration; it's the shipped, CI-exercised pattern already in this
repository. Terraform provisions the cloud primitives; platformctl consumes them as
**external-lifecycle** resources through the **`Connection`** seam: `external: true` +
`connectionRef` (docs/planning/03-resource-model-reference.md §3.3). Terraform owns "the RDS
instance exists" (docs/adr/005-database-ha-posture.md's documented path for production HA
databases); platformctl owns "CDC off it is running, healthy, and its connector config hasn't
drifted." The same seam covers object storage — a real S3 bucket Terraform provisioned becomes
an `external: true` `Provider(type: s3)`, reached through a `Connection`, that platformctl's
`Dataset`s and `s3sink` Bindings write into and manage lifecycle rules against
(docs/planning/08 C4's object-store production posture).

In short: Terraform's outer loop stands the cloud foundation up once; platformctl's inner loop
keeps the data infrastructure riding on top of it healthy, configured, and drift-free on every
subsequent run.

## Future integration ideas (not scheduled work)

These are ideas under consideration, consistent with the roadmap's existing reservation for a
Terraform runtime in Phase 8 (docs/planning/04-roadmap-and-feature-gates.md §11) — today,
`registry.PlannedRuntimes` already accepts `runtime.type: terraform` in schema for forward
compatibility and rejects it at startup as "planned but not yet available," not silently ignored
(`internal/application/registry/registry.go`). None of the following are tasks on doc 08; they
are not scheduled, sized, or committed to.

1. **A Terraform runtime adapter** — `Provider(runtime.type: terraform)` realizing resources by
   generating and applying Terraform for cloud-managed equivalents, so the same manifest that
   runs MinIO locally on Docker provisions real S3 in production, without changing the resource
   model.
2. **A platformctl Terraform provider** — the inverse: a `terraform` resource that applies a
   platformctl manifest set, for organizations whose outer loop is Terraform and want the data
   platform reconciled as one more resource in that graph.
3. **A Terraform-state bridge** — importing Terraform outputs/state as `Connection` facts, so the
   external-lifecycle wiring documented above is generated from Terraform's own state rather than
   hand-written into a `Connection` manifest.

## Comparison table

| | Terraform | platformctl |
|---|---|---|
| **Scope** | General-purpose cloud/infrastructure provisioning across almost any provider API. | Data-platform resources only (brokers, CDC, object storage, catalogs, lineage wiring) — deliberately narrow (NG2). |
| **Reconciliation depth** | Stops at resource CRUD via provider APIs. | Continues past creation into application-level configuration: topics, replication slots, connector registration/health, credential rotation, backup/restore. |
| **Drift semantics** | Typically "does the resource still exist / match its top-level attributes." | Includes deep, technology-specific drift: `wal_level` changed, a connector's live config diverged from its manifest, a lifecycle rule was edited out-of-band. |
| **Local dev** | Weak — most providers are cloud-API-only; no first-class local-equivalent story. | Docker-first by design; the same manifest that runs locally runs against Kubernetes with only `spec.runtime.type` changed. |
| **Runtimes** | Provider-specific; no unified "runtime" abstraction. | Explicit provider/runtime split — Docker (GA) and Kubernetes (Beta) today, Terraform/external reserved (not yet available). |
| **Type/validation model** | HCL type system; many capability mismatches surface at `apply`, from the provider. | JSON-Schema-typed kinds plus capability interfaces checked at `validate`, before any state exists (ADR 009/011). |
| **State** | HCL state file, remote backends, locking — the model platformctl's own state/lock design (ADR 003/012) deliberately mirrors. | Local file by default; S3-compatible shared backend with lease locking (gated, ADR 003) — same conventions, smaller ecosystem of backends. |
| **Ecosystem** | Thousands of providers, modules, a public registry, large community. | A small, hand-maintained provider set (see the README's provider table), no module registry. |
| **Maturity** | Mature, widely production-proven, industry standard for cloud provisioning. | Young — single static binary, no multi-tenant control plane (NG3), data-platform-only; some capabilities (backup/restore, monitoring, ingress) are still Alpha and off by default. |

## See also

- [docs/adr/012-determinism-and-state.md](../adr/012-determinism-and-state.md) — the
  authoritative-apply/state design this page's "same family" section draws from.
- [docs/adr/005-database-ha-posture.md](../adr/005-database-ha-posture.md) — the concrete
  Terraform-owns-the-instance / platformctl-owns-CDC boundary for production databases.
- [docs/planning/01-product-requirements.md](../planning/01-product-requirements.md) — the full
  goals/non-goals list this page's "when to use Terraform instead" section summarizes.
- [docs/planning/04-roadmap-and-feature-gates.md](../planning/04-roadmap-and-feature-gates.md)
  §11 — the existing, unscheduled Phase 8 reservation for a Terraform runtime adapter.
