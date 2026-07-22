# User onboarding

You run `platformctl`, describe a data platform in YAML, and it exists — locally on Docker
today, on a real Kubernetes cluster tomorrow, with the same manifests. This page is the guided
path through the operator-facing docs; it does not duplicate them, it tells you which one to
read next and shows the commands you'll actually type.

## What platformctl is

Datascape (`platformctl`) is a **control plane for data infrastructure**: a typed resource
model, a deterministic `plan`, an idempotent `apply`, and drift-aware `status` — the same shape
Kubernetes gives you for workloads, applied instead to databases, event streams, CDC connectors,
object storage, and the wiring between them. It stops exactly where the infrastructure is
"exists, healthy, and configured" — what runs on top of that infrastructure (dbt models, Airflow
DAGs, Spark jobs) is someone else's job (docs/planning/01-product-requirements.md §1).

## Install & your first pipeline

Follow the [README quickstart](../../README.md#quickstart) for prerequisites (Go 1.22+, a
running Docker daemon) and the `just build` step — this page doesn't repeat that. Once you have
a `platformctl` binary, the fastest way to a running pipeline is two commands:

```sh
platformctl init cdc-to-lake              # scaffold Postgres → Debezium → Redpanda → S3 sink → MinIO
platformctl validate cdc-to-lake          # green immediately, no edits
```

`init` writes a manifest set that already validates as-is, a `.env` template naming every secret
key it needs, and a blueprint-specific README explaining what you just scaffolded. Fill in
`.env`, then `apply` it — see the README quickstart for the one-time sink-image build step and
the full walkthrough from there (insert a row, watch it land in the lake). `init --list` shows
every shipped blueprint:

```console
$ platformctl init --list
cdc-to-lake      Postgres change data capture through Debezium and Redpanda, landing as objects in MinIO.
lakehouse        cdc-to-lake plus a Nessie catalog, an OpenLineage (Marquez) lineage backend, and a Connection-fronted external CDC source.
stream-basics    A Redpanda broker with a couple of EventStream topics; no databases or sinks.
external-cdc     An external database reached through a managed Connection, captured by a managed Debezium worker into Redpanda.
```

## The mental model

### Kinds

Everything is one of eight `Kind`s. Each has a generated field-by-field reference under
`docs/reference/<kind>.md` (built from `schemas/` — never hand-edited).

| Kind | What it is | Reference |
|---|---|---|
| `Provider` | A technology (`type`) and where it runs (`spec.runtime`) — redpanda, postgres, debezium, s3/minio, mysql, nessie, and more. | [docs/reference/provider.md](../reference/provider.md) |
| `Source` | A data origin — an engine-backed database asset (`spec.engine: postgres\|mysql\|...`). | [docs/reference/source.md](../reference/source.md) |
| `EventStream` | A Kafka-style topic/stream resource. | [docs/reference/eventstream.md](../reference/eventstream.md) |
| `Binding` | A directed data-movement edge between two resources, realized by a `Provider`. | [docs/reference/binding.md](../reference/binding.md) |
| `Dataset` | An object-store bucket/prefix location, with an output format (json/parquet/csv/jsonl). | [docs/reference/dataset.md](../reference/dataset.md) |
| `Catalog` | A table/metadata catalog (Iceberg REST today via Nessie) as a provider-agnostic noun. | [docs/reference/catalog.md](../reference/catalog.md) |
| `Connection` | A stable, non-secret "how to reach a system" record — address here, credentials in the `SecretReference` it names. | [docs/reference/connection.md](../reference/connection.md) |
| `SecretReference` | Names a secret and which keys to resolve from a backend — never a value. | [docs/reference/secretreference.md](../reference/secretreference.md) |

### The three lifecycles

Every resource is **Managed**, **External**, or **Imported**. The condensed test
(docs/planning/03-resource-model-reference.md §3.1): **who should own the resource going
forward?**

- **Managed** (default, no marker): platformctl creates it, updates it on spec change, deletes
  it on `destroy`.
- **External** (`spec.external: true` + a `connectionRef`): operated by someone else,
  permanently — the production database another team runs. platformctl never creates or deletes
  it, but may still configure *against* it (e.g. register a CDC connector) and always health-
  checks it as "reachable," not merely "configured." `destroy` never touches it without
  `--include-external` and the matching destructive-action flag.
- **Imported** (`platformctl import <Kind>/<name> --from <name>`): created out-of-band, but it
  *should* be platform-owned going forward — a Postgres you `docker run` last month. Adoption
  probes it once, then it behaves like Managed.

### Bindings are directed edges

Asset kinds (`Source`, `EventStream`, `Dataset`, ...) are role-neutral — a `Source` doesn't know
whether it's an origin or a destination. Direction lives entirely in the `Binding`: its
`spec.mode` names the movement mechanism (`cdc`, `sink`, `ingest`, ...) and its `sourceRef`/
`targetRef` name the two ends, admitting a *set* of legal Kind pairings per mode rather than one
fixed pair — so a new pairing (database-as-sink, object-store-as-source) is additive, never a
breaking schema change (docs/adr/001-bindings-are-directed-edges.md). The realizing `Provider`
must declare the capability matching that pairing, checked at `validate` — see Troubleshooting
below for what that failure looks like.

## Daily workflow

Six verbs cover the loop: **validate → plan → apply → status → drift → heal** (heal is just
`apply` again). Every command has a deterministic exit code so CI can branch on it without
parsing text (docs/planning/02-architecture.md §8):

| Exit code | Meaning |
|---|---|
| `0` | Success (or, for `apply`/`destroy`, nothing left to do). |
| `1` | `plan`/`drift` found changes/drift (`--detect-drift-only` on `plan` forces exit `0` even then). |
| `2` | Execution error. |
| `3` | Validation error. |
| `4` | State lock held by another operator. |

```sh
platformctl validate ./platform/                 # schema + graph + capability checks; no state, no runtime calls
platformctl plan ./platform/                     # deterministic diff of manifests vs. state; never mutates
platformctl apply ./platform/ --auto-approve      # reconcile in dependency order; state persisted after every resource
platformctl status ./platform/                    # per-resource Ready/DRIFT/conditions from recorded state
platformctl drift ./platform/                     # probe live infrastructure, record what's found; exit 1 if drifted
platformctl apply ./platform/ --auto-approve      # "heal": re-apply converges drifted resources back to spec
```

Beyond the core loop:

- **`inventory`** (aka `services`/`endpoints`) — list every applied component's reachable
  endpoint and which `SecretReference` holds its credentials; `--for psql|s3|kafka|trino|...`
  renders a paste-ready config snippet instead of raw endpoints. Use it right after `apply` to
  find the host port MinIO or Postgres landed on: `platformctl inventory ./platform/`.
- **`graph`** — render the platform's architecture (data-flow pipelines, not the internal
  reconcile order) as a tree, `dot`, or `mermaid` diagram; useful for a README or a design
  review: `platformctl graph ./platform/ --format mermaid`.
- **diagnostic help** — `platformctl explain <reason>` (paste any reason from `status`/`drift` output verbatim — dynamic ones like `ConnectorStateFAILED` resolve by prefix) is an in-progress diagnostic-catalog command for
  turning an error or a topic into a fuller explanation; it is not in every build yet, so run
  `platformctl --help` first to see whether your binary has it before relying on it.
- **`backup`/`restore`** (Alpha, `BackupRestore` gate) — stream a data-bearing resource
  (postgres, mysql, s3 in v1) to or from an object-store destination:
  `platformctl backup Source/orders --to Dataset/backups`.
- **`gc`** — `gc plan` lists every labeled runtime object (container/network/volume) that no
  state entry accounts for — e.g. left behind by a crash before state was written; `gc apply
  --yes-i-understand-this-is-destructive` removes exactly that list. Unlabeled objects are never
  touched: `platformctl gc plan ./platform/`.
- **`state doctor`** — reports state defects (stale on-disk format, orphaned legacy entries,
  Provider entries whose backing container is gone) without changing anything; `state repair`
  applies the safe fixes: `platformctl state doctor`.

## Secrets

Secret **values never appear in a manifest** — a `SecretReference` names a secret and which keys
to resolve; the schema makes a plaintext value unrepresentable. Four backends
(`spec.backend`): `env` (reads `DATASCAPE_SECRET_<NAME>_<KEY>` from the process environment),
`file`, `vault` (gated, `VaultSecretBackend`), and `kubernetes` (reads a cluster `Secret`,
enabled by default alongside the Kubernetes runtime). `--env-file path/to/.env` loads
`KEY=VALUE` lines into the environment before any secret resolves — an already-exported shell
variable always wins over the file. Before `apply` touches any infrastructure, a **Preflight**
check resolves every declared `SecretReference` and aggregates every failure into one message —
never an opaque mid-apply failure (see Troubleshooting).

## Runtimes

`spec.runtime.type` on a `Provider` picks where it runs — the same manifest works against either:

- **`docker`** (default, GA) — the local/single-node target; no extra config needed beyond a
  running daemon.
- **`kubernetes`** (Beta, enabled by default) — reconciles against a real cluster using standard
  kubeconfig loading (`config["kubeconfig"]`/`config["context"]` override). `spec.runtime.access`
  (`port-forward` default | `node-port` | `load-balancer` | `in-cluster`) controls how
  platformctl itself, running outside the cluster, reaches a Provider's admin port to reconcile
  child resources. RBAC: see [`deploy/kubernetes/rbac/README.md`](../../deploy/kubernetes/rbac/README.md)
  for the minimal Role (exactly the verbs the adapter uses) and the cluster-admin dev shortcut;
  `validate`/`plan` preflight connectivity and every required permission before any mutating
  call, naming exactly what's missing.

`external` and `terraform` are accepted by the schema for forward compatibility but rejected at
startup as "planned but not yet available" — not silently ignored (see
[docs/positioning/terraform.md](../positioning/terraform.md) for what that reservation is about).

## Feature gates

Every provider and behavior beyond the GA core ships behind a named gate, staged **Alpha**
(shape may change between minor releases; defaults *disabled*) → **Beta** (shape stable,
behavior may still change; defaults *enabled*) → **GA** (stable; changes follow a deprecation
window) — docs/planning/03-resource-model-reference.md §1,
docs/adr/014-feature-gate-strategy.md. With a gate off, there is zero behavior change for
manifests that don't opt into it. Enable one with the global `--feature-gates` flag:

```sh
platformctl apply ./platform/ --feature-gates=TrinoProvider=true,IngressProvider=true
```

Multiple `Name=true|false` pairs are comma-separated. A manifest that uses a disabled feature
(a `Provider` of a gated type, `spec.external: true`, a schema-carrying Binding format, ...)
fails at `validate`, not partway through `apply` — see Troubleshooting.

## Governance: lints vs policies

platformctl has two distinct guardrail layers — don't confuse them, because they answer
different questions and one of them a team can override, the other it can't.

| | **Lints** (`platformctl lint`, docs/adr/020) | **Policies** (`platformctl policy`, docs/adr/021) |
|---|---|---|
| Question answered | "Is this design-quality clean?" | "Is this *allowed* here?" |
| Nature | **Advisory** — findings, not gates | **Governed** — organizational rule, deny-wins |
| Source | Built into every `validate` (DL001–DL021 + provider-contributed codes) | A separate, explicit channel: `--policies <dir>` or the conventional `.datascape/policies/` |
| Override | `lint.datascape.io/waive: "DLxxx: <reason>"` — any resource, any finding, "do as they please, but informed" | `policy.datascape.io/exempt: "<rule-id>: <reason>"` — **only if the rule itself declares `exemptible: true`**; a non-exemptible deny has no in-manifest escape at all |
| Who decides the rules | Datascape (the built-in DL set is fixed) | **You** — policies are typed YAML you author, own, and version like any other manifest |

A **lint** is Datascape telling you "this looks like a mistake" (two Bindings capturing
overlapping tables, a plaintext Connection where a TLS path exists). A **policy** is your team
telling *every* `validate`/`plan`/`apply`/`destroy` "this is forbidden here, full stop" — the
policy-as-code equivalent of an AWS SCP or a Kubernetes admission webhook. Lints ship with the
tool; policies don't ship at all until you write or generate one. **A repository with no
`--policies` directory has zero policy enforcement** — enabling the `PolicyEngine` feature gate
with nothing to load is a no-op, not a trap.

### Getting started: the zero-trust starter pack

You don't hand-write a policy file from scratch. `platformctl policy init zero-trust` writes a
versioned starter pack (the "blueprint" pattern applied to governance) covering the zero-trust
posture Datascape already ships as *defaults* — TLS-terminated Connections, digest-pinned
images, `vault`/`kubernetes`-backed secrets, default-deny network isolation, protected data
resources, and a few lint findings escalated from advisory to mandatory:

```sh
platformctl policy init zero-trust                    # writes .datascape/policies/policy.yaml
platformctl policy test ./platform/                   # preview what it would deny — no state, no runtime calls
platformctl apply ./platform/ --feature-gates PolicyEngine=true   # now it's enforced
```

Review the written file before enabling it — every rule's comment cites the mechanism ADR/doc
section it enforces, and a few rules ship with a literal `REPLACE_ME`-style placeholder (e.g. the
corp-registry hostname, the external-target allowlist) that only makes sense once you fill in
your organization's own values. `PolicyEngine` stays **Alpha and disabled by default** — no
policy evaluation happens anywhere until you both load a policies directory *and* pass
`--feature-gates PolicyEngine=true`.

### A worked example: deny, then waive

Say the pack is enabled and you `validate` a manifest set with a plaintext Connection:

```
$ platformctl validate ./platform/ --feature-gates PolicyEngine=true
error: denied by policy:
  default/Connection/orders-db (rule no-plaintext-connections): Connections must terminate TLS (spec.scheme: https) — docs/adr/018, docs/planning/08 C8. Exempt for local-only dev traffic with a reason.
```

Same shape as any other validation error: the rule id, the message, and exactly which resource
tripped it. Two ways forward:

1. **Fix it** — set `spec.scheme: https` on the Connection (the actually-governed outcome the
   policy wants).
2. **Waive it, with a reason** — only because `no-plaintext-connections` is authored
   `exemptible: true` in the pack. Add the annotation directly on the resource:

   ```yaml
   apiVersion: datascape.io/v1alpha1
   kind: Connection
   metadata:
     name: orders-db
     annotations:
       policy.datascape.io/exempt: "no-plaintext-connections: local dev sandbox, no real data, TLS added before staging"
   spec:
     scheme: tcp
     ...
   ```

   `validate` now reports the decision as exempted (visible in `-o json`, never silently dropped)
   and exits `0`. The reason is mandatory — an entry with no `: <reason>` half is dropped by the
   parser and the deny still blocks.

Now compare a rule the pack authors deliberately made **non-exemptible** —
`protect-data` (`Dataset`/`Source` must set `metadata.protect: true`, docs/adr/013). The same
annotation on that resource does *nothing*: the evaluator only ever honors an exemption when the
rule itself opted in, "unlike a lint waiver, which any resource can always claim" (ADR 021 §3).
If your team hits a non-exemptible deny you genuinely can't satisfy, that's a real finding to
raise with whoever owns the pack — not something to route around.

### `policy test` in CI

`platformctl policy test <dir>` evaluates policies against a manifest set on its own — schema +
graph + lint findings, no compatibility checks, no state, no runtime calls — the fast,
CI-friendly authoring loop for policy files themselves. This repository's own CI runs the shipped
zero-trust pack via `policy test` against every example and blueprint it ships (`.github/
workflows/ci.yml`'s "Policy pack against examples and blueprints" step, docs/planning/08 §7.7
H4): a posture regression — a resource that starts tripping a rule it didn't before, or a waiver
that quietly disappears — fails the build, the same way a broken unit test would. Wire your own
policy directory into your pipeline the same way: `platformctl policy test ./platform/
--feature-gates PolicyEngine=true` as a required check before `apply` ever runs against anything
that matters.

## Troubleshooting

### `apply` was killed mid-run (CI job cancelled, laptop slept, crash)

State is written after every resource (NFR-9: atomically), so nothing is
lost — re-run `platformctl apply <dir>`; reconciliation is idempotent and
converges from wherever it stopped. If the run died hard enough to leave
the state lock held: `platformctl state unlock` force-releases it (safe
only when you know the holder is dead), then `platformctl state doctor`
reports any defects and `platformctl state repair` applies the safe
fixes. `platformctl gc plan` lists any labeled runtime objects no state
entry accounts for.

**1. "feature gate ... is disabled"** — you used a Provider type or manifest feature that's
still Alpha/off by default (Trino, ingress, backup/restore, ...). Exact shape:
```
feature gate "TrinoProvider" (stage: Alpha) is disabled; enable with --feature-gates=TrinoProvider=true
```
Remedy: add `--feature-gates=<Name>=true` to the command, as the message says.

**2. A secret is missing** — `apply`'s Preflight check refuses before touching any
infrastructure, naming every unresolved reference in one pass:
```
2 secret(s) cannot be resolved — apply would half-apply the platform, so nothing was changed:
  - SecretReference "postgres-admin": unset environment variable(s): DATASCAPE_SECRET_POSTGRES_ADMIN_PASSWORD
  - SecretReference "s3-creds": unset environment variable(s): DATASCAPE_SECRET_S3_CREDS_ACCESSKEY
```
Remedy: set the named variables, or fill in the `.env` file `init` scaffolded and pass
`--env-file`.

**3. Docker daemon isn't running** — the Docker runtime adapter fails to connect:
```
error: connect to Docker daemon: ...
```
(wrapping whatever the Docker SDK reports — usually that the socket isn't there). Remedy: start
Docker Desktop / the `docker` service, or check `DOCKER_HOST` if you're pointing at a remote
daemon.

**4. Drift after an out-of-band change** — someone (or something) touched a resource outside
platformctl: killed a container, edited a connector's config by hand, changed a bucket's
lifecycle rule. `platformctl drift ./platform/` probes live infrastructure and reports it,
exiting `1`:
```
drift detected on 1 resource(s); run apply to reconcile
```
`status`'s `DRIFT` column shows `True` for the affected resource. Remedy: `platformctl apply
./platform/ --auto-approve` heals it — a killed container is recreated, a stopped one restarted,
a drifted connector/lifecycle config reconciled back to spec. `plan`/`drift` never mutate on
their own; only `apply` does.

**5. Kubernetes RBAC / preflight errors** — `validate`/`plan` against a `kubernetes` runtime
Provider fail fast, naming exactly what's missing, before any mutating call:
```
error: kubernetes (kubeconfig=~/.kube/config, context=my-cluster): missing permission(s): create deployments.apps, list namespaces — see deploy/kubernetes/rbac/role.yaml for the minimal Role this adapter needs
```
or, if the cluster itself can't be reached:
```
error: kubernetes (kubeconfig=~/.kube/config, context=my-cluster): cluster unreachable: ...
```
Remedy: apply the minimal Role from `deploy/kubernetes/rbac/` (or the cluster-admin dev shortcut
documented there) with the missing verbs; re-run `validate`.

A capability mismatch on a `Binding` (a CDC binding against a provider that can't speak your
database's engine, a sink format the connector can't write) fails the same way, at `validate`,
naming the Provider and exactly what it supports — see the Bindings section above.
