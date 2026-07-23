# Writing a provider

This is the executable contract for a `reconciler.Provider` (docs/planning/08
E6, re-scoped by [ADR 028](../adr/028-test-tiering.md) as the fast tier's
provider middle). It extends
[docs/onboarding/developers.md](../onboarding/developers.md) — read that
first for the repo-wide reading order and the one invariant (domain/ports
never import an adapter). This page assumes you've done that and goes deep
on the one seam you'll actually implement: `internal/ports/reconciler.Provider`
plus whichever capability interfaces your technology needs.

The acceptance bar for a new provider is not "it reconciled once in a demo."
It is: **it passes `internal/ports/reconciler/conformance`**, the same suite
this guide walks through below, driven against a fake runtime and — where
your provider dials a real protocol — a small fake-technology harness you
write once, in your own `_test.go` file. That suite is provider-agnostic; if
your provider passes it, you have independently reproduced the lifecycle
contract every shipped provider already honors, without reading any of their
source.

## 1. Lifecycle semantics

A provider is three methods:

```go
type Provider interface {
    Type() string // "redpanda", "postgres", "debezium", "s3", "s3sink", ...
    Reconcile(ctx context.Context, req Request) (status.Status, error)
    Destroy(ctx context.Context, req Request) error
    Probe(ctx context.Context, req Request) (status.Status, error)
}
```

`Type()` is the string a manifest's `spec.type` (Provider) or an inferred
realizing-provider lookup resolves through `application/registry` — it is
also the discriminator `schemas.ProviderConfigFragments` keys on (§5 below).

A single provider package often reconciles more than one **Kind**: the
`Provider` resource itself (usually: stand up the underlying
instance/container) plus whatever dependent resource kinds it realizes
(`EventStream`, `Connection`, `Catalog`, ...). `req.Resource.Kind` is how you
tell them apart — every shipped provider's `Reconcile`/`Destroy`/`Probe`
opens with a `switch req.Resource.Kind` and returns a plain `fmt.Errorf` for
any Kind it doesn't handle (see `redpanda.Provider.Reconcile`,
`proxy.Provider.Reconcile`). There is no separate "which kinds do I handle"
method — the switch statement itself is the declaration.

### Settledness (NFR-11) — Ready means serving, right now

> A provider reports `Ready` only when the resource answers its declared
> protocol *at that moment* — reconcile runs the SAME serving check its own
> `Probe` uses (no weaker proxy signal: container-running ≠ serving,
> membership ≠ leadership), inside a bounded condition-poll with an honest
> timeout error naming the last observed state. Fixed-duration sleeps that
> assume completion are forbidden.
> — docs/planning/02-architecture.md §4.1

Concretely: your `Reconcile` must not set `status.Ready: True` off container
health alone if there's a real serving check available. Bring your own
container up, then **settle** to the same check `Probe` performs before
declaring Ready — `redpanda.waitTopicSettled` and `proxy.waitForwarderServing`
are the two reference implementations; both re-run their own `Probe`-side
check in a bounded loop (`runtime.ScaledWait`-scaled deadline, honest
timeout error naming the last observed state) rather than trusting a
container healthcheck to mean "serving."

**Never a fixed sleep.** If you're typing `time.Sleep` in a retry loop, or
constructing `127.0.0.1:<port>` by hand, stop — that's the exact class of
bug [ADR 015](../adr/015-connectivity-plane.md) (the connectivity/discovery
plane) exists to make impossible: all dialing goes through
`runtime.WithReachable`/`EnsureReachable`, which re-resolves a fresh address
on every attempt and absorbs the runtime-specific races (NodePort iptables
propagation, port-forward listen delay, ...).

### Idempotency (NFR-2)

Every `Ensure*` runtime method is idempotent-by-contract: a second call with
an unchanged spec makes **zero** further mutating calls. This is not
optional and not provider-specific — it's enforced structurally by
`internal/ports/runtime/conformance` against every runtime adapter, and this
guide's own conformance suite re-proves it at the provider level (a
provider that computes a slightly different `ContainerSpec` on every call —
e.g. a map iterated in nondeterministic order — will fail idempotency even
though the underlying runtime is honest).

### Statelessness (docs/planning/08 F5)

A provider's constructor takes **nothing but static config** — typically
just `New() *Provider` returning an empty struct, as every shipped provider
does. Every piece of information a method needs arrives through `Request`
(§3), not through a `Set*` call made before `Reconcile`, and not through a
field the provider mutates and reads back on a later call. This is what lets
one `*Provider` instance safely serve two completely unrelated resources
interleaved — an engine invariant this guide's conformance suite checks
directly (§6).

## 2. Required and optional interfaces

The base `Provider` interface (above) is all that's required. Everything
else is a **capability interface** — a marker or behavior-declaring
interface a provider optionally implements, discovered by the engine/
compatibility layer via a type assertion (`p.(reconciler.CDCCapableProvider)`
etc.), never a registry of "known provider kinds." Read every interface's own
doc comment in `internal/ports/reconciler/reconciler.go` before implementing
it — this table is an index, not a substitute:

| Interface | Implement when | Method(s) |
|---|---|---|
| `CDCCapableProvider` | your provider can sit behind a `mode: cdc` Binding | `SupportedSourceEngines() []string` |
| `SinkCapableProvider` | ... `mode: sink` Binding targeting a Dataset | `SupportedSinkFormats() []string` |
| `DatabaseSinkCapableProvider` | ... `mode: sink` Binding targeting a Source | `SupportedSinkEngines() []string` |
| `IngestCapableProvider` | ... `mode: ingest` Binding | `SupportedIngestFormats() []string` |
| `CatalogCapableProvider` | your provider can realize a `Catalog` | `SupportedCatalogEngines() []string` |
| `ConnectionCapableProvider` | your provider can realize a managed `Connection` | `SupportedConnectionSchemes() []string` |
| `TunnelCapableProvider` | your provider can serve as a `Connection.spec.via` egress leg | `SupportsTunnelChaining() []string` |
| `ViaConsumingProvider` | your `ConnectionCapableProvider` consumes a peer's `spec.via` | `ConsumesVia() bool` (embeds `ConnectionCapableProvider`) |
| `MediationCapableProvider` | your provider realizes the ADR 022/027 identity-mediation plane | `Mediation(ctx, req) (mediation.MediationProvider, error)` |
| `ExternalConfigurer` | your provider may configure `spec.external: true` resources naming it | `ConfigureExternal(ctx, req) (status.Status, error)` |
| `SpecValidator` | your `Provider` resource's config has cross-field rules a JSON Schema fragment can't express | `ValidateSpec(cfg provider.Provider) error` |
| `BindingOptionsValidator` | a `Binding.spec.options` block your provider reads needs validate-time checking | `ValidateBindingOptions(mode string, options map[string]any) error` |
| `StreamReplicationValidator` | your `EventStream`-realizing provider can bound `spec.replication` from its own config | `ValidateStreamReplication(cfg provider.Provider, replication int) error` |
| `SchemaRegistryCapableProvider` | your `EventStream` backend exposes a Confluent-compatible schema registry, config-gated | `SupportedSchemaFormats(cfg provider.Provider) []string` |
| `KafkaBootstrapAddressProvider` | your provider's Kafka listener address is derivable from manifest facts alone, no live reconcile needed | `KafkaBootstrapAddress(name string, cfg provider.Provider) string` |
| `VersionedProvider` | your provider's internals are coupled to a technology major version (a data mount path) | `VersionCatalog(cfg provider.Provider) versionprofile.Catalog` |
| `LineageAware` | your provider knows how to wire a lineage backend's connection details into its real integration | `ConfigureLineage(ctx, req, endpoint lineage.LineageEndpoint) error` |
| `DesignLinter` | your provider contributes technology-specific design-lint findings (ADR 020) | `LintDesign(envelopes []resource.Envelope, g *graph.Graph) []lint.Finding` |
| `BackupCapableProvider` | your realized resource's data can be dumped to / restored from object storage | `Backup(ctx, req, dest) (backup.Manifest, error)`, `Restore(ctx, req, src) error` |

None of these are required by the base `Provider` interface. A provider that
doesn't implement `CDCCapableProvider` simply can't be referenced from a
`mode: cdc` Binding — caught at `validate` time by the compatibility layer
(docs/planning/02-architecture.md §5.2), never a runtime surprise.

**A capability method that returns an error must name the concrete fact
that failed** — a field name, an observed-vs-wanted pair — never a bare
generic message. `redpanda.ValidateStreamReplication`'s doc comment is the
canonical example: "The returned error names both numbers (the declared
replication and the configured capacity)." This is what
`conformance.CapabilityCheck` (§6) mechanically verifies for whichever
capability interfaces you declare.

One capability-interface error format is **not** yours to construct: the
`Binding "X": Provider "Y" (type: Z) does not support ...` message
(docs/planning/02-architecture.md §5.2) is assembled by
`internal/application/compatibility` from your `SupportedSourceEngines()`/
`SupportedSinkFormats()` return value — you declare what you support; the
engine formats the rejection.

## 3. Request and Facts

Every method that needs more than your provider's own static config takes a
`Request` (`internal/ports/reconciler/reconciler.go`):

```go
type Request struct {
    Resource  resource.Envelope                  // the envelope being reconciled/destroyed/probed
    Runtime   runtime.ContainerRuntime           // constructed for the realizing Provider's spec.runtime
    Provider  resource.Envelope                  // the realizing Provider resource (== Resource for Kind "Provider")
    Secrets   map[string]map[string]string       // resolved spec.secretRefs, by ref name then key
    Resources map[resource.Key]resource.Envelope // the full validated set, for related-resource lookup
    Facts     Facts                              // published-endpoint-fact query — see below
    // (a handful of deprecated, single-purpose fields also exist — see §3.1)
}
```

A zero field means "not resolved/applicable for this call." Adding a field
to `Request` is non-breaking for every existing implementor — this is what
replaced an earlier accretion of `Set*`-before-`Reconcile` setter interfaces
(docs/planning/08 F5), and it's why your provider holds no state: every call
is self-contained.

### Cross-provider needs: read `Facts`, never invent a new `Request` field

If your provider needs to consume something a *different* provider
published — a warehouse's S3 endpoint, a schema registry's URL, a peer
tunnel's dial address — read it through `req.Facts`:

```go
type Facts interface {
    // Endpoint returns providerKey's own published fact named factName.
    // ok is false when it hasn't been published yet — never blocks, never
    // triggers a reconcile.
    Endpoint(providerKey resource.Key, factName string) (endpoint.Endpoint, bool)
    // ByName enumerates every published fact named factName across the
    // whole snapshot, sorted by owning resource.Key for determinism.
    ByName(factName string) []PublishedFact
}
```

`Facts` is a read-only, engine-backed snapshot taken once at request-build
time. A miss (`ok == false`, or an empty `ByName` result) means "not
published yet, by anyone, for any reason" — report it honestly and let the
manifest graph's dependency ordering (`graph.Build`'s `via`/`warehouseRef`/
`catalogRef` edges) resolve it on a later reconcile. **`Facts` is never a
scheduling primitive** — if you need "X before Y," that's a ref field on
your schema that `graph.Build` turns into a dependency edge, not a retry
loop against `Facts`.

```go
// A hypothetical third-party provider dialing a warehouse Provider's "s3"
// fact, resolved from its own configuration.warehouseProviderRef:
ref := resource.RefFromSpec(cfg.Configuration, "warehouseProviderRef")
warehouseKey := ref.Key(req.Resource.Metadata.Namespace, "Provider")
ep, ok := req.Facts.Endpoint(warehouseKey, "s3")
if !ok {
    return status.Status{}, fmt.Errorf(
        "warehouse Provider %q has not published its \"s3\" endpoint yet — re-apply once it reconciles",
        warehouseKey.Name)
}
dial(ep.Internal) // e.g. "minio:9000" — never a loopback literal
```

`internal/archtest`'s `TestReconcilerRequestFieldsFrozen` mechanically
blocks adding a new bespoke `Request` field for a cross-provider need — a
genuinely new need is consumed through `Facts`, full stop. (`Request` does
carry a handful of pre-`Facts` fields — `SchemaRegistryURL`,
`KafkaBootstrapServers`, `CatalogFacts`, `PrometheusURL`, `WarehouseFacts` —
kept as deprecated, byte-identical wrappers for their existing consumers;
this guide deliberately does not teach them. `KafkaBootstrapServers` is the
one exception that will never migrate to `Facts` — it's a graph-resolved
manifest fact, not a *published* one, outside `Facts`'s ADR 015 scope by
design. New code reads `Facts` directly, always.)

### `StaticFacts` — the test double

`reconciler.StaticFacts` (a `map[resource.Key][]endpoint.Endpoint`) is the
`Facts` implementation to use in your own tests — construct it as a literal
and pass it in `Request.Facts`. An empty `reconciler.StaticFacts{}` answers
every query honestly with "not published," which is the right default for
any fixture that doesn't specifically test fact resolution.

## 4. Fragments — your `spec.configuration` shape (docs/planning/08 E5)

`Provider.spec.configuration` (and `Source.spec.<engine>`,
`Catalog.spec.<engine>`, `Binding.spec.options`) are deliberately
open-ended blocks in the core schemas — a new provider must never require a
core schema change. Instead, ship a narrow JSON Schema **fragment** under
`schemas/v1alpha1/fragments/provider/<type>.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://datascape.io/schemas/fragments/provider/yourtype.json",
  "title": "Provider(type: yourtype).spec.configuration",
  "type": "object",
  "properties": {
    "image": { "type": "string", "minLength": 1 }
  },
  "additionalProperties": false
}
```

Register it by discriminator in `schemas.ProviderConfigFragments` (and, if
applicable, `SourceEngineFragments`/`CatalogEngineFragments`/
`BindingOptionsFragments` — see `schemas/embed.go`), and add the matching
entry to `docs/planning/03-resource-model-reference.md` **in the same
commit** — this is a hook-enforced rule (`scripts/hooks`), not a suggestion.

Fragments are **shape-only**: type/enum/range/required-field checks. A
cross-field rule (a `*SecretRef` value must also appear in
`spec.secretRefs`; a value must equal something in a sibling array) cannot
be expressed by a static JSON Schema — that stays in your `SpecValidator`/
`BindingOptionsValidator` Go code (§2). A schema-legal-but-provider-invalid
configuration should still fail at `validate`, not at `apply`/`reconcile` —
between the fragment and your `SpecValidator`, every misconfiguration class
you can anticipate should be catchable before anything is scheduled (ADR
011).

## 5. Endpoint publication (ADR 015: publish, don't construct)

If your provider exposes a network address another provider (or a human via
`platformctl inventory`) needs, publish it as an `endpoint.Endpoint` in
`status.ProviderState[endpoint.Key]`:

```go
endpoints := endpoint.List{
    {
        Name: "kafka", Scheme: "kafka",
        Host: hostAddr,             // reachable from the machine running platformctl, or "" if not published
        Internal: internalAddr,     // reachable from other containers on the shared network
        Insecure: true,             // plaintext, surfaced explicitly, never an unstated assumption
        RuntimeName: name, ContainerPort: kafkaPort, Audience: runtime.AudienceHost,
    },
}
st.ProviderState = map[string]any{
    endpoint.Key: endpoints.ToState(),
    // ... any other provider-specific facts you want to persist
}
```

Rules, non-negotiable:

1. **Never publish a blank placeholder.** An endpoint with neither `Host`
   nor `Internal` set is not "pending" — it's a bug. If the address isn't
   resolved yet, don't add the entry.
2. **`Internal`/`Host` are observed bindings, not intent.** Read them off
   the `runtime.ContainerState` `EnsureContainer`/`Inspect` actually
   returned (`ctrState.HostAddr(port)`), never reconstructed from the spec
   you asked for — a runtime is free to allocate differently than requested
   (an auto-assigned host port, e.g.).
3. **`RuntimeName`/`ContainerPort`/`Audience` are the runtime-object facts
   behind the address** — a consumer that needs to call `EnsureReachable`
   itself reads these instead of re-deriving a runtime object name from
   your resource's own name by convention (a wrong guess here was a
   repeat, live-caught bug class before these fields existed).
4. **No loopback literals.** `internal/domain` and
   `internal/adapters/providers` are arch-tested
   (`internal/archtest`) to contain no `127.0.0.1`/`localhost` construction
   outside a narrow, commented allowlist (in-container healthchecks,
   runtime adapters themselves).

This guide's conformance suite (§6) checks rule 1 mechanically for whatever
you publish — it decodes `ProviderState[endpoint.Key]` with
`endpoint.FromState` and fails if any entry has empty `Host` **and** empty
`Internal`.

## 6. The conformance suite — your acceptance bar

`internal/ports/reconciler/conformance` drives **any** `reconciler.Provider`
through the lifecycle contract above, against a fake runtime and — where
your `Reconcile`/`Probe` genuinely dials a technology protocol — a small
fake-technology harness you supply. It is the fast tier's provider middle
(ADR 028 §1): milliseconds, no Docker, every subtest `t.Parallel()`.

### What it checks

- **Settledness** — `Reconcile` reporting `Ready` implies an immediately
  following `Probe` also reports `Ready` and no drift (NFR-11), with both
  calls bounded to a small wall-clock budget (regression signal for an
  accidental real wait creeping into a fast-tier path).
- **Idempotency** — a second `Reconcile` against an unchanged `Request`
  makes zero further mutating runtime calls (`fake.Runtime.Mutations()`
  delta == 0).
- **Probe honesty** — `Probe` is a point-in-time check, not an internal
  wait/retry loop (bounded wall-clock budget, two consecutive calls).
- **Destroy convergence** — `Destroy` succeeds, and succeeds again when the
  resource is already gone.
- **Statelessness** — two independently-named fixtures, reconciled and
  probed interleaved through one `*Provider` instance, never
  cross-contaminate.
- **ProviderState publication** — every `endpoint.List` entry your
  `Reconcile` publishes carries a real fact (§5 rule 1).
- **Capability error formats** — for whichever error-returning capability
  interfaces you opt into checking, the returned error names the concrete
  fact that failed (§2).

### Writing your `Harness`

```go
package yourtype

import (
    fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
    "github.com/rezarajan/platformctl/internal/ports/reconciler"
    "github.com/rezarajan/platformctl/internal/ports/reconciler/conformance"
    "github.com/rezarajan/platformctl/internal/ports/runtime"
    "testing"
)

func TestConformance(t *testing.T) {
    conformance.Run(t, conformance.Harness{
        NewRuntime: func() runtime.ContainerRuntime { return fakeruntime.New() },
        Provider:   func() reconciler.Provider { return New() },
        Resource: func(rt runtime.ContainerRuntime, namePrefix string, i int) reconciler.Request {
            name := namePrefix + "-a"
            if i == 1 {
                name = namePrefix + "-b"
            }
            env := providerEnvelope(name /* ... your spec.configuration ... */)
            return reconciler.Request{
                Resource: env, Provider: env, Runtime: rt,
                Facts: reconciler.StaticFacts{},
            }
        },
        // CapabilityChecks is optional — nil if you declare no
        // error-returning capability interface.
    })
}
```

`Harness.Provider` is the exact
`func() reconciler.Provider { return X.New() }` shape
`application/registry.RegisterProvider` itself takes — write it once here,
copy it verbatim into `cmd/platformctl/main.go`'s `defaultWiring` at
registration time (§8).

`Harness.NewRuntime` and `Harness.Resource` are the only places a fake
runtime or fake technology is ever constructed — the conformance package
itself imports nothing under `internal/adapters` (ports import domain and
other ports packages only; see CLAUDE.md's one invariant). This is also
where your provider's own **fake-technology harness** lives, if `Reconcile`/
`Probe` dial a real protocol beyond the container runtime itself:

- If your provider's settledness/probe check dials a **raw TCP socket** (a
  liveness ping, not an application protocol), a real
  `net.Listen("tcp", "127.0.0.1:0")` standing in for "the upstream" is
  usually enough — see `internal/adapters/providers/proxy/conformance_test.go`'s
  `listenAndHoldOpen`, reused from `proxy_test.go`'s own established
  pattern: bind a real listener, hold the accepted session open, and make
  your fixture's declared port match the listener's real port. The fake
  runtime reports a `HostAddr` for that port (from the `ContainerSpec` you
  asked for); the real listener behind it is what makes the dial-through
  genuinely succeed.
- If your provider's serving check speaks a **real application-layer wire
  protocol** with no dialer/transport seam to intercept (a Kafka admin
  client, a Postgres wire-protocol client), there is currently no
  general-purpose fake for it in this repo. Scope your `Resource` fixture to
  the sub-lifecycle that doesn't need one — `internal/adapters/providers/redpanda`'s
  own exemplar drives only the `Provider` (broker) kind, which is pure
  container-lifecycle, and documents in its own file why the `EventStream`
  (topic) kind is out of scope for the fast tier and covered by the Docker
  integration suite instead. This is a legitimate, documented scoping
  decision, not a suite limitation you need to work around by faking
  Kafka's wire protocol.

If your provider's own settledness logic hard-codes a real-world wait
duration (a TLS handshake read deadline, a poll interval) with no way to
shrink it for a test, extract it to a package-level `var` — mirroring
`proxy.go`'s existing `forwarderSettleTimeout`/`forwarderSettlePoll`
pattern (and, added for this suite, `probeReadDeadline`) — so your
conformance test can shrink it once, **before** calling `conformance.Run`
(never inside a subtest — `conformance.Run`'s subtests run
`t.Parallel()`, and concurrently mutating a shared package variable
races). Production behavior (the default value) is unchanged; only your own
fast-tier test pays a smaller, deterministic cost.

### Budget

Each provider's whole suite should complete in well under a second against
the fake. If it doesn't, you've likely got a real wait/sleep on a path the
fast tier exercises — shrink it (the pattern above) rather than raising the
suite's own timing budgets.

## 7. Drift and condition reasons

`Probe` reports drift via the `status.DriftDetected` condition — `True`
means "observed state disagrees with declared spec," `False` means clean. A
new `Condition` you construct must set `Reason` to a constant from
`internal/domain/status/reasons.go` (`internal/archtest`'s
`reason_literal_test.go` enforces this at build time) — add a new constant
there if your provider needs one, grouped by area, following the file's own
"stable prefix + dynamic detail" convention for a reason that carries
runtime-observed data (e.g. `redpanda`'s
`fmt.Sprintf("%s(%d!=%d)", status.ReasonPartitionCountMismatch, got, want)`).
Every reason you add becomes visible in `platformctl explain`'s catalog
(E4) automatically — that command walks this same file.

## 8. Registration and feature gate

1. `application/registry.RegisterProvider(typeName, ctor, gateName)` — one
   line in `cmd/platformctl/main.go`'s `defaultWiring`, using the exact
   `func() reconciler.Provider { ... }` constructor your `Harness.Provider`
   already uses.
2. **A gate, in the same commit** ([ADR 014](../adr/014-feature-gate-strategy.md)):
   `gates.Register("YourProvider", featuregate.Alpha, false)`. With the gate
   off there must be **zero** behavior change for any manifest that doesn't
   opt in — not "mostly none." `RegisterProvider`'s third argument wires the
   gate name; `registry.Provider(typeName)` refuses construction with an
   error naming the gate when it's disabled.
3. Add the row to `docs/planning/04-roadmap-and-feature-gates.md` §12 (the
   master gate table) — it and `main.go` must agree.
4. Add your provider's package(s) to `scripts/test-impact.sh`'s suite↔scope
   table in the same commit as your first integration test, so
   `just test-affected` actually selects it (docs/planning/06 §10).

## 9. Fake-honesty rule ([ADR 028](../adr/028-test-tiering.md) §2)

> Fakes must be honest. The fake runtime already passes the same runtime
> conformance suite as Docker/Kubernetes — that discipline extends to
> technology fakes (fake Connect REST, fake Kafka admin): each fake's
> behavior is pinned against the real system's observed semantics in the
> deep tier, so fast-tier green is meaningful.

If you write a fake-technology harness beyond a raw `net.Listener` (a fake
HTTP admin API, a fake wire-protocol responder), it must be **pinned**
against the real technology's observed behavior by at least one deep-tier
(`-tags integration`) test — never invented from documentation alone. A
fast tier that's green against a fake nobody checked against reality is
worse than no fast tier: it teaches you to trust a signal that means
nothing. This repo currently has no such harness beyond the raw-socket
trick (§6) — the day one is built, this rule is what keeps it honest.

## 10. Worked examples

Three shipped providers, deliberately chosen to span the shape spectrum,
each with its own `conformance_test.go` you can read start to finish:

- `internal/adapters/providers/noop` — the trivial case: zero runtime
  calls, zero published state. Proves the suite doesn't demand facts a
  provider legitimately has none of.
- `internal/adapters/providers/redpanda` — the container-lifecycle case
  (`Provider`/broker kind only — see §6's note on EventStream scoping),
  plus two `CapabilityCheck` examples (`SpecValidator`,
  `StreamReplicationValidator`).
- `internal/adapters/providers/proxy` — the settledness/dial-through case:
  a real `net.Listener` fake-technology harness, and the
  `probeReadDeadline` var-extraction pattern for a hard-coded production
  wait.

These three exemplars are also this repo's evidence for doc 08 Stage E's
exit criterion ("a third-party provider can be built from the
provider-author guide + conformance suite alone") — see that task's Done
note for how the claim is scoped.
