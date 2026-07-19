# Systemic Findings from Live Testing & Core/Provider Segregation Readiness

Audit date: 2026-07-19. Audited against: the Stage A/B commit history
(`da3abe5..5da8367`), `errors.md`, `checkpoint.md`, doc 07's Cross-Runtime
Portability section, and direct inspection of `internal/ports/runtime`,
`internal/ports/reconciler`, `internal/domain/connection`,
`internal/application/engine`, and every shipped provider.

Purpose: Stage B was completed by fixing a series of bugs that **only live
Kubernetes testing caught** — none were found by unit tests, the conformance
suite, or the Docker integration suite. This document classifies those bugs,
shows they are instances of a small number of *architectural* gaps rather
than isolated mistakes, prescribes systems-level changes that close each
class **for every current and future provider at once** (no dependent-side
fixes), and defines the readiness bar that must hold **before the core
application is segregated from provider-specific logic** (doc 04 Phase 8:
out-of-process provider plugins / non-container runtime adapters).

The prescriptions here are integrated into the work backlog as
**Stage F of doc 08** (docs/planning/08-production-readiness-plan.md §7.5);
this document is the analysis and rationale, doc 08 carries the executable
tasks.

---

## 1. The bug ledger — defects found only by live testing

Every entry below shipped past `go test ./...`, the runtime conformance
suite, and (for the K8s items) in most cases the Docker integration suite.
Commit references are to this repository.

### 1.1 Kubernetes live-testing bugs (Stage B)

| # | Defect | Where found | Commit |
|---|---|---|---|
| K1 | K8s adapter set `container.Command = spec.Cmd` (ENTRYPOINT-replacing) instead of `container.Args` (CMD-appending); redpanda's image entrypoint was skipped → `unrecognised option '--node-id'` | First real provider on a real cluster | doc 07 Cross-Runtime |
| K2 | `VolumeSpec` had no namespace concept; a PVC must live in the namespace of the pod that mounts it | Building the second adapter | doc 07 Cross-Runtime |
| K3 | kube-proxy programs a NodePort's iptables rules *asynchronously* after the Service object shows the port; dialing immediately after API-visibility fails | Live cluster, B1 | `5e39a93` |
| K4 | Kafka's protocol has the broker tell clients which address to reconnect to (`advertise-kafka-addr`, baked at container start) — a value that can never be correct ahead of time on K8s; needed a custom `kgo.Dialer` redirect | Live cluster, B1 | `5e39a93` |
| K5 | **Eight providers** (postgres, mysql/mariadb, s3/minio, s3sink, debezium, nessie, openlineage — redpanda fixed in B1) built every CLI-side admin connection from a hardcoded `"127.0.0.1:" + configuredPort` — correct on Docker only because Docker always publishes there | Audit triggered by B1's redpanda fix | `81025c9` |
| K6 | `connection.Connection.DialAddress()` hardcodes `127.0.0.1:port` for *managed* Connections at the **domain** layer; consumed by debezium's preflight and (independently, a second time) by `engine.externalConnectionStatus` | Live lakehouse run | `81025c9`, `88b8329` |
| K7 | The fix's first draft resolved a managed Connection's forwarder by the realizing *Provider's* name (`"edge"`); proxy names the container after the *Connection* (`"orders-db"`). The identical name-by-convention mistake had already been made and fixed once in debezium | Live lakehouse run | `81025c9` |
| K8 | redpanda's internal Kafka listener (29092) was never declared in `ContainerSpec.Ports` — Docker's bridge network reaches every container port regardless of publication; a K8s Service forwards **only declared ports**, so in-cluster Connect workers couldn't reach the broker at all | Live cdc-attendance run, B8 | `88b8329` |
| K9 | openlineage's internal Marquez-metadata Postgres declared **no** ports ("not published to the host" — true, but K8s needs a declared port to create *any* Service, i.e. any DNS name); Marquez failed with `UnknownHostException` | Live lakehouse run, B8 | `88b8329` |
| K10 | `HostPort: 0` meant "publish to a random ephemeral host port" in `docker.go`; B8 redefined it as "in-network only, no host publish" — an overloaded magic value now carrying the fix for K8/K9 | B8 | `88b8329` |
| K11 | nessie/openlineage readiness polling reused ONE port-forward tunnel for the whole wait window; the tunnel's first dial racing the JVM's `listen()` left it silently dead forever after — every retry failed against healthy infrastructure | Live cluster, B8 | `88b8329` |
| K12 | K8s `ReadFile`-via-exec never named which pod container to run in ("container not found" on every call); pod selection during a rolling update could pick the old, terminating generation's pod | Live cluster, B3 | `f765a0f` |
| K13 | Docker network → K8s Namespace mapping gave DNS parity but silently **dropped isolation** (any pod cluster-wide could reach the Services) — a semantic weakening no one opted into | Cross-runtime analysis, B7 | `ebd95b5` |

### 1.2 The same classes, previously, on Docker (errors.md)

The K8s bugs were not new *kinds* of failure — Docker live testing had
already surfaced the same classes:

| # | Defect | Class shared with |
|---|---|---|
| D1 | `pg_isready -U user` answers over the **unix socket**; initdb's temporary socket-only server reported "healthy" before the real server listened on TCP → dependents refused | K3, K11 (exists ≠ ready ≠ reachable) |
| D2 | Providers dialed `localhost`, which can resolve to `::1` where the daemon publishes IPv4 only | K5 (address guessed, not observed) |
| D3 | `EnsureContainer`'s spec-hash reuse restarted a container without verifying network attachment → Kafka Connect up with no resolvable broker | K8 (declared intent vs. runtime reality unverified) |
| D4 | Default host ports collided with whatever already ran on the machine | K5 (topology assumptions baked into specs) |

**Conclusion of the ledger:** thirteen Kubernetes defects and four earlier
Docker defects reduce to **five recurring classes**. Each class was fixed
multiple times, at multiple independent call sites, by different sessions —
the definition of a missing system-level mechanism.

---

## 2. The five failure classes

### Class 1 — Network topology knowledge leaked into dependents (K4–K7, D2, D4)

Every provider, the engine, and the domain layer each answered "how do I
reach this container?" locally, by *constructing* an address from
configuration (`127.0.0.1` + configured port) instead of *asking the
runtime* for an observed one. The answer was correct on Docker by
coincidence — Docker happens to publish to loopback — so the leak was
invisible for five phases. Ten call sites carried the identical bug
(8 providers + engine + domain); the lesson "the forwarder is named after
the Connection, not the Provider" was learned twice (K7).

**The test for whether this is architectural:** could a new provider,
written today, reintroduce the bug? Yes — nothing prevents
`"http://127.0.0.1:" + port` in a provider; `EnsureReachable` is a
convention, not a constraint.

### Class 2 — "Exists" ≠ "ready" ≠ "reachable" conflated (K3, K11, D1)

Three distinct states were repeatedly collapsed into one:
an API object exists (Service has a NodePort number), a process is ready
(container healthcheck passes), an endpoint is reachable *by the party that
will actually dial it* (TCP connect succeeds from the CLI, through a live
tunnel, on the address the dependent will use). Each conflation produced a
race someone had to fix with a hand-rolled retry loop: every provider now
carries its own 30s ping-wait; nessie/openlineage additionally re-resolve
the tunnel per attempt. The *fix pattern* (retry + re-resolve) is correct
but lives in N copies, and the next provider must know to copy it.

### Class 3 — Under-declared intent that a permissive runtime tolerates (K1, K8, K9, K10, D3)

Docker is forgiving: all container ports are in-network reachable whether
declared or not; conformance images had no entrypoint so `Command` vs
`Args` didn't matter; a container reattached to a pruned network limps
along. Kubernetes is strict: only declared ports exist in a Service; the
entrypoint distinction is load-bearing. Every under-declaration was
invisible until the strict runtime interpreted the spec literally.
The port contract allowed specs that don't fully state their intent —
"which listeners exist and who may dial them" was implicit, and
`HostPort: 0` now carries two meanings by era (K10).

### Class 4 — Runtime-object identity by convention, not by handle (K7, K12)

Consumers re-derive the name of a runtime object ("the forwarder is called
X") at each call site from an unwritten convention. When the convention
differs from the guess, the failure is a runtime "not found" — or worse,
silently operating on the wrong object (K12's terminating pod). Nothing
type-level connects "the thing proxy created" to "the thing engine wants to
dial."

### Class 5 — Contract tests prove the port, not the translation (K1, K2, and the existence of this entire ledger)

Doc 07 already stated the lesson: *"a synthetic conformance suite proves
the port contract; only a real provider against a real cluster proves the
translation is faithful."* The conformance suite still has no
entrypoint-image subtest (the K1 class), no strict-ports model (the K8/K9
class), and no delayed-listen readiness subtest (the D1/K11 class). Each
live-caught bug was fixed, but most fixes did **not** leave behind a
contract-level reproduction, so a third runtime adapter (Phase 8's
Terraform/external) will re-discover them the same expensive way.

---

## 3. Systems-level changes

Design rule for every prescription: the fix must live in `domain`, `ports`,
or `application` such that **a provider author cannot reintroduce the bug
class** — either the compiler refuses, the conformance suite refuses, or
the capability simply isn't expressible from provider code. Providers get
*more* capable and *less* responsible. All changes are additive at the
seams (open/closed): no provider rewrite is required to adopt them, and
future providers get the behavior for free.

These are tasks **F1–F6 in doc 08 §7.5**; sizes and acceptance criteria
live there.

### F1 — Close the reachability seam: addresses become unconstructible

`EnsureReachable` (B1) is the right mechanism; make it the *only* one.

1. **Split `connection.DialAddress()`**: the managed-Connection branch
   returning `"127.0.0.1:" + port` is a domain-layer guess already wrong on
   K8s and already bypassed by both of its former consumers. Replace with
   `DeclaredAddress()` valid **only** for external Connections (compile
   error for the managed case: managed reachability requires a runtime, and
   domain has none — make the type system say so). Audit `HostEndpoint()`
   the same way.
2. **One reachability helper, owned by the ports layer**:
   `runtime.WithReachable(ctx, rt, name, port, opts, func(addr string) error)`
   — resolves `EnsureReachable`, invokes the callback, closes the tunnel,
   and on retryable failure **re-resolves a fresh address per attempt**
   (the K11 fix, generalized) with the standard 30s ready-wait (the D1 fix,
   generalized). Every provider's hand-rolled ping-wait/retry loop migrates
   to it; new providers never write one.
3. **Architecture test** (same enforcement style as the layering grep): no
   string literal loopback/localhost address in `internal/adapters/providers`
   or `internal/domain`, allowlisting the two legitimate uses —
   *in-container* healthcheck commands (they execute inside the container,
   where loopback is correct) and the runtime adapters themselves (they
   *observe* rather than guess). This turns Class 1 from a code-review
   concern into a CI failure.

### F2 — Explicit port audience: specs state their full intent

Replace the `HostPort: 0` magic value (K10) with a declared audience on the
port itself:

```go
type PortBinding struct {
    ContainerPort int
    Audience      string // "host" (published) | "internal" (in-network only)
    HostPort      int    // meaningful only for Audience: host; 0 = auto
    ...
}
```

Rules the change enforces:

- **Every listener a dependent may dial must be declared** — this is now a
  documented port-contract requirement, not folklore. K8s creates Service
  ports for both audiences; Docker publishes only `host`.
- **The fake runtime becomes the strict interpreter**: its model refuses
  in-network resolution of undeclared ports and `EnsureReachable` of
  undeclared host ports. The fake — which every engine and provider unit
  test runs against — becomes *stricter than Kubernetes*, so Class 3
  under-declarations fail in `go test ./...`, not on a cluster. (Principle:
  the most permissive runtime must not define the contract; the port spec
  does, and the fake enforces the spec's strictest reading.)
- Migration is mechanical: existing `HostPort: 0` in-network declarations
  become `Audience: internal`; the conformance suite gains subtests for
  both audiences on all adapters.

### F3 — Ready means serving: the runtime port owns readiness semantics

Codify the three-state distinction (exists / ready / reachable) in the port
contract instead of every provider's retry loops:

- **`EnsureReachable` contract hardens**: the returned address must be
  *currently dialable* — the adapter, not the caller, absorbs asynchronous
  programming races (the K3 NodePort poll generalizes to any adapter).
  Conformance subtest: an address returned by `EnsureReachable` accepts a
  TCP connection immediately, every time, including immediately after
  container (re)creation.
- **`WaitHealthy` gains a conformance subtest with a delayed-listen
  container** (healthcheck passes before the TCP listener opens — the D1
  postgres initdb shape): the suite documents whether "healthy" implies
  "declared ports accept connections" per adapter, and `WithReachable`
  (F1.2) covers the gap uniformly where it doesn't.
- Providers delete their bespoke wait loops as they adopt F1.2 — the fix
  count for this class goes from N providers to 1 helper + 1 contract.

### F4 — Identity by handle: one naming authority

Runtime-object names stop being re-derivable folklore:

- `internal/domain/naming` (or equivalent) becomes the **single** authority
  mapping a resource (kind, name) → runtime object name. The realizing
  provider and every consumer (engine probes, drift, inventory, gc) call
  the same function; K7 becomes unwritable.
- Larger direction (recommended, staged): consumers should not need names
  at all. Providers already publish **endpoint facts** into state
  (`internal/domain/endpoint`, consumed by inventory). Extend the fact to
  carry `(runtime object name, containerPort, audience)` and make
  fact-lookup the canonical path for any cross-resource dial (engine's
  Connection probes, Binding wiring). Then the naming convention is an
  implementation detail of exactly one package, and `EnsureReachable`
  callers resolve *facts*, not guesses.

### F5 — The provider invocation contract: a request struct, not accreting setters

The prerequisite for segregating core from provider logic. Today a
provider's inputs arrive through an accretion of optional setter
interfaces — `ProviderResourceAware`, `SecretsAware`, `ResourceSetAware` —
plus method parameters that widen when a need appears (`LineageAware.
ConfigureLineage` grew a `runtime.ContainerRuntime` parameter in
`81025c9`, a breaking change to every implementor). This has three costs:

1. **Interface widening is closed-world**: every new cross-cutting input
   (a reporter, an endpoint publisher, a clock) either breaks all
   implementors or adds another `*Aware` interface + engine special case.
2. **Providers are stateful**: `Set*` before `Reconcile` is a temporal
   coupling the compiler can't check, and state held across calls is
   exactly what an out-of-process plugin cannot have.
3. **The surface is unserializable**: a gRPC plugin protocol (Phase 8)
   needs a defined request/response shape; "call these setters in this
   order, then Reconcile" doesn't translate.

Change: introduce a request-scoped struct as the single provider input —

```go
type Request struct {
    Resource   resource.Envelope
    Runtime    runtime.ContainerRuntime
    Provider   resource.Envelope                  // the realizing Provider resource
    Secrets    map[string]map[string]string       // resolved, by ref name
    Resources  map[resource.Key]resource.Envelope // the validated set
    // additive fields only; a zero field means "not provided"
}
Reconcile(ctx context.Context, req Request) (status.Status, error)
```

Adding a field is non-breaking for every implementor (open/closed at the
contract level — the exact property the `*Aware` pattern lacks). Providers
become stateless per call — the property plugins, parallelism, and
testability all want. The capability *marker* interfaces
(`CDCCapableProvider`, `SinkCapableProvider`, ...) are good and stay:
declaring capabilities by interface is the discovery mechanism; only the
*data-passing* setters are the defect. Migration can be incremental
(engine supports both shapes behind one adapter shim until all nine
providers move; the shim then dies with the old interface).

This is deliberately sequenced **before** doc 08 E6 (provider-author
contract) and Phase 8 (plugins): E6 would otherwise document, and Phase 8
would otherwise serialize, an interface that is already known to be
unstable.

### F6 — The conformance ratchet: no live-found bug without a contract reproduction

Policy, enforced going forward and back-filled for the ledger above:

- **Back-fill now**: entrypoint-image faithfulness subtest (K1 — an image
  *with* an ENTRYPOINT, asserting Cmd appends rather than replaces; still
  absent from the suite today), strict-port audience subtests (F2),
  delayed-listen readiness (F3), reachable-immediately (F3).
- **Policy**: a bug found only by live testing must land with a
  conformance/contract-level reproduction in the same commit — the same
  discipline the repo already applies to schema↔doc sync. If the class
  cannot be expressed at the contract level, that itself is a finding
  (it means a semantic lives outside the port and must be documented in
  doc 07's per-runtime differences table — the K13/B7 isolation case is
  the model).
- **Translation-fidelity gate**: the runtime-parameterized real-examples
  suite (B8's `kubernetes_examples_integration_test.go`) is formalized as
  the acceptance bar for **any** future runtime adapter: conformance green
  is necessary; the unmodified example pipelines reaching Ready is
  sufficient. This is what "the port boundary is sound" means, made
  executable.

---

## 4. Production-plane analysis: what a production data platform separates, and where platformctl stands

Real production data platforms (the reference points: Kafka-ecosystem
pipelines on Kubernetes, Terraform/Helm-provisioned lakehouses, managed-
cloud equivalents) separate concerns into planes. Mapping platformctl onto
them shows *why* the live-testing bugs clustered where they did — and what
is genuinely missing versus already planned.

| Plane | What production requires | platformctl today | Gap owner |
|---|---|---|---|
| **Control plane** (desired state → actual state) | Declarative spec, deterministic plan, drift detection, safe destroy, shared state, audit | Engine/state/plan; drift + heal; NFR-3 guards; S3 shared state with locking (A4) | Sound. One posture note below (§4.1) |
| **Data plane** (brokers, databases, object stores) | Replication, quorum, disruption tolerance, sized storage, backup/restore | Single-instance everything; sized/classed volumes landed (B3) | Doc 08 Stage C (C1–C6) — already planned, correctly scoped |
| **Connectivity & discovery plane** (who can reach what, at which address, with what name) | Service discovery as a *service* — DNS, stable endpoints, gateways, tunnels; consumers never hardcode topology | **This plane did not exist as a layer** — it was smeared across providers as `127.0.0.1` guesses. `EnsureReachable` + endpoint facts + Connection are its embryo | **F1–F4 (this doc) — the missing workstream**; C7/C8/C10, D5 build on it |
| **Security plane** (secrets, identity, transport) | Secret lifecycle, TLS everywhere, authn between components | Secrets strong (preflight, rotation, fingerprints, file mounts, Vault/K8s backends); transport all-plaintext with honest labeling | Doc 08 C7/C8 (TLS/ingress) — planned |
| **Observability plane** | Metrics, logs, alerting; "is it healthy *now*" | `Logs` API, drift probes, progress reporter; no metrics stack | Doc 08 C9 — planned. Alerting stays out of scope (correctly: platformctl provisions, Prometheus alerts) |
| **Governance plane** (schemas, catalogs, lineage) | Schema evolution, columnar formats, catalog, lineage | Nessie catalog, Marquez lineage, but JSON-only (no registry) | Doc 08 D1/D2 — planned; D1 is the hard prerequisite for production formats |

The reading: doc 08 already covers five of the six planes with correctly-
scoped stages. The audit's genuine addition is the sixth — **the
connectivity/discovery plane was never named as a layer**, so its logic
precipitated into whichever provider needed it that day. That is precisely
where 10 of the 17 ledger entries live. F1–F4 constitute promoting it to a
first-class internal layer: providers *publish* endpoints and *request*
reachability; only the runtime layer *answers*; nothing else may.

### 4.1 Posture notes (recorded decisions, no action required)

- **One-shot control plane.** Production platforms typically reconcile
  continuously (operators); platformctl reconciles when invoked, with
  `drift` + heal-on-apply as the manual loop. This is a deliberate product
  posture (determinism as the core contract, doc 01) and does not block
  anything in this document; a `watch`-mode daemon would be purely additive
  on the existing engine if the posture ever changes. Record, don't build.
- **HA databases stay external.** C5's decision note direction is
  confirmed by this analysis: managed single-node + backup/restore +
  drift-heal, production HA via `external: true` through the Connection
  seam. Reimplementing Patroni is not a plane platformctl should own.

---

## 5. Segregation readiness — definition of done

"Segregating the core application from provider-specific logic" (Phase 8:
out-of-process plugins, non-container runtimes) is safe when a provider
authored *outside* this repository, by someone who has read none of the
history above, **cannot reintroduce the ledger's bug classes**. Concretely,
all of the following must hold:

1. **No provider can construct a network address** — F1 landed: domain
   offers no managed-address getter, `WithReachable` is the only dial path,
   the architecture test enforces it. *(Class 1 closed.)*
2. **No provider can under-declare its ports** — F2 landed: audiences are
   explicit, the fake runtime is the strict interpreter, conformance
   covers both audiences. *(Class 3 closed for ports; K1's entrypoint
   subtest closes the rest.)*
3. **No provider needs a hand-written readiness loop** — F3 landed:
   reachability contract + shared helper. *(Class 2 closed.)*
4. **No provider or engine code derives a runtime object name by
   convention** — F4 landed: one naming authority; endpoint facts are the
   cross-resource lookup. *(Class 4 closed.)*
5. **The provider contract is a stateless, additive, serializable
   request/response surface** — F5 landed: `reconciler.Request`, no
   `Set*`-before-Reconcile temporal coupling, no interface widening on new
   inputs. This is the surface a plugin protocol versions and serializes.
6. **The contract is executable** — doc 08 E5 (provider-owned schema
   fragments) + E6 (reconciler conformance suite + author guide) landed
   *on top of* F5, and F6's ratchet keeps the runtime port honest for the
   next adapter.
7. Already true, and must stay true: providers import only `domain` +
   `ports` (the layering invariant); secrets reach providers resolved,
   never as store handles; feature gates guard every provider.

When 1–7 hold, the plugin protocol (Phase 8) is a serialization exercise
over an already-closed contract rather than a redesign — and the Terraform/
external runtime adapter inherits a conformance suite that already encodes
every translation lesson the Kubernetes adapter paid for.

## 6. Changes made to existing plans (same commit)

- **doc 08**: new §7.5 "Stage F — Segregation readiness" carrying tasks
  F1–F6 with sizes/dependencies/acceptance; E6 gains a dependency on F5;
  §10 execution order updated; gap table row 12 added.
- **doc 07**: unchanged — it remains the historical record; its Cross-
  Runtime section's per-bug entries are the primary sources cited here.
- `checkpoint.md`, `errors.md`: untouched — both are historical records
  already superseded by doc 08 for planning purposes.
