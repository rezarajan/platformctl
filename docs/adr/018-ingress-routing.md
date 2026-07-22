# Design note 018 — Ingress and HTTP routing on the Connection seam

**Status:** accepted; implemented (docs/planning/08 Stage C, task C7).
**Prompted by:** docs/planning/08-production-readiness-plan.md §5 C7 —
docs/adr/002 designated `Connection` as *the* ingress seam; `proxy` (a socat
TCP forwarder) is the only realization today. HTTP endpoints (nessie, minio
console, Connect REST, Marquez) deserve hostname routing, not
port-per-service — the same platform-owned-address property proxy already
gives TCP systems, for HTTP.

## The question

Four things need deciding, and they interact:

1. Docker: which reverse proxy fronts every managed HTTP `Connection` on one
   shared container — Caddy or Traefik?
2. Kubernetes: which native routing object realizes a `Connection` —
   `Ingress` or Gateway API `HTTPRoute`?
3. How do per-`Connection` routes reconcile into that one shared Docker
   proxy's live configuration **without restarting it** on every add/change/
   remove — a restart drops every *other* `Connection`'s in-flight traffic,
   which a per-route TCP forwarder (proxy) never risked because each route
   already had its own container?
4. What local-dev DNS story makes `http://<connection-name>.<domain>`
   resolve to the platform's own proxy with zero manual `/etc/hosts` setup?

## Constraint that shapes the whole design: `ContainerSpec.Files` restarts the container

`internal/ports/runtime/runtime.go`'s `FileMount` doc comment states it
plainly: *"Content participates in the spec hash (one-way), so changing it
replaces the container like any other field."* Confirmed in
`internal/adapters/runtime/docker/image.go`'s `specHash` — `json.Marshal(spec)`
covers every exported `ContainerSpec` field, `Files` included — and in
`ensureOneContainer`: a hash mismatch removes and recreates the container.

The `prometheus` provider (docs/planning/08 C9) already pays this cost
deliberately: it is the *only* consumer of a scrape config, so a full
container replace per config regeneration is an acceptable, documented
trade (its own `Reconcile` waits out the restart every time targets
change). A shared HTTP reverse proxy is a different animal: it fronts every
managed `Connection` in the platform, so replaying prometheus's approach
means **every unrelated `Connection`'s add/update/remove restarts every
other `Connection`'s live proxy**, which is a strictly worse regression than
proxy's current one-container-per-route isolation. Whatever technology is
chosen, per-route reconciliation must go through a channel that never
touches `ContainerSpec` after the shared container's one-time bootstrap.

## Decision 1 — Docker reverse proxy: Caddy

### Options considered

1. **Caddy** (chosen). Ships a JSON-native admin API (default `:2019`) that
   is genuinely read-write: `POST` appends a new config node (e.g. a route)
   to an array, `PATCH`/`PUT /id/<id>` replaces an object previously tagged
   with `"@id"`, `GET /id/<id>` reads it back, `DELETE /id/<id>` removes it —
   all live, all hot (Caddy's own docs call this "config adapters and the
   admin API," designed exactly for "reconcile this from outside" use). A
   route reconciled this way never changes `ContainerSpec.Files`, so it
   never touches the spec hash and never restarts the container. Existing
   connections through *other* routes are untouched — Caddy swaps the
   in-memory config graph, not the process.
2. **Traefik**. Its file/directory dynamic-config provider *auto-reloads*
   on file change (`--providers.file.watch=true`) with no restart of its
   own — but getting a changed file *into* the container without restarting
   *the container* runs straight into the `ContainerSpec.Files` problem
   above (the only file-placement channel this runtime port has is
   spec-hashed). The alternative dynamic-config providers that don't need
   file placement (Consul KV, etcd, Redis) all introduce a **new dependency
   class purely to hold reverse-proxy config** — the same shape of tradeoff
   design note 003 already worked through for shared state and rejected for
   the identical reason (a new infrastructure dependency the rest of the
   product doesn't otherwise need). Traefik's HTTP API, unlike Caddy's, is
   deliberately **read-only** (router/service introspection only,
   `/api/http/routers` etc.) — there is no supported "PUT this one route"
   call to reconcile against, by design; Traefik's model is "config comes
   from a provider," not "config comes from API calls," so it does not fit
   this project's reconcile-by-calling-an-API-idempotently shape at all.
3. **nginx** (`nginx -s reload` after a file rewrite). Same
   `ContainerSpec.Files` hash problem as Traefik's file provider, plus the
   reload itself is a SIGHUP to the master process — briefly-open
   connections during a reload are nginx's own well-known caveat, and there
   is no live per-route API at all (config is monolithic text, not
   independently addressable objects), so drift healing would mean
   re-rendering and reloading the *entire* file for a one-route change
   rather than a scoped `PATCH`.

### The decision

**Caddy**, pinned (`caddy:2.x@sha256:...`, resolved and recorded in
`internal/adapters/providers/ingress/ingress.go`'s `defaultImage` and
`scripts/pinned-images.txt`), specifically *because* its admin API is
read-write and object-addressable (`@id` tags) — the one property that lets
per-`Connection` reconciliation stay a plain idempotent HTTP call, matching
every other config-bearing provider in this codebase (Kafka Connect's REST
API for debezium/s3sink connectors is the closest existing precedent: a
shared worker, per-Binding config reconciled via API calls, never a file
rewrite).

## Decision 2 — Kubernetes: `Ingress`, not Gateway API `HTTPRoute`

### Options considered

1. **`networking.k8s.io/v1 Ingress`** (chosen). Built into the Kubernetes
   API since 1.19 on every conformant cluster — no CRDs to install, no
   controller-compatibility matrix to document. `deploy/kubernetes/rbac`'s
   minimal-RBAC posture (docs/planning/08 B5) already depends on granting
   exactly the well-known core/apps/networking verbs this adapter uses;
   `Ingress` extends that same well-known surface (`ingresses.networking.k8s.io`)
   rather than introducing a new API group a minimal role has to discover.
2. **Gateway API `HTTPRoute`**. More expressive (traffic splitting, header
   matching, cross-namespace routing) and the forward-looking direction of
   the ecosystem, but its CRDs (`gateway.networking.k8s.io`) are **not
   installed by default** on every cluster (not even every distro's
   "batteries included" set) — the acceptance bar this codebase holds
   itself to (docs/adr/015: "unmodified example pipelines reaching Ready is
   the acceptance bar," docs/planning/08 B5: "a documented minimal RBAC
   manifest is sufficient for the full suite, verified by running CI's K8s
   job under it") would require this task to *also* ship and document CRD
   installation as a cluster prerequisite, on top of the RBAC posture,
   before a single `Connection` could reach Ready. `Ingress` needs neither.

### The decision

**One `Ingress` object per managed HTTP `Connection`** (mirrors the task's
own stated shape and proxy's "one forwarder per Connection" precedent —
just a declarative object instead of a container).
`internal/adapters/runtime/kubernetes/ingress.go` adds `EnsureIngress`/
`RemoveIngress`/`GetIngress`. No shared proxy *container* is needed on
Kubernetes at all: the cluster's own ingress controller (whichever one is
installed — this codebase never provisions one, matching NG1: platformctl
provisions the infrastructure a request routes *through*, never the
traffic-serving software layer beyond what a `Connection`'s own realizing
Provider stands up) does the actual proxying; `Ingress` objects are pure
declared intent, so there is no reload-without-restart problem to solve on
this runtime at all — the K8s ingress controller's own hot-reload is
already outside platformctl's remit, the same way Kafka's own broker
rebalancing is outside the redpanda provider's remit.

### Layering: how the `ingress` provider learns which runtime it is on

`ContainerRuntime` is deliberately a black box — no provider is supposed to
ask "which runtime am I on," and every other multi-runtime provider (C1's
replicas, B1's access modes) stays runtime-agnostic entirely through
`ContainerSpec`/`ContainerState` fields the two adapters interpret
differently. Ingress is a genuine exception: Docker's realization is a
managed container the provider creates and calls into; Kubernetes's is a
declarative object with no shared container at all — these are not two
interpretations of the same `ContainerSpec`, they are structurally
different mechanisms. The provider branches on
`provider.Provider.RuntimeType` (`internal/domain/provider`) — a plain
domain-layer string already parsed from `spec.runtime.type` for every
provider (`postgres` and `redpanda` already read sibling `RuntimeConfig`
keys the same way) — never on a type-assertion against a concrete adapter
package (which would violate the domain/ports/adapters layering
invariant). The new `runtime.IngressCapableRuntime` interface
(`internal/ports/runtime`) is declared in the port package exactly like
every other optional capability in this codebase (`SpecValidator`,
`VersionedProvider`, ...), just checked in the opposite direction (a
*provider* type-asserting a *runtime*, instead of the *engine* type-asserting
a *provider*) — `ContainerRuntime`'s existing method set is untouched, so
Docker's and fake's existing conformance behavior is unaffected; only the
Kubernetes adapter implements the new interface.

## Decision 3 — the reload-without-restart mechanism (Docker)

Established by Decision 1's reasoning, made concrete:

- **Provider-level reconcile** (`Provider(type: ingress)`, kind `Provider`):
  bootstraps the shared Caddy container exactly once via the normal
  `EnsureContainer` path — a minimal JSON config (`admin` listening
  in-network, one `http` server named `srv0` listening on
  `configuration.port`, an empty `routes` array) placed via
  `ContainerSpec.Files`. This *is* spec-hashed, and *does* restart the
  container — correctly, because it only changes when the bootstrap shape
  itself changes (the HTTP listen port, the image), never per `Connection`.
- **Connection-level reconcile**: never touches `ContainerSpec` again.
  Dials the already-running Caddy container's admin API via
  `providerkit.ReachableURL`/`runtime.WithReachable` (ADR 015: a
  freshly-resolved address per attempt, never a constructed
  `127.0.0.1:port` literal) and reconciles exactly one route, tagged
  `"@id": "route-<connection-name>"`:
  - `PATCH /id/route-<name>` (replace-if-exists) first; on 404, `POST
    /config/apps/http/servers/srv0/routes/` (append) to create it.
  - `DELETE /id/route-<name>` on `Connection` destroy.
  - `Probe` does `GET /id/route-<name>` and diffs the live route's `Host`
    match and `reverse_proxy` upstream dial address against what the
    `Connection`'s own spec would generate — the same "drifted *names*, not
    values" bar `debezium`/`s3sink`/`prometheus` already hold for connector/
    scrape config drift. A route deleted or hand-edited out-of-band
    (the C7 accept criterion's "mangled route") is caught this way and
    healed by the same `PATCH`/`POST` path on the next `apply` — no
    restart, no effect on any other route.
- **Upstream addressing** stays a straight passthrough of
  `Connection.spec.target` (`host:port`, e.g. `nessie:19120`) into Caddy's
  `reverse_proxy` `dial` field — exactly proxy's existing discipline
  (`"tcp-connect:" + conn.Target`, unchanged since docs/adr/002). This is
  not a re-derived/constructed address (the ADR 015 violation this note's
  task explicitly warns against): the provider never guesses a container
  name or port from convention, it passes through the one string the
  manifest author declared, letter for letter — the same trust boundary
  `Connection.Target` already has for proxy's TCP case. A future
  `targetRef`-style resolution against published endpoint facts (the same
  engine-resolved-`Request`-field pattern `SchemaRegistryURL`/
  `MetricsTargets` use) is a natural follow-up if hand-typed targets prove
  error-prone in practice — not required for this task, and not boxed out
  by this shape.

## Decision 4 — local-dev DNS: `*.localhost`

No code, a documented fact: modern resolvers treat the `.localhost` TLD as
loopback without any configuration — glibc/systemd-resolved and macOS's
resolver both special-case it (RFC 6761 reserves `.localhost` for exactly
this), so `nessie.localhost`, `minio-console.localhost`, etc. resolve to
`127.0.0.1`/`::1` on essentially every developer machine with zero
`/etc/hosts` editing. `Provider(type: ingress).spec.configuration.domain`
(default `"localhost"`) is the suffix every `Connection`'s `Host(...)` rule
is built from (`<connection-name>.<domain>`); documented in
docs/planning/03 §8.2 and this note so a team pointing the domain at a real
DNS zone in a shared environment knows exactly which one field to change.

## Scope: TLS is explicitly out (C8's seam)

This task ships **plaintext HTTP routing only** — every endpoint this
provider publishes carries `Insecure: true` honestly (docs/planning/07
§2.5's standing rule), the same posture every other v1 endpoint has today.
`ConnectionCapableProvider.SupportedConnectionSchemes()` returns exactly
`["http"]` — **not** `["http", "https"]` — so a `Connection` declaring
`scheme: https` fails the standard capability error at `validate`
(`internal/application/compatibility`'s existing "does not support
connection scheme" message, naming what's supported) rather than silently
serving plaintext under an `https` label. This is deliberate, not an
oversight: C8 is the task that adds `Connection.spec.tls` (a `secretRef` to
a cert/key pair, or `{selfSigned: true}` for a local CA) and teaches this
same provider to terminate TLS at the Caddy/Ingress entrypoint — at which
point `SupportedConnectionSchemes()` grows to include `"https"` and
previously-`http` `Connection`s are unaffected. Nothing in this shape boxes
that out: Caddy's JSON config already has a native `tls` app block this
provider's bootstrap config simply doesn't populate yet, and
`networking.k8s.io/v1 Ingress` already has a `spec.tls` field this
provider's `EnsureIngress` call simply leaves empty yet.

## Feature gate

`IngressProvider` — Alpha, disabled by default (docs/planning/04 §12),
following the `TrinoProvider`/`JDBCSinkProvider` posture (design note 006):
a new provider exposing a new network-reachable surface (an HTTP reverse
proxy accepting arbitrary Host headers) defaults off until soaked, not the
Phase 6.5 enabled-Alpha precedent reserved for providers with no new
externally-reachable attack surface.

## Addendum (2026-07-21) — a registry-wrapper pitfall, found live

`application/registry.Runtime` wraps every constructed runtime in
`haGuardRuntime` (the `HighAvailability` gate guard). That wrapper embeds
`runtime.ContainerRuntime` — the **interface**, not the concrete adapter
type — so it only promotes the interface's own declared method set; a
provider's `req.Runtime.(runtime.IngressCapableRuntime)` type assertion
(this note's own "Layering" section, above) failed for *every* runtime
obtained through the registry, including a real Kubernetes adapter that
genuinely implements `EnsureIngress`/`GetIngress`/`RemoveIngress` — because
Go's embedded-interface method promotion is bounded by the embedded field's
*static* type, not whatever concrete value happens to be stored in it. The
Kubernetes adapter's own fake-clientset unit tests call `EnsureIngress`
directly and never pass through this wrapper, so nothing short of an
end-to-end `apply` against a real cluster caught it — exactly the class of
bug docs/adr/015's F6 ratchet exists for. Fixed by giving `haGuardRuntime`
three explicit delegating methods (`internal/application/registry/
registry.go`), pinned by `TestRuntime_PromotesIngressCapableRuntime`. Any
future optional `ContainerRuntime` capability added the same way
(`IngressCapableRuntime`'s pattern) needs the identical explicit-delegation
treatment on `haGuardRuntime` — an embedded-interface wrapper never grows
new capabilities for free.

## Follow-ups (non-blocking)

- C8: TLS termination at this same seam (`Connection.spec.tls`), per the
  scope note above.
- A shared refactor between `proxy` (TCP) and `ingress` (HTTP) was
  considered and deliberately **not** taken in this task — both realize
  `Connection` and both forward to a `spec.target`, but proxy's
  one-container-per-route shape and ingress's one-shared-container(-or-
  none)-with-API-reconciled-routes shape are different enough (and proxy
  is explicitly out of scope / read-only reference for this task, per
  docs/planning/08's file-ownership note) that forcing a shared abstraction
  now would guess at the seam before a second HTTP-scheme provider exists
  to prove it. Revisit if/when a second `ConnectionCapableProvider`
  realization needs the same reconcile-via-API shape.
- `targetRef`-style resolution of the upstream address against published
  endpoint facts (see Decision 3's upstream-addressing note) instead of a
  hand-typed `spec.target` string, if that proves error-prone in practice.
- `Provider(type: ingress).spec.configuration.ingressClassName` for
  clusters running more than one ingress controller — not needed for the
  single-controller CI/dev clusters this task verifies against; the field
  is a natural additive follow-up, not a redesign.

## Cross-references

- docs/adr/002 (Connection's origin as the ingress seam) and docs/adr/015
  (the connectivity/discovery plane this design builds on — "any design that
  hands a provider a constructed address is wrong by definition").
- `internal/adapters/providers/proxy` — read-only reference for this task;
  the existing managed-Connection/external-Connection lifecycle split this
  provider reuses unchanged.
- docs/planning/03-resource-model-reference.md §8.2 (`Connection`).
- docs/planning/08-production-readiness-plan.md §5 C7 (this task) and C8
  (TLS, the next task on this seam).

## Addendum (2026-07-22) — C8: TLS termination and certificate handling

`Connection.spec.tls: {secretRef | selfSigned | secretName}` (exactly one),
requiring `scheme: https` — see docs/planning/03 §8.2.2 for the manifest
shape. `SupportedConnectionSchemes()` grows to `["http", "https"]`.

### The `ContainerSpec.Files`/spec-hash question, resolved by inspection

C8's task text asked whether `Files` content leaking into the spec-hash
label was a real risk. Read `internal/adapters/runtime/docker/image.go`'s
`specHash`: it is `sha256(json.Marshal(spec))` — **one-way**, so the label
itself never leaks plaintext material. The real reason certificates never
go through `ContainerSpec.Files` is the *other* half of Decision 3's
reasoning, extended: a content **change** changes the hash, and per
`ensureOneContainer` a hash mismatch **replaces the container**. For a
per-Connection leaf certificate that would reproduce exactly the
restart-blast-radius problem Decision 3 already solved for routes — one
Connection's cert rotation would restart the shared proxy and drop every
other Connection's live traffic. So: certificates load exclusively through
Caddy's admin API (`/config/apps/tls/certificates/load_pem`, `@id`-tagged
exactly like routes), never via `Files`. The **Provider-scoped local CA**
keypair is the one exception, and only because it is Provider-scoped, not
per-Connection: it changes as rarely as the bootstrap config itself (only
when genuinely missing), so it persists via `Files` using the identical
read-existing-before-regenerate pattern `postgres`'s superuser-password
rotation already established (`rt.ReadFile` before ever calling
`EnsureContainer` with new content) — never Caddy's own concern, since
Caddy only ever receives already-signed leaf certificates via the admin
API, never the CA private key itself.

### Caddy TLS admin-API shape — found live, not by reading docs

A real `caddy:2.9.1` container was used to work out the exact JSON shape
before writing any Go, because two things could not have been predicted by
reasoning about the docs alone:

1. A server's `automatic_https: {disable: true}` does **not** make it speak
   TLS — it only suppresses ACME/on-demand issuance and the automatic
   HTTP→HTTPS redirect. A listener with only that setting still speaks
   plain HTTP; `curl` failed with "wrong version number" until an explicit
   `tls_connection_policies: [{}]` (one empty policy — "select a cert by
   SNI from whatever is manually loaded") was added to the server. This is
   what actually turns a listener into a TLS terminator.
2. `GET /id/<id>` on an **unknown certificate** `@id` returns **404** —
   routes return 400 for the identical "no such object" case (already
   documented above, Decision 3). Both mean "does not exist right now";
   `getCert`/`getRoute` (`internal/adapters/providers/ingress/caddy.go`)
   each treat their own status code correctly.
3. `GET` on a **loaded** certificate's `@id` echoes the private key back in
   plaintext — Caddy's admin API is a genuinely read-write config surface,
   not a write-only secret store. This is not a new exposure: the admin
   endpoint's trust boundary (shared network, unauthenticated) was already
   documented in `caddy.go` before C8 for the identical reason Kafka
   Connect/nessie/prometheus's REST APIs carry credentials in-band.

Concretely: a second Caddy HTTP-app server (`srv1`, container-internal port
443 — Caddy's own default `https_port`, so its listener-recognition logic
needs no override) hosts every `https`-scheme Connection's route, with
`tls_connection_policies` set once at bootstrap; `srv0` (plain HTTP,
container-internal port 80) is unchanged since C7.

### Kubernetes: `Ingress.spec.tls` + a new `IngressCapableRuntime` capability

`runtime.IngressCapableRuntime` (Kubernetes-only, per this ADR's own
"Layering" section above) gains `EnsureTLSSecret`/`GetTLSSecret`/
`RemoveTLSSecret` — a plain `kubernetes.io/tls`-shaped Secret (`tls.crt`/
`tls.key`), reused for both a Connection's own leaf certificate (referenced
by `IngressSpec.TLSSecretName`) and the Provider-scoped local CA (never
referenced by any `Ingress`, stored purely for `GetTLSSecret` to read back
before regenerating — the Kubernetes-side equivalent of Docker's
`ContainerSpec.Files`/`ReadFile` persistence). **No new RBAC verb**:
`deploy/kubernetes/rbac/role.yaml`'s `secrets` entry already grants
`get/create/update/delete` cluster-wide (needed since A1/`ContainerSpec.
Files`) — confirmed by inspection before writing any Secret-handling code,
not assumed.

`spec.tls.secretName` (cert-manager) is referenced only: the Ingress
object's `spec.tls[].secretName` points at it, `GetTLSSecret` reads it to
report readiness, and `RemoveTLSSecret` is never called for it — mirroring
this whole design's "integration = referencing, not operating" rule for
external tooling (docs/planning/08 C8 task text). A not-yet-issued
cert-manager Secret is `Ready: false` / `CertMissing`, not an error — the
same eventually-consistent posture the `SchemaRegistryURL`/`CatalogFacts`
patterns already have for a not-yet-published upstream fact.

### Gate: `TLSTermination`, independent of `IngressProvider`

Registered separately (`cmd/platformctl/main.go`) because a Connection can
stay plaintext even after `IngressProvider` itself graduates — TLS is a
per-Connection opt-in, not a property of the provider type. No existing
enforcement choke point fit a manifest-declared per-*resource* field (not a
distinct provider type like `IngressProvider`/`BackupRestore`; not a
CLI-flag behavior like `DriftDetection`/`ParallelReconciliation`): a new
`registry.Registry.RequireGate` public method is the one choke point every
`Reconcile`/`Probe`/`Destroy` call for a TLS-declared Connection passes
through (`engine.resolveRequest`) — deliberately mirroring
`HighAvailability`'s own admitted-imperfect backstop-at-point-of-use
pattern (`haGuardRuntime.EnsureContainer`) rather than inventing a second
gating mechanism.

### Follow-ups (non-blocking)

- `Provider(type: ingress).spec.configuration.ingressClassName` (already
  named as a C7 follow-up) would also let a multi-controller cluster pick
  which controller serves a TLS-terminated `Ingress` — no new interaction
  with this addendum's shape.
- The self-signed leaf certificate's validity window (90 days,
  `internal/adapters/providers/ingress/tls.go`) has no rotation *warning*
  surfaced anywhere yet — Probe's structural check
  (`certValidForHost`/`certChainsToCA`) already fails Ready 24h before
  expiry, which forces a `Reconcile` to reissue on the next `apply`, but
  there is no proactive "your CA/cert expires soon" signal between applies.
  Acceptable for Alpha/dev use; a real production TLS story routes through
  `secretRef`/`secretName` (an operator- or cert-manager-owned rotation
  lifecycle) rather than this provider's own reissue-on-apply behavior.
