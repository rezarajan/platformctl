# C7: Ingress and HTTP routing on the Connection seam — progress

Task: docs/planning/08-production-readiness-plan.md §5, C7. Size L.
Protocol: doc 08 §2.1 (ADR first, then implement, commit per increment).
(Supersedes this file's previous C2 content — C2 is complete and merged,
commit `ff16dae`/`b9edeb8` on this branch.)

## Steps

0. [done] Merge main (`git merge main --no-edit`) — already up to date (C2 present, `b9edeb8`).
1. [done] Read CLAUDE.md, C7 entry, doc 02 §4.2/§5.2, doc 03 §8.2, ADR 002/015,
   proxy provider (read-only reference), reconciler.go, runtime.go,
   providerkit, status reasons, naming/endpoint packages, main.go wiring,
   doc 04 §12, deploy/kubernetes/rbac/, preflight.go, compatibility.go
   (Connection scheme check), scripts/test-impact.sh map.
2. [done] Design decision: ContainerSpec.Files content participates in the
   Docker spec hash (one-way) — confirmed in internal/ports/runtime/runtime.go
   FileMount doc comment and internal/adapters/runtime/docker/image.go
   specHash (json.Marshal(spec) incl. Files) — so per-Connection route
   config MUST NOT go through ContainerSpec.Files or every route change
   restarts the shared proxy and drops every other Connection's traffic.
   Decision: Caddy (not Traefik) chosen specifically because its Admin API
   is read-write (PATCH/POST/DELETE /id/<id>) so routes reconcile via HTTP
   calls dialed through runtime.WithReachable — never touching ContainerSpec
   after the one-time bootstrap config. Traefik's API is read-only by
   design (introspection only); its dynamic config needs either file writes
   (same hash problem) or a KV backend (new dependency class, ADR 003-style
   rejection). Kubernetes: native Ingress (not Gateway API) — zero-install
   on every cluster, matches the minimal-RBAC precedent (well-known verbs),
   Gateway API CRDs are not guaranteed present.
3. [done] Write docs/adr/018-ingress-routing.md.
4. [done] Implement:
   - internal/ports/runtime/runtime.go: additive IngressCapableRuntime
     interface + IngressSpec/IngressState (optional capability, Docker/fake
     do not implement it — K8s-only surface).
   - internal/adapters/runtime/kubernetes/ingress.go: EnsureIngress/
     RemoveIngress/GetIngress against networking.k8s.io/v1.
   - deploy/kubernetes/rbac/role.yaml + preflight.go + README.md: add
     ingresses.networking.k8s.io verbs.
   - internal/adapters/providers/ingress (new): Provider type "ingress",
     ConnectionCapableProvider{"http"}. Docker/fake path: shared Caddy
     container (EnsureInstance-shaped bootstrap, JSON config via
     ContainerSpec.Files ONCE) + per-Connection route reconcile via Caddy
     Admin API (dialed via providerkit.ReachableURL/runtime.WithReachable).
     Kubernetes path (branch on provider.Provider.RuntimeType, a domain-layer
     field, not adapter introspection): one Ingress object per Connection via
     the new runtime.IngressCapableRuntime capability.
   - cmd/platformctl/main.go: register gate IngressProvider (Alpha, disabled)
     + RegisterProvider("ingress", ...).
   - schemas/v1alpha1/provider.json: add "ingress" to x-known-values +
     configuration doc.
   - docs/planning/03-resource-model-reference.md §8.2: additive note on the
     ingress provider / scheme http / configuration.domain.
   - docs/planning/04-roadmap-and-feature-gates.md §12: append IngressProvider row.
   - scripts/test-impact.sh: append an `ingress` suite row.
5. [done] Unit tests: internal/adapters/providers/ingress/{ingress,caddy}_test.go,
   internal/adapters/runtime/kubernetes/ingress_test.go — all green.
6. [done] Gates: gofmt/build/vet/go test ./... all green.
7. [done] Integration tests written and run live:
   cmd/platformctl/ingress_integration_test.go (Docker),
   cmd/platformctl/ingress_kubernetes_integration_test.go (Kubernetes).
   Live testing found and fixed two real bugs (see below).
8. [done] Doc sync: doc 08 C7 status note appended (additive); docs/adr/018
   addendum recording the registry-wrapper pitfall; docs/reference
   regenerated (no drift after doc edits).
9. [done] scripts/test-impact.sh --base main: **10 selected, 10 ran,
   0 deduped, 0 failed** (broad selection — SHARED_CORE touched by
   runtime.go/registry.go changes cascades). Suite ids + timings
   (2026-07-21, the doc 08 §2.1 step 5 record for the merge gate to cite):
   - docker-conformance: 15.9s
   - k8s-adapter: 373.0s (live minikube, incl. the new ingress_test.go)
   - redpanda: 87.6s
   - cdc: 170.7s
   - sink: 133.5s
   - acceptance: 73.3s
   - lakehouse: 174.3s
   - prometheus: 13.5s
   - ingress (new suite row): 48.5s (TestIngressRoutingEndToEnd +
     TestIngressProviderGateGuardsApply + TestIngressKubernetesEndToEnd)
   - blueprints: 70.6s
10. [done] Squashed the WIP commits into the single task commit
   ("feat(providers): ingress HTTP routing on the Connection seam (C7)").

## Live-testing findings (fixed in this session, both with conformance pins)

1. Caddy bootstrap config used `--adapter json` (not a recognized adapter
   name — JSON is Caddy's native format, needs no adapter flag) — fixed in
   internal/adapters/providers/ingress/docker.go.
2. Caddy's `routes` field needs a real (possibly empty) JSON array in the
   bootstrap config, not an omitted key (`omitempty` dropped it since the
   nil slice), or the first POST-append 500s ("cannot unmarshal object into
   ... RouteList") — fixed in internal/adapters/providers/ingress/caddy.go.
3. **Load-bearing**: `application/registry`'s `haGuardRuntime` wrapper
   embeds the `runtime.ContainerRuntime` *interface* (not the concrete
   adapter), so it never promoted `IngressCapableRuntime`'s methods — every
   runtime obtained through the registry (100% of production paths) failed
   the provider's type assertion, even a real Kubernetes adapter that
   genuinely implements it. Fixed with three explicit delegating methods on
   haGuardRuntime; pinned by
   internal/application/registry/registry_test.go's
   TestRuntime_PromotesIngressCapableRuntime. Documented as an ADR 018
   addendum (a general pitfall for any future optional ContainerRuntime
   capability).

## Verification log

- gofmt: clean.
- go build/vet: clean.
- go test ./...: all green (docs/reference regenerated to pick up the new
  ingress provider.json description; no drift after subsequent doc edits).
- Docker integration: TestIngressRoutingEndToEnd PASS (~11s) —
  http://nessie.localhost:<port> and http://minio.localhost:<port> through
  one shared Caddy container, unrecognized Host routes to neither,
  inventory shows both routed URLs, out-of-band admin-API route mangle
  detected as RouteConfigDrift by `drift` and healed by `apply`, idempotent
  re-apply (proxy container ID unchanged — no restart), clean destroy.
  TestIngressProviderGateGuardsApply PASS — gate-disabled refusal names
  IngressProvider, no half-apply.
- Kubernetes integration: TestIngressKubernetesEndToEnd PASS (~32s, live
  minikube with ingress-nginx addon enabled) — Ingress object created with
  correct Host/backend, idempotent re-apply, out-of-band-mangled Ingress
  heals on next apply, clean destroy. Ran under the ambient (cluster-admin)
  kubeconfig, NOT a freshly minted minimal-RBAC one — minting one needs
  `kubectl apply` of deploy/kubernetes/rbac/*.yaml, which this session's
  auto-mode permission classifier blocked as a protected cluster-mutating
  action. RBAC manifests were still updated with the new verbs (matching
  every other verb this adapter uses) but their *sufficiency* is unproven
  live — recorded as a deviation, remedy given in the doc 08 status note.
- scripts/test-impact.sh --base main: 10/10 green, 0 failed (see step 9
  for suite ids + timings).

## Deviations (record here if any arise)
