# E6 (doc 08 §7, re-scoped by ADR 028 as the fast-tier cornerstone) — task progress

Worktree: agent-a6e3c4b7cec38c5e6. Branch: worktree-agent-a6e3c4b7cec38c5e6.
Started from main @ ed9dffd (`git merge main --no-edit`, fast-forward — pulled
in ADR 028 test-tiering + the E6 re-scope note).

This file replaces the prior (H6/openziti, already merged) TASK_PROGRESS.md —
that task closed and its content is preserved in git history
(`git log --follow`).

## Reading done (in order)

- [x] CLAUDE.md.
- [x] docs/planning/08 §7 task E6 entry (Size L, Depends E5+F5, Context doc
      07 §3.4, Do/Accept as originally written) + the Stage E exit criteria
      block (line ~1727) — the unticked bullet is the target of this task's
      evidence.
- [x] docs/planning/08 §2.1 (task execution protocol, step 0 checkpoint
      rule) — doc 06 has no §2.1; the task prompt's "doc 06 §2.1" reference
      resolves to doc 08 §2.1 (doc 06 §10 is the impact-economy section,
      correctly named elsewhere).
- [x] docs/adr/028-test-tiering.md in full — the rescoping ADR: E6's suite
      IS the fast-tier's provider middle, must run against fakes in
      milliseconds, "fakes must be honest" (§2), CI is the arbiter.
- [x] docs/planning/07-production-grade-docker-runtime-gap-analysis.md §3.4
      (Contributor-Facing Provider Runtime Contract) — the original
      requirement list this task's Do items trace to.
- [x] internal/ports/reconciler/reconciler.go in full (681 lines) — Request/
      Facts doc comments read as the guide's source material per the task
      prompt; Provider + all capability interfaces (ExternalConfigurer,
      CDCCapableProvider, SinkCapableProvider, DatabaseSinkCapableProvider,
      IngestCapableProvider, CatalogCapableProvider,
      ConnectionCapableProvider, MediationCapableProvider,
      TunnelCapableProvider, ViaConsumingProvider, VersionedProvider,
      SpecValidator, BindingOptionsValidator, SchemaRegistryCapableProvider,
      KafkaBootstrapAddressProvider, StreamReplicationValidator,
      LineageAware, DesignLinter, BackupCapableProvider).
- [x] internal/ports/runtime/conformance (all 8 files) — the pattern to
      mirror: Run(t, rt, namePrefix) driving per-area run*(t, rt, fx)
      helpers against fixtures; MutationCounter optional interface; adapter
      test files (fake_test.go, docker_integration_test.go) call
      conformance.Run with their own constructed adapter instance — the
      conformance package itself imports ONLY ports, never an adapter
      (layering). This is the exact shape internal/ports/reconciler/
      conformance must mirror.
- [x] docs/planning/02-architecture.md §4.1 (settledness/NFR-11 exact
      wording, ScaledWait), §4.2 (Provider/capability interfaces, Facts,
      the five-deprecated-field history), §5.2 (the ONE place an exact
      error-string format is specified in doc 02 — the Binding compatibility
      message; owned by internal/application/compatibility, NOT reachable
      from internal/ports/reconciler/conformance by layering — ports may
      not import application), §9 (testing strategy — names
      `runtime.ConformanceSuite` as the existing pattern), §11 (extensibility
      guide — new-provider recipe).
- [x] docs/onboarding/developers.md in full — "Your first contribution:
      adding a provider" section explicitly says "The full author guide +
      conformance suite is doc 08's task E6 (not yet landed)" — this task
      updates that line once the guide exists.
- [x] README.md's "Writing your own provider" section (line 198) — the
      link target this task's guide fulfils.
- [x] internal/adapters/providers/noop/noop.go (42 lines, trivial —
      Reconcile/Destroy/Probe, no runtime calls, no ProviderState).
- [x] internal/adapters/providers/redpanda/redpanda.go (973 lines) +
      kafka.go — Provider-kind reconcile (reconcileBroker) is pure
      container-lifecycle against runtime.ContainerRuntime, no real Kafka
      wire protocol; EventStream-kind reconcile (reconcileTopic) dials a
      real Kafka admin client (kadm/kgo) with NO port/interface seam to
      fake — confirmed by reading redpanda_test.go: existing unit tests
      (TestReconcileBrokerRegistryDisabled etc.) already only exercise the
      Provider-kind path against the fake; EventStream is integration-only.
      This task's redpanda exemplar therefore drives the Provider (broker)
      kind only — a deliberate, documented scoping decision, not an
      oversight.
- [x] internal/adapters/providers/proxy/proxy.go + proxy_test.go — Connection-
      kind reconcile (reconcileConnection) is the richer exemplar: real
      settledness logic (waitForwarderServing dials THROUGH the fake
      container's reported host address). proxy_test.go's own established
      "fake-technology harness" trick: a real net.Listen("tcp","127.0.0.1:0")
      stands in for "the upstream", with the Connection's spec.port set to
      that real listener's port — the fake runtime's EnsureContainer/Inspect
      never runs a real socat process, but observedPorts reports a
      dialable 127.0.0.1:<port> HostAddr matching the real listener, so
      probeThroughForwarder's dial genuinely succeeds. This is the
      fake-technology-harness pattern the conformance suite's proxy
      exemplar reuses directly.
- [x] internal/adapters/runtime/fake/fake.go in full — MutationCount
      increments ONLY on real state change (idempotent Ensure* calls are
      confirmed zero-cost) — this is what makes "second-reconcile
      idempotency (zero mutating runtime calls)" a mechanical assertion
      (Mutations() delta == 0 across two Reconcile calls).
- [x] internal/domain/status/status.go (Condition/Status/IsReady/
      SetCondition), internal/domain/provider/provider.go (FromEnvelope),
      internal/domain/resource/resource.go (Envelope/Key/Metadata),
      internal/domain/endpoint/endpoint.go (List/ToState/FromState/Key —
      the providerState publication decode target), internal/domain/
      connection/connection.go (Connection/TLS shapes for the proxy
      fixture).
- [x] internal/application/registry/registry.go + cmd/platformctl/main.go's
      defaultWiring (noop/redpanda/proxy RegisterProvider call sites) — the
      exact `func() reconciler.Provider { return X.New() }` constructor
      shape the task's "registered constructors" phrase names; the
      exemplar tests use this identical shape.

## Design decisions locked in before coding

1. **Package location:** `internal/ports/reconciler/conformance` — mirrors
   `internal/ports/runtime/conformance` exactly (ports-only import set:
   `reconciler`, `runtime`, `resource`, `status`, `endpoint`, `testing`,
   `context`, `time`; never an adapter — the fake runtime is supplied BY THE
   CALLER, exactly like runtime/conformance.Run takes an already-constructed
   `runtime.ContainerRuntime`).
2. **Harness shape** (the "provided fake-technology harness" the task
   names): a struct of caller-supplied closures, not an interface a provider
   package must implement on its own Provider type — keeps the contract
   test-only and out of the provider's own production API surface.
   - `NewRuntime func() runtime.ContainerRuntime` — a fresh, isolated fake
     runtime per subtest (parallel-safe).
   - `Provider func() reconciler.Provider` — the registered-constructor
     shape verbatim.
   - `Resource func(rt runtime.ContainerRuntime, namePrefix string, i int) reconciler.Request`
     — builds fixture `i`'s Request; `i` (0/1) lets one Harness produce two
     independently-named fixtures for the statelessness/interleaving
     subtest. This is where a provider's own "fake-technology" trick
     (proxy's real net.Listener) lives — entirely in the calling
     `_test.go` file, never in the conformance package itself.
   - `CapabilityChecks func(p reconciler.Provider) []CapabilityCheck`
     (optional/nilable) — doc 02 §4.2 capability-interface error-format
     checks, for providers that declare an error-returning capability
     method (SpecValidator, StreamReplicationValidator, ...). nil when a
     provider declares none (proxy, noop) — the suite must not fabricate a
     check where the provider doesn't own one; the Binding-vs-Provider
     compatibility message format (doc 02 §5.2's exact string) is owned by
     `internal/application/compatibility` and out of reach here by
     layering (ports must not import application) — noted explicitly in
     the guide, not silently glossed over.
3. **Scope is the Reconcile/Probe/Destroy lifecycle contract**, not every
   capability interface's own behavior (those are provider-specific and
   already unit-tested per-provider) — the suite proves: settledness
   (Ready implies immediately probe-clean), idempotency (zero mutating
   calls on re-reconcile), Probe honesty (point-in-time, no internal
   wait-loop), Destroy convergence (incl. already-gone), Request
   statelessness (interleaved fixtures don't cross-contaminate), and —
   generically, from providerState alone — that any published `endpoint.List`
   entry carries a real fact (non-empty Host or Internal), never a blank
   placeholder (ADR 015).
4. **Exemplars:** noop (trivial — proves the suite doesn't crash on a
   provider with zero runtime calls and zero ProviderState), redpanda
   Provider-kind/broker (container-lifecycle shape, exercises
   CapabilityChecks via SpecValidator+StreamReplicationValidator), proxy
   Connection-kind (real settledness/dial-through shape, real
   fake-technology-harness net.Listener trick). Three distinct shapes —
   this is the "generality" proof the task asks for.

## Status

- [x] Design locked (this file, step 0 commit)
- [x] internal/ports/reconciler/conformance package (conformance.go) —
      Harness{NewRuntime, Provider, Resource, CapabilityChecks}, 7 subtests
      (Settledness, Idempotency, Probe honesty, Destroy convergence,
      Statelessness, ProviderState publication, CapabilityErrorFormats),
      all t.Parallel().
- [x] noop exemplar test (internal/adapters/providers/noop/conformance_test.go)
      — trivial provider, CapabilityChecks nil, no ProviderState.
- [x] redpanda exemplar test (.../redpanda/conformance_test.go) — Provider
      (broker) kind only, deliberately not EventStream (needs real Kafka
      wire protocol — documented in the file's own doc comment);
      CapabilityChecks exercises SpecValidator + StreamReplicationValidator.
- [x] proxy exemplar test (.../proxy/conformance_test.go) — Connection kind,
      real net.Listener fake-technology harness (reused from proxy_test.go's
      own established trick). **Found + fixed live:** proxy.go's
      probeThroughForwarder had a hardcoded 1500ms read-deadline (no var to
      shrink, unlike forwarderSettleTimeout/forwarderSettlePoll in the same
      file) — blew the fast-tier sub-second budget (suite measured 6.008s).
      Extracted it to a package-level `probeReadDeadline` var (default
      unchanged, 1500ms — zero production behavior change), mirroring the
      existing forwarderSettleTimeout pattern exactly; the conformance test
      shrinks it once, before any t.Parallel() subtest starts (never
      per-subtest — would race). Re-verified: proxy suite now 0.086s.
      go test -race clean across proxy/redpanda/noop/fake/ports/... after
      the fix.
      Also found: fake.Runtime.Mutations() was defined in fake_test.go
      (test-only), invisible to an external importer's own _test.go file —
      moved it into fake.go as a proper exported method (same body, now
      also mutex-guarded for -race safety under concurrent subtests); this
      is what makes MutationCounter usable from ANY provider's own
      conformance exemplar, not just fake's own package.
- [x] docs/contributing/provider-authoring.md — lifecycle semantics,
      capability-interface index table, Request/Facts (generic form only),
      fragments (E5), endpoint publication (ADR 015), drift/reason
      conventions, feature-gate procedure (ADR 014), conformance-suite
      Harness walkthrough, ADR 028 §2 fake-honesty rule, worked-examples
      pointer to the three exemplars.
- [x] README.md "Writing your own provider" section links the guide;
      developers.md's "not yet landed" line replaced with a link (both
      files are outside docs/planning/, edited directly — no guard hook).
- [x] doc 08: Stage E exit criterion ticked (line ~1732) + E6 Done-note
      appended (additive, guard-hook-passing) — states the scope honestly
      against the pre-ADR-028 Accept text (3 exemplars, not "all
      nine-plus"; guide validated via 3 real exemplar conformance_test.go
      files, not a from-scratch noop rebuild) and names two explicit
      follow-ups (retrofitting remaining providers; the compiled-in-vs-
      plugin decision note).
- [x] gofmt -l . empty; go build/vet ./... and -tags integration ./... all
      clean; golangci-lint v2.12.2 (CI-pinned) run ./... = 0 issues.
- [x] go test ./... ; echo true-exit=$? → true-exit=0 (unfiltered, every
      package, ~15s wall total for the whole repo).
- [x] Conformance suite runtime reported (see the doc 08 Done-note for the
      full breakdown): plain `go test -run TestConformance` per exemplar —
      noop 0.002s, redpanda 0.003s, proxy 0.084s; under -race — noop
      1.012s, redpanda 1.012s, proxy 1.093s (race instrumentation +
      startup overhead, not suite logic). All sub-second per the Gates bar.
- [x] final squashed commit

## Deviations (recorded as found)

(none yet)
