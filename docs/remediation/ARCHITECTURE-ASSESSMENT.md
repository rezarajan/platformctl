# Architecture Assessment — audit of `ae99505`

Companion to the findings index in [README.md](README.md). This records the
coherence/extensibility review and, deliberately, the **verified-OK**
results — claims from `docs/planning/07` that were checked against the code
and held, plus audit hypotheses that were disproven. An audit that only
records failures invites re-auditing the same ground.

## 1. Layering invariant

**Verdict: holds in production code.**

- `internal/domain`: imports nothing else in the repo (verified by grep;
  only stdlib + sibling domain packages).
- `internal/ports`: imports `domain` only.
- `internal/application` (excluding `registry`): no adapter imports in
  production code. Two **test** files import adapters — see F-004 (bounded
  fix + waiver documentation).
- Concrete adapters are imported only by `cmd/platformctl/main.go` and
  through `internal/application/registry` constructors, as documented.

## 2. Provider/runtime boundary

**Verdict: coherent and demonstrably runtime-agnostic.**

- `internal/ports/runtime` is consumed by all providers with zero
  runtime-specific imports; the same provider binaries drove Docker and a
  real Kubernetes cluster (conformance suite + unmodified redpanda provider
  — the Cross-Runtime Portability section's claims were re-verified against
  the adapter code: `Cmd`→`Args` mapping, `VolumeSpec.Networks`, alias
  Services, files-Secret, spec-hash annotation all present as described).
- Genuine per-runtime differences are documented at the port
  (`RestartPolicy` under Deployments, `LogConfig`, `SecurityOpt`,
  CPU reservation) rather than papered over — consistent with the
  "never overfit unless truly technology-specific" standard.
- Capability seams (`CDCCapableProvider`, `SinkCapableProvider`,
  `DatabaseSinkCapableProvider`, `IngestCapableProvider`,
  `CatalogCapableProvider`, `ConnectionCapableProvider`,
  `ExternalConfigurer`, `SpecValidator`, `BindingOptionsValidator`,
  `VersionedProvider`, `SecretsAware`, `ResourceSetAware`,
  `ProviderResourceAware`) are all optional interfaces checked at one seam
  each (compatibility or engine) — no provider-type switches outside the
  registry. Extensible by construction.

## 3. Stage-gate verdicts (claims vs. code)

| Gate | Doc claim | Audit verdict |
|---|---|---|
| 0 | complete | **One checkbox unsupported**: machine-readable output — F-001 (graph/validate/`--for` with `-o json`). Everything else in 0.1–0.4, 0.6, 0.7 verified: DNS-label policy + Go-side validation + tests; escaped v2 state keys + migration + tests; ambiguity rejection incl. observers (kind-scoped to Provider in `graph.Build`); `ExternalConfigurer` engine enforcement + test; authoritative deletes + `ActionOrphanUnknown` + rename/type-change tests; hex renderer ids + adversarial tests; loopback default + ownership refusal + live integration tests. |
| 1 | complete | Verified: `ContainerState.Ports`/`HostAddr` + conformance; aliases (+ live DNS test); pull policy + `PullNever` test; `FileMount`/`ReadFile` + conformance incl. env-leak assertion; providers publish observed bindings; postgres/mysql/minio `*_FILE` adoption with rotation fallback; state-dir fsync. Deferrals (registry auth, host paths, 1.3 GC, 1.4 tooling) recorded with reasons — accurate. |
| 2 | complete | Verified: connector-name escaping + tests; `topics.regex` quoting; URL/DSN round-trip tests with real drivers; per-connector `serverID` (+F-006 migration note gap); `BindingOptionsValidator` seam + implementations + negative test; `deletionPolicy` schema/domain/providers/03-doc in one commit; probe upgrades match the §2.1 equivalence table line-by-line (incl. nessie branch check — verified); pinned images (no `latest` remains in providers/examples/testdata); `Endpoint.Insecure` set by all nine publishers + SECURITY column; `--for` views + tests (+F-010 plurality gap); §7.2 pairing availability statement present in 03. |
| 3 | open | No completion claims beyond the two `[x]` rotation items (verified: fingerprint drift + rotation logic + lakehouse rotation coverage exist). Open items re-verified as genuinely open: F-007 (TestProbeTCPReachable), F-008 (`just check`); CI's own gofmt gate is correct (captures output, exits 1) — only the justfile is broken. |

## 4. Hypotheses tested and disproven (verified-OK)

1. **"Probes run without resolved secrets → false ConnectorConfigDrift"** —
   disproven. `Engine.resolveProviderAndRuntime` (engine.go:1001) satisfies
   `SecretsAware`/`ResourceSetAware`/`ProviderResourceAware` before `Probe`,
   identically to reconcile. The `drift` command therefore feeds
   `desiredConnector` everything it needs.
2. **"Observed providerState merge clobbers reconcile facts"** — disproven:
   probe facts land under `providerState.observed`; the reconcile-written
   keys are only replaced by the next reconcile.
3. **"k8s alias Services orphaned on Remove"** — disproven: Services carry
   `app=<container>` + managed-by labels; `Remove` deletes by that
   selector.
4. **"File-mounted secrets leak via spec-hash"** — disproven: both adapters
   hash the spec one-way (sha256) into a label/annotation.

## 5. Recorded risks (no bounded task yet — monitor)

- **Connect config drift relies on GET /config echoing secrets**: the
  probe's equality check on `database.password`/`aws.secret.access.key`
  works because Kafka Connect returns stored config verbatim. If a future
  Connect version or a config-provider indirection masks values, every
  Binding reports permanent drift on those keys. If that lands, the diff
  must special-case masked values (detectable: Connect returns
  `${...}`-style or fixed-mask strings) — do not preemptively weaken the
  check.
- **Kubernetes subPath Secret mounts don't propagate updates**: acceptable
  today because any `Files` content change alters the spec hash and rolls
  the Deployment; if in-place secret refresh is ever wanted, subPath is the
  wrong mount mode.
- **Fake runtime reports observed ports for never-started containers** —
  semantically ahead of Docker (which reports bindings only after start).
  Conformance passes on both because probes run post-start. Worth one
  sentence in `conformance.go`'s doc comment if it ever bites.
- **`graph --format` vs global `-o`** is a two-flag surface for one concern;
  F-001 makes them coexist coherently, but a future consolidation decision
  is flagged for a maintainer, not an implementer.

## 6. Extensibility spot-checks

- Adding a provider: registry + gate + schema + SpecValidator — the
  documented recipe matches reality (checked against the most recent
  provider additions).
- Adding a runtime: the Kubernetes adapter needed one port change
  (`VolumeSpec.Networks`) — evidence the port is near-complete for
  container-shaped runtimes; a Terraform/external adapter will need the
  "parallel, narrower port" already anticipated in 02-architecture §Future.
- Adding a tool view: one renderer + map entry in `toolconfig.go`
  (`knownTools`) — no dispatch drift possible between help text and
  implementation.
