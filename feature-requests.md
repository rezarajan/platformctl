## Better Progress Output

**Status: delivered.** `apply` now streams a Docker-style progress display
(a `Reporter` interface in the engine, rendered by
`internal/cliutil/ProgressReporter`):

```
Reconciling 14 resources:
  [1/14] ◐ create Provider/local-redpanda
  [1/14] ✓ create Provider/local-redpanda (2.7s)
  [2/14] ◐ create EventStream/attendance-events
  [2/14] ✓ create EventStream/attendance-events (0.21s)
  ...
✓ 14 applied
```

- **Order + remaining**: every step is tagged `[n/total]`.
- **Currently running**: a `◐` "started" line precedes each `✓`/`✗` "done"
  line, so in-flight steps are visible (with `--parallelism>1`, multiple
  show as started before they finish).
- **Per-step timing** on completion; **drift-heal** (`⟳`) and dependency
  **skip** (`⊘`) lines; a coloured final summary. Colour is TTY-gated and
  honours `NO_COLOR`; non-TTY (CI, pipes) gets clean plain lines. A clean
  re-apply stays quiet. Progress → stderr, so stdout stays scriptable.

Tests: `internal/cliutil/progress_test.go`.

### Original request

The current implementation simply says ok/fail on apply steps, but does not list the order of steps, give the user an idea of remaining output, outlog status logs, or have an indicator showing a current step which is running (or those running in parallel). The idea is to have something like how docker's cli interface is.

## Interactive Service/Inventory Explorer

**Status: delivered.** Two complementary commands:

- **`platformctl inventory [path]`** (aliases `services`, `endpoints`) reads
  recorded state and prints every service endpoint each applied component
  publishes — logical name, scheme, host-reachable address, in-network
  address — paired with the SecretReference that holds its credentials. This
  is the reference chart you configure Dagster/Metabase/psql against, no
  YAML-parsing required. `-o table|json|yaml`. Providers publish structured
  endpoints via the new `internal/domain/endpoint` type (a stable
  access-identifier vocabulary independent of any technology's private port
  conventions).
- **`platformctl graph [path]`** now renders the actual architecture — data
  pipelines and the technology layer — replacing the raw dependency dump
  (see errors.md "Graph does not render…"). `-o tree|dot|mermaid|json`.

Together they give a dependable, always-accurate view of the platform's
service topology and connection details.

### Original request

The cli should have the ability to provide an overview of the service-level components of the platform, with the purpose of being able to reference this chart to configure external tools like orchestrators to connect. Having to manually parse the YAML configuration - especially without the ability to produce a dependable visual graph chain - is a terrible developer experience, prone to lots of errors.

## All components must interface through (or be defined by) a service/connector

**Status: delivered**, across three pieces that together make port allocation
error-free and give every resource a stable access identifier:

1. **Host ports are optional and auto-allocated** (`internal/domain/hostport`).
   Omit a provider's port and one is derived deterministically from the
   component's unique name (range 20000–29999): different components never
   collide, the same component is stable across reconciles and dependent
   reconciles (no shared state needed), and nobody hand-picks a port to clash.
   Pin a port only when an external tool needs a fixed one. Verified: a
   redpanda provider with empty configuration published `9092 → 29798`
   automatically.
2. **Stable access identifiers** (`internal/domain/endpoint` + the
   `Connection` kind). The in-network address (`<container>:<fixed-port>`) is
   always stable; managed `Connection`s give external systems a fixed
   platform-owned entrypoint. See the Inventory Explorer item.
3. **Runtime-dependent materialization.** The provider *states* the port
   intent; the runtime realises it — Docker publishes the port today, a
   Kubernetes runtime would materialise the same intent as a Service. The
   provider code is runtime-agnostic (it asks the `ContainerRuntime` port to
   publish), so no provider changes when a second runtime lands.

Discovery ties it together: `platformctl inventory` surfaces the actual
allocated host port + the stable in-network address + the credential
SecretReference for every component.

Tests: `hostport_test.go`; inventory/endpoint tests; verified end-to-end.

### Original request

No provider should have to manually declare their port configurations - this is prone to definition errors when two components in a large platform accidentally manually specify the same port. All ports should be dynamically allocated, or specified manually through a service/connector configuration, similar to Kubernetes services. Note that this is NOT the same as a Kubernetes service, but rather configuration information that the provider can use - the actual materialization will depend on the runtime (e.g., Docker may expose ports through the instantiated provider, or use some sidecar connector proxy, while Kubernetes may actually instantiate a service and handle the rest itself. Refine this idea, and make it production-ready. Long-story short is that port allocation should be error-free and easy, while providing the user/other compoents with a stable access identifier for whatever resources they require access to.
