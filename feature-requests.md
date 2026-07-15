## Better Progress Output

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

No provider should have to manually declare their port configurations - this is prone to definition errors when two components in a large platform accidentally manually specify the same port. All ports should be dynamically allocated, or specified manually through a service/connector configuration, similar to Kubernetes services. Note that this is NOT the same as a Kubernetes service, but rather configuration information that the provider can use - the actual materialization will depend on the runtime (e.g., Docker may expose ports through the instantiated provider, or use some sidecar connector proxy, while Kubernetes may actually instantiate a service and handle the rest itself. Refine this idea, and make it production-ready. Long-story short is that port allocation should be error-free and easy, while providing the user/other compoents with a stable access identifier for whatever resources they require access to.
