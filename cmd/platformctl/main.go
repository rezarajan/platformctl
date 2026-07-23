// Command platformctl is the Datascape CLI. This package does wiring/DI only:
// it is one of exactly two places allowed to import concrete adapters (the
// other is application/registry consumers created here).
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/adapters/providers/debezium"
	"github.com/rezarajan/platformctl/internal/adapters/providers/grafana"
	"github.com/rezarajan/platformctl/internal/adapters/providers/ingress"
	"github.com/rezarajan/platformctl/internal/adapters/providers/jdbcsink"
	"github.com/rezarajan/platformctl/internal/adapters/providers/mysql"
	"github.com/rezarajan/platformctl/internal/adapters/providers/nessie"
	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
	"github.com/rezarajan/platformctl/internal/adapters/providers/openlineage"
	"github.com/rezarajan/platformctl/internal/adapters/providers/openziti"
	"github.com/rezarajan/platformctl/internal/adapters/providers/placeholder"
	"github.com/rezarajan/platformctl/internal/adapters/providers/postgres"
	"github.com/rezarajan/platformctl/internal/adapters/providers/prometheus"
	"github.com/rezarajan/platformctl/internal/adapters/providers/proxy"
	"github.com/rezarajan/platformctl/internal/adapters/providers/redpanda"
	"github.com/rezarajan/platformctl/internal/adapters/providers/s3"
	"github.com/rezarajan/platformctl/internal/adapters/providers/s3sink"
	"github.com/rezarajan/platformctl/internal/adapters/providers/s3source"
	"github.com/rezarajan/platformctl/internal/adapters/providers/trino"
	"github.com/rezarajan/platformctl/internal/adapters/providers/wireguard"
	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// version is the binary's semantic version; overridable at build time via
// -ldflags "-X main.version=...".
var version = "v1.2.0"

func main() {
	root := newRootCmd(defaultWiring)
	root.Version = version
	if err := root.Execute(); err != nil {
		var exitErr cliutil.ExitError
		if errors.As(err, &exitErr) {
			if exitErr.Err != nil {
				writeStructuredError(root, exitErr.Code, exitErr.Err)
				fmt.Fprintln(os.Stderr, "error:", exitErr.Err)
			}
			os.Exit(exitErr.Code)
		}
		writeStructuredError(root, cliutil.ExitExecution, err)
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(cliutil.ExitExecution)
	}
}

func writeStructuredError(root *cobra.Command, code int, err error) {
	flags := root.PersistentFlags()
	format, ferr := flags.GetString("output")
	if ferr != nil || !isStructured(format) {
		return
	}
	_ = cliutil.WriteOutput(os.Stdout, format, map[string]any{
		"error": err.Error(),
		"code":  code,
	}, nil)
}

// defaultWiring registers every adapter this build ships. This is the single
// place concrete adapters are wired to the registry.
func defaultWiring(gates *featuregate.Registry) *registry.Registry {
	// Stages follow the feature-gate master table in
	// docs/planning/04-roadmap-and-feature-gates.md §12 at the close of
	// Phase 5 (v1.0.0): the seven phase-1..4 gates graduate to GA.
	gates.Register("CoreReconciler", featuregate.GA, true)
	gates.Register("DockerRuntime", featuregate.GA, true)
	gates.Register("RedpandaProvider", featuregate.GA, true)
	gates.Register("PostgresProvider", featuregate.GA, true)
	gates.Register("DebeziumCDCProvider", featuregate.GA, true)
	gates.Register("CDCBinding", featuregate.GA, true)
	gates.Register("LineageObservability", featuregate.Beta, true) // Beta: the openlineage (Marquez) provider exists and is exercised
	gates.Register("ObjectStoreProvider", featuregate.GA, true)
	gates.Register("SinkBinding", featuregate.GA, true)
	gates.Register("DriftDetection", featuregate.Beta, true)
	gates.Register("ExternalResourceConfiguration", featuregate.Beta, true)
	gates.Register("ImportedResources", featuregate.Beta, true) // Beta per the Phase 6 graduation
	// Phase 6.
	gates.Register("ParallelReconciliation", featuregate.Alpha, false)
	gates.Register("VaultSecretBackend", featuregate.Alpha, false)
	gates.Register("SharedStateBackend", featuregate.Alpha, false)    // docs/adr/003-shared-state.md
	gates.Register("KubernetesSecretBackend", featuregate.Beta, true) // docs/planning/08 B4; graduated with KubernetesRuntime at B9
	// Phase 6.5: orchestrator-ready infrastructure.
	// Phase 6.5 providers: graduated Alpha -> Beta at docs/planning/08 Stage A
	// close (their hardening period, per doc 08 §8's graduation intent).
	gates.Register("MySQLProvider", featuregate.Beta, true)
	gates.Register("NessieProvider", featuregate.Beta, true)
	gates.Register("OpenLineageProvider", featuregate.Beta, true)
	gates.Register("ProxyProvider", featuregate.Beta, true)
	// Phase 7 / docs/planning/08 Stage B: second runtime adapter, proving
	// the provider/runtime split for real
	// (docs/planning/04-roadmap-and-feature-gates.md §10). Graduated to
	// Beta (enabled by default) at Stage B close (B9): external
	// reachability (B1/B2), storage sizing/persistence (B3), a Kubernetes
	// SecretStore backend (B4), a minimal RBAC posture proven sufficient
	// in CI (B5), connection preflight (B6), NetworkPolicy parity (B7),
	// and the full cdc-attendance/lakehouse example scenarios (B8) all
	// verified against a real cluster.
	gates.Register("KubernetesRuntime", featuregate.Beta, true)
	// docs/planning/08 Stage C (C1): ContainerSpec.Replicas > 1 requires this
	// gate, enforced by application/registry's runtime decorator
	// (docs/adr/004-replicas-and-identity.md) since no provider yet
	// surfaces a schema field that sets Replicas.
	gates.Register("HighAvailability", featuregate.Alpha, false)
	// docs/planning/08 D1: Redpanda's built-in Confluent-compatible schema
	// registry (Provider.spec.configuration.schemaRegistry: enabled) and a
	// Binding's schema-carrying spec.options.format (avro, protobuf).
	// Graduated Alpha -> Beta/enabled when D2 (Parquet end-to-end) landed,
	// per doc 04 §12's recorded graduation intent.
	gates.Register("SchemaRegistrySupport", featuregate.Beta, true)
	// docs/planning/08 C6: backup/restore capability (BackupCapableProvider).
	// Alpha/disabled until restore drills have soaked in CI (§8 graduation
	// intent).
	gates.Register("BackupRestore", featuregate.Alpha, false)
	// docs/planning/08 C9: the prometheus provider (managed monitoring
	// stack), plus its C9-completion follow-up — postgres/mysql metrics
	// exporter sidecars and the grafana provider, all gated by this same
	// gate (it guards the monitoring stack as a class). Alpha/disabled.
	gates.Register("MonitoringStackProvider", featuregate.Alpha, false)
	// docs/planning/08 C7, docs/adr/018: the ingress provider (HTTP routing
	// on the Connection seam). Alpha/disabled — a new provider exposing a
	// new network-reachable surface (an HTTP reverse proxy accepting
	// arbitrary Host headers) defaults off until soaked, matching the
	// TrinoProvider/JDBCSinkProvider posture (design note 006), not the
	// Phase 6.5 enabled-Alpha precedent.
	gates.Register("IngressProvider", featuregate.Alpha, false)
	// docs/planning/08 C8, docs/adr/018 addendum: TLS termination on the
	// same ingress provider's Connection seam (Connection.spec.tls).
	// Alpha/disabled, independent of IngressProvider itself — a Connection
	// can decline TLS and stay plaintext HTTP even once IngressProvider
	// graduates, so this needs its own off switch rather than riding
	// IngressProvider's gate.
	gates.Register("TLSTermination", featuregate.Alpha, false)
	// docs/planning/08 I2, docs/adr/025: outbound TLS to a TLS-requiring
	// external database (Connection.spec.tls.mode on an external
	// Connection) — the client-side sibling of TLSTermination above.
	// Alpha/enabled: an additive opt-in field (absent spec.tls.mode leaves
	// every consumer's plaintext dial byte-for-byte unchanged), so unlike
	// TLSTermination there is no new attack surface to soak disabled-by-
	// default — declining the field is itself the off switch.
	gates.Register("ExternalDatabaseTLS", featuregate.Alpha, true)
	// docs/planning/08 D10 / docs/adr/006-compute-engines.md: the trino
	// compute-engine provider. Alpha/disabled — unlike NessieProvider/
	// OpenLineageProvider's enabled-Alpha Phase 6.5 precedent, a query
	// engine accepting arbitrary SQL from whoever can reach its coordinator
	// port is a meaningfully different risk profile and defaults off until
	// reviewed (ADR 006's "Feature gate" section).
	gates.Register("TrinoProvider", featuregate.Alpha, false)
	// docs/planning/08 D3/D4, docs/adr/001 + 009: the two capability seams
	// (sink -> Source, ingest) modeled with no shipped provider since
	// v1.0.0. Alpha/disabled — new providers exposing new capability
	// surfaces default off until soaked, matching the IngressProvider/
	// TrinoProvider posture (design note 006), not the Phase 6.5
	// enabled-Alpha precedent.
	gates.Register("JDBCSinkProvider", featuregate.Alpha, false)
	gates.Register("IngestProvider", featuregate.Alpha, false)
	// docs/planning/08 D5, docs/adr/023: the wireguard tunnel provider —
	// a managed Connection whose upstream is only reachable through a
	// WireGuard peer. Alpha/disabled — grants NET_ADMIN and opens a routed
	// path into a private network, a meaningfully different risk profile
	// from the Phase 6.5 enabled-Alpha precedent.
	gates.Register("TunnelProvider", featuregate.Alpha, false)

	// docs/planning/08 H6, as amended by docs/adr/027: the OpenZiti
	// mediation provider — docs/adr/027's Layer 1, the authoritative
	// zero-trust plane (identity-attested, per-edge-authorized
	// connections). Alpha/disabled, matching TunnelProvider's posture
	// immediately above: a new provider running its own control plane
	// (a pinned controller + router) and minting cryptographic identity
	// is a meaningfully different risk profile from the Phase 6.5
	// enabled-Alpha precedent, and the owner-scenario dual-runtime accept
	// suite (docs/planning/08 §7.7 H6) has not yet soaked.
	gates.Register("MediatedConnections", featuregate.Alpha, false)

	// docs/planning/08 H7 (docs/adr/026, as amended 2026-07-23): resource-
	// granular least privilege compiled from the declared reference graph
	// — Layer 2 (docs/adr/027) of the zero-trust progression. Flips
	// reachability semantics for every existing manifest set the instant
	// it's on (a resource that previously reached its whole domain/shared
	// network now reaches only what it declared), so this is opt-in until
	// the dual-runtime negative-proof suite (docs/planning/08 §7.7 H7)
	// soaks — the same bar ADR 026's own Gate section states.
	gates.Register("GraphScopedAccess", featuregate.Alpha, false)

	// docs/planning/08 H1 (ADR 020): read-only reporting only — the gate
	// exists to switch `validate`'s one-line design-findings summary off,
	// not to hide the `lint` command itself, so it defaults enabled.
	gates.Register("DesignLints", featuregate.Alpha, true)

	// docs/planning/08 H3 (ADR 021): the typed-rule policy engine — unlike
	// DesignLints, disabled means a full no-op (no directory read, no
	// evaluation at all), and a deny blocks validate/plan/apply/destroy, so
	// this defaults off until the zero-trust pack has soaked (doc 04 §12's
	// graduation intent: "Beta after the zero-trust pack soaks in this
	// repo's own CI").
	gates.Register("PolicyEngine", featuregate.Alpha, false)

	reg := registry.New(gates)
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	// "container" (the Phase 1 "prove the runtime" placeholder) is
	// registered ungated, exactly like "noop": the `ContainerProvider`
	// feature gate was retired (docs/planning/08 E7, doc 04 §12) once
	// evidence showed it is genuinely load-bearing for integration tests
	// (docker_integration_test.go, domains_integration_test.go,
	// domains_kubernetes_integration_test.go — H5's segmentation scenario)
	// and never advertised as a manifest-authoring surface for real
	// pipelines — a feature gate exists to stage user-facing maturity, not
	// to gate a test fixture from itself. See docs/history/checkpoint.md
	// item 4.
	reg.RegisterProvider("container", func() reconciler.Provider { return placeholder.New() }, "")
	reg.RegisterProvider("redpanda", func() reconciler.Provider { return redpanda.New() }, "RedpandaProvider")
	reg.RegisterProvider("postgres", func() reconciler.Provider { return postgres.New() }, "PostgresProvider")
	reg.RegisterProvider("debezium", func() reconciler.Provider { return debezium.New() }, "DebeziumCDCProvider")
	// "s3" and "minio" are the same adapter: MinIO is the reference
	// S3-API-compatible target (05-v1-first-version-spec.md §3).
	reg.RegisterProvider("s3", func() reconciler.Provider { return s3.New() }, "ObjectStoreProvider")
	reg.RegisterProvider("minio", func() reconciler.Provider { return s3.New() }, "ObjectStoreProvider")
	reg.RegisterProvider("s3sink", func() reconciler.Provider { return s3sink.New() }, "ObjectStoreProvider")
	reg.RegisterProvider("jdbcsink", func() reconciler.Provider { return jdbcsink.New() }, "JDBCSinkProvider")
	reg.RegisterProvider("s3source", func() reconciler.Provider { return s3source.New() }, "IngestProvider")
	// "mysql" and "mariadb" are the same adapter (same protocol; per-type
	// image and binlog flags).
	reg.RegisterProvider("mysql", func() reconciler.Provider { return mysql.New() }, "MySQLProvider")
	reg.RegisterProvider("mariadb", func() reconciler.Provider { return mysql.New() }, "MySQLProvider")
	reg.RegisterProvider("nessie", func() reconciler.Provider { return nessie.New() }, "NessieProvider")
	reg.RegisterProvider("openlineage", func() reconciler.Provider { return openlineage.New() }, "OpenLineageProvider")
	reg.RegisterProvider("proxy", func() reconciler.Provider { return proxy.New() }, "ProxyProvider")
	reg.RegisterProvider("prometheus", func() reconciler.Provider { return prometheus.New() }, "MonitoringStackProvider")
	// docs/planning/08 C9 completion: the grafana provider reuses the
	// existing MonitoringStackProvider gate rather than a new one — it
	// gates the monitoring stack as a class, not each provider in it
	// separately.
	reg.RegisterProvider("grafana", func() reconciler.Provider { return grafana.New() }, "MonitoringStackProvider")
	reg.RegisterProvider("ingress", func() reconciler.Provider { return ingress.New() }, "IngressProvider")
	reg.RegisterProvider("trino", func() reconciler.Provider { return trino.New() }, "TrinoProvider")
	reg.RegisterProvider("wireguard", func() reconciler.Provider { return wireguard.New() }, "TunnelProvider")
	reg.RegisterProvider("openziti", func() reconciler.Provider { return openziti.New() }, "MediatedConnections")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	reg.RegisterRuntime("docker", func(cfg map[string]any) (runtime.ContainerRuntime, error) {
		if err := gates.Require("DockerRuntime"); err != nil {
			return nil, err
		}
		return dockerruntime.New(cfg)
	})
	reg.RegisterRuntime("kubernetes", func(cfg map[string]any) (runtime.ContainerRuntime, error) {
		if err := gates.Require("KubernetesRuntime"); err != nil {
			return nil, err
		}
		return k8sruntime.New(cfg)
	})
	return reg
}
