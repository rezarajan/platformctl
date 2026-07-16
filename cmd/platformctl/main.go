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
	"github.com/rezarajan/platformctl/internal/adapters/providers/mysql"
	"github.com/rezarajan/platformctl/internal/adapters/providers/nessie"
	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
	"github.com/rezarajan/platformctl/internal/adapters/providers/openlineage"
	"github.com/rezarajan/platformctl/internal/adapters/providers/placeholder"
	"github.com/rezarajan/platformctl/internal/adapters/providers/postgres"
	"github.com/rezarajan/platformctl/internal/adapters/providers/proxy"
	"github.com/rezarajan/platformctl/internal/adapters/providers/redpanda"
	"github.com/rezarajan/platformctl/internal/adapters/providers/s3"
	"github.com/rezarajan/platformctl/internal/adapters/providers/s3sink"
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
var version = "v1.0.0"

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
	gates.Register("ContainerProvider", featuregate.Alpha, false) // not in the master table; test-only provider
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
	// Phase 6.5: orchestrator-ready infrastructure.
	gates.Register("MySQLProvider", featuregate.Alpha, true)
	gates.Register("NessieProvider", featuregate.Alpha, true)
	gates.Register("OpenLineageProvider", featuregate.Alpha, true)
	gates.Register("ProxyProvider", featuregate.Alpha, true)
	// Phase 7 (early): second runtime adapter, proving the provider/runtime
	// split for real (docs/planning/04-roadmap-and-feature-gates.md §10).
	// Alpha and disabled by default given the blast radius of a second
	// runtime; long hardening period expected before Beta.
	gates.Register("KubernetesRuntime", featuregate.Alpha, false)

	reg := registry.New(gates)
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterProvider("container", func() reconciler.Provider { return placeholder.New() }, "ContainerProvider")
	reg.RegisterProvider("redpanda", func() reconciler.Provider { return redpanda.New() }, "RedpandaProvider")
	reg.RegisterProvider("postgres", func() reconciler.Provider { return postgres.New() }, "PostgresProvider")
	reg.RegisterProvider("debezium", func() reconciler.Provider { return debezium.New() }, "DebeziumCDCProvider")
	// "s3" and "minio" are the same adapter: MinIO is the reference
	// S3-API-compatible target (05-v1-first-version-spec.md §3).
	reg.RegisterProvider("s3", func() reconciler.Provider { return s3.New() }, "ObjectStoreProvider")
	reg.RegisterProvider("minio", func() reconciler.Provider { return s3.New() }, "ObjectStoreProvider")
	reg.RegisterProvider("s3sink", func() reconciler.Provider { return s3sink.New() }, "ObjectStoreProvider")
	// "mysql" and "mariadb" are the same adapter (same protocol; per-type
	// image and binlog flags).
	reg.RegisterProvider("mysql", func() reconciler.Provider { return mysql.New() }, "MySQLProvider")
	reg.RegisterProvider("mariadb", func() reconciler.Provider { return mysql.New() }, "MySQLProvider")
	reg.RegisterProvider("nessie", func() reconciler.Provider { return nessie.New() }, "NessieProvider")
	reg.RegisterProvider("openlineage", func() reconciler.Provider { return openlineage.New() }, "OpenLineageProvider")
	reg.RegisterProvider("proxy", func() reconciler.Provider { return proxy.New() }, "ProxyProvider")
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
