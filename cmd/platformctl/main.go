// Command platformctl is the Datascape CLI. This package does wiring/DI only:
// it is one of exactly two places allowed to import concrete adapters (the
// other is application/registry consumers created here).
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/rezarajan/platformctl/internal/adapters/providers/debezium"
	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
	"github.com/rezarajan/platformctl/internal/adapters/providers/placeholder"
	"github.com/rezarajan/platformctl/internal/adapters/providers/postgres"
	"github.com/rezarajan/platformctl/internal/adapters/providers/redpanda"
	"github.com/rezarajan/platformctl/internal/adapters/providers/s3"
	"github.com/rezarajan/platformctl/internal/adapters/providers/s3sink"
	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func main() {
	root := newRootCmd(defaultWiring)
	if err := root.Execute(); err != nil {
		var exitErr cliutil.ExitError
		if errors.As(err, &exitErr) {
			if exitErr.Err != nil {
				fmt.Fprintln(os.Stderr, "error:", exitErr.Err)
			}
			os.Exit(exitErr.Code)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(cliutil.ExitExecution)
	}
}

// defaultWiring registers every adapter this build ships. This is the single
// place concrete adapters are wired to the registry.
func defaultWiring(gates *featuregate.Registry) *registry.Registry {
	// Defaults follow the feature-gate master table in
	// docs/planning/04-roadmap-and-feature-gates.md §12.
	gates.Register("CoreReconciler", featuregate.GA, true)
	gates.Register("DockerRuntime", featuregate.Alpha, true)
	gates.Register("ContainerProvider", featuregate.Alpha, false) // not in the master table; test-only provider
	gates.Register("RedpandaProvider", featuregate.Alpha, true)
	gates.Register("PostgresProvider", featuregate.Alpha, true)
	gates.Register("DebeziumCDCProvider", featuregate.Alpha, true)
	gates.Register("CDCBinding", featuregate.Alpha, true)
	gates.Register("LineageObservability", featuregate.Alpha, false)
	gates.Register("ObjectStoreProvider", featuregate.Alpha, true)
	gates.Register("SinkBinding", featuregate.Alpha, true)
	// Beta at the close of Phase 5 per the master table (it was briefly
	// registered Alpha/enabled ahead of schedule — see checkpoint.md).
	gates.Register("DriftDetection", featuregate.Beta, true)
	gates.Register("ExternalResourceConfiguration", featuregate.Beta, true)
	gates.Register("ImportedResources", featuregate.Alpha, false)

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
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	reg.RegisterRuntime("docker", func(cfg map[string]any) (runtime.ContainerRuntime, error) {
		if err := gates.Require("DockerRuntime"); err != nil {
			return nil, err
		}
		return dockerruntime.New(cfg)
	})
	return reg
}
