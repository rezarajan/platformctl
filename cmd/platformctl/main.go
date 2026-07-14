// Command platformctl is the Datascape CLI. This package does wiring/DI only:
// it is one of exactly two places allowed to import concrete adapters (the
// other is application/registry consumers created here).
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
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

// defaultWiring registers every adapter this build ships. Phase 0: the noop
// provider and the fake runtime. Phase 1+ adds docker here.
func defaultWiring(gates *featuregate.Registry) *registry.Registry {
	gates.Register("CoreReconciler", featuregate.GA, true)

	reg := registry.New(gates)
	reg.RegisterProvider("noop", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	return reg
}
