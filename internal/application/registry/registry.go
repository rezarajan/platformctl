// Package registry is the single place mapping provider.spec.type → Provider
// constructor and runtime.type → ContainerRuntime constructor. Registration is
// explicit, called from cmd/platformctl's wiring — domain and ports never
// import adapters directly. See docs/planning/02-architecture.md §5.6.
package registry

import (
	"fmt"
	"sort"

	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type ProviderConstructor func() reconciler.Provider
type RuntimeConstructor func(config map[string]any) (runtime.ContainerRuntime, error)

// PlannedRuntimes are accepted by schema for forward compatibility but
// rejected at registry-construction time with a "planned, not yet available"
// error — never silently ignored. See docs/planning/05-v1-first-version-spec.md §4.
var PlannedRuntimes = map[string]bool{
	"kubernetes": true,
	"external":   true,
	"terraform":  true,
}

type Registry struct {
	providers map[string]ProviderConstructor
	runtimes  map[string]RuntimeConstructor
	// providerGate maps provider type → feature gate name guarding it.
	providerGate map[string]string
	gates        *featuregate.Registry
}

func New(gates *featuregate.Registry) *Registry {
	return &Registry{
		providers:    make(map[string]ProviderConstructor),
		runtimes:     make(map[string]RuntimeConstructor),
		providerGate: make(map[string]string),
		gates:        gates,
	}
}

func (r *Registry) RegisterProvider(typeName string, ctor ProviderConstructor, gateName string) {
	r.providers[typeName] = ctor
	if gateName != "" {
		r.providerGate[typeName] = gateName
	}
}

func (r *Registry) RegisterRuntime(typeName string, ctor RuntimeConstructor) {
	r.runtimes[typeName] = ctor
}

func (r *Registry) Provider(typeName string) (reconciler.Provider, error) {
	ctor, ok := r.providers[typeName]
	if !ok {
		return nil, fmt.Errorf("unknown provider type %q (registered: %s)", typeName, joinKeys(r.providers))
	}
	if gate, gated := r.providerGate[typeName]; gated {
		if err := r.gates.Require(gate); err != nil {
			return nil, fmt.Errorf("provider type %q: %w", typeName, err)
		}
	}
	return ctor(), nil
}

func (r *Registry) Runtime(typeName string, config map[string]any) (runtime.ContainerRuntime, error) {
	if PlannedRuntimes[typeName] {
		return nil, fmt.Errorf("runtime type %q is planned but not yet available in this version", typeName)
	}
	ctor, ok := r.runtimes[typeName]
	if !ok {
		return nil, fmt.Errorf("unknown runtime type %q (registered: %s)", typeName, joinKeys(r.runtimes))
	}
	return ctor(config)
}

func joinKeys[V any](m map[string]V) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += ", "
		}
		out += k
	}
	return out
}
