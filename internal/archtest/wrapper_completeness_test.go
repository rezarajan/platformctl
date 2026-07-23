package archtest

import (
	"reflect"
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	"github.com/rezarajan/platformctl/internal/application/engine"
	"github.com/rezarajan/platformctl/internal/application/registry"
	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// optionalCapabilities is the closed list of optional ContainerRuntime
// capability interfaces (the type-assert-a-runtime pattern). Adding a new
// capability interface to ports/runtime REQUIRES adding it here — the
// reflect sweep below then forces every runtime WRAPPER to forward it.
var optionalCapabilities = []reflect.Type{
	reflect.TypeOf((*runtimeport.MemberSetRuntime)(nil)).Elem(),
	reflect.TypeOf((*runtimeport.IngressCapableRuntime)(nil)).Elem(),
	reflect.TypeOf((*runtimeport.IsolationObserver)(nil)).Elem(),
	reflect.TypeOf((*runtimeport.ExecCapableRuntime)(nil)).Elem(),
}

// TestRuntimeWrappersForwardEveryOptionalCapability closes the ADR 018
// promotion-trap CLASS (three live occurrences: the original ingress
// registry gap, I7's preemptive fix, and H5's domainRuntime shipping with
// ZERO capability forwarding — found at the 2026-07-23 single gate as
// systematic K8s failures). The rule it enforces: any capability interface
// implemented by ANY real runtime adapter must also be implemented by
// EVERY runtime wrapper, so a type assertion through a wrapped
// Request.Runtime can never silently lose a capability the underlying
// adapter has. A wrapper is anything that decorates a ContainerRuntime on
// the path to a provider (registry's haGuard wrapper, engine's domain
// decorator — enumerated via their exported test constructors).
func TestRuntimeWrappersForwardEveryOptionalCapability(t *testing.T) {
	t.Parallel()

	adapters := map[string]reflect.Type{
		"docker":     reflect.TypeOf(&docker.Runtime{}),
		"kubernetes": reflect.TypeOf(&kubernetes.Runtime{}),
		"fake":       reflect.TypeOf(fake.New()),
	}
	wrappers := map[string]reflect.Type{
		"registry.haGuardRuntime": reflect.TypeOf(registry.WrapForTest(fake.New())),
		"engine.domainRuntime":    reflect.TypeOf(engine.WrapDomainRuntimeForTest(fake.New())),
	}

	for _, cap := range optionalCapabilities {
		implementedBySomeAdapter := false
		for name, at := range adapters {
			if at.Implements(cap) {
				implementedBySomeAdapter = true
				_ = name
			}
		}
		if !implementedBySomeAdapter {
			continue // capability not realized by any adapter yet — nothing to forward
		}
		for wname, wt := range wrappers {
			if !wt.Implements(cap) {
				t.Errorf("%s does not forward %s — a provider's type assertion through this wrapper silently loses the capability every real adapter path depends on (the ADR 018 promotion trap; add the delegating method + keep this guard green)", wname, cap.String())
			}
		}
	}
}
