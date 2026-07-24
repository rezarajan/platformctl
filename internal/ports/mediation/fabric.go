package mediation

import (
	"context"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// FabricRequest carries what a FabricProvisioner needs to ensure or destroy
// the platform-owned mediation fabric (docs/planning/08 L2, docs/adr/034):
// one controller + router per deployment, engine-owned like a network, never
// manifest-declared. Deliberately minimal — unlike reconciler.Request, there
// is no Resource/Provider envelope to carry, because nothing in a manifest
// declares this object; the engine is the sole caller and constructs this
// value itself from whichever runtime/labels it has already decided govern
// the platform fabric for this deployment (see
// internal/application/engine/mediation_fabric.go's own doc comment for how
// that choice is made).
type FabricRequest struct {
	// Runtime is the constructed ContainerRuntime the fabric's controller
	// and router are realized through — resolved by the engine exactly the
	// way any other Provider's runtime is (internal/application/registry.
	// Registry.Runtime), never a fixed/default construction inside the
	// FabricProvisioner itself (docs/planning/08 F5 statelessness).
	Runtime runtime.ContainerRuntime
	// Labels are the ownership labels (runtime.ManagedLabels) every runtime
	// object the fabric creates must carry, so gc/ListManaged* see the
	// fabric like any other managed object (docs/planning/08 L2's accept
	// criterion).
	Labels map[string]string
	// Networks are additional networks/namespaces the router should also
	// join, beyond the platform's own default network — the same
	// declaration precedent instanceConfig.TargetNetworks already
	// establishes for the manifest-declared Provider(type: openziti) path
	// (internal/adapters/providers/openziti/instance.go). Empty is valid:
	// L2 stands up the fabric with no mediated target reachable through it
	// yet — wiring per-edge targets into this list is L3's job.
	Networks []string
}

// FabricState is what EnsureFabric reports back for the engine to persist —
// deliberately non-secret (docs/adr/013's fingerprints-only discipline,
// mirrored from mediation.WorkloadIdentity's own doc comment): no
// credential, no private key, ever crosses this boundary.
type FabricState struct {
	ControllerContainerID string
	RouterContainerID     string
	// ControllerInternal is the in-network address (Docker network DNS /
	// Kubernetes Service DNS) other engine-owned facilities dial the
	// controller's Edge Management API through — never a host-published
	// address, which is ephemeral (a fresh port-forward per call) on
	// Kubernetes.
	ControllerInternal string
	RouterID           string
}

// FabricProvisioner is the engine-owned facility docs/planning/08 L2 adds:
// "ensure the mesh like we ensure networks." Distinct from
// MediationProvider/AddressResolver (L1): those answer per-edge identity/
// authorization/dial-address questions once the fabric exists; this
// answers only "does the fabric itself exist" — the platform infrastructure
// every future mediated edge (L3) will need, provisioned before any edge
// asks for it. Idempotent by the same Ensure*-idempotent discipline every
// port in this codebase holds: a second EnsureFabric call for an unchanged,
// already-realized fabric makes no additional Docker/Kubernetes API calls
// (the runtime.ContainerRuntime Ensure* bar) and reuses the same minted
// admin credential rather than minting a new one.
type FabricProvisioner interface {
	// EnsureFabric stands up (or reuses) the platform mediation fabric.
	EnsureFabric(ctx context.Context, req FabricRequest) (FabricState, error)
	// DestroyFabric tears the fabric down. The engine calls this ONLY when
	// it has determined no mediated edge remains anywhere in the deployment
	// (docs/adr/013's bar for implicit infrastructure: destroy only when
	// nothing needs it anymore) — DestroyFabric itself does not re-check
	// that condition, it trusts the caller the same way
	// runtime.ContainerRuntime.Remove trusts its own caller. Idempotent:
	// destroying an already-absent fabric is a no-op, never an error
	// (mirrors Remove's "already gone is success" contract).
	DestroyFabric(ctx context.Context, req FabricRequest) error
}
