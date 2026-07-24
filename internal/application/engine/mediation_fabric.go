// This file is docs/planning/08 L2's engine-owned fabric-provisioning
// facility (docs/adr/034): "ensure the mesh like we ensure networks."
//
// # Boundary against L1 and the pre-existing per-Connection openziti path
//
// L1 (mediation_transport.go) proved the engine can substitute a mediated
// address into resolveRequest's resolution surfaces given SOME
// mediation.AddressResolver — it shipped with Engine.Mediation nil in
// production (no real fabric existed). L2 stands up the real fabric this
// file provisions, but does NOT yet wire Engine.Mediation to a real
// AddressResolver over it: DialAddress needs per-workload identity
// minting, a per-target service/terminator, and per-edge dial policy —
// exactly L3's scope ("per-workload identity + tunneler sidecar... per-
// target service + bind, per-edge dial policies"). So today, even with
// this file's fabric standing, Engine.Mediation stays nil and L1's
// resolveRequest call sites keep resolving unmediated addresses
// byte-identically — L2 is infrastructure-only, on purpose.
//
// The pre-existing per-Connection openziti path
// (internal/adapters/providers/openziti's Provider(type: openziti) +
// Connection Kind, H6/H9/H10) realizes its OWN controller + router,
// entirely independent of the platform fabric this file provisions —
// different container names, different admin credential, no shared state.
// Fully reconciling the two (making every MediatedConnection ride the ONE
// platform fabric instead of standing up its own) is explicitly deferred:
// this file's fabric is ADDITIVE, and convergence is recorded as an L3
// precondition (docs/planning/08 L2's Done-note states this decision in
// full). Nothing here changes instance.go/connection.go's behavior in any
// way — a manifest that declares Provider(type: openziti) today gets
// exactly what it got before this task, plus (if MediatedTransport is also
// on and it declares a non-direct edge) a SEPARATE platform fabric
// standing alongside it.
package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// mediationFabricKey is the single synthetic resource.Key identity the
// platform fabric's runtime objects are labeled under (runtime.
// ManagedLabels) — one per deployment, namespace-less like the shared
// platform network (docs/planning/08 L2's Done-note: "one controller +
// router per deployment"), regardless of how many namespaces a manifest
// set touches. Kind "MediationFabric" is not a schema-declared Kind
// (nothing in a manifest ever names it) — purely a label identity, exactly
// the way runtime.ManagedLabels' kind argument is just a label value, not
// a validated vocabulary entry. Deliberately NOT a state.State.Resources
// map key — see state.State.MediationFabric's own doc comment for why.
func mediationFabricKey() resource.Key {
	return resource.Key{Namespace: resource.DefaultNamespace, Kind: "MediationFabric", Name: "platform-fabric"}
}

// anyMediatedEdgeDeclared reports whether byKey contains at least one
// Binding or Connection NOT declaring spec.transport: direct — L2's
// trigger condition for standing up the platform fabric at all. Reuses
// L1's own transportDirect predicate over EVERY edge-declaring Kind in the
// deployment, not merely the two resolveRequest call sites L1 wired
// (SchemaRegistryURL, KafkaBootstrapServers): ADR 034's "every declared
// graph edge is mediated by default" is a property of the DECLARATION,
// independent of which consumer surfaces the engine has migrated to
// actually dial through the fabric so far (L1's own open item; widening
// that set is L3's job, not a reason to under-provision the fabric today).
func anyMediatedEdgeDeclared(byKey map[resource.Key]resource.Envelope) bool {
	for _, env := range byKey {
		if env.Kind != "Binding" && env.Kind != "Connection" {
			continue
		}
		if !transportDirect(env) {
			return true
		}
	}
	return false
}

// fabricRuntimeSource picks the runtime type/config used to stand up (and,
// symmetrically via the persisted state below, to tear down) the platform
// mediation fabric: the FIRST Provider (sorted by resource.Key for
// determinism) in the resource set. In every deployment this codebase's
// own examples/scenarios exercise, every Provider shares one
// spec.runtime.type — the platform fabric being "one per deployment" (like
// the shared platform network) needs exactly one choice, and this mirrors
// domainRuntime's own "translate the default token" convention rather than
// inventing a second one. Falls back to provider.RuntimeTypeDocker with no
// config when the resource set carries no Provider at all — the same
// default `gc`'s own --runtime flag uses (cmd/platformctl/gc.go).
func fabricRuntimeSource(byKey map[resource.Key]resource.Envelope) (runtimeType string, runtimeConfig map[string]any) {
	keys := make([]resource.Key, 0, len(byKey))
	for k, env := range byKey {
		if env.Kind == "Provider" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return provider.RuntimeTypeDocker, nil
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	p, err := provider.FromEnvelope(byKey[keys[0]])
	if err != nil || p.RuntimeType == "" {
		return provider.RuntimeTypeDocker, nil
	}
	return p.RuntimeType, p.RuntimeConfig
}

// ensureMediationFabric is docs/planning/08 L2's Apply-time hook: "ensure
// the mesh like we ensure networks." A no-op — zero Registry.Runtime
// calls, zero Fabric calls — unless Fabric is wired, the MediatedTransport
// gate is on, AND at least one declared edge in byKey is mediated; this
// mirrors mediatedAddress's own gate/nil/direct short-circuit (L1) so the
// entire gate-off/no-Fabric/no-mediated-edge cost is a handful of bool
// checks, never a Registry.Runtime construction.
func (e *Engine) ensureMediationFabric(ctx context.Context, byKey map[resource.Key]resource.Envelope, st *state.State) error {
	if e.Fabric == nil || !e.Registry.GateEnabled("MediatedTransport") {
		return nil
	}
	if !anyMediatedEdgeDeclared(byKey) {
		return nil
	}
	runtimeType, runtimeConfig := fabricRuntimeSource(byKey)
	rt, err := e.Registry.Runtime(runtimeType, runtimeConfig)
	if err != nil {
		return fmt.Errorf("mediation fabric: construct runtime: %w", err)
	}
	key := mediationFabricKey()
	labels := runtimeport.ManagedLabels(key.Namespace, key.Kind, key.Name, key.Name)
	fs, ferr := e.Fabric.EnsureFabric(ctx, mediation.FabricRequest{Runtime: rt, Labels: labels})

	now := e.Clock.Now()
	cst := status.Status{}
	if ferr != nil {
		cst.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonMediationPlaneUnhealthy, Message: ferr.Error()}, now)
	} else {
		cst.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonMediationPlaneHealthy}, now)
		cst.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	}

	e.stateMu.Lock()
	st.MediationFabric = &state.MediationFabricState{
		Status: cst,
		Provider: map[string]any{
			// Never the admin credential (docs/adr/013's fingerprints-only
			// discipline, mirrored from mediation.WorkloadIdentity's own
			// doc comment) — only host facts and the runtime choice this
			// fabric must be torn down through later.
			"controllerContainerId": fs.ControllerContainerID,
			"controllerInternal":    fs.ControllerInternal,
			"routerId":              fs.RouterID,
			"runtimeType":           runtimeType,
			"runtimeConfig":         runtimeConfig,
		},
	}
	saveErr := e.StateStore.Save(ctx, *st)
	e.stateMu.Unlock()

	if ferr != nil {
		return fmt.Errorf("mediation fabric: %w", ferr)
	}
	return saveErr
}

// maybeDestroyMediationFabric is docs/planning/08 L2's Destroy-time hook,
// realizing docs/adr/013's bar for implicit infrastructure: destroy tears
// the fabric down ONLY when no mediated edge remains anywhere in the
// deployment. st must already reflect every successful deletion this
// Destroy call made (the caller's own st.Resources mutations) — remaining
// is computed from what's LEFT in st.Resources, not from any envelopes
// parameter, so this sees the deployment's true post-destroy shape
// regardless of which resources this particular destroy call happened to
// target.
func (e *Engine) maybeDestroyMediationFabric(ctx context.Context, st *state.State) error {
	if e.Fabric == nil || !e.Registry.GateEnabled("MediatedTransport") {
		return nil
	}
	if st.MediationFabric == nil {
		return nil // never stood up (or already torn down) — nothing to do
	}
	remaining := make(map[resource.Key]resource.Envelope, len(st.Resources))
	for k, s := range st.Resources {
		if s.LastApplied == nil {
			continue
		}
		remaining[k] = *s.LastApplied
	}
	if anyMediatedEdgeDeclared(remaining) {
		return nil
	}

	// Reuse the EXACT runtime type/config EnsureFabric persisted at
	// creation time, never re-derive it from what remains — by
	// construction, "no mediated edge remains" may mean few or zero
	// Providers are left in remaining at all, and re-deriving from an
	// empty/changed set risks constructing the WRONG runtime and silently
	// leaking the fabric (Remove's "already gone is success" contract
	// would mask exactly that mistake).
	runtimeType, _ := st.MediationFabric.Provider["runtimeType"].(string)
	runtimeConfig, _ := st.MediationFabric.Provider["runtimeConfig"].(map[string]any)
	if runtimeType == "" {
		runtimeType = provider.RuntimeTypeDocker
	}
	rt, err := e.Registry.Runtime(runtimeType, runtimeConfig)
	if err != nil {
		return fmt.Errorf("mediation fabric: construct runtime for teardown: %w", err)
	}
	key := mediationFabricKey()
	labels := runtimeport.ManagedLabels(key.Namespace, key.Kind, key.Name, key.Name)
	if err := e.Fabric.DestroyFabric(ctx, mediation.FabricRequest{Runtime: rt, Labels: labels}); err != nil {
		return fmt.Errorf("mediation fabric: destroy: %w", err)
	}

	e.stateMu.Lock()
	st.MediationFabric = nil
	saveErr := e.StateStore.Save(ctx, *st)
	e.stateMu.Unlock()
	return saveErr
}
