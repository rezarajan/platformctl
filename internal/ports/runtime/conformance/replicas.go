package conformance

import (
	"fmt"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// runReplicasScaleAndOrdinal registers ReplicaSet_ScaleUp_Idempotent and
// ReplicaSet_OrdinalHostnameResolution.
func runReplicasScaleAndOrdinal(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	ctx := fx.ctx

	// ReplicaSet_ScaleUp_Idempotent proves the core C1 primitive
	// (docs/adr/004-replicas-and-identity.md): Replicas > 1 fans out to N
	// individually-managed units reported through one aggregate
	// ContainerState (ReadyReplicas), a second identical EnsureContainer call
	// is a no-op (NFR-2), and scaling 2 -> 3 is detected as a real change and
	// converges ReadyReplicas to the new count — across all three adapters,
	// since none of this depends on StableIdentity (D10's simpler,
	// interchangeable-workers shape).
	t.Run("ReplicaSet_ScaleUp_Idempotent", func(t *testing.T) {
		name := fx.namePrefix + "-replicaset-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{fx.netSpec.Name},
			Labels:   fx.labels,
			Replicas: 2,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("first EnsureContainer (Replicas: 2): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy after scale to 2: %v", err)
		}
		// WaitHealthy returns at 1-of-N ready; converge to the full count
		// rather than asserting it immediately (racy on a real cluster).
		waitReadyReplicas(ctx, t, rt, name, 2, 60*time.Second)
		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect after scale to 2: found=%v err=%v", found, err)
		}
		if !st.Healthy {
			t.Errorf("aggregate Healthy = false with 2/2 replicas ready")
		}

		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("second EnsureContainer (Replicas: 2, unchanged): %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Errorf("second EnsureContainer with identical Replicas: 2 spec mutated state (NFR-2 violation)")
		}

		// Capture ordinal 0's identity before scaling: a scale-up must only
		// add new ordinals, never recreate existing ones (Replicas is
		// set-level state and must not leak into per-ordinal spec hashes).
		// Adapters whose ordinals are literal named units (Docker, fake)
		// resolve the ordinal name here; Kubernetes' Deployment path has no
		// deterministically-named ordinal to inspect, so the identity half of
		// the check is skipped there and covered by the mutation counter on
		// the fake instead.
		ordName := runtime.OrdinalName(name, 0)
		var ord0IDBefore string
		if ordSt, ordFound, err := rt.Inspect(ctx, ordName); err != nil {
			t.Fatalf("Inspect(%q) before scale-up: %v", ordName, err)
		} else if ordFound {
			ord0IDBefore = ordSt.ID
		}

		scaled := spec
		scaled.Replicas = 3
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, scaled); err != nil {
			t.Fatalf("EnsureContainer (scale 2 -> 3): %v", err)
		}
		if hasCounter && mc.Mutations() == before {
			t.Errorf("scaling Replicas 2 -> 3 was not detected as a spec change")
		}
		if hasCounter && mc.Mutations() != before+1 {
			t.Errorf("scaling Replicas 2 -> 3 caused %d mutations, want exactly 1 (the new ordinal) — existing ordinals must not be recreated on scale-up", mc.Mutations()-before)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy after scale to 3: %v", err)
		}
		waitReadyReplicas(ctx, t, rt, name, 3, 60*time.Second)
		if ord0IDBefore != "" {
			ordSt, ordFound, err := rt.Inspect(ctx, ordName)
			if err != nil || !ordFound {
				t.Fatalf("Inspect(%q) after scale-up: found=%v err=%v", ordName, ordFound, err)
			}
			if ordSt.ID != ord0IDBefore {
				t.Errorf("scale-up 2 -> 3 recreated existing ordinal %q (ID %q -> %q); existing ordinals must be untouched", ordName, ord0IDBefore, ordSt.ID)
			}
		}
	})

	// ReplicaSet_OrdinalHostnameResolution proves every ordinal of a
	// StableIdentity set is individually, distinctly addressable by its own
	// stable name ("<Name>-<i>", runtime.OrdinalName) — the port-level
	// meaning of "ordinal hostname resolution": Inspect against an ordinal
	// name resolves to that one replica's own state, not the aggregate, and
	// two different ordinals are never the same underlying unit.
	t.Run("ReplicaSet_OrdinalHostnameResolution", func(t *testing.T) {
		name := fx.namePrefix + "-ordinal-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:           name,
			Image:          "alpine:3.20",
			Cmd:            []string{"sleep", "300"},
			Networks:       []string{fx.netSpec.Name},
			Labels:         fx.labels,
			Replicas:       2,
			StableIdentity: true,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer (StableIdentity, Replicas: 2): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}
		// WaitHealthy returns at 1-of-N ready; converge to the full count
		// before inspecting individual ordinals (racy on a real cluster).
		waitReadyReplicas(ctx, t, rt, name, 2, 60*time.Second)

		var ordinalIDs []string
		for i := 0; i < 2; i++ {
			ordName := runtime.OrdinalName(name, i)
			st, found, err := rt.Inspect(ctx, ordName)
			if err != nil {
				t.Fatalf("Inspect(%q): %v", ordName, err)
			}
			if !found {
				t.Fatalf("ordinal %q not individually resolvable", ordName)
			}
			if st.Name != ordName {
				t.Errorf("Inspect(%q).Name = %q, want %q", ordName, st.Name, ordName)
			}
			ordinalIDs = append(ordinalIDs, st.ID)
		}
		if ordinalIDs[0] == ordinalIDs[1] {
			t.Errorf("ordinal 0 and ordinal 1 resolved to the same underlying unit (ID %q)", ordinalIDs[0])
		}

		aggregate, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect(aggregate %q): found=%v err=%v", name, found, err)
		}
		if aggregate.Name != name {
			t.Errorf("aggregate Inspect Name = %q, want %q", aggregate.Name, name)
		}
	})
}

// runReplicasShapeTransition registers ReplicaSet_ShapeTransition_Refused,
// pinning the consistent posture for unsupported shape transitions
// (docs/adr/004, Known limitations): collapsing an existing multi-replica
// set to a single container in place is refused with an error naming the
// remedy (destroy and recreate), on every adapter — rather than silently
// leaving stale ordinals (Docker) or a stale StatefulSet (Kubernetes)
// serving the shared name.
func runReplicasShapeTransition(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("ReplicaSet_ShapeTransition_Refused", func(t *testing.T) {
		ctx := fx.ctx
		name := fx.namePrefix + "-shape-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:           name,
			Image:          "alpine:3.20",
			Cmd:            []string{"sleep", "300"},
			Networks:       []string{fx.netSpec.Name},
			Labels:         fx.labels,
			Replicas:       2,
			StableIdentity: true,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer (StableIdentity, Replicas: 2): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}

		// Collapsing to the bare single-container shape (StableIdentity off,
		// one replica) is the transition that must be refused. Note the
		// docs/adr/017 §a.2 amendment: StableIdentity with Replicas: 1 is
		// NOT this shape — it is a valid 1-member ordinal set, covered by
		// ReplicaSet_StableIdentitySingleOrdinal below.
		collapsed := spec
		collapsed.Replicas = 1
		collapsed.StableIdentity = false
		if _, err := rt.EnsureContainer(ctx, collapsed); err == nil {
			t.Fatal("EnsureContainer converted a multi-replica set to a single container in place; the transition must be refused (remedy: destroy and recreate)")
		}
		// The refused call must have changed nothing: the set is still there
		// and still aggregates as before.
		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect after refused shape transition: found=%v err=%v", found, err)
		}
		if !st.Healthy {
			t.Errorf("replica set unhealthy after a refused shape transition")
		}
	})

	// ReplicaSet_StableIdentitySingleOrdinal pins docs/adr/017 §a.2's
	// amendment to docs/adr/004: StableIdentity selects the ordinal-set
	// shape at ANY replica count. A StableIdentity spec with one replica is
	// a 1-member set (ordinal "<name>-0", never a bare "<name>" container),
	// which is exactly what lets a stateful cluster (C2's brokers) scale
	// 1 -> N -> 1 in place without crossing the shape boundary the previous
	// subtest refuses.
	t.Run("ReplicaSet_StableIdentitySingleOrdinal", func(t *testing.T) {
		ctx := fx.ctx
		name := fx.namePrefix + "-sid1-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:           name,
			Image:          "alpine:3.20",
			Cmd:            []string{"sleep", "300"},
			Networks:       []string{fx.netSpec.Name},
			Labels:         fx.labels,
			Replicas:       1,
			StableIdentity: true,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer (StableIdentity, Replicas: 1): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}
		waitReadyReplicas(ctx, t, rt, name, 1, 60*time.Second)
		ordName := runtime.OrdinalName(name, 0)
		if _, found, err := rt.Inspect(ctx, ordName); err != nil || !found {
			t.Fatalf("ordinal %q of a 1-member StableIdentity set not resolvable: found=%v err=%v", ordName, found, err)
		}

		// Idempotency at the 1-member shape (NFR-2).
		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("second EnsureContainer (unchanged 1-member set): %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Errorf("second EnsureContainer with identical 1-member StableIdentity spec mutated state (NFR-2 violation)")
		}

		// Scale 1 -> 2 in place: same shape, one new ordinal.
		scaled := spec
		scaled.Replicas = 2
		if _, err := rt.EnsureContainer(ctx, scaled); err != nil {
			t.Fatalf("EnsureContainer (scale 1 -> 2 within the StableIdentity shape): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy after scale to 2: %v", err)
		}
		waitReadyReplicas(ctx, t, rt, name, 2, 60*time.Second)

		// And back 2 -> 1: a scale-down WITHIN the set shape is runtime
		// mechanics (whether it is safe is the provider's call —
		// docs/adr/017 §a.5), unlike the cross-shape collapse above.
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer (scale 2 -> 1 within the StableIdentity shape): %v", err)
		}
		waitReadyReplicas(ctx, t, rt, name, 1, 60*time.Second)
	})

	// ReplicaSet_OrdinalInNetworkDNS pins the cross-runtime claim
	// docs/adr/004 stated and C2 caught unimplemented live on Kubernetes
	// (docs/adr/017 §a.3): an ordinal's bare short name ("<name>-<i>") is
	// dialable from an in-network vantage point on every adapter — Docker
	// resolves the ordinal container name natively; Kubernetes needs one
	// Service per ordinal (a StatefulSet pod's DNS record is not covered by
	// the namespace's search domain); the fake's strict interpreter
	// resolves its managed ordinal records. Without this, a stateful
	// cluster's seed list (peers dialing "<name>-0") and any in-network
	// consumer's bootstrap list silently fail on Kubernetes only.
	t.Run("ReplicaSet_OrdinalInNetworkDNS", func(t *testing.T) {
		ctx := fx.ctx
		name := fx.namePrefix + "-orddns-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		type commandRunner interface{ RunsContainerCommands() bool }
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{fx.netSpec.Name},
			Ports:    []runtime.PortBinding{{ContainerPort: 8080, Audience: runtime.AudienceInternal}},
			Labels:   fx.labels,
			// Two members so the probe's vantage point can be the *other*
			// member — cross-member resolution is the claim, not loopback.
			Replicas:       2,
			StableIdentity: true,
		}
		if execCapable {
			// A real listener a real in-network dial can succeed against
			// (the ProbeReachable_InNetwork fixture's pattern).
			spec.Cmd = []string{"sh", "-c", "while true; do nc -l -p 8080; done"}
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer (StableIdentity, Replicas: 2): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}
		waitReadyReplicas(ctx, t, rt, name, 2, 60*time.Second)
		for i := 0; i < 2; i++ {
			target := fmt.Sprintf("%s:%d", runtime.OrdinalName(name, i), 8080)
			if err := rt.ProbeReachable(ctx, fx.netSpec.Name, target); err != nil {
				t.Errorf("ProbeReachable(%q, %q) = %v; ordinal short names must resolve in-network on every adapter (docs/adr/017 §a.3)", fx.netSpec.Name, target, err)
			}
		}
	})

	// ReplicaSet_SingleToSetRefused pins the mirror direction of
	// ReplicaSet_ShapeTransition_Refused (docs/adr/017 §a.2): an existing
	// bare single container is never converted to a replica set in place —
	// fanning ordinals out beside it would leave the stale single serving
	// the shared name.
	t.Run("ReplicaSet_SingleToSetRefused", func(t *testing.T) {
		ctx := fx.ctx
		name := fx.namePrefix + "-single2set-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		single := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{fx.netSpec.Name},
			Labels:   fx.labels,
		}
		if _, err := rt.EnsureContainer(ctx, single); err != nil {
			t.Fatalf("EnsureContainer (single): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}

		set := single
		set.Replicas = 2
		set.StableIdentity = true
		if _, err := rt.EnsureContainer(ctx, set); err == nil {
			t.Fatal("EnsureContainer converted a single container to a replica set in place; the transition must be refused (remedy: destroy and recreate)")
		}
		// The refused call must have changed nothing.
		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect after refused single-to-set transition: found=%v err=%v", found, err)
		}
		if !st.Running {
			t.Errorf("single container not running after a refused single-to-set transition")
		}
	})
}
