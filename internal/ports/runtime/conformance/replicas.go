package conformance

import (
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

		collapsed := spec
		collapsed.Replicas = 1
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
}
