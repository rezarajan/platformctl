package conformance

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// runNetworkEnsure registers EnsureNetwork_idempotent.
func runNetworkEnsure(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("EnsureNetwork_idempotent", func(t *testing.T) {
		if err := rt.EnsureNetwork(fx.ctx, fx.netSpec); err != nil {
			t.Fatalf("first EnsureNetwork: %v", err)
		}
		if err := rt.EnsureNetwork(fx.ctx, fx.netSpec); err != nil {
			t.Fatalf("second EnsureNetwork: %v", err)
		}
	})
}

// runNetworkAndVolumeListing registers
// ListManagedNetworks_and_Volumes_only_labeled. It runs after
// runContainerLifecycle (conformance.go's Run) has created fx.netSpec's
// network and fx.volSpec's volume, both of which it asserts are present.
func runNetworkAndVolumeListing(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("ListManagedNetworks_and_Volumes_only_labeled", func(t *testing.T) {
		nets, err := rt.ListManagedNetworks(fx.ctx)
		if err != nil {
			t.Fatalf("ListManagedNetworks: %v", err)
		}
		foundNet := false
		for _, n := range nets {
			if n.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
				t.Errorf("ListManagedNetworks returned unlabeled object %q", n.Name)
			}
			if n.Name == fx.netSpec.Name {
				foundNet = true
			}
		}
		if !foundNet {
			t.Errorf("ListManagedNetworks did not include %q", fx.netSpec.Name)
		}

		vols, err := rt.ListManagedVolumes(fx.ctx)
		if err != nil {
			t.Fatalf("ListManagedVolumes: %v", err)
		}
		foundVol := false
		for _, v := range vols {
			if v.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
				t.Errorf("ListManagedVolumes returned unlabeled object %q", v.Name)
			}
			if v.Name == fx.volSpec.Name {
				foundVol = true
			}
		}
		if !foundVol {
			t.Errorf("ListManagedVolumes did not include %q", fx.volSpec.Name)
		}
	})
}

// runNetworkTeardown registers RemoveNetwork_refuses_while_container_attached
// and Remove_then_absent — the suite's final teardown. See Run's sequencing
// comment in conformance.go for why this must run last.
func runNetworkTeardown(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("RemoveNetwork_refuses_while_container_attached", func(t *testing.T) {
		// fx.ctrSpec's container (created by runContainerLifecycle, back in
		// conformance.go's Run) is still attached to fx.netSpec here —
		// Remove_then_absent (below) is what finally tears it down. Removing
		// a network out from under a container still attached to it must
		// fail and change nothing. The shared-network destroy pattern
		// depends on this: every provider best-effort-calls RemoveNetwork on
		// Destroy, so the network must outlive every member but the last,
		// and RemoveNetwork must never cascade-delete a member. Docker
		// enforces it via "network has active endpoints"; the Kubernetes
		// adapter must not let a Namespace deletion cascade to a
		// still-running Deployment (regression: a shared-namespace destroy
		// that wiped its siblings and any unmanaged workload alongside them
		// — docs/history/errors.md, 2026-07-20).
		if err := rt.RemoveNetwork(fx.ctx, fx.netSpec.Name); err == nil {
			t.Fatal("RemoveNetwork removed a network that still has a container attached")
		}
		if _, found, err := rt.Inspect(fx.ctx, fx.ctrSpec.Name); err != nil {
			t.Fatalf("Inspect after refused RemoveNetwork: %v", err)
		} else if !found {
			t.Fatal("container was deleted as a side effect of RemoveNetwork")
		}
		nets, err := rt.ListManagedNetworks(fx.ctx)
		if err != nil {
			t.Fatalf("ListManagedNetworks: %v", err)
		}
		var stillThere bool
		for _, n := range nets {
			if n.Name == fx.netSpec.Name {
				stillThere = true
			}
		}
		if !stillThere {
			t.Errorf("network %q missing after a refused RemoveNetwork", fx.netSpec.Name)
		}
	})

	t.Run("Remove_then_absent", func(t *testing.T) {
		// Depends on RemoveNetwork_refuses_while_container_attached (above)
		// having left fx.ctrSpec's container and fx.netSpec's network
		// intact; this subtest is what finally removes both, plus
		// fx.volSpec's volume.
		if err := rt.Remove(fx.ctx, fx.ctrSpec.Name); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		_, found, err := rt.Inspect(fx.ctx, fx.ctrSpec.Name)
		if err != nil {
			t.Fatalf("Inspect after Remove: %v", err)
		}
		if found {
			t.Fatalf("container still present after Remove")
		}
		if err := rt.RemoveNetwork(fx.ctx, fx.netSpec.Name); err != nil {
			t.Fatalf("RemoveNetwork: %v", err)
		}
		if err := rt.RemoveVolume(fx.ctx, fx.volSpec.Name); err != nil {
			t.Fatalf("RemoveVolume: %v", err)
		}
	})
}
