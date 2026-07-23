//go:build integration

package docker

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types/volume"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestRemoveLeavesNoAnonymousVolumeResidue enforces docs/adr/029's
// residue-free removal contract at its Docker-specific edge: an image
// that declares a VOLUME (postgres does — /var/lib/postgresql/data) gets
// an anonymous volume per container creation, and ContainerRemove leaks
// it unless RemoveVolumes is passed. That exact leak accumulated 3853
// dangling volumes (8.4GB) before the 2026-07-23 residue audit found it
// — no functional test could, because leaked volumes affect nothing a
// behavior test observes. So this test watches the daemon's own volume
// count: after Ensure -> Remove of a volume-declaring image, the count
// must return to its pre-test value.
func TestRemoveLeavesNoAnonymousVolumeResidue(t *testing.T) {
	rt, err := New(nil)
	if err != nil {
		t.Skipf("no docker daemon; skipping: %v", err)
	}
	ctx := context.Background()

	countVolumes := func() int {
		list, err := rt.cli.VolumeList(ctx, volume.ListOptions{})
		if err != nil {
			t.Fatalf("list volumes: %v", err)
		}
		return len(list.Volumes)
	}
	before := countVolumes()

	const name = "datascape-residue-probe"
	// Same pinned postgres image the backup suites use — its VOLUME
	// declaration is the fixture; the container never needs to be healthy,
	// only created (the anonymous volume exists from creation).
	spec := runtime.ContainerSpec{
		Name:  name,
		Image: "postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20",
		Env:   map[string]string{"POSTGRES_PASSWORD": "residue-probe"},
		Labels: map[string]string{
			runtime.LabelManagedBy: runtime.ManagedByValue,
		},
	}
	if _, err := rt.EnsureContainer(ctx, spec); err != nil {
		t.Fatalf("EnsureContainer: %v", err)
	}
	// Belt: if the Remove under test fails, don't strand the probe.
	t.Cleanup(func() { _ = rt.Remove(ctx, name) })

	if got := countVolumes(); got <= before {
		t.Fatalf("fixture image declared no volume (count %d -> %d) — the probe proves nothing; pick a VOLUME-declaring image", before, got)
	}

	if err := rt.Remove(ctx, name); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if after := countVolumes(); after != before {
		t.Errorf("Remove leaked derived volume(s): daemon volume count %d before, %d after (docs/adr/029: removal is residue-free — ContainerRemove must pass RemoveVolumes)", before, after)
	}
}
