package conformance

import (
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// runVolumeEnsure registers EnsureVolume_idempotent.
func runVolumeEnsure(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("EnsureVolume_idempotent", func(t *testing.T) {
		if err := rt.EnsureVolume(fx.ctx, fx.volSpec); err != nil {
			t.Fatalf("first EnsureVolume: %v", err)
		}
		if err := rt.EnsureVolume(fx.ctx, fx.volSpec); err != nil {
			t.Fatalf("second EnsureVolume: %v", err)
		}
	})
}

// runVolumePersistence registers Volume_persists_across_container_update.
//
// commandRunner is optionally implemented by adapters whose containers
// actually execute their declared Cmd (Docker, Kubernetes) — the fake
// adapter never runs anything, so it cannot meaningfully prove "the
// container's own process wrote a file that survived a recreate."
// Structural coverage for the fake (the volume identity itself isn't lost
// across generations) still runs; the real write-recreate-readback proof is
// skipped there, not faked.
func runVolumePersistence(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("Volume_persists_across_container_update", func(t *testing.T) {
		ctx := fx.ctx
		ctrName := fx.namePrefix + "-persist-ctr"
		volName := fx.namePrefix + "-persist-vol"
		t.Cleanup(func() {
			_ = rt.Remove(ctx, ctrName)
			_ = rt.RemoveVolume(ctx, volName)
		})
		persistVol := runtime.VolumeSpec{Name: volName, Labels: fx.labels, Networks: []string{fx.netSpec.Name}, SizeBytes: 256 * 1024 * 1024}
		if err := rt.EnsureVolume(ctx, persistVol); err != nil {
			t.Fatalf("EnsureVolume: %v", err)
		}

		type commandRunner interface {
			RunsContainerCommands() bool
		}
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()

		const path = "/data/marker"
		const content = "persisted-across-update"
		cmd := []string{"sleep", "300"}
		if execCapable {
			// The container's own process writes the marker — a real write
			// into the volume's backing storage. Placing it via
			// ContainerSpec.Files instead would be misleading here: on
			// Kubernetes a FileMount is a Secret bind-mount overlay, not a
			// write into the PVC itself, and would prove nothing about real
			// volume durability.
			cmd = []string{"sh", "-c", "echo -n '" + content + "' > " + path + " && sleep 300"}
		}
		gen1 := runtime.ContainerSpec{
			Name:     ctrName,
			Image:    "alpine:3.20",
			Cmd:      cmd,
			Networks: []string{fx.netSpec.Name},
			Volumes:  []runtime.VolumeMount{{VolumeName: volName, MountPath: "/data"}},
			Env:      map[string]string{"GENERATION": "1"},
			Labels:   fx.labels,
		}
		if _, err := rt.EnsureContainer(ctx, gen1); err != nil {
			t.Fatalf("EnsureContainer (generation 1): %v", err)
		}
		if err := rt.WaitHealthy(ctx, ctrName, 30*time.Second); err != nil {
			t.Fatalf("generation 1 did not become healthy: %v", err)
		}

		if !execCapable {
			// Structural check only: EnsureVolume against the same spec a
			// second generation would also request stays a no-op, and the
			// volume is still there to be mounted again.
			if err := rt.EnsureVolume(ctx, persistVol); err != nil {
				t.Fatalf("EnsureVolume (re-check): %v", err)
			}
			return
		}

		// Generation 2: a different env value forces recreation (a new
		// spec hash) without rewriting the marker — only the volume's own
		// persistence can make it survive.
		gen2 := gen1
		gen2.Cmd = []string{"sleep", "300"}
		gen2.Env = map[string]string{"GENERATION": "2"}
		if _, err := rt.EnsureContainer(ctx, gen2); err != nil {
			t.Fatalf("EnsureContainer (generation 2): %v", err)
		}
		if err := rt.WaitHealthy(ctx, ctrName, 30*time.Second); err != nil {
			t.Fatalf("generation 2 did not become healthy: %v", err)
		}

		got, err := rt.ReadFile(ctx, ctrName, path)
		if err != nil {
			t.Fatalf("ReadFile after update: %v", err)
		}
		if string(got) != content {
			t.Errorf("volume content after container update = %q, want %q (volume did not persist)", got, content)
		}
	})
}

// runVolumeReplicaPersistence registers ReplicaSet_PerOrdinalVolumePersistence,
// proving StableIdentity's other half: each ordinal owns its own volume set,
// isolated from its siblings' and surviving a container recreation — the
// StatefulSet/per-ordinal-Docker-volume path C2 (Redpanda) and C4 (MinIO)
// will build on. commandRunner-gated exactly like
// Volume_persists_across_container_update above: only an adapter whose
// containers run a real process can prove the write-recreate-readback round
// trip; the fake proves the structural half (the runtime itself creates one
// volume per ordinal).
func runVolumeReplicaPersistence(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	t.Run("ReplicaSet_PerOrdinalVolumePersistence", func(t *testing.T) {
		ctx := fx.ctx
		name := fx.namePrefix + "-stateful-ctr"
		volBase := fx.namePrefix + "-stateful-vol"
		t.Cleanup(func() {
			_ = rt.Remove(ctx, name)
			for i := 0; i < 2; i++ {
				// Docker/fake name per-ordinal volumes "<claim>-<i>" ...
				_ = rt.RemoveVolume(ctx, runtime.OrdinalName(volBase, i))
				// ... while Kubernetes StatefulSet VolumeClaimTemplates name
				// the per-ordinal PVCs "<claim>-<setName>-<i>" — best-effort
				// delete those too so integration runs don't leak claims.
				_ = rt.RemoveVolume(ctx, runtime.OrdinalName(volBase+"-"+name, i))
			}
		})

		type commandRunner interface {
			RunsContainerCommands() bool
		}
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()

		const path = "/data/marker"
		cmd := []string{"sleep", "300"}
		if execCapable {
			// Each ordinal's own hostname equals its ordinal name on every
			// adapter by construction (Docker: container name; Kubernetes:
			// StatefulSet-assigned pod name) — embedding it in the written
			// content proves both per-ordinal isolation (no cross-writes)
			// and that the hostname really is ordinal-specific, without
			// needing any per-ordinal Env templating from the port itself.
			cmd = []string{"sh", "-c", "echo -n \"data-for-$(hostname)\" > " + path + " && sleep 300"}
		}
		gen1 := runtime.ContainerSpec{
			Name:           name,
			Image:          "alpine:3.20",
			Cmd:            cmd,
			Networks:       []string{fx.netSpec.Name},
			Volumes:        []runtime.VolumeMount{{VolumeName: volBase, MountPath: "/data"}},
			Env:            map[string]string{"GENERATION": "1"},
			Labels:         fx.labels,
			Replicas:       2,
			StableIdentity: true,
		}
		if _, err := rt.EnsureContainer(ctx, gen1); err != nil {
			t.Fatalf("EnsureContainer (generation 1): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("generation 1 did not become healthy: %v", err)
		}

		if !execCapable {
			vols, err := rt.ListManagedVolumes(ctx)
			if err != nil {
				t.Fatalf("ListManagedVolumes: %v", err)
			}
			for i := 0; i < 2; i++ {
				want := runtime.OrdinalName(volBase, i)
				found := false
				for _, v := range vols {
					if v.Name == want {
						found = true
					}
				}
				if !found {
					t.Errorf("ListManagedVolumes missing per-ordinal volume %q", want)
				}
			}
			return
		}

		// Generation 2: a different env value forces recreation (a new spec
		// hash) without rewriting the markers — only each ordinal's own
		// volume persistence can make its content survive.
		gen2 := gen1
		gen2.Cmd = []string{"sleep", "300"}
		gen2.Env = map[string]string{"GENERATION": "2"}
		if _, err := rt.EnsureContainer(ctx, gen2); err != nil {
			t.Fatalf("EnsureContainer (generation 2): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("generation 2 did not become healthy: %v", err)
		}
		// WaitHealthy returns at 1-of-N ready; both ordinals must be back up
		// before reading each one's marker (racy on a real cluster).
		waitReadyReplicas(ctx, t, rt, name, 2, 60*time.Second)

		for i := 0; i < 2; i++ {
			ordName := runtime.OrdinalName(name, i)
			want := "data-for-" + ordName
			got, err := rt.ReadFile(ctx, ordName, path)
			if err != nil {
				t.Fatalf("ReadFile(%q) after update: %v", ordName, err)
			}
			if string(got) != want {
				t.Errorf("ordinal %d content after update = %q, want %q (per-ordinal volume did not persist, or ordinals are not isolated)", i, got, want)
			}
		}
	})
}
