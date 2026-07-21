// Package conformance is the shared contract test suite every
// ContainerRuntime adapter must pass — both adapters/runtime/fake and
// adapters/runtime/docker. This is what keeps the fake honest and catches
// adapters that violate the Ensure* idempotency contract.
// See docs/planning/02-architecture.md §9.
package conformance

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// MutationCounter is optionally implemented by adapters that can report how
// many real state mutations occurred (the fake does; the Docker adapter may
// approximate via API call counting in its test harness).
type MutationCounter interface {
	Mutations() int
}

// Run executes the conformance suite against the given runtime. namePrefix
// isolates this run's objects (important against a real Docker daemon).
func Run(t *testing.T, rt runtime.ContainerRuntime, namePrefix string) {
	t.Helper()
	ctx := context.Background()

	labels := map[string]string{
		runtime.LabelManagedBy:  runtime.ManagedByValue,
		runtime.LabelGeneration: "conformance",
	}

	netSpec := runtime.NetworkSpec{Name: namePrefix + "-net", Labels: labels}
	volSpec := runtime.VolumeSpec{Name: namePrefix + "-vol", Labels: labels, Networks: []string{netSpec.Name}}
	ctrSpec := runtime.ContainerSpec{
		Name:     namePrefix + "-ctr",
		Image:    "alpine:3.20",
		Cmd:      []string{"sleep", "300"}, // must outlive the suite against a real daemon
		Networks: []string{netSpec.Name},
		Volumes:  []runtime.VolumeMount{{VolumeName: volSpec.Name, MountPath: "/data"}},
		Labels:   labels,
	}

	t.Run("EnsureNetwork_idempotent", func(t *testing.T) {
		if err := rt.EnsureNetwork(ctx, netSpec); err != nil {
			t.Fatalf("first EnsureNetwork: %v", err)
		}
		if err := rt.EnsureNetwork(ctx, netSpec); err != nil {
			t.Fatalf("second EnsureNetwork: %v", err)
		}
	})

	t.Run("EnsureVolume_idempotent", func(t *testing.T) {
		if err := rt.EnsureVolume(ctx, volSpec); err != nil {
			t.Fatalf("first EnsureVolume: %v", err)
		}
		if err := rt.EnsureVolume(ctx, volSpec); err != nil {
			t.Fatalf("second EnsureVolume: %v", err)
		}
	})

	t.Run("EnsureContainer_idempotent", func(t *testing.T) {
		if _, err := rt.EnsureContainer(ctx, ctrSpec); err != nil {
			t.Fatalf("first EnsureContainer: %v", err)
		}
		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, ctrSpec); err != nil {
			t.Fatalf("second EnsureContainer: %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Fatalf("second EnsureContainer with identical spec mutated state (NFR-2 violation)")
		}
	})

	t.Run("Inspect_found", func(t *testing.T) {
		st, found, err := rt.Inspect(ctx, ctrSpec.Name)
		if err != nil {
			t.Fatalf("Inspect: %v", err)
		}
		if !found {
			t.Fatalf("Inspect: container %q not found after EnsureContainer", ctrSpec.Name)
		}
		if st.Name != ctrSpec.Name {
			t.Errorf("Inspect name = %q, want %q", st.Name, ctrSpec.Name)
		}
	})

	t.Run("Inspect_absent", func(t *testing.T) {
		_, found, err := rt.Inspect(ctx, namePrefix+"-does-not-exist")
		if err != nil {
			t.Fatalf("Inspect absent: %v", err)
		}
		if found {
			t.Fatalf("Inspect reported a nonexistent container as found")
		}
	})

	t.Run("WaitHealthy", func(t *testing.T) {
		if err := rt.WaitHealthy(ctx, ctrSpec.Name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}
	})

	t.Run("ListManaged_only_labeled", func(t *testing.T) {
		states, err := rt.ListManaged(ctx)
		if err != nil {
			t.Fatalf("ListManaged: %v", err)
		}
		foundOurs := false
		for _, s := range states {
			if s.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
				t.Errorf("ListManaged returned unlabeled object %q", s.Name)
			}
			if s.Name == ctrSpec.Name {
				foundOurs = true
			}
		}
		if !foundOurs {
			t.Errorf("ListManaged did not include %q", ctrSpec.Name)
		}
	})

	t.Run("ListManagedNetworks_and_Volumes_only_labeled", func(t *testing.T) {
		nets, err := rt.ListManagedNetworks(ctx)
		if err != nil {
			t.Fatalf("ListManagedNetworks: %v", err)
		}
		foundNet := false
		for _, n := range nets {
			if n.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
				t.Errorf("ListManagedNetworks returned unlabeled object %q", n.Name)
			}
			if n.Name == netSpec.Name {
				foundNet = true
			}
		}
		if !foundNet {
			t.Errorf("ListManagedNetworks did not include %q", netSpec.Name)
		}

		vols, err := rt.ListManagedVolumes(ctx)
		if err != nil {
			t.Fatalf("ListManagedVolumes: %v", err)
		}
		foundVol := false
		for _, v := range vols {
			if v.Labels[runtime.LabelManagedBy] != runtime.ManagedByValue {
				t.Errorf("ListManagedVolumes returned unlabeled object %q", v.Name)
			}
			if v.Name == volSpec.Name {
				foundVol = true
			}
		}
		if !foundVol {
			t.Errorf("ListManagedVolumes did not include %q", volSpec.Name)
		}
	})

	// EnsureContainer_imagePullAuth_accepted proves ContainerSpec.ImagePullAuth
	// round-trips safely through every adapter (docs/planning/07 §1.1
	// deferral, docs/planning/08 A1) — PullNever so it never attempts a real
	// pull with these (deliberately fake) credentials against a real
	// registry; a real authenticated pull is proven separately by the
	// Docker-only integration test against a local htpasswd registry, which
	// controls credentials it knows are correct rather than risking a public
	// registry rejecting bogus ones.
	t.Run("EnsureContainer_imagePullAuth_accepted", func(t *testing.T) {
		name := namePrefix + "-auth-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		authSpec := runtime.ContainerSpec{
			Name:       name,
			Image:      ctrSpec.Image, // already ensured present by an earlier subtest
			PullPolicy: runtime.PullNever,
			Cmd:        []string{"sleep", "60"},
			Networks:   []string{netSpec.Name},
			Labels:     labels,
			ImagePullAuth: &runtime.ImagePullAuth{
				Username: "conformance-user",
				Password: "conformance-password",
				Registry: "registry.example.com",
			},
		}
		if _, err := rt.EnsureContainer(ctx, authSpec); err != nil {
			t.Fatalf("EnsureContainer with ImagePullAuth: %v", err)
		}
		if _, found, err := rt.Inspect(ctx, name); err != nil || !found {
			t.Fatalf("Inspect after EnsureContainer with ImagePullAuth: found=%v err=%v", found, err)
		}
		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, authSpec); err != nil {
			t.Fatalf("second EnsureContainer with ImagePullAuth: %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Errorf("second EnsureContainer with identical ImagePullAuth mutated state (NFR-2 violation)")
		}
	})

	t.Run("EnsureContainer_productionFields_idempotent", func(t *testing.T) {
		name := namePrefix + "-prod-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		prodSpec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{netSpec.Name},
			Labels:   labels,
			RestartPolicy: &runtime.RestartPolicy{
				Mode:       "on-failure",
				MaxRetries: 3,
			},
			Resources: &runtime.Resources{
				CPULimit:               0.5,
				MemoryLimitBytes:       128 * 1024 * 1024,
				MemoryReservationBytes: 64 * 1024 * 1024,
			},
			Security: &runtime.SecurityContext{
				ReadOnlyRootFS: false, // alpine needs a writable rootfs to sleep
			},
			LogConfig: &runtime.LogConfig{Driver: "json-file"},
		}
		if _, err := rt.EnsureContainer(ctx, prodSpec); err != nil {
			t.Fatalf("first EnsureContainer with production fields: %v", err)
		}
		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, prodSpec); err != nil {
			t.Fatalf("second EnsureContainer with production fields: %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Fatalf("second EnsureContainer with identical production-field spec mutated state (NFR-2 violation)")
		}

		// Changing a production field (not name/image/labels/env/networks)
		// must still be detected as drift, not silently ignored.
		changed := prodSpec
		changed.RestartPolicy = &runtime.RestartPolicy{Mode: "always"}
		if _, err := rt.EnsureContainer(ctx, changed); err != nil {
			t.Fatalf("EnsureContainer with changed restart policy: %v", err)
		}
		if hasCounter && mc.Mutations() == before {
			t.Fatalf("changing RestartPolicy alone was not detected as a spec change")
		}
	})

	t.Run("Logs_returns_without_error", func(t *testing.T) {
		if _, err := rt.Logs(ctx, ctrSpec.Name, 5); err != nil {
			t.Fatalf("Logs: %v", err)
		}
	})

	t.Run("EnsureContainer_aliases_idempotent", func(t *testing.T) {
		name := namePrefix + "-alias-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{netSpec.Name},
			Aliases:  []string{namePrefix + "-stable-alias"},
			Labels:   labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("first EnsureContainer with aliases: %v", err)
		}
		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("second EnsureContainer with aliases: %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Fatalf("second EnsureContainer with identical alias spec mutated state (NFR-2 violation)")
		}
	})

	t.Run("FileMount_readable_by_process_and_ReadFile", func(t *testing.T) {
		name := namePrefix + "-files-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		const path = "/run/datascape/secret-material"
		const content = "hunter2-from-file"
		spec := runtime.ContainerSpec{
			Name:  name,
			Image: "alpine:3.20",
			// The container only stays alive (= healthy) if the mounted
			// file exists with the exact expected content — proving the
			// file is present before PID 1 runs, not injected after.
			Cmd:      []string{"sh", "-c", `[ "$(cat ` + path + `)" = "` + content + `" ] && sleep 300`},
			Networks: []string{netSpec.Name},
			Files:    []runtime.FileMount{{Path: path, Content: []byte(content)}},
			Labels:   labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer with file mount: %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("container did not see the mounted file content: %v", err)
		}
		got, err := rt.ReadFile(ctx, name, path)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if string(got) != content {
			t.Errorf("ReadFile = %q, want %q", got, content)
		}
		// Secret material must not leak into inspectable env.
		st, _, err := rt.Inspect(ctx, name)
		if err != nil {
			t.Fatalf("Inspect: %v", err)
		}
		for k, v := range st.Env {
			if v == content {
				t.Errorf("file-mounted secret material leaked into env var %q", k)
			}
		}
	})

	// commandRunner is optionally implemented by adapters whose containers
	// actually execute their declared Cmd (Docker, Kubernetes) — the fake
	// adapter never runs anything, so it cannot meaningfully prove "the
	// container's own process wrote a file that survived a recreate."
	// Structural coverage for the fake (the volume identity itself isn't
	// lost across generations) still runs; the real write-recreate-readback
	// proof is skipped there, not faked.
	t.Run("Volume_persists_across_container_update", func(t *testing.T) {
		ctrName := namePrefix + "-persist-ctr"
		volName := namePrefix + "-persist-vol"
		t.Cleanup(func() {
			_ = rt.Remove(ctx, ctrName)
			_ = rt.RemoveVolume(ctx, volName)
		})
		persistVol := runtime.VolumeSpec{Name: volName, Labels: labels, Networks: []string{netSpec.Name}, SizeBytes: 256 * 1024 * 1024}
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
			Networks: []string{netSpec.Name},
			Volumes:  []runtime.VolumeMount{{VolumeName: volName, MountPath: "/data"}},
			Env:      map[string]string{"GENERATION": "1"},
			Labels:   labels,
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

	t.Run("Inspect_reports_observed_ports", func(t *testing.T) {
		name := namePrefix + "-ports-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{netSpec.Name},
			Ports:    []runtime.PortBinding{{HostPort: 28999, ContainerPort: 80, Audience: runtime.AudienceHost}},
			Labels:   labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect: found=%v err=%v", found, err)
		}
		var got *runtime.PortBinding
		for i := range st.Ports {
			if st.Ports[i].ContainerPort == 80 {
				got = &st.Ports[i]
			}
		}
		if got == nil {
			t.Fatalf("Inspect did not report the published container port 80; ports = %+v", st.Ports)
		}
		// A runtime with host publishing (Docker, fake) must report the
		// concrete bind address, never an empty HostIP for a bound port.
		// A runtime without host publishing (Kubernetes) reports HostPort 0
		// and may leave HostIP empty.
		if got.HostPort != 0 && got.HostIP == "" {
			t.Errorf("published port reported with empty HostIP: %+v", *got)
		}
	})

	// PortBinding_audience_internal_never_host_bound proves the core F2
	// invariant across every adapter: a port declared Audience: internal
	// never gets a host-reachable address, no matter how permissive the
	// adapter otherwise is (docs/planning/08 F2, docs/planning/09 K10).
	// Audience: host is accepted alongside it in the same spec so the test
	// also proves EnsureContainer tolerates both audiences declared at once
	// — the shape every multi-listener provider (e.g. redpanda) actually
	// sends.
	t.Run("PortBinding_audience_internal_never_host_bound", func(t *testing.T) {
		name := namePrefix + "-audience-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{netSpec.Name},
			Ports: []runtime.PortBinding{
				{HostPort: 28998, ContainerPort: 81, Audience: runtime.AudienceHost},
				{ContainerPort: 82, Audience: runtime.AudienceInternal},
			},
			Labels: labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		mc, hasCounter := rt.(MutationCounter)
		before := 0
		if hasCounter {
			before = mc.Mutations()
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("second EnsureContainer with identical audience spec: %v", err)
		}
		if hasCounter && mc.Mutations() != before {
			t.Errorf("second EnsureContainer with identical audience spec mutated state (NFR-2 violation)")
		}

		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect: found=%v err=%v", found, err)
		}
		for _, p := range st.Ports {
			if p.ContainerPort == 82 && p.HostPort != 0 {
				t.Errorf("Audience: internal port 82 reported a host binding: %+v", p)
			}
		}
	})

	// EnsureReachable_dialable_immediately_after_WaitHealthy proves the F3
	// contract: once WaitHealthy returns, the very first EnsureReachable
	// call — no caller-side retry loop — must hand back an address that
	// accepts a real connection right now (docs/planning/08 F3,
	// docs/planning/09 Class 2 / K3 / K11). commandRunner-gated the same way
	// Volume_persists_across_container_update is: only an adapter whose
	// containers actually run a process can be dialed for real; the fake
	// proves the plumbing (EnsureReachable succeeds, returns a non-empty
	// address) without claiming to prove networking it doesn't have.
	t.Run("EnsureReachable_dialable_immediately_after_WaitHealthy", func(t *testing.T) {
		name := namePrefix + "-reachable-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })

		type commandRunner interface {
			RunsContainerCommands() bool
		}
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()

		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "nginx:1.27-alpine", // listens on 80 the instant its process starts — no artificial healthy-before-listening gap
			Networks: []string{netSpec.Name},
			Ports:    []runtime.PortBinding{{ContainerPort: 80, Audience: runtime.AudienceHost}},
			Labels:   labels,
		}
		if !execCapable {
			// The fake never runs a real process; nginx would never
			// actually listen, and the fake doesn't simulate Docker's
			// ephemeral host-port assignment for HostPort: 0. Exercise the
			// same spec shape with an explicit host port so the fake still
			// proves EnsureContainer/EnsureReachable's contract plumbing,
			// just not real dialability.
			spec.Image = "alpine:3.20"
			spec.Cmd = []string{"sleep", "300"}
			spec.Ports = []runtime.PortBinding{{HostPort: 28997, ContainerPort: 80, Audience: runtime.AudienceHost}}
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}

		addr, closeFn, err := rt.EnsureReachable(ctx, name, 80)
		if err != nil {
			t.Fatalf("EnsureReachable immediately after WaitHealthy: %v", err)
		}
		defer func() { _ = closeFn() }()
		if addr == "" {
			t.Fatal("EnsureReachable returned an empty address")
		}
		if !execCapable {
			return
		}
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %q immediately after EnsureReachable: %v (address was not actually dialable)", addr, err)
		}
		_ = conn.Close()
	})

	// DelayedListenReadiness_HealthyBeforeListening backfills the D1/K11
	// class (docs/planning/08 F3, docs/planning/09 Class 2): "healthy"
	// (no declared HealthCheck means healthy-when-running, the same
	// contract postgres's own pg_isready-over-the-unix-socket gap
	// exercised live) can report true before the container's declared port
	// actually accepts connections — a container that sleeps briefly
	// before opening its listener reproduces the shape generically,
	// documenting whatever gap this adapter has (t.Skip when the adapter
	// happens to have none) and proving runtime.WithReachable (F1/F3)
	// absorbs it regardless: a caller that dials once right after
	// WaitHealthy can lose the race, but one using WithReachable does not.
	t.Run("DelayedListenReadiness_HealthyBeforeListening", func(t *testing.T) {
		type commandRunner interface {
			RunsContainerCommands() bool
		}
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()
		if !execCapable {
			// The fake reports healthy without ever running a process, so
			// it has no healthy-vs-listening gap to document.
			return
		}

		name := namePrefix + "-delayed-listen-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		const listenDelay = 5 * time.Second
		spec := runtime.ContainerSpec{
			Name:  name,
			Image: "alpine:3.20",
			// No HealthCheck declared: healthy means running, the instant
			// the process starts — well before the delayed listener opens.
			Cmd:      []string{"sh", "-c", fmt.Sprintf("sleep %d && nc -l -p 8080", int(listenDelay.Seconds()))},
			Networks: []string{netSpec.Name},
			Ports:    []runtime.PortBinding{{HostPort: 28995, ContainerPort: 8080, Audience: runtime.AudienceHost}},
			Labels:   labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		start := time.Now()
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}
		if elapsed := time.Since(start); elapsed >= listenDelay {
			t.Skipf("WaitHealthy itself took %s (>= the %s listen delay); this adapter/environment left no healthy-before-listening gap to document here", elapsed, listenDelay)
		}

		err := runtime.WithReachable(ctx, rt, name, 8080, runtime.ReachableOptions{Timeout: listenDelay + 15*time.Second, Interval: 500 * time.Millisecond}, func(ctx context.Context, addr string) error {
			conn, derr := net.DialTimeout("tcp", addr, 2*time.Second)
			if derr != nil {
				return derr
			}
			return conn.Close()
		})
		if err != nil {
			t.Fatalf("WithReachable never dialed the delayed listener despite the container being healthy well before it opened: %v", err)
		}
	})

	// EntrypointFaithfulness_CmdAppendsNotReplaces backfills the K1 class
	// (docs/planning/08 F6, docs/planning/09 Class 5): the Kubernetes
	// adapter once mapped ContainerSpec.Cmd onto corev1.Container.Command
	// (Docker's ENTRYPOINT-*replacing* field) instead of .Args
	// (ENTRYPOINT-*appending*), silently skipping redpanda's image
	// entrypoint script and failing with "unrecognised option '--node-id'"
	// — found live against a real cluster, with no contract-level
	// reproduction until now. postgres's official image is the fixture:
	// its docker-entrypoint.sh performs initdb before exec-ing the given
	// Cmd (["postgres", "-c", ...], the exact shape postgres.go's own
	// provider sends); if Cmd replaced ENTRYPOINT instead of appending to
	// it, the raw postgres binary would run against an uninitialized data
	// directory and exit almost immediately, which WaitHealthy surfaces as
	// a hard failure, not a timeout.
	t.Run("EntrypointFaithfulness_CmdAppendsNotReplaces", func(t *testing.T) {
		type commandRunner interface {
			RunsContainerCommands() bool
		}
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()
		if !execCapable {
			// The fake never runs a real image/ENTRYPOINT; there is nothing
			// to prove faithful translation of on an adapter that doesn't
			// execute anything.
			return
		}

		name := namePrefix + "-entrypoint-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "postgres:16-alpine",
			Cmd:      []string{"postgres", "-c", "wal_level=logical"},
			Env:      map[string]string{"POSTGRES_PASSWORD": "conformance"},
			Networks: []string{netSpec.Name},
			Labels:   labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
			t.Fatalf("container did not become healthy — an image with a real ENTRYPOINT (postgres's docker-entrypoint.sh runs initdb) did not complete initialization before Cmd ran, exactly the symptom of Cmd replacing ENTRYPOINT instead of appending to it (K1): %v", err)
		}
	})

	// ReplicaSet_ScaleUp_Idempotent proves the core C1 primitive
	// (docs/design/004-replicas-and-identity.md): Replicas > 1 fans out to N
	// individually-managed units reported through one aggregate
	// ContainerState (ReadyReplicas), a second identical EnsureContainer call
	// is a no-op (NFR-2), and scaling 2 -> 3 is detected as a real change and
	// converges ReadyReplicas to the new count — across all three adapters,
	// since none of this depends on StableIdentity (D10's simpler,
	// interchangeable-workers shape).
	t.Run("ReplicaSet_ScaleUp_Idempotent", func(t *testing.T) {
		name := namePrefix + "-replicaset-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{netSpec.Name},
			Labels:   labels,
			Replicas: 2,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("first EnsureContainer (Replicas: 2): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy after scale to 2: %v", err)
		}
		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect after scale to 2: found=%v err=%v", found, err)
		}
		if st.ReadyReplicas != 2 {
			t.Errorf("ReadyReplicas after scale to 2 = %d, want 2", st.ReadyReplicas)
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
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy after scale to 3: %v", err)
		}
		st, found, err = rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect after scale to 3: found=%v err=%v", found, err)
		}
		if st.ReadyReplicas != 3 {
			t.Errorf("ReadyReplicas after scale to 3 = %d, want 3", st.ReadyReplicas)
		}
	})

	// ReplicaSet_OrdinalHostnameResolution proves every ordinal of a
	// StableIdentity set is individually, distinctly addressable by its own
	// stable name ("<Name>-<i>", runtime.OrdinalName) — the port-level
	// meaning of "ordinal hostname resolution": Inspect against an ordinal
	// name resolves to that one replica's own state, not the aggregate, and
	// two different ordinals are never the same underlying unit.
	t.Run("ReplicaSet_OrdinalHostnameResolution", func(t *testing.T) {
		name := namePrefix + "-ordinal-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:           name,
			Image:          "alpine:3.20",
			Cmd:            []string{"sleep", "300"},
			Networks:       []string{netSpec.Name},
			Labels:         labels,
			Replicas:       2,
			StableIdentity: true,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer (StableIdentity, Replicas: 2): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 30*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}

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
		if aggregate.ReadyReplicas != 2 {
			t.Errorf("aggregate ReadyReplicas = %d, want 2", aggregate.ReadyReplicas)
		}
	})

	// ReplicaSet_PerOrdinalVolumePersistence proves StableIdentity's other
	// half: each ordinal owns its own volume set, isolated from its
	// siblings' and surviving a container recreation — the StatefulSet/
	// per-ordinal-Docker-volume path C2 (Redpanda) and C4 (MinIO) will build
	// on. commandRunner-gated exactly like Volume_persists_across_container_
	// update: only an adapter whose containers run a real process can prove
	// the write-recreate-readback round trip; the fake proves the
	// structural half (the runtime itself creates one volume per ordinal).
	t.Run("ReplicaSet_PerOrdinalVolumePersistence", func(t *testing.T) {
		name := namePrefix + "-stateful-ctr"
		volBase := namePrefix + "-stateful-vol"
		t.Cleanup(func() {
			_ = rt.Remove(ctx, name)
			_ = rt.RemoveVolume(ctx, runtime.OrdinalName(volBase, 0))
			_ = rt.RemoveVolume(ctx, runtime.OrdinalName(volBase, 1))
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
			Networks:       []string{netSpec.Name},
			Volumes:        []runtime.VolumeMount{{VolumeName: volBase, MountPath: "/data"}},
			Env:            map[string]string{"GENERATION": "1"},
			Labels:         labels,
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

	t.Run("RemoveNetwork_refuses_while_container_attached", func(t *testing.T) {
		// ctrSpec is still attached to netSpec here — Remove_then_absent
		// (below) is what finally tears it down. Removing a network out from
		// under a container still attached to it must fail and change nothing.
		// The shared-network destroy pattern depends on this: every provider
		// best-effort-calls RemoveNetwork on Destroy, so the network must
		// outlive every member but the last, and RemoveNetwork must never
		// cascade-delete a member. Docker enforces it via "network has active
		// endpoints"; the Kubernetes adapter must not let a Namespace deletion
		// cascade to a still-running Deployment (regression: a shared-namespace
		// destroy that wiped its siblings and any unmanaged workload alongside
		// them — errors.md, 2026-07-20).
		if err := rt.RemoveNetwork(ctx, netSpec.Name); err == nil {
			t.Fatal("RemoveNetwork removed a network that still has a container attached")
		}
		if _, found, err := rt.Inspect(ctx, ctrSpec.Name); err != nil {
			t.Fatalf("Inspect after refused RemoveNetwork: %v", err)
		} else if !found {
			t.Fatal("container was deleted as a side effect of RemoveNetwork")
		}
		nets, err := rt.ListManagedNetworks(ctx)
		if err != nil {
			t.Fatalf("ListManagedNetworks: %v", err)
		}
		var stillThere bool
		for _, n := range nets {
			if n.Name == netSpec.Name {
				stillThere = true
			}
		}
		if !stillThere {
			t.Errorf("network %q missing after a refused RemoveNetwork", netSpec.Name)
		}
	})

	t.Run("Remove_then_absent", func(t *testing.T) {
		if err := rt.Remove(ctx, ctrSpec.Name); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		_, found, err := rt.Inspect(ctx, ctrSpec.Name)
		if err != nil {
			t.Fatalf("Inspect after Remove: %v", err)
		}
		if found {
			t.Fatalf("container still present after Remove")
		}
		if err := rt.RemoveNetwork(ctx, netSpec.Name); err != nil {
			t.Fatalf("RemoveNetwork: %v", err)
		}
		if err := rt.RemoveVolume(ctx, volSpec.Name); err != nil {
			t.Fatalf("RemoveVolume: %v", err)
		}
	})
}
