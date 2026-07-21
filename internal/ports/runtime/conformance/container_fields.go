package conformance

import (
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// runContainerFields registers EnsureContainer_imagePullAuth_accepted,
// EnsureContainer_productionFields_idempotent, Logs_returns_without_error,
// EnsureContainer_aliases_idempotent and
// FileMount_readable_by_process_and_ReadFile.
func runContainerFields(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	ctx := fx.ctx

	// EnsureContainer_imagePullAuth_accepted proves ContainerSpec.ImagePullAuth
	// round-trips safely through every adapter (docs/planning/07 §1.1
	// deferral, docs/planning/08 A1) — PullNever so it never attempts a real
	// pull with these (deliberately fake) credentials against a real
	// registry; a real authenticated pull is proven separately by the
	// Docker-only integration test against a local htpasswd registry, which
	// controls credentials it knows are correct rather than risking a public
	// registry rejecting bogus ones.
	t.Run("EnsureContainer_imagePullAuth_accepted", func(t *testing.T) {
		name := fx.namePrefix + "-auth-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		authSpec := runtime.ContainerSpec{
			Name:       name,
			Image:      fx.ctrSpec.Image, // already ensured present by an earlier subtest
			PullPolicy: runtime.PullNever,
			Cmd:        []string{"sleep", "60"},
			Networks:   []string{fx.netSpec.Name},
			Labels:     fx.labels,
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
		name := fx.namePrefix + "-prod-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		prodSpec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{fx.netSpec.Name},
			Labels:   fx.labels,
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
		// Depends on fx.ctrSpec's container already existing — created by
		// runContainerLifecycle earlier in Run's sequencing (conformance.go).
		if _, err := rt.Logs(ctx, fx.ctrSpec.Name, 5); err != nil {
			t.Fatalf("Logs: %v", err)
		}
	})

	t.Run("EnsureContainer_aliases_idempotent", func(t *testing.T) {
		name := fx.namePrefix + "-alias-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "alpine:3.20",
			Cmd:      []string{"sleep", "300"},
			Networks: []string{fx.netSpec.Name},
			Aliases:  []string{fx.namePrefix + "-stable-alias"},
			Labels:   fx.labels,
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
		name := fx.namePrefix + "-files-ctr"
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
			Networks: []string{fx.netSpec.Name},
			Files:    []runtime.FileMount{{Path: path, Content: []byte(content)}},
			Labels:   fx.labels,
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
}
