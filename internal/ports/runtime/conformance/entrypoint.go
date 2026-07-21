package conformance

import (
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// runEntrypointFaithfulness registers EntrypointFaithfulness_
// CmdAppendsNotReplaces and EntrypointFaithfulness_EntrypointReplaces.
func runEntrypointFaithfulness(t *testing.T, rt runtime.ContainerRuntime, fx fixtures) {
	ctx := fx.ctx

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

		name := fx.namePrefix + "-entrypoint-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:     name,
			Image:    "postgres:16-alpine",
			Cmd:      []string{"postgres", "-c", "wal_level=logical"},
			Env:      map[string]string{"POSTGRES_PASSWORD": "conformance"},
			Networks: []string{fx.netSpec.Name},
			Labels:   fx.labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
			t.Fatalf("container did not become healthy — an image with a real ENTRYPOINT (postgres's docker-entrypoint.sh runs initdb) did not complete initialization before Cmd ran, exactly the symptom of Cmd replacing ENTRYPOINT instead of appending to it (K1): %v", err)
		}
	})

	// EntrypointFaithfulness_EntrypointReplaces backfills docs/planning/08
	// C6 review finding 1 (docs/adr/007-backup-restore.md): dbjob's job
	// containers set ContainerSpec.Entrypoint to force a shell regardless of
	// the image's own ENTRYPOINT — found live when minio/mc's image
	// ENTRYPOINT (["mc"]) swallowed a bare Cmd: ["sh", "-c", script] as
	// "mc sh -c ...", an instant exit the FIFO's other side then blocked on
	// forever. curlimages/curl is the fixture: its ENTRYPOINT is ["curl"]
	// with no argument-forwarding fallback (unlike postgres/mysql's official
	// images, whose entrypoint scripts happen to exec an unrecognized
	// command as-is) — if this adapter merely appended Entrypoint after the
	// image's own ENTRYPOINT instead of replacing it (the K1 mistake,
	// inverted), the real process would be "curl sh -c ...", which treats
	// "sh" as a URL, fails immediately, and the container never reaches
	// Running. The container staying Running also proves Cmd still appends
	// after the replaced Entrypoint: "sh -c" alone (Cmd dropped) is invalid
	// usage and sh would exit immediately too.
	t.Run("EntrypointFaithfulness_EntrypointReplaces", func(t *testing.T) {
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

		name := fx.namePrefix + "-entrypoint-replace-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:       name,
			Image:      "curlimages/curl:8.10.1",
			Entrypoint: []string{"sh", "-c"},
			Cmd:        []string{"sleep 60"},
			Networks:   []string{fx.netSpec.Name},
			Labels:     fx.labels,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer: %v", err)
		}
		// Give the process a moment to exit if Entrypoint didn't take.
		time.Sleep(2 * time.Second)
		st, found, err := rt.Inspect(ctx, name)
		if err != nil {
			t.Fatalf("Inspect: %v", err)
		}
		if !found || !st.Running {
			t.Fatal("container is not running — Entrypoint did not replace the image's own ENTRYPOINT (it looks like it was appended after \"curl\" instead, which then failed treating \"sh\" as a URL): the K1 mistake, inverted")
		}
	})

	// ReplicaSet_EntrypointReplaces_OnSet backfills a C2 live catch
	// (docs/adr/017; docs/adr/015 F6 ratchet): the Kubernetes StatefulSet
	// builder mapped Cmd->Args but silently dropped Entrypoint — no
	// StableIdentity consumer had set it until redpanda's multi-broker
	// start script, whose pods then crash-looped with rpk's usage help
	// (the whole script arrived as one Args token through the image's own
	// entrypoint). Same fixture as EntrypointFaithfulness_EntrypointReplaces,
	// but on the ordinal-set shape, so every adapter proves Entrypoint
	// replacement holds there too.
	t.Run("ReplicaSet_EntrypointReplaces_OnSet", func(t *testing.T) {
		type commandRunner interface {
			RunsContainerCommands() bool
		}
		cr, execCapable := rt.(commandRunner)
		execCapable = execCapable && cr.RunsContainerCommands()
		if !execCapable {
			return
		}

		name := fx.namePrefix + "-set-entrypoint-ctr"
		t.Cleanup(func() { _ = rt.Remove(ctx, name) })
		spec := runtime.ContainerSpec{
			Name:           name,
			Image:          "curlimages/curl:8.10.1",
			Entrypoint:     []string{"sh", "-c"},
			Cmd:            []string{"sleep 60"},
			Networks:       []string{fx.netSpec.Name},
			Labels:         fx.labels,
			Replicas:       2,
			StableIdentity: true,
		}
		if _, err := rt.EnsureContainer(ctx, spec); err != nil {
			t.Fatalf("EnsureContainer (StableIdentity set with Entrypoint): %v", err)
		}
		if err := rt.WaitHealthy(ctx, name, 60*time.Second); err != nil {
			t.Fatalf("WaitHealthy: %v", err)
		}
		waitReadyReplicas(ctx, t, rt, name, 2, 60*time.Second)
		st, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("Inspect: found=%v err=%v", found, err)
		}
		if !st.Running {
			t.Fatal("set is not running — Entrypoint was not translated on the replica-set/StatefulSet path (the C2 live catch: a dropped Command hands the start script to the image's own entrypoint)")
		}
	})
}
