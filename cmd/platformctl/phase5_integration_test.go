//go:build integration

package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// TestImportEndToEnd covers the Phase 5 exit criterion: importing a
// pre-existing, out-of-band-created Docker container as a Provider's backing
// Postgres instance results in Ready status without any create call.
func TestImportEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_IMP_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_IMP_PG_ADMIN_PASSWORD", "admin-secret-pw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	cleanup := func() {
		_ = exec.Command("docker", "rm", "-f", "datascape-imp-pg").Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	// The pre-existing instance: plain `docker run`, no Datascape labels.
	if out, err := exec.Command("docker", "run", "-d", "--name", "datascape-imp-pg",
		"-e", "POSTGRES_USER=datascape_admin", "-e", "POSTGRES_PASSWORD=admin-secret-pw",
		"-p", "15546:5432", "postgres:16").CombinedOutput(); err != nil {
		t.Fatalf("out-of-band docker run: %v\n%s", err, out)
	}

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/import-scenario"

	// Without the gate, import refuses.
	out, _, code := run(t, "import", "Provider/datascape-imp-pg", manifests, "--from", "datascape-imp-pg", "--state-file", stateFile)
	if code == 0 {
		t.Fatalf("import without ImportedResources gate should refuse:\n%s", out)
	}

	out, err, code = run(t, "import", "Provider/datascape-imp-pg", manifests, "--from", "datascape-imp-pg",
		"--state-file", stateFile, "--feature-gates", "ImportedResources=true")
	if err != nil || code != 0 {
		t.Fatalf("import failed (code %d): %v\n%s", code, err, out)
	}

	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "Imported") {
		t.Errorf("status does not show Imported lifecycle:\n%s", out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if strings.Contains(line, "datascape-imp-pg") && !strings.Contains(line, "True") {
			t.Errorf("imported resource not Ready: %s", line)
		}
	}

	before, found, err := rt.Inspect(ctx, "datascape-imp-pg")
	if err != nil || !found {
		t.Fatalf("imported container missing: %v", err)
	}

	// Apply after import: the imported Provider plans a no-op — creation is
	// never re-attempted. (The SecretReference still needs its first apply.)
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply after import failed (code %d): %v\n%s", code, err, out)
	}
	if strings.Contains(out, "Provider/datascape-imp-pg     create") {
		t.Errorf("apply planned a create for the imported Provider:\n%s", out)
	}
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("second apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("second apply after import is not a no-op:\n%s", out)
	}
	after, found, err := rt.Inspect(ctx, "datascape-imp-pg")
	if err != nil || !found {
		t.Fatalf("imported container missing after apply: %v", err)
	}
	if after.ID != before.ID {
		t.Errorf("imported container was recreated (ID %s -> %s); import must never create", before.ID, after.ID)
	}

	// Destroy without --include-imported skips it.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	if _, found, _ := rt.Inspect(ctx, "datascape-imp-pg"); !found {
		t.Error("destroy removed an Imported resource without --include-imported")
	}
}

// TestExternalSourceEndToEnd covers the Phase 5 exit criterion: a Binding
// against an external: true Source reconciles the connector against the
// out-of-band database, and destroy honors the NFR-3 double lock.
func TestExternalSourceEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_EXT_DB_CONN_USERNAME", "extuser")
	t.Setenv("DATASCAPE_SECRET_EXT_DB_CONN_PASSWORD", "extpw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	cleanup := func() {
		for _, c := range []string{"datascape-ext-dbz", "datascape-ext-rp"} {
			_ = rt.Remove(ctx, c)
		}
		_ = exec.Command("docker", "rm", "-f", "datascape-ext-outofband-pg").Run()
		_ = rt.RemoveVolume(ctx, "datascape-ext-rp-data")
		_ = exec.Command("docker", "network", "rm", "datascape-ext-net").Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	// The "production" database: out-of-band, on the shared network, never
	// managed by Datascape.
	if out, err := exec.Command("docker", "network", "create", "datascape-ext-net").CombinedOutput(); err != nil {
		t.Fatalf("create network: %v\n%s", err, out)
	}
	if out, err := exec.Command("docker", "run", "-d", "--name", "datascape-ext-outofband-pg",
		"--network", "datascape-ext-net",
		"-e", "POSTGRES_USER=extuser", "-e", "POSTGRES_PASSWORD=extpw", "-e", "POSTGRES_DB=attendance",
		"postgres:16", "postgres", "-c", "wal_level=logical").CombinedOutput(); err != nil {
		t.Fatalf("out-of-band docker run: %v\n%s", err, out)
	}
	// No healthcheck on the out-of-band container; give initdb a moment.
	time.Sleep(5 * time.Second)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/external-scenario"

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready: %s", line)
		}
	}
	if !strings.Contains(out, "External") {
		t.Errorf("status does not show External lifecycle:\n%s", out)
	}

	// Plain destroy: managed resources go, the external Source is skipped
	// and its database untouched.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "include-external") {
		t.Errorf("destroy did not explain how to include the external resource:\n%s", out)
	}
	if st, found, _ := rt.Inspect(ctx, "datascape-ext-outofband-pg"); !found || !st.Running {
		t.Fatal("destroy touched the external database container")
	}
	st, err := localfile.New(stateFile).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Resources[resource.Key{Kind: "Source", Name: "ext-students"}]; !ok {
		t.Error("external Source vanished from state without --include-external")
	}

	// --include-external alone refuses (NFR-3 double lock, CLI half).
	out, _, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--include-external")
	if code == 0 {
		t.Errorf("destroy --include-external without the destructive flag must refuse:\n%s", out)
	}
	if st, found, _ := rt.Inspect(ctx, "datascape-ext-outofband-pg"); !found || !st.Running {
		t.Fatal("refused destroy still touched the external database container")
	}

	// Both flags: the external Source is forgotten (removed from state);
	// the database itself is still never touched — nothing manages it.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve",
		"--include-external", "--yes-i-understand-this-is-destructive")
	if err != nil || code != 0 {
		t.Fatalf("destroy with both flags failed (code %d): %v\n%s", code, err, out)
	}
	st, err = localfile.New(stateFile).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Resources) != 0 {
		t.Errorf("state not empty after full destroy: %v", st.Resources)
	}
	if ctr, found, _ := rt.Inspect(ctx, "datascape-ext-outofband-pg"); !found || !ctr.Running {
		t.Error("external database container was touched by destroy")
	}
}
