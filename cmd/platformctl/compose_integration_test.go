//go:build integration

package main

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/application/compose"
)

// TestComposeOwnerScenario is the E9 Accept criterion (docs/planning/08
// §7, docs/adr/024-interactive-composition.md): init -> add pipeline
// (flags mode, reusing the blueprint's broker+Dataset with a prefix
// override) -> the second run's candidate computation lists the existing
// broker+Dataset (asserted directly against the engine, not just the CLI)
// -> expose Source/<first> --scheme tcp -> the resulting set validates
// green, applies to Ready on Docker, idempotent re-add proposes zero
// changes, destroy leaves nothing behind.
//
// H1 (lint) has not merged into this tree as of this task — confirmed by
// the absence of internal/application/lint. The Accept criterion's
// "assert lint-clean if H1 has merged" is therefore deferred-pending-H1,
// recorded here and in the doc 08 status note rather than silently
// skipped.
func TestComposeOwnerScenario(t *testing.T) {
	dir := t.TempDir()
	manifests := filepath.Join(dir, "cdc-to-lake")

	out, err, code := run(t, "init", "cdc-to-lake", "--dir", manifests)
	if err != nil || code != 0 {
		t.Fatalf("init cdc-to-lake failed (code %d): %v\n%s", code, err, out)
	}

	// The blueprint's own placeholder credentials.
	t.Setenv("DATASCAPE_SECRET_DB_ADMIN_CREDS_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_DB_ADMIN_CREDS_PASSWORD", "admin-pw")
	t.Setenv("DATASCAPE_SECRET_DB_REPLICATION_CREDS_USERNAME", "repl")
	t.Setenv("DATASCAPE_SECRET_DB_REPLICATION_CREDS_PASSWORD", "repl-pw")
	t.Setenv("DATASCAPE_SECRET_LAKE_ROOT_CREDS_USERNAME", "minioadmin")
	t.Setenv("DATASCAPE_SECRET_LAKE_ROOT_CREDS_PASSWORD", "minioadmin-pw")

	build := exec.Command("docker", "build", "-t", "datascape-s3sink-connect:local", filepath.Join(manifests, "s3sink-image"))
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sink connect image: %v\n%s", out, err)
	}

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	cleanup := func() {
		managed, _ := rt.ListManaged(ctx)
		for _, m := range managed {
			_ = rt.Remove(ctx, m.Name)
		}
		vols, _ := rt.ListManagedVolumes(ctx)
		for _, v := range vols {
			_ = rt.RemoveVolume(ctx, v.Name)
		}
		_ = rt.RemoveNetwork(ctx, "datascape")
		_ = exec.Command("docker", "network", "rm", "datascape").Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")

	// validate: green with zero manifest edits, before any composition.
	out, err, code = run(t, "validate", manifests)
	if err != nil || code != 0 {
		t.Fatalf("validate (pre-compose) failed (code %d): %v\n%s", code, err, out)
	}

	// --- engine-level assertion: candidate computation for the *second*
	// add pipeline lists the first broker+Dataset (the Accept criterion's
	// own wording) — asserted directly against internal/application/compose,
	// not inferred from CLI text output. ---
	snap, loadErr := compose.LoadTolerant(manifests, nil)
	if loadErr != nil {
		t.Fatalf("compose.LoadTolerant: %v", loadErr)
	}
	if snap.Warning != "" {
		t.Fatalf("compose.LoadTolerant degraded against a validate-green set: %s", snap.Warning)
	}
	brokers := snap.BrokerCandidates()
	if len(brokers) != 1 || brokers[0].Name != "broker" {
		t.Fatalf("BrokerCandidates() = %+v, want exactly the blueprint's \"broker\"", brokers)
	}
	datasets := snap.DatasetCandidates()
	if len(datasets) != 1 || datasets[0].Name != "raw-lake" {
		t.Fatalf("DatasetCandidates() = %+v, want exactly the blueprint's \"raw-lake\"", datasets)
	}

	// --- add pipeline (flags mode): new postgres source, reusing the
	// broker+Dataset at a new prefix. ---
	addArgs := []string{"add", "pipeline", manifests,
		"--name", "second", "--engine", "postgres",
		"--broker", "existing:broker", "--sink", "existing:raw-lake", "--sink-prefix", "other/",
	}
	out, err, code = run(t, addArgs...)
	if err != nil || code != 0 {
		t.Fatalf("add pipeline failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, `reusing broker Provider "broker"`) || !strings.Contains(out, `sink worker Provider "sink"`) {
		t.Errorf("add pipeline did not report reusing the existing broker/sink worker:\n%s", out)
	}
	t.Setenv("DATASCAPE_SECRET_SECOND_ADMIN_CREDS_USERNAME", "admin2")
	t.Setenv("DATASCAPE_SECRET_SECOND_ADMIN_CREDS_PASSWORD", "admin2-pw")
	t.Setenv("DATASCAPE_SECRET_SECOND_REPLICATION_CREDS_USERNAME", "repl2")
	t.Setenv("DATASCAPE_SECRET_SECOND_REPLICATION_CREDS_PASSWORD", "repl2-pw")

	// --- expose Source/app-db --scheme tcp. ---
	out, err, code = run(t, "expose", "Source/app-db", "--dir", manifests, "--scheme", "tcp", "--port", "28543")
	if err != nil || code != 0 {
		t.Fatalf("expose Source/app-db failed (code %d): %v\n%s", code, err, out)
	}

	// --- resulting set validates green with zero further edits. ---
	out, err, code = run(t, "validate", manifests)
	if err != nil || code != 0 {
		t.Fatalf("validate (post-compose) failed (code %d): %v\n%s", code, err, out)
	}

	// --- applies to Ready on Docker. ---
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
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
	out, err, code = run(t, "plan", manifests, "--state-file", stateFile, "--detect-drift-only")
	if err != nil || code != 0 {
		t.Fatalf("plan (post-apply) reported drift or failed (code %d): %v\n%s", code, err, out)
	}

	// --- idempotent re-add: `add pipeline` with identical answers proposes
	// zero changes (both the engine's Patch.HasChanges() via -o json, and
	// the human-readable "no changes" line). ---
	jsonOut, _, jerr := runSplit(t, append(append([]string{}, addArgs...), "--dry-run", "-o", "json")...)
	if jerr != nil {
		t.Fatalf("add pipeline --dry-run -o json (idempotent check): %v\n%s", jerr, jsonOut)
	}
	var got struct {
		Changed bool `json:"changed"`
	}
	if jsonErr := json.Unmarshal([]byte(jsonOut), &got); jsonErr != nil {
		t.Fatalf("add pipeline --dry-run -o json did not parse: %v\n%s", jsonErr, jsonOut)
	}
	if got.Changed {
		t.Errorf("re-running add pipeline with identical answers reported changed=true:\n%s", jsonOut)
	}
	out, err, code = run(t, addArgs...)
	if err != nil || code != 0 {
		t.Fatalf("add pipeline (idempotent rerun) failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("idempotent add pipeline rerun did not report 'no changes':\n%s", out)
	}

	// --- destroy tears down every managed resource, no labeled leftovers. ---
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	managed, err := rt.ListManaged(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(managed) != 0 {
		t.Errorf("labeled leftover container(s) after destroy: %+v", managed)
	}
}
