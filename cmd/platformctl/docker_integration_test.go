//go:build integration

package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// The "container" placeholder provider is registered ungated (docs/planning/08
// E7 retired the ContainerProvider gate — see cmd/platformctl/main.go), so
// only DockerRuntime needs enabling here.
const phase1Gates = "DockerRuntime=true"

// TestDockerProviderEndToEnd covers the Phase 1 exit criteria: a manifest
// with a Docker-typed Provider creates a real network, volume, and container,
// waits for health, reports Ready — and destroy removes exactly what was
// created, verified by diffing the daemon's managed-object list before/after.
func TestDockerProviderEndToEnd(t *testing.T) {
	rt := requireDocker(t)
	ctx := context.Background()

	managedNames := func() map[string]bool {
		states, err := rt.ListManaged(ctx)
		if err != nil {
			t.Fatalf("ListManaged: %v", err)
		}
		out := make(map[string]bool, len(states))
		for _, s := range states {
			out[s.Name] = true
		}
		return out
	}

	before := managedNames()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/docker-scenario"

	// Belt-and-braces cleanup if the test fails mid-way.
	registerDockerCleanup(t, rt, []string{"datascape-phase1-probe"}, []string{"datascape-phase1-probe-data"}, "datascape-phase1")

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile,
		"--auto-approve", "--feature-gates", phase1Gates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	afterApply := managedNames()
	if !afterApply["datascape-phase1-probe"] {
		t.Fatalf("managed container not present after apply; managed set: %v", afterApply)
	}

	out, err, code = run(t, "status", manifests, "--state-file", stateFile,
		"--feature-gates", phase1Gates)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "True") {
		t.Fatalf("status does not report Ready=True:\n%s", out)
	}

	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile,
		"--auto-approve", "--feature-gates", phase1Gates)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}

	afterDestroy := managedNames()
	for name := range afterDestroy {
		if !before[name] && name != "" {
			t.Errorf("object %q still present after destroy but absent before apply", name)
		}
	}
	for name := range before {
		if !afterDestroy[name] {
			t.Errorf("pre-existing managed object %q was removed by destroy", name)
		}
	}
}
