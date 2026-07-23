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
	"github.com/rezarajan/platformctl/internal/testkit"
)

// TestGCPlanAndApply covers docs/planning/08 A2: a container/network/volume
// carrying platformctl's ownership labels, created out-of-band (simulating
// objects left behind by a crash before state was ever written — the
// scenario doc 07 §1.3 named), must be listed by `gc plan`, refused by `gc
// apply` without the destructive flag, and removed by `gc apply` with it —
// and only those objects, nothing else.
func TestGCPlanAndApply(t *testing.T) {
	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	const (
		containerName = "datascape-gc-test-ctr"
		networkName   = "datascape-gc-test-net"
		volumeName    = "datascape-gc-test-vol"
	)
	labels := []string{
		"--label", "io.datascape.managed-by=platformctl",
		"--label", "io.datascape.namespace=default",
		"--label", "io.datascape.kind=Provider",
	}
	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:          rt,
		Workloads:   []string{containerName},
		RawVolumes:  []string{volumeName},
		RawNetworks: []string{networkName},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	// Out-of-band creation: platformctl never ran, so no state entry will
	// ever account for these — exactly the "left behind by a crash" case.
	netArgs := append([]string{"network", "create"}, labels...)
	netArgs = append(netArgs, "--label", "io.datascape.name=orphan-net", networkName)
	if out, err := exec.Command("docker", netArgs...).CombinedOutput(); err != nil {
		t.Fatalf("create orphan network: %v\n%s", err, out)
	}
	volArgs := append([]string{"volume", "create"}, labels...)
	volArgs = append(volArgs, "--label", "io.datascape.name=orphan-vol", volumeName)
	if out, err := exec.Command("docker", volArgs...).CombinedOutput(); err != nil {
		t.Fatalf("create orphan volume: %v\n%s", err, out)
	}
	ctrArgs := append([]string{"run", "-d", "--name", containerName, "--network", networkName}, labels...)
	ctrArgs = append(ctrArgs, "--label", "io.datascape.name=orphan-ctr", "alpine:3.20", "sleep", "300")
	if out, err := exec.Command("docker", ctrArgs...).CombinedOutput(); err != nil {
		t.Fatalf("create orphan container: %v\n%s", err, out)
	}

	stateFile := filepath.Join(t.TempDir(), "state.json") // empty state: nothing is accounted for

	out, _, code := run(t, "gc", "plan", "--state-file", stateFile, "-o", "json")
	if code != 0 {
		t.Fatalf("gc plan failed (code %d): %s", code, out)
	}
	var plan gcPlanOutput
	if err := json.Unmarshal([]byte(out), &plan); err != nil {
		t.Fatalf("decode gc plan output: %v\n%s", err, out)
	}
	objects := make(map[string]bool, len(plan.Orphans))
	for _, o := range plan.Orphans {
		objects[o.Object] = true
	}
	for _, want := range []string{
		"container:" + containerName,
		"network:" + networkName,
		"volume:" + volumeName,
	} {
		if !objects[want] {
			t.Errorf("gc plan output missing %s:\n%s", want, out)
		}
	}

	// Refuses without the destructive flag; nothing removed.
	_, err, _ = run(t, "gc", "apply", "--state-file", stateFile)
	if err == nil {
		t.Fatal("gc apply accepted without --yes-i-understand-this-is-destructive")
	}
	if !strings.Contains(err.Error(), "yes-i-understand-this-is-destructive") {
		t.Errorf("gc apply refusal does not name the remedy: %v", err)
	}
	if _, found, _ := rt.Inspect(ctx, containerName); !found {
		t.Fatal("gc apply without flags removed the container anyway")
	}

	// With the flag: removes exactly the planned objects.
	out, err, code = run(t, "gc", "apply", "--state-file", stateFile, "--yes-i-understand-this-is-destructive", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("gc apply failed (code %d): %v\n%s", code, err, out)
	}
	if _, found, _ := rt.Inspect(ctx, containerName); found {
		t.Error("container still present after gc apply")
	}
	if nets, lerr := rt.ListManagedNetworks(ctx); lerr != nil {
		t.Errorf("list managed networks after gc apply: %v", lerr)
	} else {
		for _, n := range nets {
			if n.Name == networkName {
				t.Error("network still present after gc apply")
			}
		}
	}
	if vols, lerr := rt.ListManagedVolumes(ctx); lerr != nil {
		t.Errorf("list managed volumes after gc apply: %v", lerr)
	} else {
		for _, v := range vols {
			if v.Name == volumeName {
				t.Error("volume still present after gc apply")
			}
		}
	}

	// A clean re-plan reports no orphans from this test.
	out, _, code = run(t, "gc", "plan", "--state-file", stateFile, "-o", "json")
	if code != 0 {
		t.Fatalf("gc plan (post-cleanup) failed (code %d): %s", code, out)
	}
	if strings.Contains(out, containerName) || strings.Contains(out, networkName) || strings.Contains(out, volumeName) {
		t.Errorf("gc plan still reports removed objects:\n%s", out)
	}
}
