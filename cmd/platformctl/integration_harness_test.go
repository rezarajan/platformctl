//go:build integration

package main

import (
	"context"
	"testing"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
)

// This file is G6 (docs/planning/08 §7.6): the setup/cleanup shape repeated
// across cmd/platformctl/*_integration_test.go's Docker-backed suites,
// extracted once. It intentionally does not try to cover every suite's
// bespoke needs (chaos's mid-apply kill, shared_state's raw MinIO container,
// the kubernetes_examples/kubernetes suites' cluster-config guard) — see
// docs/planning/06-agentic-execution-guide.md's integration-test-harness
// note for which shapes belong here versus staying local to a suite.

// requireDocker connects to the Docker daemon from the environment — the
// same `dockerruntime.New(nil)` + Fatalf-on-error two-liner every
// Docker-backed suite already opened with — and returns the runtime. It
// fails the test outright (never skips): Docker is this repo's baseline
// integration dependency, unlike the optional Kubernetes cluster
// `requireK8s` (kubernetes_integration_test.go) guards against.
func requireDocker(t *testing.T) *dockerruntime.Runtime {
	t.Helper()
	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	return rt
}

// registerDockerCleanup builds a best-effort cleanup func that removes the
// named containers, then volumes, then (if non-empty) the network — the
// order every suite's hand-written cleanup already used: containers before
// the network they attach to, volumes after the containers that mount them.
// It registers the func via t.Cleanup and also returns it, so a caller whose
// original test additionally ran cleanup once up front (belt-and-braces
// against leftovers from a prior failed run) can do so explicitly:
//
//	cleanup := registerDockerCleanup(t, rt, containers, volumes, network)
//	cleanup() // only if the suite being migrated did this before
//
// A suite that only registered a deferred cleanup (no pre-run call) should
// just call registerDockerCleanup and ignore the returned func, to keep the
// migration behavior-neutral.
func registerDockerCleanup(t *testing.T, rt *dockerruntime.Runtime, containers, volumes []string, network string) func() {
	t.Helper()
	ctx := context.Background()
	cleanup := func() {
		for _, c := range containers {
			_ = rt.Remove(ctx, c)
		}
		for _, v := range volumes {
			_ = rt.RemoveVolume(ctx, v)
		}
		if network != "" {
			_ = rt.RemoveNetwork(ctx, network)
		}
	}
	t.Cleanup(cleanup)
	return cleanup
}
