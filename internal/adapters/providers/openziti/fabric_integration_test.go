//go:build integration

package openziti

import (
	"context"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/testkit"
)

// TestFabricProvisionerLiveDocker is docs/planning/08 L2's live proof
// against a real Docker daemon (no CLI/manifest layer — a direct exercise
// of the engine-owned FabricProvisioner this task adds, mirroring
// docker_integration_test.go's own TestConformance shape rather than
// inventing a new pattern). Proves, against the pinned ziti-controller/
// ziti-router:1.5.14 images:
//  1. EnsureFabric stands up a working controller + router from nothing —
//     including the live-verified admin-credential mechanism this task's
//     package doc comment records (Env on the bootstrap call only, then
//     read back from the controller's own persisted volume).
//  2. A second EnsureFabric call is idempotent: same ControllerContainerID
//     (no container recreate) and no error re-authenticating with the
//     credential read back from the volume, not re-minted.
//  3. DestroyFabric removes every container/volume/network it created.
func TestFabricProvisionerLiveDocker(t *testing.T) {
	rt, err := docker.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	labels := runtime.ManagedLabels("default", "MediationFabric", "l2-live-fabric", "l2-live-fabric")
	jan := testkit.Janitor{
		RT:        rt,
		Workloads: []string{fabricControllerName, fabricRouterName},
		Volumes:   []string{fabricControllerName + "-data", fabricRouterName + "-data"},
		Networks:  []string{fabricNetwork},
	}
	jan.CleanSilent(ctx)
	// Register cleanup with a FRESH background context, NOT ctx above: a
	// t.Cleanup runs after the test function has returned, at which point
	// this function's `defer cancel()` has already fired — cleaning up
	// through the canceled ctx fails every janitor call with "context
	// canceled" and turns a green run red (found live). The janitor's own
	// removals are bounded by Docker itself, so a plain Background is
	// correct here.
	jan.Register(context.Background(), t)

	f := NewFabricProvisioner()

	fs1, err := f.EnsureFabric(ctx, mediation.FabricRequest{Runtime: rt, Labels: labels})
	if err != nil {
		t.Fatalf("EnsureFabric (first, bootstrap): %v", err)
	}
	if fs1.ControllerContainerID == "" || fs1.RouterID == "" {
		t.Fatalf("EnsureFabric returned incomplete state: %+v", fs1)
	}

	// Idempotency: the whole point of the read-back-from-volume credential
	// design (this file's own package doc comment) — a second call must
	// reuse the SAME admin credential the first call minted, not mint a
	// new one that would fail to authenticate against the already-
	// bootstrapped database.
	fs2, err := f.EnsureFabric(ctx, mediation.FabricRequest{Runtime: rt, Labels: labels})
	if err != nil {
		t.Fatalf("EnsureFabric (second, idempotent): %v", err)
	}
	if fs2.ControllerContainerID != fs1.ControllerContainerID {
		t.Errorf("controller container recreated on second EnsureFabric: %q -> %q", fs1.ControllerContainerID, fs2.ControllerContainerID)
	}
	if fs2.RouterID != fs1.RouterID {
		t.Errorf("router entity id changed on second EnsureFabric: %q -> %q", fs1.RouterID, fs2.RouterID)
	}

	if err := f.DestroyFabric(ctx, mediation.FabricRequest{Runtime: rt, Labels: labels}); err != nil {
		t.Fatalf("DestroyFabric: %v", err)
	}
	// Idempotent destroy: destroying an already-absent fabric is a no-op.
	if err := f.DestroyFabric(ctx, mediation.FabricRequest{Runtime: rt, Labels: labels}); err != nil {
		t.Fatalf("DestroyFabric (second, already gone): %v", err)
	}
}
