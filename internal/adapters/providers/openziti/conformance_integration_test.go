//go:build integration

package openziti

import (
	"context"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/mediation/conformance"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/testkit"
)

// TestOpenZitiMediationConformanceLiveDocker runs the SAME contract suite
// the fake passes fast-tier (internal/adapters/mediation/fake) against the
// real OpenZiti adapter on a live controller — docs/planning/08 L2a's deep-
// tier leg. This is what makes "the mediation port is not overfit to
// OpenZiti" a tested claim rather than an assertion: the OpenZiti session
// and the in-memory fake are proven against one identical Run.
//
// The fabric (L2's FabricProvisioner) stands up the controller + router;
// the session is built directly against that fabric's controller using the
// same in-package dial/credential helpers DestroyFabric itself uses. No
// AddressResolver leg — OpenZiti does not implement DialAddress until L3.
func TestOpenZitiMediationConformanceLiveDocker(t *testing.T) {
	rt, err := docker.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	labels := runtime.ManagedLabels("default", "MediationFabric", "l2a-conf-fabric", "l2a-conf-fabric")
	jan := testkit.Janitor{
		RT:        rt,
		Workloads: []string{fabricControllerName, fabricRouterName},
		Volumes:   []string{fabricControllerName + "-data", fabricRouterName + "-data"},
		Networks:  []string{fabricNetwork},
	}
	jan.CleanSilent(ctx)
	jan.Register(context.Background(), t)

	f := NewFabricProvisioner()
	if _, err := f.EnsureFabric(ctx, mediation.FabricRequest{Runtime: rt, Labels: labels}); err != nil {
		t.Fatalf("EnsureFabric: %v", err)
	}

	// Build a session against the fabric's own controller, exactly the way
	// DestroyFabric reaches it: dial + the volume-persisted admin credential.
	client, closeCtrl, err := dialController(ctx, rt, fabricControllerName, fabricControllerPort)
	if err != nil {
		t.Fatalf("dial fabric controller: %v", err)
	}
	defer func() { _ = closeCtrl() }()
	password, _, err := resolveFabricAdminCredential(ctx, rt)
	if err != nil {
		t.Fatalf("resolve fabric admin credential: %v", err)
	}
	if err := client.Authenticate(ctx, fabricAdminUsername, password); err != nil {
		t.Fatalf("authenticate to fabric controller: %v", err)
	}
	sess := &session{client: client, closeTunnel: closeCtrl}

	conformance.Run(t, conformance.Subject{
		Provider: sess,
		Fabric:   f,
		Runtime:  rt,
	})
}
