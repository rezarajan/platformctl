// This file is docs/planning/08 L2's ENGINE-OWNED platform mediation fabric
// — "ensure the mesh like we ensure networks" (docs/adr/034): one
// controller + router per deployment, standing up before any manifest ever
// declares a mediated edge, distinct from instance.go's per-manifest
// Provider(type: openziti) path. It reuses instance.go/client.go's own
// bootstrap/enroll/settle mechanics and the H10 pinned-CA client
// (dialController/waitControllerServing) rather than duplicating them —
// only the naming (fixed, engine-chosen, not derived from a user-declared
// Provider name) and the admin-credential handling differ.
//
// # Admin credential (docs/planning/08 L2's "engine-minted... file-mounted"
// requirement, live-verified before writing this)
//
// The manifest-declared path (instance.go) resolves ZITI_USER/ZITI_PWD from
// a user's own adminSecretRef — there is no such declaration here; the
// engine must mint the credential itself and never lose track of it across
// separate `platformctl apply` processes (a fresh Go process every call, no
// held session — docs/planning/08 F5). Extracting and reading the pinned
// ziti-controller:1.5.14 image's real entrypoint.bash/bootstrap.bash (the
// same live-verification discipline H10's own done-note records) found
// that the containerized entrypoint (`entrypoint.bash run config.yml` ->
// `bootstrap()`) calls straight into `makeDatabase`/`initializeDatabase`,
// which reads ZITI_USER/ZITI_PWD only from the process environment — unlike
// ziti-router's bootstrap.bash (instance.go's own H10 finding for
// ZITI_ENROLL_TOKEN), there is NO file-path-detection branch for these two
// variables anywhere in the controller's containerized code path (the
// env-file-sourcing logic in bootstrap.bash's loadEnvFiles/importZitiVars
// only runs when bootstrap.bash is invoked directly as a systemd
// ExecStartPre, a code path the Docker/Kubernetes container never takes).
// Confirmed live: a controller started with the credential ONLY in a
// FileMount (no matching Env) refused with "ERROR: unable to create
// default admin in database because ZITI_USER and ZITI_PWD must both be
// set". So the credential necessarily transits Env on the ONE bootstrap
// call that creates the admin account — exactly the same live-verified,
// documented deviation H10 already accepted for the router's enrollment
// JWT, applied here to a secret the image genuinely offers no file-based
// input for. What IS achievable, and what this file does: (1) Env carries
// the credential on the bootstrap EnsureContainer call ONLY, then a
// settle-recreate strips it (the same settle-then-strip discipline
// instance.go/connection.go already hold for their own one-time secrets);
// (2) the SAME credential is durably written into the controller's own
// persisted volume (/ziti-controller, unlike the enrollment-JWT FileMounts
// elsewhere in this package which are deliberately kept OUTSIDE any volume
// so they never survive a recreate) at fabricCredentialPath, so every LATER
// EnsureFabric call — a fresh process, nothing held in memory — reads the
// SAME credential back via ContainerRuntime.ReadFile (the same mechanism
// connection.go's waitTunnelEnrolled already proves works unchanged on
// both runtimes) instead of ever needing Env again, and instead of minting
// a new (and therefore wrong) credential.
package openziti

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Fixed platform-fabric identifiers (docs/planning/08 L2): unlike
// instance.go's controllerName/routerName (derived from a user-declared
// Provider's own name), the platform fabric has no manifest declaration to
// derive from — one fabric per deployment, named deterministically, the
// same singularity the shared platform network already has.
const (
	fabricControllerName = "datascape-mediation-ctrl"
	fabricRouterName     = "datascape-mediation-router"
	fabricNetwork        = "datascape"
	fabricControllerPort = 1280
	fabricRouterPort     = 3022
	fabricAdminUsername  = "admin"

	// fabricCredentialPath is where the engine-minted admin credential is
	// durably file-resident inside the controller's OWN persisted volume
	// (mounted at /ziti-controller by reconcileFabricController below) —
	// see this file's package doc comment for why this, and not Env, is
	// where every call after the first reads it from.
	fabricCredentialPath = "/ziti-controller/.admin-credential"
)

// FabricProvisioner implements mediation.FabricProvisioner (docs/planning/08
// L2). Stateless (docs/planning/08 F5) — every call re-derives what it
// needs from req and the runtime's own observed/persisted state, never a
// field on this type.
type FabricProvisioner struct{}

// NewFabricProvisioner constructs the engine-owned platform fabric
// facility. Referenced only from cmd/platformctl (this package's own
// wiring exception, internal/archtest/mediation_layering_test.go).
func NewFabricProvisioner() *FabricProvisioner { return &FabricProvisioner{} }

func (f *FabricProvisioner) EnsureFabric(ctx context.Context, req mediation.FabricRequest) (mediation.FabricState, error) {
	rt := req.Runtime
	labels := req.Labels

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: fabricNetwork, Labels: labels}); err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: ensure network: %w", err)
	}
	for _, n := range req.Networks {
		if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: n, Labels: labels}); err != nil {
			return mediation.FabricState{}, fmt.Errorf("mediation fabric: ensure target network %q: %w", n, err)
		}
	}

	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: fabricControllerName + "-data", Labels: labels, Networks: []string{fabricNetwork}}); err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: ensure controller volume: %w", err)
	}

	password, minted, err := resolveFabricAdminCredential(ctx, rt)
	if err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: resolve admin credential: %w", err)
	}

	ctrlSpec := runtime.ContainerSpec{
		Name:     fabricControllerName,
		Image:    defaultControllerImage,
		Networks: []string{fabricNetwork},
		Volumes:  []runtime.VolumeMount{{VolumeName: fabricControllerName + "-data", MountPath: "/ziti-controller"}},
		Env: map[string]string{
			"ZITI_BOOTSTRAP":               "true",
			"ZITI_BOOTSTRAP_CONFIG":        "true",
			"ZITI_BOOTSTRAP_DATABASE":      "true",
			"ZITI_BOOTSTRAP_PKI":           "true",
			"ZITI_CTRL_ADVERTISED_ADDRESS": fabricControllerName,
			"ZITI_CTRL_ADVERTISED_PORT":    fmt.Sprintf("%d", fabricControllerPort),
		},
		Ports:  []runtime.PortBinding{{ContainerPort: fabricControllerPort, Audience: runtime.AudienceHost}},
		Labels: labels,
	}
	if minted {
		// First bootstrap only — see this file's package doc comment for
		// why Env is unavoidable here and nowhere else in this file.
		ctrlSpec.Env["ZITI_USER"] = fabricAdminUsername
		ctrlSpec.Env["ZITI_PWD"] = password
		ctrlSpec.Files = []runtime.FileMount{{
			Path:    fabricCredentialPath,
			Content: []byte(fabricAdminUsername + ":" + password),
			Mode:    0o600,
		}}
	}
	ctrlState, err := rt.EnsureContainer(ctx, ctrlSpec)
	if err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: ensure controller: %w", err)
	}

	if err := waitControllerServing(ctx, rt, fabricControllerName, fabricControllerPort); err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: controller did not become ready: %w", err)
	}

	if minted {
		// Settle to the steady-state spec (no Env secret, no Files) within
		// this SAME call — instance.go's reconcileInstance doc comment
		// explains at length why this must happen before any later
		// probe/drift call recomputes the spec and observes a mismatch
		// against what a fresh EnsureContainer call would now compute.
		stableSpec := ctrlSpec
		stableEnv := make(map[string]string, len(ctrlSpec.Env)-2)
		for k, v := range ctrlSpec.Env {
			if k != "ZITI_USER" && k != "ZITI_PWD" {
				stableEnv[k] = v
			}
		}
		stableSpec.Env = stableEnv
		stableSpec.Files = nil
		if ctrlState, err = rt.EnsureContainer(ctx, stableSpec); err != nil {
			return mediation.FabricState{}, fmt.Errorf("mediation fabric: settle controller: %w", err)
		}
		// The settle recreate above restarts the controller container
		// (Docker: stop+remove+recreate to change Env/Files) — dialing
		// immediately afterward raced a not-yet-listening controller and
		// failed live with a connection EOF (docs/planning/08 L2's Done-
		// note). Re-wait the same bounded, re-resolved-tunnel-per-attempt
		// check waitControllerServing already performs before the first
		// dial, exactly the discipline reconcileInstance's own settle
		// comment documents for the router/tunneler equivalents.
		if err := waitControllerServing(ctx, rt, fabricControllerName, fabricControllerPort); err != nil {
			return mediation.FabricState{}, fmt.Errorf("mediation fabric: controller did not settle after credential strip: %w", err)
		}
	}

	client, closeCtrl, err := dialController(ctx, rt, fabricControllerName, fabricControllerPort)
	if err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: dial controller: %w", err)
	}
	defer func() { _ = closeCtrl() }()
	if err := client.Authenticate(ctx, fabricAdminUsername, password); err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: controller authentication: %w", err)
	}

	routerID, enrollJWT, verified, err := client.upsertEdgeRouter(ctx, fabricRouterName)
	if err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: ensure edge-router: %w", err)
	}

	routerNetworks := append([]string{fabricNetwork}, req.Networks...)
	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: fabricRouterName + "-data", Labels: labels, Networks: routerNetworks}); err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: ensure router volume: %w", err)
	}
	routerSpec := runtime.ContainerSpec{
		Name:     fabricRouterName,
		Image:    defaultRouterImage,
		Networks: routerNetworks,
		Volumes:  []runtime.VolumeMount{{VolumeName: fabricRouterName + "-data", MountPath: "/ziti-router"}},
		Env: map[string]string{
			"ZITI_BOOTSTRAP":                 "true",
			"ZITI_CTRL_ADVERTISED_ADDRESS":   fabricControllerName,
			"ZITI_CTRL_ADVERTISED_PORT":      fmt.Sprintf("%d", fabricControllerPort),
			"ZITI_ROUTER_ADVERTISED_ADDRESS": fabricRouterName,
			"ZITI_ROUTER_PORT":               fmt.Sprintf("%d", fabricRouterPort),
			"ZITI_ROUTER_MODE":               "host",
		},
		Ports:  []runtime.PortBinding{{ContainerPort: fabricRouterPort, Audience: runtime.AudienceInternal}},
		Labels: labels,
	}
	if !verified {
		if enrollJWT == "" {
			return mediation.FabricState{}, fmt.Errorf("mediation fabric: edge-router %q has no enrollment JWT and is not yet verified", fabricRouterName)
		}
		// docs/planning/08 H10 discipline, identical to instance.go's own
		// router enrollment handling (see that file's routerSpec doc
		// comment for the full live-verified rationale, including the
		// 0o644 mode).
		routerSpec.Env["ZITI_ENROLL_TOKEN"] = routerEnrollTokenPath
		routerSpec.Files = []runtime.FileMount{{Path: routerEnrollTokenPath, Content: []byte(enrollJWT), Mode: 0o644}}
	}
	if _, err := rt.EnsureContainer(ctx, routerSpec); err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: ensure router: %w", err)
	}
	if !verified {
		if err := waitEdgeRouterVerified(ctx, client, fabricRouterName); err != nil {
			return mediation.FabricState{}, fmt.Errorf("mediation fabric: edge-router did not verify: %w", err)
		}
		stableSpec := routerSpec
		stableEnv := make(map[string]string, len(routerSpec.Env)-1)
		for k, v := range routerSpec.Env {
			if k != "ZITI_ENROLL_TOKEN" {
				stableEnv[k] = v
			}
		}
		stableSpec.Env = stableEnv
		stableSpec.Files = nil
		if _, err := rt.EnsureContainer(ctx, stableSpec); err != nil {
			return mediation.FabricState{}, fmt.Errorf("mediation fabric: settle router: %w", err)
		}
	}

	if err := client.upsertCatchAllRouterPolicies(ctx, routerID); err != nil {
		return mediation.FabricState{}, fmt.Errorf("mediation fabric: %w", err)
	}

	return mediation.FabricState{
		ControllerContainerID: ctrlState.ID,
		ControllerInternal:    fmt.Sprintf("%s:%d", fabricControllerName, fabricControllerPort),
		RouterID:              routerID,
	}, nil
}

func (f *FabricProvisioner) DestroyFabric(ctx context.Context, req mediation.FabricRequest) error {
	rt := req.Runtime

	// Best-effort REST-side edge-router cleanup, mirroring
	// instance.go's destroyInstance: a controller already gone (partially
	// applied, prior failed destroy) means nothing to clean up REST-side —
	// idempotent no-op, matching Remove's own "already gone is success"
	// contract, never a hard failure that would block removing the
	// containers/volumes below.
	if client, closeCtrl, err := dialController(ctx, rt, fabricControllerName, fabricControllerPort); err == nil {
		if password, _, cerr := resolveFabricAdminCredential(ctx, rt); cerr == nil {
			if aerr := client.Authenticate(ctx, fabricAdminUsername, password); aerr == nil {
				if routerID, _, _, rerr := client.upsertEdgeRouter(ctx, fabricRouterName); rerr == nil {
					_ = client.deleteEdgeRouter(ctx, routerID)
				}
			}
		}
		_ = closeCtrl()
	}

	if err := rt.Remove(ctx, fabricRouterName); err != nil {
		return err
	}
	if err := rt.Remove(ctx, fabricControllerName); err != nil {
		return err
	}
	if err := rt.RemoveVolume(ctx, fabricRouterName+"-data"); err != nil {
		return err
	}
	if err := rt.RemoveVolume(ctx, fabricControllerName+"-data"); err != nil {
		return err
	}
	return nil
}

// resolveFabricAdminCredential reads the engine-minted admin credential
// back from the controller's own persisted volume (fabricCredentialPath),
// or mints a fresh one via crypto/rand when none exists yet — minted is
// true only in the latter case, telling EnsureFabric's caller this is a
// first-time bootstrap (so Env must carry it on this one call). A read
// failure (container absent — first-ever call; or, defensively, the file
// genuinely absent) is treated as "mint fresh": the normal first-bootstrap
// path. A read SUCCESS with malformed content is likewise treated as
// "mint fresh" rather than propagating a parse error — this file is
// engine-authored and never hand-edited, so malformed content only
// happens if bootstrap never actually completed, in which case minting
// again (and re-running the harmless, idempotent-on-existing-DB bootstrap
// path) is the correct recovery.
func resolveFabricAdminCredential(ctx context.Context, rt runtime.ContainerRuntime) (password string, minted bool, err error) {
	if b, rerr := rt.ReadFile(ctx, fabricControllerName, fabricCredentialPath); rerr == nil {
		if _, pass, ok := parseFabricCredential(b); ok {
			return pass, false, nil
		}
	}
	pass, err := randomFabricPassword()
	if err != nil {
		return "", false, err
	}
	return pass, true, nil
}

func parseFabricCredential(b []byte) (username, password string, ok bool) {
	s := strings.TrimSpace(string(b))
	u, p, found := strings.Cut(s, ":")
	if !found || u == "" || p == "" {
		return "", "", false
	}
	return u, p, true
}

// randomFabricPassword mints a fresh, cryptographically random admin
// credential (docs/planning/08 L2: "engine-minted, never user-declared") —
// 24 bytes of crypto/rand, hex-encoded (48 chars), well within the pinned
// ziti-controller image's own password acceptance.
func randomFabricPassword() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("mint admin credential: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
