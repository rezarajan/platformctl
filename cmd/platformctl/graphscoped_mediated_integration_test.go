//go:build integration

package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// TestGraphScopedMediatedConnectionEndToEnd is docs/planning/08 M5's live
// accept bar — the EXACT failing case docs/planning/11's 2026-07-23
// capstone diagnosed: GraphScopedAccess + a MediatedConnection on the
// same edge. testdata/graphscoped-mediated-scenario is
// testdata/openziti-scenario's topology (a CDC Binding reaching a
// cross-network Postgres source ONLY through an openziti-realized
// Connection) with every spec.runtime.network pin removed, so
// GraphScopedAccess's Docker realization (each owner's home network
// EXCLUSIVE to itself) actually applies — with a pinned network, the
// consumer and the tunneler would already share the flat pinned network
// regardless of the gate, which would prove nothing.
//
// Before docs/planning/08 M5 (graphaccess.MediatedConsumerEdges), this
// exact scenario reproduced the capstone finding live: gsm-dbz (Debezium)
// reaches the mediated Connection only transitively
// (Binding.sourceRef -> Source.connectionRef -> Connection), a chain
// DeriveEdges alone never turns into a consumer -> tunneler-Provider
// container edge, so gsm-dbz and gsm-mesh's dial-side tunneler
// (container "gsm-orders-mediated") shared zero networks and the
// connector never reached RUNNING ("connection attempt failed").
func TestGraphScopedMediatedConnectionEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_GSM_MESH_ADMIN_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_GSM_MESH_ADMIN_PASSWORD", "gsm-test-admin-pw")
	t.Setenv("DATASCAPE_SECRET_GSM_DB_CREDS_USERNAME", "gsm_orders_ro")
	t.Setenv("DATASCAPE_SECRET_GSM_DB_CREDS_PASSWORD", "gsm-orders-pw")

	rt := requireDocker(t)
	ctx := context.Background()

	const (
		gsmVPCNetwork  = "gsm-vpc"
		gsmDBContainer = "gsm-vpc-db"
	)

	dbzKey := resource.Key{Namespace: "default", Kind: "Provider", Name: "gsm-dbz"}
	meshKey := resource.Key{Namespace: "default", Kind: "Provider", Name: "gsm-mesh"}
	rpKey := resource.Key{Namespace: "default", Kind: "Provider", Name: "gsm-rp"}
	sourceKey := resource.Key{Namespace: "default", Kind: "Source", Name: "gsm-orders-db"}
	connKey := resource.Key{Namespace: "default", Kind: "Connection", Name: "gsm-orders-mediated"}
	dbCredsKey := resource.Key{Namespace: "default", Kind: "SecretReference", Name: "gsm-db-creds"}
	meshAdminKey := resource.Key{Namespace: "default", Kind: "SecretReference", Name: "gsm-mesh-admin"}

	// docs/adr/029: janitor-owned cleanup, named-only (testkit.Janitor,
	// never a docker-state grep — a shared host may hold the user's own
	// containers/volumes). Private home networks + per-edge networks
	// (docs/adr/026 H7's Docker realization) are cleaned explicitly, the
	// same pattern graphscoped_integration_test.go's own t.Cleanup uses.
	cleanup := registerDockerCleanup(t,
		rt,
		[]string{"gsm-orders-to-events", "gsm-dbz", "gsm-rp", "gsm-orders-mediated", "gsm-mesh-ctrl", "gsm-mesh-router"},
		[]string{"gsm-mesh-ctrl-data", "gsm-mesh-router-data", "gsm-orders-mediated-identity"},
		"",
	)
	cleanup()
	t.Cleanup(func() {
		bg := context.Background()
		_ = rt.RemoveNetwork(bg, naming.PrivateNetworkName("datascape", "", dbzKey))
		_ = rt.RemoveNetwork(bg, naming.PrivateNetworkName("datascape", "", meshKey))
		_ = rt.RemoveNetwork(bg, naming.PrivateNetworkName("datascape", "", rpKey))
		_ = rt.RemoveNetwork(bg, naming.EdgeNetworkName(dbzKey, meshKey))
		_ = rt.RemoveNetwork(bg, naming.EdgeNetworkName(dbzKey, rpKey))
		// The remaining edge networks below are the coarser, pre-existing
		// (not M5-introduced) graph-scoped compiler's known wasted-but-
		// harmless edges for pass-through resources with no providerRef of
		// their own (a Source's connectionRef, a Provider/Connection's
		// secretRef(s), a Connection's own providerRef edge) — none of
		// them is ever joined by a second container, but the compiler
		// still allocates the network, so cleanup names them explicitly
		// here rather than leaving derelict networks on a shared host.
		_ = rt.RemoveNetwork(bg, naming.EdgeNetworkName(dbzKey, sourceKey))
		_ = rt.RemoveNetwork(bg, naming.EdgeNetworkName(dbzKey, dbCredsKey))
		_ = rt.RemoveNetwork(bg, naming.EdgeNetworkName(meshKey, connKey))
		_ = rt.RemoveNetwork(bg, naming.EdgeNetworkName(meshKey, meshAdminKey))
		_ = exec.Command("docker", "rm", "-f", gsmDBContainer).Run()
		_ = exec.Command("docker", "network", "rm", gsmVPCNetwork).Run()
	})

	// --- Raw fixture: the isolated "VPC" network + database, exactly
	// testdata/openziti-scenario's own precedent (docs/adr/023 Decision 7).
	mustRunZ(t, "docker", "network", "create", "--label", zitiManagedByLabel, gsmVPCNetwork)
	mustRunZ(t, "docker", "run", "-d", "--name", gsmDBContainer,
		"--network", gsmVPCNetwork,
		"-e", "POSTGRES_USER=gsm_orders_ro", "-e", "POSTGRES_PASSWORD=gsm-orders-pw", "-e", "POSTGRES_DB=ordersdb",
		"postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20",
		"postgres", "-c", "wal_level=logical")
	waitPostgresReadyZ(t, gsmDBContainer, "gsm_orders_ro")

	// --- apply, GraphScopedAccess + MediatedConnections BOTH on --------
	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/graphscoped-mediated-scenario"
	const gates = "MediatedConnections=true,GraphScopedAccess=true"

	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("apply took %s", time.Since(start).Round(time.Second))
	t.Cleanup(func() {
		_, _, _ = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gates)
	})

	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "apply")

	// --- THE proof: the CDC connector reached RUNNING — through the
	// mediated Connection, under GraphScopedAccess, exactly the case that
	// failed pre-M5 (Debezium had no network path to the tunneler at
	// all). -----------------------------------------------------------
	if state := zitiConnectorStatusOn(t, "gsm-orders-to-events", 18390); state != "RUNNING" {
		t.Errorf("connector state = %q, want RUNNING (graph-scoped + mediated composition)", state)
	}

	// --- Structural proof: gsm-dbz and the tunneler (container
	// "gsm-orders-mediated") actually share the M5 per-edge network. ----
	edgeNet := naming.EdgeNetworkName(dbzKey, meshKey)
	if err := rt.ProbeReachable(ctx, edgeNet, "gsm-orders-mediated:25890"); err != nil {
		t.Errorf("gsm-dbz must reach the tunneler via their M5 per-edge network %s: %v", edgeNet, err)
	}

	// --- Negative proof: the dark target never gets a network edge
	// directly — no container named after the raw VPC database is ever
	// managed by platformctl, so there is nothing for graph-scoped access
	// to expose it through in the first place; the only path in is the
	// mediated Connection's tunneler, proven above.
	if err := rt.ProbeReachable(ctx, edgeNet, gsmDBContainer+":5432"); err == nil {
		t.Error("the dark target must not be reachable via the consumer->tunneler edge network")
	}
}

// zitiConnectorStatusOn mirrors zitiConnectorStatus but against an
// arbitrary published Connect REST port — this scenario's gsm-dbz uses a
// distinct port from testdata/openziti-scenario's own datascape-ziti-dbz,
// so both suites can run without colliding.
func zitiConnectorStatusOn(t *testing.T, name string, port int) string {
	t.Helper()
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
	}
	getJSON(t, "http://localhost:"+strconv.Itoa(port)+"/connectors/"+name+"/status", &body)
	return body.Connector.State
}
