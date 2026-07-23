//go:build integration

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/testkit"
)

// TestCrossDomainMediatedPolicyEndToEnd is docs/planning/08 H9's Docker
// leg: Stage H exit criterion 3, COMPOSED — every prior H-series component
// (H3/H4 policy engine + pack, H5 domains, H6/H9 mediation) proven together
// in one live scenario, not merely in isolation (the failure class this
// task exists to close: "components-green is not composition-green," per
// the graphscoped/E7 merge-order incident doc 08 H9 itself cites).
//
// Topology (testdata/crossdomain-mediated-scenario/manifests.yaml, full
// rationale in its own header comment): a cdc Binding (xd-cdc) whose Source
// (xd-src, domain "payments", matching the REAL postgres backend xd-pg's
// own domain) feeds an EventStream (xd-events, domain "analytics",
// matching its realizing redpanda Provider xd-rp) — crossing a
// policy-governed domain boundary legitimately mediated by an openziti
// Connection (xd-conn, domain "analytics", co-located with the whole
// consumer chain: xd-mesh, xd-rp, xd-dbz). This produces TWO crossDomain
// edges over the same (payments, analytics) pair — the Binding's own
// sourceRef->targetRef edge, and xd-src's own connectionRef->xd-conn edge
// (unavoidable: xd-src must hold connectionRef per the external-Source
// resource model, and must differ in domain from both xd-events AND
// xd-conn for the topology to both trigger governance and physically
// route data — see the manifest's header comment for the full argument) —
// both denied by the SAME rule (testdata/.../policies/policy.yaml:
// deny-payments-to-analytics, exemptible) and both exempted the same way.
//
// Five legs, per docs/adr/021's 2026-07-23 severing amendment (defines what
// "removing the allow and re-applying severs it" actually means: admission
// refusal at the door, manifest-driven teardown — never continuous
// policy-to-infrastructure convergence):
//
//  1. validate WITHOUT the exemption refuses, naming the rule id, both
//     domains, and the denied edges.
//  2. WITH the exemption, apply reaches Ready and the CDC connector runs
//     RUNNING through the mediated path.
//  3. POSITIVE mediator evidence: the Ziti management API's own services/
//     service-policies/identities are asserted to be EXACTLY the expected
//     set for this one edge (not merely non-empty).
//  4. Removing the exemption and re-applying is REFUSED, fail-closed,
//     naming the edge — while the previously-authorized path keeps
//     running (validate/plan report the denial; status, which the ADR 021
//     policy wiring deliberately does not gate, proves the infra stands).
//  5. Removing the Binding+Connection+Source (Source must go too — an
//     External Source's connectionRef must resolve in-set, graph.Build's
//     own rule) and applying tears the mediation down: the Ziti service/
//     policies/identities for the edge are GONE — manifest-driven
//     teardown, the amendment's severing leg (b).
func TestCrossDomainMediatedPolicyEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_XD_PG_SUPER_USERNAME", "xd_super")
	t.Setenv("DATASCAPE_SECRET_XD_PG_SUPER_PASSWORD", "xd-super-pw")
	t.Setenv("DATASCAPE_SECRET_XD_MESH_ADMIN_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_XD_MESH_ADMIN_PASSWORD", "xd-mesh-admin-pw")

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	jan := testkit.Janitor{
		RT:        rt,
		Workloads: []string{"xd-pg", "xd-mesh-ctrl", "xd-mesh-router", "xd-conn", "xd-rp", "xd-dbz"},
		Volumes:   []string{"xd-pg-data", "xd-mesh-ctrl-data", "xd-mesh-router-data", "xd-conn-identity", "xd-rp-data"},
		Networks:  []string{"datascape-payments", "datascape-analytics"},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	const manifestsWith = "testdata/crossdomain-mediated-scenario"
	const policies = "testdata/crossdomain-mediated-scenario/policies"
	const gates = "PolicyEngine=true,MediatedConnections=true"

	manifestBytes, rerr := readFileT(t, manifestsWith+"/manifests.yaml")
	if rerr != nil {
		t.Fatal(rerr)
	}
	withoutExemption := stripCrossDomainExemptions(manifestBytes)
	withoutMediation := removeYAMLDocsContaining(manifestBytes, "kind: Connection", "kind: Source", "kind: Binding")

	// --- Leg 1: validate WITHOUT the exemption refuses -----------------
	noExemptDir := writeManifest(t, withoutExemption)
	out, verr, code := run(t, "validate", noExemptDir, "--policies", policies, "--feature-gates", gates)
	if code != cliutil.ExitValidation {
		t.Fatalf("leg1: validate exit code = %d, want %d (ExitValidation); err=%v\n%s", code, cliutil.ExitValidation, verr, out)
	}
	if verr == nil {
		t.Fatal("leg1: expected a denial error")
	}
	for _, want := range []string{"deny-payments-to-analytics", "payments", "analytics", "xd-src", "xd-cdc"} {
		if !strings.Contains(verr.Error(), want) {
			t.Errorf("leg1: validate error %q missing %q (rule id, both domains, both denied edges)", verr.Error(), want)
		}
	}

	// --- Leg 2: WITH the exemption, apply reaches Ready; the CDC
	// connector runs RUNNING through the mediated path. ------------------
	stateFile := t.TempDir() + "/state.json"

	start := time.Now()
	out, err, code = run(t, "apply", manifestsWith, "--state-file", stateFile, "--auto-approve", "--policies", policies, "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("leg2: apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("leg2: apply took %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifestsWith, "--state-file", stateFile, "--policies", policies, "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("leg2: status failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "leg2 apply")

	if state := xdConnectorStatus(t, "xd-cdc"); state != "RUNNING" {
		t.Errorf("leg2: connector state = %q, want RUNNING", state)
	}

	// --- Leg 3: POSITIVE mediator evidence — the Ziti management API's
	// own services/service-policies/identities are EXACTLY the expected
	// set for this one edge. ---------------------------------------------
	client, ctrlAddr, token := zitiClient(t, rt, ctx)
	assertMediatorStateExactly(t, client, ctrlAddr, token)

	// --- Leg 4: remove the exemption, re-apply is REFUSED fail-closed,
	// naming the edge; validate/plan report the denial while the path
	// (leg 2's infra) keeps standing — proven via `status`, which ADR
	// 021's wiring deliberately never policy-gates. ----------------------
	noExemptDir2 := writeManifest(t, withoutExemption)
	out, verr, code = run(t, "apply", noExemptDir2, "--state-file", stateFile, "--auto-approve", "--policies", policies, "--feature-gates", gates)
	if code != cliutil.ExitValidation {
		t.Fatalf("leg4: re-apply exit code = %d, want %d (ExitValidation); err=%v\n%s", code, cliutil.ExitValidation, verr, out)
	}
	for _, want := range []string{"deny-payments-to-analytics", "payments", "analytics"} {
		if verr == nil || !strings.Contains(verr.Error(), want) {
			t.Errorf("leg4: re-apply error missing %q: %v", want, verr)
		}
	}
	out, verr, code = run(t, "plan", noExemptDir2, "--state-file", stateFile, "--policies", policies, "--feature-gates", gates)
	if code != cliutil.ExitValidation {
		t.Fatalf("leg4: plan exit code = %d, want %d (ExitValidation); err=%v\n%s", code, cliutil.ExitValidation, verr, out)
	}

	out, err, code = run(t, "status", manifestsWith, "--state-file", stateFile, "--policies", policies, "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("leg4: status (unaffected by policy, per ADR 021 wiring) failed (code %d): %v\n%s", code, err, out)
	}
	assertAllStatusReady(t, out, "leg4: path stands after the withdrawn exemption")
	if state := xdConnectorStatus(t, "xd-cdc"); state != "RUNNING" {
		t.Errorf("leg4: connector state = %q after withdrawal, want RUNNING (severing refuses NEW authorization, it does not tear down the standing path — docs/adr/021's 2026-07-23 amendment)", state)
	}

	// --- Leg 5: remove the Binding+Connection+Source, apply — manifest-
	// driven teardown (the amendment's severing leg (b)): the Ziti
	// service/policies/identities for the edge are GONE. ------------------
	teardownDir := writeManifest(t, withoutMediation)
	out, err, code = run(t, "apply", teardownDir, "--state-file", stateFile, "--auto-approve", "--policies", policies, "--feature-gates", gates)
	if err != nil || code != 0 {
		t.Fatalf("leg5: apply (teardown) failed (code %d): %v\n%s", code, err, out)
	}
	if _, found, ierr := rt.Inspect(ctx, "xd-conn"); ierr != nil || found {
		t.Errorf("leg5: xd-conn container still present after manifest-driven teardown (found=%v, err=%v)", found, ierr)
	}
	client5, ctrlAddr5, token5 := zitiClient(t, rt, ctx)
	assertMediatorStateEmpty(t, client5, ctrlAddr5, token5)
}

// xdConnectURL is xd-dbz's published Connect REST port (see
// testdata/crossdomain-mediated-scenario/manifests.yaml's connectPort).
const xdConnectURL = "http://localhost:18295"

func xdConnectorStatus(t *testing.T, name string) string {
	t.Helper()
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
	}
	getJSON(t, xdConnectURL+"/connectors/"+name+"/status", &body)
	return body.Connector.State
}

// xdSourceURI/xdConnectionURI are naming.WorkloadIdentityURI's documented
// output (internal/domain/naming's own doc comment: "spiffe://datascape/
// <namespace>/<domain>/<kind>/<name> when a non-default domain is
// declared") computed by hand for the scenario's two fixed resources —
// this test lives outside the naming/openziti packages (matching
// openziti_integration_test.go's own zitiServiceRoleAttribute precedent:
// "this test lives outside the adapter package").
const (
	xdSourceURI     = "spiffe://datascape/default/payments/source/xd-src"
	xdConnectionURI = "spiffe://datascape/default/analytics/connection/xd-conn"
)

// zitiEntity decodes the fields every collection this test lists shares.
type zitiEntity struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	RoleAttributes []string `json:"roleAttributes"`
}

type zitiDialPolicyEntity struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	IdentityRoles []string `json:"identityRoles"`
	ServiceRoles  []string `json:"serviceRoles"`
}

// zitiClient authenticates against xd-mesh's controller (published host
// port, Docker) and returns a management-API client plus the session
// token.
func zitiClient(t *testing.T, rt *dockerruntime.Runtime, ctx context.Context) (*http.Client, string, string) {
	t.Helper()
	ctrlState, found, err := rt.Inspect(ctx, "xd-mesh-ctrl")
	if err != nil || !found {
		t.Fatalf("xd-mesh-ctrl not found: %v", err)
	}
	ctrlAddr := ctrlState.HostAddr(12895)
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // test-only, mirrors the adapter's own documented trust posture
	token := zitiAuthenticate(t, client, ctrlAddr, "admin", "xd-mesh-admin-pw")
	return client, ctrlAddr, token
}

func zitiListEntities(t *testing.T, client *http.Client, ctrlAddr, token, collection string) []zitiEntity {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, "https://"+ctrlAddr+"/edge/management/v1/"+collection+"?limit=500", nil)
	req.Header.Set("zt-session", token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("list %s: %v", collection, err)
	}
	defer resp.Body.Close()
	var out struct {
		Data []zitiEntity `json:"data"`
	}
	if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
		t.Fatalf("decode %s: %v", collection, derr)
	}
	return out.Data
}

func zitiListDialPolicies(t *testing.T, client *http.Client, ctrlAddr, token string) []zitiDialPolicyEntity {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, "https://"+ctrlAddr+"/edge/management/v1/service-policies?filter=type=%22Dial%22&limit=500", nil)
	req.Header.Set("zt-session", token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("list dial service-policies: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Data []zitiDialPolicyEntity `json:"data"`
	}
	if derr := json.NewDecoder(resp.Body).Decode(&out); derr != nil {
		t.Fatalf("decode dial service-policies: %v", derr)
	}
	return out.Data
}

// datascapeMediated filters entities to those this adapter itself minted
// (identity.go's "datascape-mediated" roleAttributes tag) — excluding
// Ziti's own built-in bootstrap admin identity, which carries no such tag.
func datascapeMediated(entities []zitiEntity) []zitiEntity {
	var out []zitiEntity
	for _, e := range entities {
		for _, ra := range e.RoleAttributes {
			if ra == "datascape-mediated" {
				out = append(out, e)
				break
			}
		}
	}
	return out
}

// assertMediatorStateExactly is leg 3: the Ziti management API's own
// services/service-policies/identities are EXACTLY the expected set for
// this one mediated edge — not merely non-empty (docs/planning/08 H9's own
// "assert the policy state itself, as the criterion's own wording
// demands"). Runtime-agnostic (takes an already-authenticated client and
// reachable controller address) so both the Docker leg (a published host
// port) and the Kubernetes leg (an ephemeral port-forward,
// runtime.WithReachable) can share it.
func assertMediatorStateExactly(t *testing.T, client *http.Client, ctrlAddr, token string) {
	t.Helper()

	wantService := zitiServiceRoleAttribute(xdConnectionURI)
	services := zitiListEntities(t, client, ctrlAddr, token, "services")
	if len(services) != 1 {
		t.Fatalf("leg3: services = %d, want exactly 1: %+v", len(services), services)
	}
	if services[0].Name != wantService {
		t.Errorf("leg3: service name = %q, want %q", services[0].Name, wantService)
	}

	wantIdentity := zitiServiceRoleAttribute(xdSourceURI)
	identities := datascapeMediated(zitiListEntities(t, client, ctrlAddr, token, "identities"))
	if len(identities) != 1 {
		t.Fatalf("leg3: datascape-mediated identities = %d, want exactly 1: %+v", len(identities), identities)
	}
	if identities[0].Name != wantIdentity {
		t.Errorf("leg3: identity name = %q, want %q", identities[0].Name, wantIdentity)
	}

	wantPolicy := "dial-" + wantIdentity + "-" + wantService
	policies := zitiListDialPolicies(t, client, ctrlAddr, token)
	if len(policies) != 1 {
		t.Fatalf("leg3: Dial service-policies = %d, want exactly 1: %+v", len(policies), policies)
	}
	pol := policies[0]
	if pol.Name != wantPolicy {
		t.Errorf("leg3: Dial policy name = %q, want %q", pol.Name, wantPolicy)
	}
	if len(pol.IdentityRoles) != 1 || pol.IdentityRoles[0] != "@"+identities[0].ID {
		t.Errorf("leg3: Dial policy identityRoles = %v, want [@%s]", pol.IdentityRoles, identities[0].ID)
	}
	if len(pol.ServiceRoles) != 1 || pol.ServiceRoles[0] != "@"+services[0].ID {
		t.Errorf("leg3: Dial policy serviceRoles = %v, want [@%s]", pol.ServiceRoles, services[0].ID)
	}
}

// assertMediatorStateEmpty is leg 5's teardown proof: after removing the
// Binding+Connection+Source and applying, the Ziti service/policies/
// identities for the edge are GONE (docs/adr/021's 2026-07-23 amendment,
// severing leg (b): manifest-driven teardown). Runtime-agnostic — see
// assertMediatorStateExactly's doc comment.
func assertMediatorStateEmpty(t *testing.T, client *http.Client, ctrlAddr, token string) {
	t.Helper()

	if services := zitiListEntities(t, client, ctrlAddr, token, "services"); len(services) != 0 {
		t.Errorf("leg5: services after teardown = %d, want 0: %+v", len(services), services)
	}
	if identities := datascapeMediated(zitiListEntities(t, client, ctrlAddr, token, "identities")); len(identities) != 0 {
		t.Errorf("leg5: datascape-mediated identities after teardown = %d, want 0: %+v", len(identities), identities)
	}
	if policies := zitiListDialPolicies(t, client, ctrlAddr, token); len(policies) != 0 {
		t.Errorf("leg5: Dial service-policies after teardown = %d, want 0: %+v", len(policies), policies)
	}
}

// readFileT reads path, failing the test on error.
func readFileT(t *testing.T, path string) (string, error) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// xdExemptionBlock is the exact two-line annotations block
// testdata/crossdomain-mediated-scenario/manifests.yaml repeats verbatim
// on both xd-src and xd-cdc (the two resources each of the scenario's two
// crossDomain decisions attaches to, docs/planning/08 H9's own doc
// comment above) — stripCrossDomainExemptions removes both occurrences in
// one pass.
const xdExemptionBlock = "  annotations:\n    policy.datascape.io/exempt: \"deny-payments-to-analytics: mediated by xd-conn (openziti), ADR 022 Ring 2\"\n"

// stripCrossDomainExemptions derives leg 1/4's "no exemption" manifest
// variant from the committed, fully-exempted testdata file.
func stripCrossDomainExemptions(manifest string) string {
	return strings.ReplaceAll(manifest, xdExemptionBlock, "")
}

// removeYAMLDocsContaining derives leg 5's teardown manifest variant:
// every "---"-separated YAML document containing any of substrings is
// dropped, the rest rejoined in original order. Used to remove the
// Binding+Connection+Source docs wholesale (docs/adr/021's 2026-07-23
// severing amendment, leg (b): manifest-driven teardown) — Source must go
// alongside Connection because an External Source's connectionRef must
// resolve in-set (internal/domain/graph.Build's own rule: "every reference
// — connectionRef included — must resolve in-set"), so leaving it in a
// manifest with no xd-conn would be a validate error, not a teardown.
func removeYAMLDocsContaining(manifest string, substrings ...string) string {
	docs := strings.Split(manifest, "\n---\n")
	var kept []string
	for _, doc := range docs {
		drop := false
		for _, s := range substrings {
			if strings.Contains(doc, s) {
				drop = true
				break
			}
		}
		if !drop {
			kept = append(kept, doc)
		}
	}
	return strings.Join(kept, "\n---\n")
}
