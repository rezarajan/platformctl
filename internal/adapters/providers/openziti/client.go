package openziti

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// edgeClient is a minimal, hand-rolled client for OpenZiti's Edge
// Management REST API — deliberately not a generated SDK: this codebase's
// established ethos (docs/adr/023: "drive the tool directly") is to talk
// to a pinned technology's own native surface with the smallest client that
// covers exactly what's needed, the same way postgres uses pgx directly
// rather than an ORM. The handful of endpoints used here (authenticate,
// identities, edge-routers, services, service-policies, terminators) were
// verified live against the pinned controller image at authorship time.
//
// TLS: the controller's PKI is self-signed (bootstrap-generated CA) —
// InsecureSkipVerify is used rather than fetching and pinning the
// bootstrap CA bundle. Recorded as a follow-up (fetch
// GET /.well-known/est/cacerts and pin it), not a silent gap: this
// adapter's own trust boundary is the Docker/Kubernetes network the
// controller is only reachable on plus the admin credential
// (configuration.adminSecretRef) — matching the honest, narrower trust
// model docs/adr/013 already accepts for other bootstrap-time secrets.
type edgeClient struct {
	baseURL string
	http    *http.Client
	token   string
}

func newEdgeClient(baseURL string) *edgeClient {
	return &edgeClient{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // see doc comment: follow-up is CA pinning, not verification proper
			},
		},
	}
}

type apiEnvelope struct {
	Data  json.RawMessage `json:"data"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (c *edgeClient) do(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("openziti: marshal request body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return fmt.Errorf("openziti: build request %s %s: %w", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("zt-session", c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("openziti: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if tok := resp.Header.Get("zt-session"); tok != "" {
		c.token = tok
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("openziti: read response %s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 300 {
		var env apiEnvelope
		msg := string(respBody)
		if json.Unmarshal(respBody, &env) == nil && env.Error != nil {
			msg = fmt.Sprintf("%s: %s", env.Error.Code, env.Error.Message)
		}
		return &apiError{StatusCode: resp.StatusCode, Message: msg}
	}
	if out == nil {
		return nil
	}
	var env apiEnvelope
	if err := json.Unmarshal(respBody, &env); err != nil {
		return fmt.Errorf("openziti: decode response %s %s: %w", method, path, err)
	}
	if len(env.Data) == 0 {
		return nil
	}
	return json.Unmarshal(env.Data, out)
}

// apiError carries the HTTP status so callers can distinguish "not found"
// (idempotent no-op territory) from a real failure.
type apiError struct {
	StatusCode int
	Message    string
}

func (e *apiError) Error() string { return fmt.Sprintf("openziti API: %s", e.Message) }

func isNotFound(err error) bool {
	ae, ok := err.(*apiError)
	return ok && ae.StatusCode == http.StatusNotFound
}

// Authenticate exchanges username/password for a session token (the
// zt-session header) — verified live: the controller's default admin
// identity, created by ZITI_BOOTSTRAP, authenticates against
// /edge/management/v1/authenticate?method=password.
func (c *edgeClient) Authenticate(ctx context.Context, username, password string) error {
	body := map[string]string{"username": username, "password": password}
	return c.do(ctx, http.MethodPost, "/edge/management/v1/authenticate?method=password", body, nil)
}

// Version reports whether the controller's REST API answers at all — the
// bounded-poll settledness check reconcileInstance uses instead of a
// Docker-level HEALTHCHECK (docs/planning/02 §4.1: "reconcile runs the
// SAME serving check its own Probe uses").
func (c *edgeClient) Version(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/edge/client/v1/version", nil, nil)
}

type entityRef struct {
	ID string `json:"id"`
}

// findByName performs the list-and-filter GET every Ensure* method below
// uses for idempotency: "does an entity with this name already exist."
func (c *edgeClient) findByName(ctx context.Context, collection, name string) (string, bool, error) {
	var page struct {
		Items []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"-"`
	}
	var raw []json.RawMessage
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/edge/management/v1/%s?filter=name=%%22%s%%22", collection, name), nil, &raw); err != nil {
		return "", false, err
	}
	for _, r := range raw {
		var item struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(r, &item); err != nil {
			continue
		}
		if item.Name == name {
			return item.ID, true, nil
		}
	}
	_ = page
	return "", false, nil
}

// upsertIdentity ensures a Device identity named name exists, tagged with
// roleAttributes, returning its id and — only when freshly created — its
// one-time enrollment JWT (empty for an already-existing identity: Ziti
// issues an OTT enrollment JWT once; re-fetching one for an
// already-enrolled identity is a distinct, deliberately not-implemented
// operation here, matching this method's idempotency contract: a second
// call for the same name makes no additional control-plane WRITE, but the
// caller can no longer obtain a fresh enrollment token from it).
func (c *edgeClient) upsertIdentity(ctx context.Context, name string, roleAttributes []string) (id string, enrollmentJWT string, err error) {
	if existingID, ok, ferr := c.findByName(ctx, "identities", name); ferr != nil {
		return "", "", ferr
	} else if ok {
		return existingID, "", nil
	}
	body := map[string]any{
		"name":           name,
		"type":           "Device",
		"isAdmin":        false,
		"enrollment":     map[string]any{"ott": true},
		"roleAttributes": roleAttributes,
	}
	var created entityRef
	if err := c.do(ctx, http.MethodPost, "/edge/management/v1/identities", body, &created); err != nil {
		return "", "", err
	}
	jwt, err := c.identityEnrollmentJWT(ctx, created.ID)
	if err != nil {
		return "", "", err
	}
	return created.ID, jwt, nil
}

func (c *edgeClient) identityEnrollmentJWT(ctx context.Context, id string) (string, error) {
	var out struct {
		Enrollment struct {
			OTT struct {
				JWT string `json:"jwt"`
			} `json:"ott"`
		} `json:"enrollment"`
	}
	if err := c.do(ctx, http.MethodGet, "/edge/management/v1/identities/"+id, nil, &out); err != nil {
		return "", err
	}
	return out.Enrollment.OTT.JWT, nil
}

func (c *edgeClient) deleteIdentity(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodDelete, "/edge/management/v1/identities/"+id, nil, nil)
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

func (c *edgeClient) identityFingerprint(ctx context.Context, id string) (string, bool, error) {
	var out struct {
		Authenticators struct {
			Cert *struct {
				Fingerprint string `json:"fingerprint"`
			} `json:"cert"`
		} `json:"authenticators"`
	}
	if err := c.do(ctx, http.MethodGet, "/edge/management/v1/identities/"+id, nil, &out); err != nil {
		if isNotFound(err) {
			return "", false, nil
		}
		return "", false, err
	}
	if out.Authenticators.Cert == nil {
		return "", false, nil
	}
	return out.Authenticators.Cert.Fingerprint, true, nil
}

// upsertEdgeRouter ensures an edge-router entity exists, tunneler-enabled
// (required for the router-hosted-terminator mechanism connection.go
// uses). Returns the enrollment JWT only when the router is not yet
// verified (isVerified false) — an already-enrolled router's persisted
// identity (docs/planning/08 NFR-11-style persistence: the router
// container's own volume) makes re-enrollment both impossible (Ziti
// rejects reusing a consumed OTT) and unnecessary.
func (c *edgeClient) upsertEdgeRouter(ctx context.Context, name string) (id string, enrollmentJWT string, verified bool, err error) {
	existingID, ok, ferr := c.findByName(ctx, "edge-routers", name)
	if ferr != nil {
		return "", "", false, ferr
	}
	if ok {
		var out struct {
			IsVerified    bool   `json:"isVerified"`
			EnrollmentJwt string `json:"enrollmentJwt"`
		}
		if err := c.do(ctx, http.MethodGet, "/edge/management/v1/edge-routers/"+existingID, nil, &out); err != nil {
			return "", "", false, err
		}
		if out.IsVerified {
			return existingID, "", true, nil
		}
		return existingID, out.EnrollmentJwt, false, nil
	}
	body := map[string]any{"name": name, "roleAttributes": []string{"public"}, "isTunnelerEnabled": true}
	var created entityRef
	if err := c.do(ctx, http.MethodPost, "/edge/management/v1/edge-routers", body, &created); err != nil {
		return "", "", false, err
	}
	var out struct {
		EnrollmentJwt string `json:"enrollmentJwt"`
	}
	if err := c.do(ctx, http.MethodGet, "/edge/management/v1/edge-routers/"+created.ID, nil, &out); err != nil {
		return "", "", false, err
	}
	return created.ID, out.EnrollmentJwt, false, nil
}

func (c *edgeClient) deleteEdgeRouter(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodDelete, "/edge/management/v1/edge-routers/"+id, nil, nil)
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

// upsertCatchAllRouterPolicies ensures the two structural policies every
// router needs before it can serve ANY identity/service — found live, not
// anticipated from ADR 022/026/027's own design text: an Edge Router
// Policy (which identities may USE this router at all) and a Service Edge
// Router Policy (which services this router may HOST/serve) are Ziti's own
// separate concern from per-edge Dial/Bind authorization
// (upsertDialPolicy) — a real deployment's `ziti edge quickstart` creates
// a permissive catch-all pair of these automatically; this adapter's
// env-var-driven bootstrap (openziti.go's doc comment) does not, so it
// must create the equivalent explicitly. This is infrastructure plumbing,
// not per-edge policy — the ADR 026 authorization decision itself still
// lives entirely in upsertDialPolicy/RealizeEdge, exactly as a Kubernetes
// pod being schedulable on a node is a distinct concern from a
// NetworkPolicy governing what it may talk to. "#all" is Ziti's built-in
// wildcard role attribute.
func (c *edgeClient) upsertCatchAllRouterPolicies(ctx context.Context, routerID string) error {
	if err := c.upsertNamedPolicy(ctx, "edge-router-policies", "datascape-mediation-erp", map[string]any{
		"name":            "datascape-mediation-erp",
		"edgeRouterRoles": []string{"@" + routerID},
		"identityRoles":   []string{"#all"},
		"semantic":        "AnyOf",
	}); err != nil {
		return fmt.Errorf("openziti: ensure catch-all edge-router-policy: %w", err)
	}
	if err := c.upsertNamedPolicy(ctx, "service-edge-router-policies", "datascape-mediation-serp", map[string]any{
		"name":            "datascape-mediation-serp",
		"edgeRouterRoles": []string{"@" + routerID},
		"serviceRoles":    []string{"#all"},
		"semantic":        "AnyOf",
	}); err != nil {
		return fmt.Errorf("openziti: ensure catch-all service-edge-router-policy: %w", err)
	}
	return nil
}

// upsertNamedPolicy is the shared create-or-update-in-place body
// upsertCatchAllRouterPolicies uses for both policy collections — the same
// idempotency shape upsertDialPolicy already gives Dial service-policies.
func (c *edgeClient) upsertNamedPolicy(ctx context.Context, collection, name string, body map[string]any) error {
	existingID, ok, err := c.findByName(ctx, collection, name)
	if err != nil {
		return err
	}
	if !ok {
		return c.do(ctx, http.MethodPost, "/edge/management/v1/"+collection, body, nil)
	}
	return c.do(ctx, http.MethodPut, "/edge/management/v1/"+collection+"/"+existingID, body, nil)
}

// upsertService ensures a Service named name exists. encryptionRequired is
// deliberately false — found live, not anticipated: Ziti's end-to-end
// SDK-level encryption (on top of, not instead of, the fabric's own
// always-on router mTLS — docs/adr/022's "mTLS between mediator legs is
// the mediator's own" claim is about that fabric layer, unaffected by this
// setting) requires BOTH the dial AND bind side to be SDK/tunneler-aware;
// this adapter's bind side is a router-native "transport"-binding
// terminator (openziti.go's doc comment — no per-target tunneler
// process), which cannot participate in that handshake and fails every
// dial with "encryption required on service, terminator did not send
// public header" when this is true. The mutual-authentication guarantee
// itself (docs/adr/027 Layer 1: "the receiving side refuses any peer not
// presenting the identity the graph authorizes") is unaffected — that is
// enforced by the Dial service-policy's identity check
// (upsertDialPolicy), not by this per-service transport-encryption flag.
//
// Idempotency includes CONVERGENCE, not just existence: if a service by
// this name already exists but carries a divergent encryptionRequired
// (e.g. one created before this adapter settled on false, or edited
// out-of-band), it is PATCHed back to the desired value rather than left
// as-is — the same "drift is healed, not merely detected" contract every
// other Ensure* in this adapter holds (docs/planning/08 H6 accept: "drift
// on out-of-band Ziti policy edits detected and healed").
func (c *edgeClient) upsertService(ctx context.Context, name string) (id string, err error) {
	const desiredEncryption = false
	existingID, ok, ferr := c.findByName(ctx, "services", name)
	if ferr != nil {
		return "", ferr
	}
	if ok {
		var out struct {
			EncryptionRequired bool `json:"encryptionRequired"`
		}
		if err := c.do(ctx, http.MethodGet, "/edge/management/v1/services/"+existingID, nil, &out); err != nil {
			return "", err
		}
		if out.EncryptionRequired != desiredEncryption {
			patch := map[string]any{"encryptionRequired": desiredEncryption}
			if err := c.do(ctx, http.MethodPatch, "/edge/management/v1/services/"+existingID, patch, nil); err != nil {
				return "", fmt.Errorf("openziti: converge service %q encryptionRequired: %w", name, err)
			}
		}
		return existingID, nil
	}
	body := map[string]any{"name": name, "encryptionRequired": desiredEncryption}
	var created entityRef
	if err := c.do(ctx, http.MethodPost, "/edge/management/v1/services", body, &created); err != nil {
		return "", err
	}
	return created.ID, nil
}

func (c *edgeClient) deleteService(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodDelete, "/edge/management/v1/services/"+id, nil, nil)
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

// upsertTransportTerminator ensures a router-hosted, raw-TCP "transport"
// binding terminator exists for service, forwarding to address ("host:port")
// — OpenZiti's own native mechanism for exposing a plain TCP backend
// without a per-target tunneler process (this package's doc comment). Ziti
// terminators of the same (service, router, binding) triple are unique by
// construction; a changed address requires delete-then-recreate (Ziti has
// no in-place terminator address update in the version pinned here).
func (c *edgeClient) upsertTransportTerminator(ctx context.Context, serviceID, routerID, address string) error {
	var existing []struct {
		ID      string `json:"id"`
		Address string `json:"address"`
	}
	path := fmt.Sprintf("/edge/management/v1/terminators?filter=service=%%22%s%%22+and+router=%%22%s%%22", serviceID, routerID)
	if err := c.do(ctx, http.MethodGet, path, nil, &existing); err != nil {
		return err
	}
	for _, e := range existing {
		if e.Address == address {
			return nil // already exactly as desired
		}
		if err := c.do(ctx, http.MethodDelete, "/edge/management/v1/terminators/"+e.ID, nil, nil); err != nil && !isNotFound(err) {
			return err
		}
	}
	body := map[string]any{
		"service": serviceID,
		"router":  routerID,
		"binding": "transport",
		"address": "tcp:" + address,
	}
	return c.do(ctx, http.MethodPost, "/edge/management/v1/terminators", body, nil)
}

func (c *edgeClient) deleteTerminatorsForService(ctx context.Context, serviceID string) error {
	var existing []struct {
		ID string `json:"id"`
	}
	path := fmt.Sprintf("/edge/management/v1/terminators?filter=service=%%22%s%%22", serviceID)
	if err := c.do(ctx, http.MethodGet, path, nil, &existing); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	for _, e := range existing {
		if err := c.do(ctx, http.MethodDelete, "/edge/management/v1/terminators/"+e.ID, nil, nil); err != nil && !isNotFound(err) {
			return err
		}
	}
	return nil
}

// upsertDialPolicy ensures a Dial service-policy exists scoping exactly
// identityIDs to serviceID (direct @id references, not role-attribute
// groups — the ADR 026 per-EDGE, not per-group, authorization this task
// requires). Idempotent by name; re-applying a changed identityIDs set
// updates the policy's identityRoles in place.
func (c *edgeClient) upsertDialPolicy(ctx context.Context, name, serviceID string, identityIDs []string) error {
	roles := make([]string, len(identityIDs))
	for i, id := range identityIDs {
		roles[i] = "@" + id
	}
	body := map[string]any{
		"name":          name,
		"type":          "Dial",
		"semantic":      "AnyOf",
		"identityRoles": roles,
		"serviceRoles":  []string{"@" + serviceID},
	}
	existingID, ok, err := c.findByName(ctx, "service-policies", name)
	if err != nil {
		return err
	}
	if !ok {
		return c.do(ctx, http.MethodPost, "/edge/management/v1/service-policies", body, nil)
	}
	return c.do(ctx, http.MethodPut, "/edge/management/v1/service-policies/"+existingID, body, nil)
}

func (c *edgeClient) deleteDialPolicy(ctx context.Context, name string) error {
	id, ok, err := c.findByName(ctx, "service-policies", name)
	if err != nil || !ok {
		return err
	}
	err = c.do(ctx, http.MethodDelete, "/edge/management/v1/service-policies/"+id, nil, nil)
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

// listServicePolicies/listIdentities back ObservedEdges/ObservedIdentities
// (the drift-detection primitive, mediation.MediationProvider's doc
// comment). listDialPolicies lists ALL service-policies and filters to
// type "Dial" client-side rather than server-side — found live
// (docs/planning/08 H9): the controller's own filter query language
// rejects `filter=type=%22Dial%22` outright (HTTP 400 INVALID_FILTER,
// "operation type = Dial is not supported with operands types number,
// string" — "type" resolves internally to a numeric enum, not a
// string-comparable field, in the pinned controller version), so this
// method has been silently returning a hard error on every real call
// since H6 shipped it — ObservedEdges (drift detection) was broken for
// every Connection with at least one authorized edge. An undocumented
// numeric ordinal (`filter=type%3D1`) does work but is exactly the kind
// of implementation-detail coupling docs/adr/023's "drive the tool
// directly" ethos argues against relying on when a simple, robust
// alternative exists — the whole collection is small (bounded by
// declared edges) and cheap to filter in Go.
func (c *edgeClient) listDialPolicies(ctx context.Context) ([]struct {
	Name          string   `json:"name"`
	IdentityRoles []string `json:"identityRoles"`
	ServiceRoles  []string `json:"serviceRoles"`
}, error) {
	var all []struct {
		Name          string   `json:"name"`
		Type          string   `json:"type"`
		IdentityRoles []string `json:"identityRoles"`
		ServiceRoles  []string `json:"serviceRoles"`
	}
	if err := c.do(ctx, http.MethodGet, "/edge/management/v1/service-policies?limit=500", nil, &all); err != nil {
		return nil, err
	}
	out := make([]struct {
		Name          string   `json:"name"`
		IdentityRoles []string `json:"identityRoles"`
		ServiceRoles  []string `json:"serviceRoles"`
	}, 0, len(all))
	for _, p := range all {
		if p.Type != "Dial" {
			continue
		}
		out = append(out, struct {
			Name          string   `json:"name"`
			IdentityRoles []string `json:"identityRoles"`
			ServiceRoles  []string `json:"serviceRoles"`
		}{Name: p.Name, IdentityRoles: p.IdentityRoles, ServiceRoles: p.ServiceRoles})
	}
	return out, nil
}

func (c *edgeClient) listIdentities(ctx context.Context) ([]struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}, error) {
	var out []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	err := c.do(ctx, http.MethodGet, "/edge/management/v1/identities?limit=500", nil, &out)
	return out, err
}
