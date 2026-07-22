// This file holds the Caddy JSON config shape and the admin-API client used
// to reconcile one Connection's route without ever touching
// ContainerSpec.Files (docs/adr/018 Decision 3): after the shared
// container's one-time bootstrap (docker.go), every route add/update/remove
// is a plain HTTP call against Caddy's admin API
// (https://caddyserver.com/docs/api), addressed by the "@id" tag this
// package assigns each route.
package ingress

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

const (
	serverName = "srv0" // plain HTTP — unchanged since C7
	// httpsServerName (docs/planning/08 C8) is a second Caddy HTTP-app
	// server, container-internal port 443 (Caddy's own default https_port,
	// so its automatic-HTTPS machinery recognizes the listener without
	// needing a port override), hosting every https-scheme Connection's
	// route. tls_connection_policies is what actually makes it terminate
	// TLS — found live (2026-07-21, real caddy:2.9.1): automatic_https.
	// disable: true alone still leaves the listener speaking plain HTTP;
	// an explicit (even empty) policy is required.
	httpsServerName = "srv1"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// caddyConfig is the full bootstrap document placed once via
// ContainerSpec.Files — an empty routes list; every route thereafter is
// added/updated/removed through the admin API below, never by rewriting
// this file (which would restart the container on every Connection change).
type caddyConfig struct {
	Admin caddyAdmin `json:"admin"`
	Apps  caddyApps  `json:"apps"`
}

// caddyTLSApp holds every manually-loaded certificate (docs/planning/08
// C8) — never Caddy's automatic ACME/on-demand issuance, which this
// provider never enables. Certificates is a map keyed by loader-module
// name; "load_pem" is Caddy's inline-PEM loader, the one this provider
// uses for both provided (secretRef) and self-signed leaf certificates —
// loaded via the admin API after bootstrap, exactly like routes, never via
// ContainerSpec.Files (see docs/adr/018 addendum: a per-Connection cert
// change would otherwise restart the shared proxy and drop every other
// Connection's live traffic, the identical Decision-3 problem routes
// already solved).
type caddyTLSApp struct {
	Certificates caddyCertificates `json:"certificates"`
}

type caddyCertificates struct {
	// LoadPEM deliberately has no `omitempty`, for the identical reason
	// caddyServer.Routes doesn't (bootstrapConfig always sets a real,
	// possibly-empty array so the first POST-append has something to
	// append into).
	LoadPEM []caddyPEMCert `json:"load_pem"`
}

// caddyPEMCert is one certificate manually loaded into Caddy's cert cache.
// ID becomes its "@id" tag — the same PATCH-or-POST-append,
// GET/DELETE-by-@id addressing scheme routes use. Found live: GET on a
// loaded cert's @id returns the Key field in plaintext (Caddy's admin API
// is a genuinely read-write config surface, not a write-only secret
// store) — the same already-documented trust boundary as the admin
// endpoint itself (shared network, unauthenticated, matching Kafka
// Connect/nessie/prometheus's own posture); recorded in docs/adr/018's
// addendum, not a new exposure.
type caddyPEMCert struct {
	ID          string `json:"@id,omitempty"`
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
}

// caddyAdmin.Listen is bound to 0.0.0.0 deliberately: Docker's host-port
// publish forwards to the container's network-namespace interface, not to
// its loopback — a process bound only to 127.0.0.1 inside the container is
// unreachable via a published host port. The published host-side bind stays
// loopback-only (Docker adapter's own default, docker.go's portMaps), so
// this is not internet-facing; it is reachable from the docker host and
// from any other container sharing the network — the same trust boundary
// every other admin surface in this codebase already has (nessie/prometheus/
// Kafka Connect's REST APIs are all unauthenticated plaintext HTTP inside
// the shared network).
type caddyAdmin struct {
	Listen string `json:"listen"`
}

type caddyApps struct {
	HTTP caddyHTTPApp `json:"http"`
	// TLS is omitempty at the Go-struct level for readability only —
	// bootstrapConfig always populates it (never leaves this nil), so the
	// admin API's "empty array to append into" requirement (see Routes'
	// doc comment below) is satisfied for load_pem too.
	TLS *caddyTLSApp `json:"tls,omitempty"`
}

type caddyHTTPApp struct {
	Servers map[string]caddyServer `json:"servers"`
}

type caddyServer struct {
	Listen []string `json:"listen"`
	// Routes deliberately has no `omitempty`: the bootstrap config must
	// place a real (possibly empty) JSON array at this path, not omit the
	// key — found live (2026-07-21): with the key absent, POST-appending
	// the first route ("create" path in ensureRoute) failed with "cannot
	// unmarshal object into Go struct field Server.servers.routes of type
	// caddyhttp.RouteList" because there was no existing array for Caddy to
	// append into. bootstrapConfig always sets this to a non-nil []caddyRoute{}.
	Routes []caddyRoute `json:"routes"`
	// AutomaticHTTPS suppresses Caddy's automatic ACME/on-demand cert
	// issuance and its automatic HTTP->HTTPS redirect — this provider only
	// ever loads certs manually, and its "hosts" are always .localhost-style
	// dev domains ACME could never issue for anyway. Set on both servers,
	// though only httpsServerName's setting matters in practice.
	AutomaticHTTPS *caddyAutomaticHTTPS `json:"automatic_https,omitempty"`
	// TLSConnectionPolicies is what actually makes a server terminate TLS
	// at all (docs/planning/08 C8) — found live: without at least one
	// (even empty, meaning "default cert selection by SNI against whatever
	// is manually loaded") policy, a listener on the https port still
	// speaks plain HTTP; AutomaticHTTPS.Disable alone does not enable TLS,
	// it only suppresses Caddy's *automatic* cert management. Left nil on
	// serverName (the plain-HTTP server) — never set there.
	TLSConnectionPolicies []caddyTLSConnectionPolicy `json:"tls_connection_policies,omitempty"`
}

type caddyAutomaticHTTPS struct {
	Disable bool `json:"disable,omitempty"`
}

// caddyTLSConnectionPolicy is deliberately an empty struct today — Caddy's
// default cert-selection-by-SNI behavior against the manually loaded
// load_pem cache is exactly what this provider needs, so bootstrapConfig
// sets exactly one empty policy on httpsServerName and this provider never
// needs to populate any of its (many) optional matcher/cipher fields.
type caddyTLSConnectionPolicy struct{}

// caddyRoute is one Connection's route: match on Host, reverse-proxy to the
// declared upstream. ID becomes the route's "@id" tag, the admin API's
// addressing handle for PATCH/DELETE/GET below.
type caddyRoute struct {
	ID     string         `json:"@id,omitempty"`
	Match  []caddyMatch   `json:"match"`
	Handle []caddyHandler `json:"handle"`
}

type caddyMatch struct {
	Host []string `json:"host"`
}

type caddyHandler struct {
	Handler   string          `json:"handler"`
	Upstreams []caddyUpstream `json:"upstreams,omitempty"`
}

type caddyUpstream struct {
	Dial string `json:"dial"`
}

// bootstrapConfig builds the minimal config placed at container creation:
// a plain-HTTP server (serverName) on httpPort, a TLS-terminating server
// (httpsServerName) on httpsPort, admin listening on adminPort, zero
// routes/certificates (both are added afterward via the admin API, docs/
// adr/018 Decision 3 extended to certs by this task).
func bootstrapConfig(httpPort, httpsPort, adminPort int) caddyConfig {
	return caddyConfig{
		Admin: caddyAdmin{Listen: fmt.Sprintf("0.0.0.0:%d", adminPort)},
		Apps: caddyApps{
			HTTP: caddyHTTPApp{
				Servers: map[string]caddyServer{
					serverName: {
						Listen:         []string{fmt.Sprintf(":%d", httpPort)},
						Routes:         []caddyRoute{},
						AutomaticHTTPS: &caddyAutomaticHTTPS{Disable: true},
					},
					httpsServerName: {
						Listen:                []string{fmt.Sprintf(":%d", httpsPort)},
						Routes:                []caddyRoute{},
						AutomaticHTTPS:        &caddyAutomaticHTTPS{Disable: true},
						TLSConnectionPolicies: []caddyTLSConnectionPolicy{{}},
					},
				},
			},
			TLS: &caddyTLSApp{Certificates: caddyCertificates{LoadPEM: []caddyPEMCert{}}},
		},
	}
}

// desiredRoute builds the route this package's Connection reconcile always
// wants for one Connection: Host(host) -> reverse_proxy to target. target is
// Connection.spec.target, passed straight through — never constructed
// (docs/adr/018 Decision 3).
func desiredRoute(connName, host, target string) caddyRoute {
	return caddyRoute{
		ID:     routeID(connName),
		Match:  []caddyMatch{{Host: []string{host}}},
		Handle: []caddyHandler{{Handler: "reverse_proxy", Upstreams: []caddyUpstream{{Dial: target}}}},
	}
}

// ensureRoute reconciles one route against the admin API at baseURL, on the
// named server (serverName for http Connections, httpsServerName for
// https): PATCH /id/<id> if it already exists, else POST-append it to that
// server's routes array. Idempotent — a PATCH with unchanged content is a
// Caddy no-op (it still returns 200; no drift, no restart, no dropped
// connections on any other route).
func ensureRoute(ctx context.Context, baseURL, server string, route caddyRoute) error {
	body, err := json.Marshal(route)
	if err != nil {
		return fmt.Errorf("marshal route %q: %w", route.ID, err)
	}
	if err := caddyDo(ctx, http.MethodPatch, baseURL+"/id/"+route.ID, body); err == nil {
		return nil
	}
	// PATCH against a not-yet-existing @id fails (Caddy has nothing to
	// replace) — create it by appending to the server's routes array.
	appendURL := baseURL + "/config/apps/http/servers/" + server + "/routes/"
	if err := caddyDo(ctx, http.MethodPost, appendURL, body); err != nil {
		return fmt.Errorf("create route %q: %w", route.ID, err)
	}
	return nil
}

// getRoute reads the live route tagged id, or found=false if no such route
// exists (removed out-of-band — the C7 "mangled route" drift case).
func getRoute(ctx context.Context, baseURL, id string) (caddyRoute, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/id/"+id, nil)
	if err != nil {
		return caddyRoute{}, false, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return caddyRoute{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest {
		// Caddy answers an unknown @id with 400 (no such object), not 404 —
		// both mean "this route does not exist right now".
		return caddyRoute{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return caddyRoute{}, false, fmt.Errorf("GET %s: HTTP %d: %s", req.URL, resp.StatusCode, string(b))
	}
	var route caddyRoute
	if err := json.NewDecoder(resp.Body).Decode(&route); err != nil {
		return caddyRoute{}, false, fmt.Errorf("decode route %q: %w", id, err)
	}
	route.ID = id // Caddy's stored object doesn't echo its own @id back
	return route, true, nil
}

// deleteRoute removes the route tagged id — a no-op (not an error) if it is
// already gone, matching every other Remove*/Destroy idempotency contract
// in this codebase.
func deleteRoute(ctx context.Context, baseURL, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, baseURL+"/id/"+id, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest {
		return nil
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE %s: HTTP %d: %s", req.URL, resp.StatusCode, string(b))
	}
	return nil
}

// certID is the Caddy "@id" tag for a Connection's TLS certificate —
// distinct from routeID's "route-<name>" so a route and its cert never
// collide in Caddy's flat @id namespace.
func certID(connName string) string { return "cert-" + connName }

// ensureCert reconciles one certificate against the admin API at baseURL:
// PATCH /id/<id> if it already exists, else POST-append it to the tls
// app's load_pem array — the identical PATCH-or-POST-append shape
// ensureRoute uses, so a cert reconcile is exactly as idempotent and never
// touches ContainerSpec.Files (docs/adr/018 addendum).
func ensureCert(ctx context.Context, baseURL string, cert caddyPEMCert) error {
	body, err := json.Marshal(cert)
	if err != nil {
		return fmt.Errorf("marshal certificate %q: %w", cert.ID, err)
	}
	if err := caddyDo(ctx, http.MethodPatch, baseURL+"/id/"+cert.ID, body); err == nil {
		return nil
	}
	appendURL := baseURL + "/config/apps/tls/certificates/load_pem/"
	if err := caddyDo(ctx, http.MethodPost, appendURL, body); err != nil {
		return fmt.Errorf("create certificate %q: %w", cert.ID, err)
	}
	return nil
}

// getCert reads the live certificate tagged id, or found=false if none is
// loaded (removed out-of-band, or never reconciled yet). Caddy answers an
// unknown certificate @id with 404 (routes answer 400 for the analogous
// case — both mean "does not exist right now", found live 2026-07-21).
func getCert(ctx context.Context, baseURL, id string) (caddyPEMCert, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/id/"+id, nil)
	if err != nil {
		return caddyPEMCert{}, false, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return caddyPEMCert{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest {
		return caddyPEMCert{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return caddyPEMCert{}, false, fmt.Errorf("GET %s: HTTP %d: %s", req.URL, resp.StatusCode, string(b))
	}
	var cert caddyPEMCert
	if err := json.NewDecoder(resp.Body).Decode(&cert); err != nil {
		return caddyPEMCert{}, false, fmt.Errorf("decode certificate %q: %w", id, err)
	}
	cert.ID = id
	return cert, true, nil
}

// deleteCert removes the certificate tagged id — a no-op (not an error) if
// already gone, matching deleteRoute's idempotency contract.
func deleteCert(ctx context.Context, baseURL, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, baseURL+"/id/"+id, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest {
		return nil
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE %s: HTTP %d: %s", req.URL, resp.StatusCode, string(b))
	}
	return nil
}

// caddyReady reports whether the admin API itself answers — the Provider
// probe's health signal beyond the container healthcheck.
func caddyReady(ctx context.Context, baseURL string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/config/", nil)
	if err != nil {
		return false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func caddyDo(ctx context.Context, method, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s: HTTP %d: %s", method, url, resp.StatusCode, string(b))
	}
	return nil
}

// probeThroughRoute dials the shared proxy's published HTTP port with the
// Host header set to host and reports whether the upstream answered — a
// 502/504 from Caddy means the route is fine but the upstream is not (the
// same "beyond the container's own health, dial through it" discipline
// proxy.probeThroughForwarder already applies for TCP).
func probeThroughRoute(ctx context.Context, addr, host string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/", nil)
	if err != nil {
		return err
	}
	req.Host = host
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusGatewayTimeout {
		return fmt.Errorf("proxy is up but upstream is unreachable: HTTP %d", resp.StatusCode)
	}
	return nil
}

// probeThroughRouteTLS is probeThroughRoute's https counterpart: it dials
// with SNI set to host over TLS and checks the same 502/504-means-upstream-
// down signal. Chain/cert validity is a separate, already-covered check
// (certValidForHost/certChainsToCA against the loaded certificate itself);
// this probe's only job is "is the proxy able to reach the upstream through
// this route," so it skips certificate verification deliberately — the
// same "beyond the container's own health" scope probeThroughRoute has,
// not a chain-trust decision.
func probeThroughRouteTLS(ctx context.Context, addr, host string) error {
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true, ServerName: host}, // #nosec G402 -- upstream-reachability probe only, not a trust decision (see doc comment)
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+"/", nil)
	if err != nil {
		return err
	}
	req.Host = host
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusGatewayTimeout {
		return fmt.Errorf("proxy is up but upstream is unreachable: HTTP %d", resp.StatusCode)
	}
	return nil
}
