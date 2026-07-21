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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const serverName = "srv0"

var httpClient = &http.Client{Timeout: 10 * time.Second}

// caddyConfig is the full bootstrap document placed once via
// ContainerSpec.Files — an empty routes list; every route thereafter is
// added/updated/removed through the admin API below, never by rewriting
// this file (which would restart the container on every Connection change).
type caddyConfig struct {
	Admin caddyAdmin `json:"admin"`
	Apps  caddyApps  `json:"apps"`
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
}

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
// one HTTP server named serverName listening on httpPort, admin listening
// on adminPort, zero routes (every route is added afterward via the admin
// API).
func bootstrapConfig(httpPort, adminPort int) caddyConfig {
	return caddyConfig{
		Admin: caddyAdmin{Listen: fmt.Sprintf("0.0.0.0:%d", adminPort)},
		Apps: caddyApps{
			HTTP: caddyHTTPApp{
				Servers: map[string]caddyServer{
					serverName: {Listen: []string{fmt.Sprintf(":%d", httpPort)}, Routes: []caddyRoute{}},
				},
			},
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

// ensureRoute reconciles one route against the admin API at baseURL:
// PATCH /id/<id> if it already exists, else POST-append it to the server's
// routes array. Idempotent — a PATCH with unchanged content is a Caddy
// no-op (it still returns 200; no drift, no restart, no dropped connections
// on any other route).
func ensureRoute(ctx context.Context, baseURL string, route caddyRoute) error {
	body, err := json.Marshal(route)
	if err != nil {
		return fmt.Errorf("marshal route %q: %w", route.ID, err)
	}
	if err := caddyDo(ctx, http.MethodPatch, baseURL+"/id/"+route.ID, body); err == nil {
		return nil
	}
	// PATCH against a not-yet-existing @id fails (Caddy has nothing to
	// replace) — create it by appending to the server's routes array.
	appendURL := baseURL + "/config/apps/http/servers/" + serverName + "/routes/"
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
