package ingress

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeCaddyAdmin simulates just enough of Caddy's admin API for this
// package's client functions: PATCH /id/<id> only succeeds if the id was
// already POST-appended; GET/DELETE /id/<id> answer 400 for an unknown
// route id (Caddy's real behavior for routes) or 404 for an unknown
// certificate id (Caddy's real, distinct behavior for certs — found live
// 2026-07-21, docs/planning/08 C8), matching caddy.go's handling of both.
// Certificates and routes share the flat "/id/" namespace but never
// collide (routeID/certID prefix "route-"/"cert-" respectively), so one
// fake distinguishes by that prefix.
type fakeCaddyAdmin struct {
	mu     sync.Mutex
	routes map[string]caddyRoute
	certs  map[string]caddyPEMCert
}

func newFakeCaddyAdmin() *httptest.Server {
	f := &fakeCaddyAdmin{routes: map[string]caddyRoute{}, certs: map[string]caddyPEMCert{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/config/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case http.MethodPost:
			if strings.Contains(r.URL.Path, "load_pem") {
				var cert caddyPEMCert
				_ = json.NewDecoder(r.Body).Decode(&cert)
				f.certs[cert.ID] = cert
				w.WriteHeader(http.StatusOK)
				return
			}
			var route caddyRoute
			_ = json.NewDecoder(r.Body).Decode(&route)
			f.routes[route.ID] = route
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/id/", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len("/id/"):]
		f.mu.Lock()
		defer f.mu.Unlock()
		if strings.HasPrefix(id, "cert-") {
			handleFakeCert(f, w, r, id)
			return
		}
		route, ok := f.routes[id]
		switch r.Method {
		case http.MethodGet:
			if !ok {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(route)
		case http.MethodPatch:
			if !ok {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var updated caddyRoute
			_ = json.NewDecoder(r.Body).Decode(&updated)
			f.routes[id] = updated
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			if !ok {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			delete(f.routes, id)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return httptest.NewServer(mux)
}

// handleFakeCert mirrors the route handler above but with Caddy's real,
// distinct 404-for-unknown-certificate behavior (routes answer 400 for the
// same "no such object" case).
func handleFakeCert(f *fakeCaddyAdmin, w http.ResponseWriter, r *http.Request, id string) {
	cert, ok := f.certs[id]
	switch r.Method {
	case http.MethodGet:
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(cert)
	case http.MethodPatch:
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var updated caddyPEMCert
		_ = json.NewDecoder(r.Body).Decode(&updated)
		f.certs[id] = updated
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		delete(f.certs, id)
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestEnsureRouteCreatesThenUpdates(t *testing.T) {
	srv := newFakeCaddyAdmin()
	defer srv.Close()
	ctx := context.Background()

	route := desiredRoute("nessie", "nessie.localhost", "nessie:19120")
	if err := ensureRoute(ctx, srv.URL, serverName, route); err != nil {
		t.Fatalf("create: %v", err)
	}
	live, found, err := getRoute(ctx, srv.URL, route.ID)
	if err != nil || !found {
		t.Fatalf("getRoute after create: found=%v err=%v", found, err)
	}
	if !routesEquivalent(live, route) {
		t.Errorf("live route %+v does not match desired %+v", live, route)
	}

	// Update: same @id, different upstream — must PATCH the existing
	// object, not fail or duplicate it.
	updated := desiredRoute("nessie", "nessie.localhost", "nessie:9999")
	if err := ensureRoute(ctx, srv.URL, serverName, updated); err != nil {
		t.Fatalf("update: %v", err)
	}
	live, found, err = getRoute(ctx, srv.URL, route.ID)
	if err != nil || !found {
		t.Fatalf("getRoute after update: found=%v err=%v", found, err)
	}
	if live.Handle[0].Upstreams[0].Dial != "nessie:9999" {
		t.Errorf("route not updated: dial = %q, want nessie:9999", live.Handle[0].Upstreams[0].Dial)
	}
}

func TestGetRouteNotFound(t *testing.T) {
	srv := newFakeCaddyAdmin()
	defer srv.Close()
	_, found, err := getRoute(context.Background(), srv.URL, "route-missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("found = true, want false")
	}
}

func TestDeleteRouteIdempotent(t *testing.T) {
	srv := newFakeCaddyAdmin()
	defer srv.Close()
	ctx := context.Background()
	route := desiredRoute("nessie", "nessie.localhost", "nessie:19120")
	if err := ensureRoute(ctx, srv.URL, serverName, route); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := deleteRoute(ctx, srv.URL, route.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Second delete of an already-gone route must not error (matches every
	// other Remove*/Destroy idempotency contract in this codebase).
	if err := deleteRoute(ctx, srv.URL, route.ID); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if _, found, _ := getRoute(ctx, srv.URL, route.ID); found {
		t.Error("route still present after delete")
	}
}

func TestCaddyReady(t *testing.T) {
	srv := newFakeCaddyAdmin()
	defer srv.Close()
	if !caddyReady(context.Background(), srv.URL) {
		t.Error("caddyReady = false against a live fake admin API")
	}
	if caddyReady(context.Background(), "http://127.0.0.1:1") {
		t.Error("caddyReady = true against an unreachable address")
	}
}

func TestRoutesEquivalent(t *testing.T) {
	a := desiredRoute("nessie", "nessie.localhost", "nessie:19120")
	b := desiredRoute("nessie", "nessie.localhost", "nessie:19120")
	if !routesEquivalent(a, b) {
		t.Error("identical routes reported as different")
	}
	c := desiredRoute("nessie", "nessie.localhost", "nessie:9999")
	if routesEquivalent(a, c) {
		t.Error("routes with different upstreams reported as equivalent")
	}
	d := desiredRoute("nessie", "mangled.localhost", "nessie:19120")
	if routesEquivalent(a, d) {
		t.Error("routes with different hosts reported as equivalent (the C7 mangled-route drift case)")
	}
}

func TestEnsureRouteOnHTTPSServer(t *testing.T) {
	srv := newFakeCaddyAdmin()
	defer srv.Close()
	ctx := context.Background()
	route := desiredRoute("nessie", "nessie.localhost", "nessie:19120")
	if err := ensureRoute(ctx, srv.URL, httpsServerName, route); err != nil {
		t.Fatalf("create on %s: %v", httpsServerName, err)
	}
	live, found, err := getRoute(ctx, srv.URL, route.ID)
	if err != nil || !found {
		t.Fatalf("getRoute: found=%v err=%v", found, err)
	}
	if !routesEquivalent(live, route) {
		t.Errorf("live route %+v does not match desired %+v", live, route)
	}
}

func TestEnsureCertCreatesThenUpdates(t *testing.T) {
	srv := newFakeCaddyAdmin()
	defer srv.Close()
	ctx := context.Background()

	cert := caddyPEMCert{ID: certID("nessie"), Certificate: "cert-v1", Key: "key-v1"}
	if err := ensureCert(ctx, srv.URL, cert); err != nil {
		t.Fatalf("create: %v", err)
	}
	live, found, err := getCert(ctx, srv.URL, cert.ID)
	if err != nil || !found {
		t.Fatalf("getCert after create: found=%v err=%v", found, err)
	}
	if live.Certificate != "cert-v1" || live.Key != "key-v1" {
		t.Errorf("live cert = %+v, want cert-v1/key-v1", live)
	}

	updated := caddyPEMCert{ID: certID("nessie"), Certificate: "cert-v2", Key: "key-v2"}
	if err := ensureCert(ctx, srv.URL, updated); err != nil {
		t.Fatalf("update: %v", err)
	}
	live, found, err = getCert(ctx, srv.URL, cert.ID)
	if err != nil || !found {
		t.Fatalf("getCert after update: found=%v err=%v", found, err)
	}
	if live.Certificate != "cert-v2" {
		t.Errorf("cert not updated: got %q, want cert-v2", live.Certificate)
	}
}

func TestGetCertNotFound(t *testing.T) {
	srv := newFakeCaddyAdmin()
	defer srv.Close()
	_, found, err := getCert(context.Background(), srv.URL, certID("missing"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("found = true, want false")
	}
}

func TestDeleteCertIdempotent(t *testing.T) {
	srv := newFakeCaddyAdmin()
	defer srv.Close()
	ctx := context.Background()
	cert := caddyPEMCert{ID: certID("nessie"), Certificate: "c", Key: "k"}
	if err := ensureCert(ctx, srv.URL, cert); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := deleteCert(ctx, srv.URL, cert.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := deleteCert(ctx, srv.URL, cert.ID); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if _, found, _ := getCert(ctx, srv.URL, cert.ID); found {
		t.Error("cert still present after delete")
	}
}

func TestBootstrapConfigHasBothServersAndTLSApp(t *testing.T) {
	cfg := bootstrapConfig(80, 443, 2019)
	httpSrv, ok := cfg.Apps.HTTP.Servers[serverName]
	if !ok {
		t.Fatalf("bootstrap config missing %s", serverName)
	}
	if httpSrv.Listen[0] != ":80" {
		t.Errorf("%s listen = %v, want :80", serverName, httpSrv.Listen)
	}
	if len(httpSrv.TLSConnectionPolicies) != 0 {
		t.Errorf("%s (plain HTTP) should carry no tls_connection_policies, got %+v", serverName, httpSrv.TLSConnectionPolicies)
	}
	httpsSrv, ok := cfg.Apps.HTTP.Servers[httpsServerName]
	if !ok {
		t.Fatalf("bootstrap config missing %s", httpsServerName)
	}
	if httpsSrv.Listen[0] != ":443" {
		t.Errorf("%s listen = %v, want :443", httpsServerName, httpsSrv.Listen)
	}
	if len(httpsSrv.TLSConnectionPolicies) != 1 {
		t.Errorf("%s must carry exactly one tls_connection_policies entry (found live: required to actually terminate TLS), got %+v", httpsServerName, httpsSrv.TLSConnectionPolicies)
	}
	if httpsSrv.AutomaticHTTPS == nil || !httpsSrv.AutomaticHTTPS.Disable {
		t.Errorf("%s must disable automatic_https (no ACME/on-demand for .localhost dev hosts)", httpsServerName)
	}
	if cfg.Apps.TLS == nil || cfg.Apps.TLS.Certificates.LoadPEM == nil {
		t.Fatal("bootstrap config must set a non-nil (possibly empty) apps.tls.certificates.load_pem array, mirroring the routes-array requirement")
	}
}
