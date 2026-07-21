package ingress

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// fakeCaddyAdmin simulates just enough of Caddy's admin API for this
// package's client functions: PATCH /id/<id> only succeeds if the id was
// already POST-appended; GET/DELETE /id/<id> answer 400 for an unknown id
// (Caddy's real behavior — not 404), matching caddy.go's handling.
type fakeCaddyAdmin struct {
	mu     sync.Mutex
	routes map[string]caddyRoute
}

func newFakeCaddyAdmin() *httptest.Server {
	f := &fakeCaddyAdmin{routes: map[string]caddyRoute{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/config/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case http.MethodPost:
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

func TestEnsureRouteCreatesThenUpdates(t *testing.T) {
	srv := newFakeCaddyAdmin()
	defer srv.Close()
	ctx := context.Background()

	route := desiredRoute("nessie", "nessie.localhost", "nessie:19120")
	if err := ensureRoute(ctx, srv.URL, route); err != nil {
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
	if err := ensureRoute(ctx, srv.URL, updated); err != nil {
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
	if err := ensureRoute(ctx, srv.URL, route); err != nil {
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
