package openziti

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeZitiController is a minimal in-memory stand-in for OpenZiti's Edge
// Management REST API — just enough surface (authenticate, identities,
// edge-routers, services, service-policies, terminators) to prove client.go's
// idempotency contract (docs/planning/02 §4.1's Ensure*-idempotent rule)
// without live infra. The real controller's exact wire shape was verified
// live against the pinned image at authorship time (see openziti.go's doc
// comment); this fake mirrors that verified shape for the fields client.go
// actually reads.
type fakeZitiController struct {
	mu          sync.Mutex
	identities  map[string]map[string]any // id -> record
	routers     map[string]map[string]any
	services    map[string]map[string]any
	policies    map[string]map[string]any
	terminators map[string]map[string]any
	nextID      int
	authCalls   int
}

func newFakeZitiController() *fakeZitiController {
	return &fakeZitiController{
		identities:  map[string]map[string]any{},
		routers:     map[string]map[string]any{},
		services:    map[string]map[string]any{},
		policies:    map[string]map[string]any{},
		terminators: map[string]map[string]any{},
	}
}

func (f *fakeZitiController) newID() string {
	f.nextID++
	return "id" + itoa(f.nextID)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func writeEnvelope(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "meta": map[string]any{}})
}

func (f *fakeZitiController) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/edge/client/v1/version", func(w http.ResponseWriter, r *http.Request) {
		writeEnvelope(w, http.StatusOK, map[string]any{"version": "vFake"})
	})

	mux.HandleFunc("/edge/management/v1/authenticate", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.authCalls++
		f.mu.Unlock()
		w.Header().Set("zt-session", "fake-session-token")
		writeEnvelope(w, http.StatusOK, map[string]any{"token": "fake-session-token"})
	})

	mux.HandleFunc("/edge/management/v1/identities", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			name := filterName(r.URL.RawQuery)
			var out []map[string]any
			for id, rec := range f.identities {
				if name == "" || rec["name"] == name {
					out = append(out, map[string]any{"id": id, "name": rec["name"]})
				}
			}
			writeEnvelope(w, http.StatusOK, out)
		case http.MethodPost:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			id := f.newID()
			body["jwt"] = "ott-jwt-" + id
			f.identities[id] = body
			writeEnvelope(w, http.StatusCreated, map[string]any{"id": id})
		}
	})

	mux.HandleFunc("/edge/management/v1/identities/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := strings.TrimPrefix(r.URL.Path, "/edge/management/v1/identities/")
		rec, ok := f.identities[id]
		if !ok {
			http.Error(w, `{"error":{"code":"NOT_FOUND","message":"not found"}}`, http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodGet:
			writeEnvelope(w, http.StatusOK, map[string]any{
				"enrollment":     map[string]any{"ott": map[string]any{"jwt": rec["jwt"]}},
				"authenticators": map[string]any{"cert": map[string]any{"fingerprint": "fp-" + id}},
			})
		case http.MethodDelete:
			delete(f.identities, id)
			w.WriteHeader(http.StatusOK)
		}
	})

	mux.HandleFunc("/edge/management/v1/edge-routers", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			name := filterName(r.URL.RawQuery)
			var out []map[string]any
			for id, rec := range f.routers {
				if name == "" || rec["name"] == name {
					out = append(out, map[string]any{"id": id, "name": rec["name"]})
				}
			}
			writeEnvelope(w, http.StatusOK, out)
		case http.MethodPost:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			id := f.newID()
			body["isVerified"] = false
			body["enrollmentJwt"] = "router-jwt-" + id
			f.routers[id] = body
			writeEnvelope(w, http.StatusCreated, map[string]any{"id": id})
		}
	})

	mux.HandleFunc("/edge/management/v1/edge-routers/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := strings.TrimPrefix(r.URL.Path, "/edge/management/v1/edge-routers/")
		rec, ok := f.routers[id]
		if !ok {
			http.Error(w, `{"error":{"code":"NOT_FOUND","message":"not found"}}`, http.StatusNotFound)
			return
		}
		writeEnvelope(w, http.StatusOK, map[string]any{
			"isVerified":    rec["isVerified"],
			"enrollmentJwt": rec["enrollmentJwt"],
		})
	})

	mux.HandleFunc("/edge/management/v1/services", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			name := filterName(r.URL.RawQuery)
			var out []map[string]any
			for id, rec := range f.services {
				if name == "" || rec["name"] == name {
					out = append(out, map[string]any{"id": id, "name": rec["name"]})
				}
			}
			writeEnvelope(w, http.StatusOK, out)
		case http.MethodPost:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			id := f.newID()
			f.services[id] = body
			writeEnvelope(w, http.StatusCreated, map[string]any{"id": id})
		}
	})

	mux.HandleFunc("/edge/management/v1/services/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := strings.TrimPrefix(r.URL.Path, "/edge/management/v1/services/")
		rec, ok := f.services[id]
		if !ok {
			http.Error(w, `{"error":{"code":"NOT_FOUND","message":"not found"}}`, http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodGet:
			enc, _ := rec["encryptionRequired"].(bool)
			writeEnvelope(w, http.StatusOK, map[string]any{"id": id, "name": rec["name"], "encryptionRequired": enc})
		case http.MethodPatch:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			for k, v := range body {
				rec[k] = v
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			delete(f.services, id)
			w.WriteHeader(http.StatusOK)
		}
	})

	mux.HandleFunc("/edge/management/v1/service-policies", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			name := filterName(r.URL.RawQuery)
			var out []map[string]any
			for id, rec := range f.policies {
				if name == "" || rec["name"] == name {
					out = append(out, map[string]any{"id": id, "name": rec["name"], "identityRoles": rec["identityRoles"], "serviceRoles": rec["serviceRoles"]})
				}
			}
			writeEnvelope(w, http.StatusOK, out)
		case http.MethodPost:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			id := f.newID()
			f.policies[id] = body
			writeEnvelope(w, http.StatusCreated, map[string]any{"id": id})
		}
	})

	mux.HandleFunc("/edge/management/v1/service-policies/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := strings.TrimPrefix(r.URL.Path, "/edge/management/v1/service-policies/")
		if _, ok := f.policies[id]; !ok {
			http.Error(w, `{"error":{"code":"NOT_FOUND","message":"not found"}}`, http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodPut:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.policies[id] = body
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			delete(f.policies, id)
			w.WriteHeader(http.StatusOK)
		}
	})

	mux.HandleFunc("/edge/management/v1/terminators", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			var out []map[string]any
			for id, rec := range f.terminators {
				out = append(out, map[string]any{"id": id, "address": rec["address"]})
			}
			writeEnvelope(w, http.StatusOK, out)
		case http.MethodPost:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			id := f.newID()
			f.terminators[id] = body
			writeEnvelope(w, http.StatusCreated, map[string]any{"id": id})
		}
	})

	mux.HandleFunc("/edge/management/v1/terminators/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		id := strings.TrimPrefix(r.URL.Path, "/edge/management/v1/terminators/")
		if _, ok := f.terminators[id]; !ok {
			http.Error(w, `{"error":{"code":"NOT_FOUND","message":"not found"}}`, http.StatusNotFound)
			return
		}
		if r.Method == http.MethodDelete {
			delete(f.terminators, id)
		}
		w.WriteHeader(http.StatusOK)
	})

	return mux
}

// filterName extracts the name="X" filter value client.go's findByName
// sends — not a real Ziti filter-language parser, just enough for tests.
func filterName(rawQuery string) string {
	const marker = "name=%22"
	i := strings.Index(rawQuery, marker)
	if i < 0 {
		return ""
	}
	rest := rawQuery[i+len(marker):]
	j := strings.Index(rest, "%22")
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func newTestClient(t *testing.T) (*edgeClient, *fakeZitiController) {
	t.Helper()
	f := newFakeZitiController()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	// nil RootCAs: srv is plain HTTP (httptest.NewServer, no TLS), so the
	// TLSClientConfig is never consulted — CA pinning only matters for the
	// https:// dial dialController/waitControllerServing perform against a
	// real controller (see client.go's pinnedCAPool).
	c := newEdgeClient(srv.URL, nil)
	if err := c.Authenticate(context.Background(), "admin", "pw"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	return c, f
}

func TestUpsertIdentityIsIdempotent(t *testing.T) {
	t.Parallel()
	c, f := newTestClient(t)
	ctx := context.Background()

	id1, jwt1, err := c.upsertIdentity(ctx, "consumer-a", []string{"tag"})
	if err != nil {
		t.Fatalf("first upsertIdentity: %v", err)
	}
	if jwt1 == "" {
		t.Fatal("first upsertIdentity should return a fresh enrollment JWT")
	}
	if len(f.identities) != 1 {
		t.Fatalf("identities = %d, want 1", len(f.identities))
	}

	id2, jwt2, err := c.upsertIdentity(ctx, "consumer-a", []string{"tag"})
	if err != nil {
		t.Fatalf("second upsertIdentity: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("second upsertIdentity minted a new id: %s != %s", id1, id2)
	}
	if jwt2 != "" {
		t.Fatalf("second upsertIdentity should not re-issue an enrollment JWT, got %q", jwt2)
	}
	if len(f.identities) != 1 {
		t.Fatalf("identities = %d after second call, want still 1 (no duplicate write)", len(f.identities))
	}
}

func TestUpsertServiceIsIdempotent(t *testing.T) {
	t.Parallel()
	c, f := newTestClient(t)
	ctx := context.Background()

	id1, err := c.upsertService(ctx, "orders-service")
	if err != nil {
		t.Fatalf("first upsertService: %v", err)
	}
	id2, err := c.upsertService(ctx, "orders-service")
	if err != nil {
		t.Fatalf("second upsertService: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("upsertService not idempotent: %s != %s", id1, id2)
	}
	if len(f.services) != 1 {
		t.Fatalf("services = %d, want 1", len(f.services))
	}
}

// TestUpsertServiceConvergesEncryptionRequired is the drift-heal assertion
// for the encryptionRequired field: a service that already exists carrying
// the WRONG value (e.g. created before this adapter settled on false, or
// edited out-of-band in the Ziti console) must be PATCHed back to the
// desired value on the next upsert — not left as create-only drift. This is
// the exact defect the H6 continuation flagged: create-only idempotency
// would let a stale encryptionRequired: true survive forever, silently
// breaking every dial through the router-native terminator.
func TestUpsertServiceConvergesEncryptionRequired(t *testing.T) {
	t.Parallel()
	c, f := newTestClient(t)
	ctx := context.Background()

	// Simulate a service created out-of-band with encryptionRequired: true.
	f.mu.Lock()
	f.services["drifted"] = map[string]any{"name": "orders-service", "encryptionRequired": true}
	f.mu.Unlock()

	id, err := c.upsertService(ctx, "orders-service")
	if err != nil {
		t.Fatalf("upsertService: %v", err)
	}
	if id != "drifted" {
		t.Fatalf("upsertService minted a new service instead of reusing the existing one: %s", id)
	}
	f.mu.Lock()
	got := f.services["drifted"]["encryptionRequired"]
	f.mu.Unlock()
	if got != false {
		t.Fatalf("encryptionRequired = %v after convergence, want false (drift not healed)", got)
	}
}

func TestUpsertDialPolicyCreatesThenUpdatesInPlace(t *testing.T) {
	t.Parallel()
	c, f := newTestClient(t)
	ctx := context.Background()

	if err := c.upsertDialPolicy(ctx, "dial-a-svc", "svc1", []string{"identA"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(f.policies) != 1 {
		t.Fatalf("policies = %d, want 1", len(f.policies))
	}

	if err := c.upsertDialPolicy(ctx, "dial-a-svc", "svc1", []string{"identA", "identB"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(f.policies) != 1 {
		t.Fatalf("policies = %d after update, want still 1 (updated in place, not duplicated)", len(f.policies))
	}
	var roles []string
	for _, rec := range f.policies {
		if r, ok := rec["identityRoles"].([]any); ok {
			for _, x := range r {
				roles = append(roles, x.(string))
			}
		}
	}
	if len(roles) != 2 {
		t.Fatalf("identityRoles = %v, want 2 entries after update", roles)
	}
}

func TestUpsertTransportTerminatorSkipsWhenAddressUnchanged(t *testing.T) {
	t.Parallel()
	c, f := newTestClient(t)
	ctx := context.Background()

	if err := c.upsertTransportTerminator(ctx, "svc1", "router1", "vpc-postgres:5432"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if len(f.terminators) != 1 {
		t.Fatalf("terminators = %d, want 1", len(f.terminators))
	}
	if err := c.upsertTransportTerminator(ctx, "svc1", "router1", "vpc-postgres:5432"); err != nil {
		t.Fatalf("second (unchanged): %v", err)
	}
	if len(f.terminators) != 1 {
		t.Fatalf("terminators = %d after unchanged re-apply, want still 1 (no delete+recreate)", len(f.terminators))
	}
}

func TestUpsertTransportTerminatorRecreatesOnAddressChange(t *testing.T) {
	t.Parallel()
	c, f := newTestClient(t)
	ctx := context.Background()

	if err := c.upsertTransportTerminator(ctx, "svc1", "router1", "vpc-postgres:5432"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := c.upsertTransportTerminator(ctx, "svc1", "router1", "vpc-postgres:5433"); err != nil {
		t.Fatalf("second (changed address): %v", err)
	}
	if len(f.terminators) != 1 {
		t.Fatalf("terminators = %d, want 1 (old removed, new created)", len(f.terminators))
	}
	for _, rec := range f.terminators {
		if rec["address"] != "tcp:vpc-postgres:5433" {
			t.Fatalf("terminator address = %v, want tcp:vpc-postgres:5433", rec["address"])
		}
	}
}

func TestDeleteIdentityOnAlreadyAbsentIsNotAnError(t *testing.T) {
	t.Parallel()
	c, _ := newTestClient(t)
	if err := c.deleteIdentity(context.Background(), "does-not-exist"); err != nil {
		t.Fatalf("deleteIdentity on absent id should be a no-op, got: %v", err)
	}
}

func TestFindByNameReturnsFalseWhenAbsent(t *testing.T) {
	t.Parallel()
	c, _ := newTestClient(t)
	_, ok, err := c.findByName(context.Background(), "identities", "nope")
	if err != nil {
		t.Fatalf("findByName: %v", err)
	}
	if ok {
		t.Fatal("findByName reported found for an absent name")
	}
}
