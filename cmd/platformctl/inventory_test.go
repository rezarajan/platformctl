package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// TestInventorySurfacesEndpoints: inventory reads recorded endpoints and
// pairs each with the SecretReference holding its credentials.
func TestInventorySurfacesEndpoints(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	store := localfile.New(stateFile)

	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{}}
	st.Resources[resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "datascape-rp-test"}] = state.ResourceState{
		Lifecycle: "Managed",
		Provider: map[string]any{
			endpoint.Key: endpoint.List{
				{Name: "kafka", Scheme: "kafka", Host: "127.0.0.1:19192", Internal: "datascape-rp-test:29092"},
			}.ToState(),
		},
	}
	if err := store.Save(context.Background(), st); err != nil {
		t.Fatal(err)
	}

	out, err, code := run(t, "inventory", "testdata/redpanda-scenario", "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("inventory failed (code %d): %v\n%s", code, err, out)
	}
	for _, want := range []string{"kafka", "127.0.0.1:19192", "datascape-rp-test:29092", "default/Provider/datascape-rp-test"} {
		if !strings.Contains(out, want) {
			t.Errorf("inventory output missing %q:\n%s", want, out)
		}
	}

	// json output is machine-readable.
	out, _, _ = run(t, "inventory", "testdata/redpanda-scenario", "--state-file", stateFile, "-o", "json")
	if !strings.Contains(out, `"endpoint": "kafka"`) {
		t.Errorf("json inventory missing endpoint field:\n%s", out)
	}
}

// TestInventorySurfacesSelfSignedCALocation covers docs/planning/08 C8's
// accept criterion "inventory names the CA location": a self-signed
// ingress Provider's published providerState.tls.caCert (public part
// only, per docs/planning/03 §8.2.2) surfaces both as a human-readable
// pointer and, structured, as the actual PEM for tools to consume.
func TestInventorySurfacesSelfSignedCALocation(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	store := localfile.New(stateFile)

	const caPEM = "-----BEGIN CERTIFICATE-----\nfake-ca-cert\n-----END CERTIFICATE-----\n"
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{}}
	st.Resources[resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "ing-test-edge"}] = state.ResourceState{
		Lifecycle: "Managed",
		Provider: map[string]any{
			"tls": map[string]any{"caCert": caPEM},
		},
	}
	if err := store.Save(context.Background(), st); err != nil {
		t.Fatal(err)
	}

	// Human-readable: names where to find it, never prints the raw PEM
	// inline in the table (the pointer text, not the material itself).
	// This seeded Provider has no endpoint facts (only tls), so the
	// "no service endpoints" empty-state branch fires — the CA note must
	// still print there, not only on the non-empty table path.
	out, err, code := run(t, "inventory", "testdata/ingress-scenario", "--state-file", stateFile, "--feature-gates", "IngressProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("inventory failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no service endpoints") {
		t.Errorf("expected the empty-endpoint-state message, got:\n%s", out)
	}
	if !strings.Contains(out, "self-signed CA for default/Provider/ing-test-edge") {
		t.Errorf("inventory output does not name the self-signed CA location:\n%s", out)
	}
	if strings.Contains(out, "fake-ca-cert") {
		t.Errorf("human-readable inventory output must never inline the raw CA PEM, only where to find it:\n%s", out)
	}

	// Structured output: the actual PEM is present so a tool can extract
	// and trust it programmatically.
	out, err, code = run(t, "inventory", "testdata/ingress-scenario", "--state-file", stateFile, "-o", "json", "--feature-gates", "IngressProvider=true")
	if err != nil || code != 0 {
		t.Fatalf("inventory -o json failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "certificateAuthorities") || !strings.Contains(out, "fake-ca-cert") {
		t.Errorf("json inventory missing certificateAuthorities/CA PEM:\n%s", out)
	}
	if !strings.Contains(out, "default/Provider/ing-test-edge") {
		t.Errorf("json inventory does not name the owning Provider:\n%s", out)
	}
}

// TestInventoryEmptyState: with nothing applied, inventory says so rather
// than printing an empty table.
func TestInventoryEmptyState(t *testing.T) {
	t.Parallel()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	out, err, code := run(t, "inventory", "testdata/redpanda-scenario", "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("inventory on empty state failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "no service endpoints") {
		t.Errorf("expected empty-state message, got:\n%s", out)
	}
}
