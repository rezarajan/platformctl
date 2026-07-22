package providerkit

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

func strPtr(s string) *string { return &s }

// TestResolveEndpointProviderRefOnly covers the lowest-preference branch: a
// managed (non-external) Source with no Connection resolves to its own
// Provider container name at the engine's default port.
func TestResolveEndpointProviderRefOnly(t *testing.T) {
	src := source.Source{ProviderRef: strPtr("pg")}
	srcEnv := resource.Envelope{Metadata: resource.Metadata{Name: "src1", Namespace: "default"}}
	req := reconciler.Request{Resources: map[resource.Key]resource.Envelope{}}

	ep, ok := ResolveEndpoint(req, src, srcEnv, 5432, nil)
	if !ok {
		t.Fatal("want ok=true")
	}
	if ep.Host != "pg" || ep.Port != 5432 {
		t.Errorf("ep = %+v, want Host=pg Port=5432", ep)
	}
	if ep.PreflightHost != "" || ep.PreflightConnectionName != "" {
		t.Errorf("ep = %+v, want no preflight fields set (no Connection)", ep)
	}
}

// TestResolveEndpointManagedConnection covers an external Source pointing at
// an in-cluster (managed, non-external) Connection: the address is the
// Connection's own name on the shared network, and preflight resolves
// through the Connection's forwarder name/port, not a bare host:port dial.
func TestResolveEndpointManagedConnection(t *testing.T) {
	src := source.Source{External: true, ConnectionRef: strPtr("conn1")}
	srcEnv := resource.Envelope{
		Metadata: resource.Metadata{Name: "src1", Namespace: "default"},
		Spec:     map[string]any{"connectionRef": map[string]any{"name": "conn1"}},
	}
	connEnv := resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Connection"},
		Metadata:         resource.Metadata{Name: "conn1", Namespace: "default"},
		Spec: map[string]any{
			"providerRef": map[string]any{"name": "proxy"},
			"target":      "real-db:5432",
			"port":        5432,
			"secretRef":   map[string]any{"name": "conn-creds"},
		},
	}
	req := reconciler.Request{Resources: map[resource.Key]resource.Envelope{
		connEnv.Key(): connEnv,
	}}

	ep, ok := ResolveEndpoint(req, src, srcEnv, 9999, nil)
	if !ok {
		t.Fatal("want ok=true")
	}
	if ep.Host != "conn1" || ep.Port != 5432 {
		t.Errorf("ep.Host/Port = %q/%d, want conn1/5432", ep.Host, ep.Port)
	}
	if ep.PreflightConnectionName != "conn1" || ep.PreflightPort != 5432 {
		t.Errorf("ep.PreflightConnectionName/Port = %q/%d, want conn1/5432", ep.PreflightConnectionName, ep.PreflightPort)
	}
	if ep.PreflightHost != "" {
		t.Errorf("ep.PreflightHost = %q, want empty for a managed Connection", ep.PreflightHost)
	}
	if ep.ConnectionSecretRef != "conn-creds" {
		t.Errorf("ep.ConnectionSecretRef = %q, want conn-creds", ep.ConnectionSecretRef)
	}
}

// TestResolveEndpointExternalConnection covers an external Source pointing
// at an external Connection: the address is the Connection's declared host,
// and preflight dials that host:port directly (no runtime involved).
func TestResolveEndpointExternalConnection(t *testing.T) {
	src := source.Source{External: true, ConnectionRef: strPtr("conn1")}
	srcEnv := resource.Envelope{
		Metadata: resource.Metadata{Name: "src1", Namespace: "default"},
		Spec:     map[string]any{"connectionRef": map[string]any{"name": "conn1"}},
	}
	connEnv := resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{Kind: "Connection"},
		Metadata:         resource.Metadata{Name: "conn1", Namespace: "default"},
		Spec: map[string]any{
			"external": true,
			"host":     "db.example.com",
			"port":     5432,
		},
	}
	req := reconciler.Request{Resources: map[resource.Key]resource.Envelope{
		connEnv.Key(): connEnv,
	}}

	ep, ok := ResolveEndpoint(req, src, srcEnv, 9999, nil)
	if !ok {
		t.Fatal("want ok=true")
	}
	if ep.Host != "db.example.com" || ep.Port != 5432 {
		t.Errorf("ep.Host/Port = %q/%d, want db.example.com/5432", ep.Host, ep.Port)
	}
	if ep.PreflightHost != "db.example.com" || ep.PreflightPort != 5432 {
		t.Errorf("ep.PreflightHost/Port = %q/%d, want db.example.com/5432", ep.PreflightHost, ep.PreflightPort)
	}
	if ep.PreflightConnectionName != "" {
		t.Errorf("ep.PreflightConnectionName = %q, want empty for an external Connection", ep.PreflightConnectionName)
	}
	if ep.ConnectionSecretRef != "" {
		t.Errorf("ep.ConnectionSecretRef = %q, want empty (Connection declared none)", ep.ConnectionSecretRef)
	}
}

// TestResolveEndpointOptionsOverride covers the highest-preference branch:
// options.databaseHostname/databasePort override whatever the Connection or
// ProviderRef resolved to.
func TestResolveEndpointOptionsOverride(t *testing.T) {
	src := source.Source{ProviderRef: strPtr("pg")}
	srcEnv := resource.Envelope{Metadata: resource.Metadata{Name: "src1", Namespace: "default"}}
	req := reconciler.Request{Resources: map[resource.Key]resource.Envelope{}}

	// int form (a hand-built Go value)
	ep, ok := ResolveEndpoint(req, src, srcEnv, 5432, map[string]any{
		"databaseHostname": "override-host",
		"databasePort":     15432,
	})
	if !ok || ep.Host != "override-host" || ep.Port != 15432 {
		t.Errorf("ep = %+v (ok=%v), want override-host/15432", ep, ok)
	}

	// float64 form (JSON-decoded options)
	ep, ok = ResolveEndpoint(req, src, srcEnv, 5432, map[string]any{
		"databaseHostname": "override-host",
		"databasePort":     float64(15432),
	})
	if !ok || ep.Host != "override-host" || ep.Port != 15432 {
		t.Errorf("ep = %+v (ok=%v), want override-host/15432 (float64 port)", ep, ok)
	}
}

// TestResolveEndpointNoAddress covers the "nothing resolved" failure: no
// ProviderRef, no Connection, no options override — ok is false so the
// caller can construct its own error message.
func TestResolveEndpointNoAddress(t *testing.T) {
	src := source.Source{}
	srcEnv := resource.Envelope{Metadata: resource.Metadata{Name: "src1", Namespace: "default"}}
	req := reconciler.Request{Resources: map[resource.Key]resource.Envelope{}}

	ep, ok := ResolveEndpoint(req, src, srcEnv, 5432, nil)
	if ok {
		t.Fatalf("want ok=false, got %+v", ep)
	}
	if ep != (EndpointResolution{}) {
		t.Errorf("ep = %+v, want the zero value on failure", ep)
	}
}

// TestResolveEndpointCredentialsConnectionSecretPreferred covers the
// fallback chain's first tier: the Connection's own secretRef wins over the
// Provider-level key when both resolved into req.Secrets.
func TestResolveEndpointCredentialsConnectionSecretPreferred(t *testing.T) {
	req := reconciler.Request{Secrets: map[string]map[string]string{
		"conn-creds":     {"username": "conn-user", "password": "conn-pass"},
		"provider-creds": {"username": "prov-user", "password": "prov-pass"},
	}}
	cfg := provider.Provider{Configuration: map[string]any{"replicationSecretRef": "provider-creds"}}

	creds, ok := ResolveEndpointCredentials(req, cfg, "conn-creds", "replicationSecretRef")
	if !ok {
		t.Fatal("want ok=true")
	}
	if creds["username"] != "conn-user" {
		t.Errorf("creds = %v, want the Connection's own secret (conn-user)", creds)
	}
}

// TestResolveEndpointCredentialsProviderFallback covers the second tier: no
// Connection secretRef (or one not resolved into req.Secrets) falls back to
// the Provider-level configuration key.
func TestResolveEndpointCredentialsProviderFallback(t *testing.T) {
	req := reconciler.Request{Secrets: map[string]map[string]string{
		"provider-creds": {"username": "prov-user", "password": "prov-pass"},
	}}
	cfg := provider.Provider{Configuration: map[string]any{"credentialsSecretRef": "provider-creds"}}

	creds, ok := ResolveEndpointCredentials(req, cfg, "", "credentialsSecretRef")
	if !ok {
		t.Fatal("want ok=true")
	}
	if creds["username"] != "prov-user" {
		t.Errorf("creds = %v, want the Provider-level secret (prov-user)", creds)
	}
}

// TestResolveEndpointCredentialsNoneResolved covers the terminal failure:
// neither the Connection's secretRef nor the Provider-level key resolved
// into req.Secrets.
func TestResolveEndpointCredentialsNoneResolved(t *testing.T) {
	req := reconciler.Request{Secrets: map[string]map[string]string{}}
	cfg := provider.Provider{Configuration: map[string]any{}}

	if _, ok := ResolveEndpointCredentials(req, cfg, "", "replicationSecretRef"); ok {
		t.Fatal("want ok=false")
	}
}
