package jdbcsink

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// TestJDBCURLNilPosturePlaintext guards docs/planning/08 I2's back-compat
// requirement: no spec.tls declared appends nothing to connection.url.
func TestJDBCURLNilPosturePlaintext(t *testing.T) {
	for _, engine := range []string{"postgres", "mysql", "mariadb"} {
		url, err := jdbcURL(engine, "db.example.com", 5432, "analytics", nil)
		if err != nil {
			t.Fatalf("%s: jdbcURL: %v", engine, err)
		}
		if strings.Contains(url, "?") {
			t.Errorf("%s: url = %q, want no query string for a nil posture", engine, url)
		}
	}
}

// TestJDBCURLPostgresTLSModes covers all three modes plus the CA-file
// reference (docs/planning/08 I2) — pgjdbc's own sslmode/sslrootcert
// vocabulary, matching connection.TLSModeRequire/VerifyCA/VerifyFull
// exactly.
func TestJDBCURLPostgresTLSModes(t *testing.T) {
	cases := []struct {
		mode        string
		caSecretRef string
	}{
		{"require", ""},
		{"verify-ca", "rds-ca"},
		{"verify-full", "rds-ca"},
	}
	for _, tc := range cases {
		posture := &providerkit.DatabaseTLS{Mode: tc.mode, CASecretRefName: tc.caSecretRef}
		url, err := jdbcURL("postgres", "db.example.com", 5432, "analytics", posture)
		if err != nil {
			t.Fatalf("mode %q: jdbcURL: %v", tc.mode, err)
		}
		if !strings.Contains(url, "sslmode="+tc.mode) {
			t.Errorf("mode %q: url = %q, want sslmode=%s", tc.mode, url, tc.mode)
		}
		if tc.caSecretRef == "" {
			if strings.Contains(url, "sslrootcert") {
				t.Errorf("mode %q: unexpected sslrootcert with no caSecretRef: %q", tc.mode, url)
			}
			continue
		}
		wantPath := providerkit.CAFilePath(tc.caSecretRef)
		if !strings.Contains(url, "sslrootcert="+strings.ReplaceAll(wantPath, "/", "%2F")) {
			t.Errorf("mode %q: url = %q, want sslrootcert=%s (URL-encoded)", tc.mode, url, wantPath)
		}
	}
}

// TestJDBCURLMySQLTLSModes covers Connector/J's sslMode enum plus the
// trustCertificateKeyStoreType=PEM CA reference — unlike Debezium's own
// MySQL binlog client, Connector/J accepts a raw PEM CA directly, so full
// verification is supported here.
func TestJDBCURLMySQLTLSModes(t *testing.T) {
	cases := []struct {
		mode     string
		wantSSL  string
		caSecRef string
	}{
		{"require", "REQUIRED", ""},
		{"verify-ca", "VERIFY_CA", "rds-ca"},
		{"verify-full", "VERIFY_IDENTITY", "rds-ca"},
	}
	for _, engine := range []string{"mysql", "mariadb"} {
		for _, tc := range cases {
			posture := &providerkit.DatabaseTLS{Mode: tc.mode, CASecretRefName: tc.caSecRef}
			url, err := jdbcURL(engine, "db.example.com", 3306, "analytics", posture)
			if err != nil {
				t.Fatalf("%s mode %q: jdbcURL: %v", engine, tc.mode, err)
			}
			if !strings.Contains(url, "sslMode="+tc.wantSSL) {
				t.Errorf("%s mode %q: url = %q, want sslMode=%s", engine, tc.mode, url, tc.wantSSL)
			}
			if tc.caSecRef == "" {
				continue
			}
			if !strings.Contains(url, "trustCertificateKeyStoreType=PEM") {
				t.Errorf("%s mode %q: url = %q, want trustCertificateKeyStoreType=PEM", engine, tc.mode, url)
			}
		}
	}
}

// TestBuildDesiredConnectorAppliesTLSFromExternalConnection is an
// end-to-end unit test of the real wiring: a target Source's external
// Connection declares spec.tls.mode + caSecretRef, the jdbcsink worker
// declares that secretRef in its own spec.secretRefs (the discipline
// providerkit.ResolveDatabaseTLS enforces), and the resulting
// connection.url must carry the resolved TLS query params.
func TestBuildDesiredConnectorAppliesTLSFromExternalConnection(t *testing.T) {
	worker := workerEnvelope("jsink-worker", map[string]any{
		"image":            "datascape-jdbcsink-connect:local",
		"bootstrapServers": "broker:29092",
	})
	worker.Spec["secretRefs"] = []any{"conn-creds", "rds-ca"}
	es := simpleEnvelope("EventStream", "events", map[string]any{
		"providerRef": map[string]any{"name": "broker"},
	})
	tgt := simpleEnvelope("Source", "ext-rds", map[string]any{
		"engine":        "postgres",
		"external":      true,
		"connectionRef": map[string]any{"name": "rds-conn"},
		"postgres":      map[string]any{"database": "analytics"},
	})
	conn := simpleEnvelope("Connection", "rds-conn", map[string]any{
		"external":  true,
		"host":      "prod-db.rds.amazonaws.com",
		"port":      float64(5432),
		"secretRef": map[string]any{"name": "conn-creds"},
		"tls": map[string]any{
			"mode":        "verify-full",
			"caSecretRef": map[string]any{"name": "rds-ca"},
		},
	})
	bindingSpec := map[string]any{
		"mode":        "sink",
		"sourceRef":   map[string]any{"name": "events"},
		"targetRef":   map[string]any{"name": "ext-rds"},
		"providerRef": map[string]any{"name": "jsink-worker"},
		"options":     map[string]any{"format": "avro"},
	}
	b := simpleEnvelope("Binding", "events-to-rds", bindingSpec)
	req := reconciler.Request{
		Resource: b,
		Provider: worker,
		Resources: map[resource.Key]resource.Envelope{
			es.Key():     es,
			tgt.Key():    tgt,
			conn.Key():   conn,
			worker.Key(): worker,
		},
		Secrets: map[string]map[string]string{
			"conn-creds": {"username": "conn-user", "password": "conn-pass"},
			"rds-ca":     {"ca": "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"},
		},
		SchemaRegistryURL: "http://broker:8081",
	}
	d, err := buildDesiredConnector(req)
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	url := d.config["connection.url"]
	if !strings.Contains(url, "sslmode=verify-full") {
		t.Errorf("connection.url = %q, want sslmode=verify-full", url)
	}
	wantPath := providerkit.CAFilePath("rds-ca")
	if !strings.Contains(url, strings.ReplaceAll(wantPath, "/", "%2F")) {
		t.Errorf("connection.url = %q, want the CA file path %q", url, wantPath)
	}
	if d.tlsPosture == nil || d.tlsPosture.Mode != "verify-full" {
		t.Errorf("tlsPosture = %+v, want mode verify-full", d.tlsPosture)
	}
}

// TestBuildDesiredConnectorTLSCASecretRefMustBeDeclared guards the
// secretRefs discipline docs/planning/08 I2 requires: a caSecretRef the
// worker Provider itself never declared in spec.secretRefs must fail
// clearly, not silently resolve.
func TestBuildDesiredConnectorTLSCASecretRefMustBeDeclared(t *testing.T) {
	worker := workerEnvelope("jsink-worker", map[string]any{
		"image":            "datascape-jdbcsink-connect:local",
		"bootstrapServers": "broker:29092",
	})
	worker.Spec["secretRefs"] = []any{"conn-creds"} // rds-ca deliberately omitted
	es := simpleEnvelope("EventStream", "events", map[string]any{
		"providerRef": map[string]any{"name": "broker"},
	})
	tgt := simpleEnvelope("Source", "ext-rds", map[string]any{
		"engine":        "postgres",
		"external":      true,
		"connectionRef": map[string]any{"name": "rds-conn"},
		"postgres":      map[string]any{"database": "analytics"},
	})
	conn := simpleEnvelope("Connection", "rds-conn", map[string]any{
		"external":  true,
		"host":      "prod-db.rds.amazonaws.com",
		"port":      float64(5432),
		"secretRef": map[string]any{"name": "conn-creds"},
		"tls": map[string]any{
			"mode":        "verify-full",
			"caSecretRef": map[string]any{"name": "rds-ca"},
		},
	})
	bindingSpec := map[string]any{
		"mode":        "sink",
		"sourceRef":   map[string]any{"name": "events"},
		"targetRef":   map[string]any{"name": "ext-rds"},
		"providerRef": map[string]any{"name": "jsink-worker"},
		"options":     map[string]any{"format": "avro"},
	}
	b := simpleEnvelope("Binding", "events-to-rds", bindingSpec)
	req := reconciler.Request{
		Resource: b,
		Provider: worker,
		Resources: map[resource.Key]resource.Envelope{
			es.Key():     es,
			tgt.Key():    tgt,
			conn.Key():   conn,
			worker.Key(): worker,
		},
		Secrets: map[string]map[string]string{
			"conn-creds": {"username": "conn-user", "password": "conn-pass"},
		},
		SchemaRegistryURL: "http://broker:8081",
	}
	if _, err := buildDesiredConnector(req); err == nil {
		t.Fatal("expected error: spec.tls.caSecretRef not listed in the worker Provider's own spec.secretRefs")
	}
}
