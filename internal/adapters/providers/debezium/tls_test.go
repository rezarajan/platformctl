package debezium

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

func simpleEnv(kind, name string, spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = kind
	e.Metadata.Name = name
	e.Spec = spec
	return e
}

// TestApplyTLSConfigNilPostureLeavesConfigUntouched guards docs/planning/08
// I2's back-compat requirement: no spec.tls declared means the connector
// config is byte-for-byte the pre-I2 shape.
func TestApplyTLSConfigNilPostureLeavesConfigUntouched(t *testing.T) {
	t.Parallel()
	config := map[string]string{"database.hostname": "db.example.com"}
	applyTLSConfig(config, "postgres", nil)
	if len(config) != 1 {
		t.Fatalf("config = %+v, want untouched", config)
	}
}

// TestApplyTLSConfigPostgres covers all three modes plus the CA-file
// reference, which must resolve to providerkit.CAFilePath's deterministic
// path (the same path CATrustFileMounts placed the CA at during this
// Provider's own worker-level reconcile).
func TestApplyTLSConfigPostgres(t *testing.T) {
	t.Parallel()
	cases := []struct {
		mode        string
		caSecretRef string
		wantSSLMode string
	}{
		{"require", "", "require"},
		{"verify-ca", "rds-ca", "verify-ca"},
		{"verify-full", "rds-ca", "verify-full"},
	}
	for _, tc := range cases {
		config := map[string]string{}
		posture := &providerkit.DatabaseTLS{Mode: tc.mode, CASecretRefName: tc.caSecretRef}
		applyTLSConfig(config, "postgres", posture)
		if config["database.sslmode"] != tc.wantSSLMode {
			t.Errorf("mode %q: database.sslmode = %q, want %q", tc.mode, config["database.sslmode"], tc.wantSSLMode)
		}
		if tc.caSecretRef == "" {
			if _, ok := config["database.sslrootcert"]; ok {
				t.Errorf("mode %q: database.sslrootcert set with no caSecretRef", tc.mode)
			}
			continue
		}
		want := providerkit.CAFilePath(tc.caSecretRef)
		if config["database.sslrootcert"] != want {
			t.Errorf("mode %q: database.sslrootcert = %q, want %q", tc.mode, config["database.sslrootcert"], want)
		}
	}
}

// TestApplyTLSConfigMySQL covers the database.ssl.mode mapping for
// mysql/mariadb — Debezium's own enum, distinct from the require/verify-ca/
// verify-full vocabulary spec.tls.mode carries.
func TestApplyTLSConfigMySQL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		mode string
		want string
	}{
		{"require", "required"},
		{"verify-ca", "verify_ca"},
		{"verify-full", "verify_identity"},
	}
	for _, engine := range []string{"mysql", "mariadb"} {
		for _, tc := range cases {
			config := map[string]string{}
			applyTLSConfig(config, engine, &providerkit.DatabaseTLS{Mode: tc.mode})
			if config["database.ssl.mode"] != tc.want {
				t.Errorf("%s mode %q: database.ssl.mode = %q, want %q", engine, tc.mode, config["database.ssl.mode"], tc.want)
			}
			// mysqlSSLMode intentionally never sets a truststore property —
			// see applyTLSConfig's doc comment for the documented scope
			// boundary (no Java truststore generation).
			if _, ok := config["database.ssl.truststore"]; ok {
				t.Errorf("%s mode %q: unexpected database.ssl.truststore set", engine, tc.mode)
			}
		}
	}
}

// TestBuildDesiredConnectorAppliesTLSFromExternalConnection is an
// end-to-end unit test of the real wiring: a Source's external Connection
// declares spec.tls.mode + caSecretRef, this Provider declares that
// secretRef in its own spec.secretRefs (the discipline
// providerkit.ResolveDatabaseTLS enforces), and the resulting connector
// config must carry the resolved database.sslmode/database.sslrootcert.
func TestBuildDesiredConnectorAppliesTLSFromExternalConnection(t *testing.T) {
	t.Parallel()
	worker := workerEnvelope("dbz-worker", map[string]any{
		"bootstrapServers": "broker:29092",
	})
	worker.Spec["secretRefs"] = []any{"conn-creds", "rds-ca"}
	src := simpleEnv("Source", "ext-rds", map[string]any{
		"engine":        "postgres",
		"external":      true,
		"connectionRef": map[string]any{"name": "rds-conn"},
		"postgres":      map[string]any{"database": "orders"},
	})
	conn := simpleEnv("Connection", "rds-conn", map[string]any{
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
		"mode":        "cdc",
		"sourceRef":   map[string]any{"name": "ext-rds"},
		"targetRef":   map[string]any{"name": "orders-events"},
		"providerRef": map[string]any{"name": "dbz-worker"},
	}
	b := simpleEnv("Binding", "orders-cdc", bindingSpec)
	req := reconciler.Request{
		Resource: b,
		Provider: worker,
		Resources: map[resource.Key]resource.Envelope{
			src.Key():    src,
			conn.Key():   conn,
			worker.Key(): worker,
		},
		Secrets: map[string]map[string]string{
			"conn-creds": {"username": "conn-user", "password": "conn-pass"},
			"rds-ca":     {"ca": "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"},
		},
	}
	d, err := buildDesiredConnector(req)
	if err != nil {
		t.Fatalf("buildDesiredConnector: %v", err)
	}
	if d.config["database.sslmode"] != "verify-full" {
		t.Errorf("database.sslmode = %q, want verify-full", d.config["database.sslmode"])
	}
	wantPath := providerkit.CAFilePath("rds-ca")
	if d.config["database.sslrootcert"] != wantPath {
		t.Errorf("database.sslrootcert = %q, want %q", d.config["database.sslrootcert"], wantPath)
	}
	if d.tlsPosture == nil || d.tlsPosture.Mode != "verify-full" {
		t.Errorf("tlsPosture = %+v, want mode verify-full", d.tlsPosture)
	}
}

// TestBuildDesiredConnectorTLSCASecretRefMustBeDeclared guards the
// secretRefs discipline: a caSecretRef this Provider never declared in its
// own spec.secretRefs must fail clearly, not silently resolve.
func TestBuildDesiredConnectorTLSCASecretRefMustBeDeclared(t *testing.T) {
	t.Parallel()
	worker := workerEnvelope("dbz-worker", map[string]any{
		"bootstrapServers": "broker:29092",
	})
	worker.Spec["secretRefs"] = []any{"conn-creds"} // rds-ca deliberately omitted
	src := simpleEnv("Source", "ext-rds", map[string]any{
		"engine":        "postgres",
		"external":      true,
		"connectionRef": map[string]any{"name": "rds-conn"},
		"postgres":      map[string]any{"database": "orders"},
	})
	conn := simpleEnv("Connection", "rds-conn", map[string]any{
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
		"mode":        "cdc",
		"sourceRef":   map[string]any{"name": "ext-rds"},
		"targetRef":   map[string]any{"name": "orders-events"},
		"providerRef": map[string]any{"name": "dbz-worker"},
	}
	b := simpleEnv("Binding", "orders-cdc", bindingSpec)
	req := reconciler.Request{
		Resource: b,
		Provider: worker,
		Resources: map[resource.Key]resource.Envelope{
			src.Key():    src,
			conn.Key():   conn,
			worker.Key(): worker,
		},
		Secrets: map[string]map[string]string{
			"conn-creds": {"username": "conn-user", "password": "conn-pass"},
		},
	}
	if _, err := buildDesiredConnector(req); err == nil {
		t.Fatal("expected error: spec.tls.caSecretRef not listed in the worker Provider's own spec.secretRefs")
	}
}
