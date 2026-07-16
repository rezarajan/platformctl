package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadOne(t *testing.T, doc string) error {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "m.yaml"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(dir)
	return err
}

func TestSchemaRejectsMalformedManifests(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string // substring expected in the schema error
	}{
		{
			name: "secret with inline plaintext value",
			doc: "apiVersion: datascape.io/v1alpha1\nkind: SecretReference\nmetadata: {name: creds}\n" +
				"spec:\n  backend: env\n  keys: [password]\n  value: hunter2\n",
			want: "schema validation failed",
		},
		{
			name: "eventstream with zero partitions",
			doc: "apiVersion: datascape.io/v1alpha1\nkind: EventStream\nmetadata: {name: t}\n" +
				"spec:\n  providerRef: {name: rp}\n  partitions: 0\n",
			want: "schema validation failed",
		},
		{
			name: "binding with unknown mode",
			doc: "apiVersion: datascape.io/v1alpha1\nkind: Binding\nmetadata: {name: b}\n" +
				"spec:\n  mode: teleport\n  sourceRef: {name: a}\n  targetRef: {name: b2}\n  providerRef: {name: p}\n",
			want: "schema validation failed",
		},
		{
			name: "external source without connectionRef",
			doc: "apiVersion: datascape.io/v1alpha1\nkind: Source\nmetadata: {name: s}\n" +
				"spec:\n  engine: postgres\n  external: true\n",
			want: "schema validation failed",
		},
		{
			name: "provider without runtime",
			doc: "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: p}\n" +
				"spec:\n  type: redpanda\n",
			want: "schema validation failed",
		},
		{
			name: "hand-authored status",
			doc: "apiVersion: datascape.io/v1alpha1\nkind: Dataset\nmetadata: {name: d}\n" +
				"spec: {providerRef: {name: m}, bucket: b, format: json}\nstatus: {conditions: []}\n",
			want: "schema validation failed",
		},
		{
			name: "uppercase metadata name",
			doc: "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: BadName}\n" +
				"spec: {type: noop, runtime: {type: fake}}\n",
			want: "schema validation failed",
		},
		{
			name: "uppercase ref name",
			doc: "apiVersion: datascape.io/v1alpha1\nkind: EventStream\nmetadata: {name: events}\n" +
				"spec: {providerRef: {name: BadRef}}\n",
			want: "schema validation failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := loadOne(t, tc.doc)
			if err == nil {
				t.Fatal("schema accepted a malformed manifest")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not mention schema validation: %v", err)
			}
		})
	}
}

func TestSchemaAcceptsValidManifest(t *testing.T) {
	doc := "apiVersion: datascape.io/v1alpha1\nkind: EventStream\nmetadata: {name: events, namespace: analytics}\n" +
		"spec:\n  providerRef: {namespace: infra, name: rp}\n  partitions: 3\n  retention: {duration: 7d}\n"
	if err := loadOne(t, doc); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
}

func TestSchemaValidatesNewKinds(t *testing.T) {
	valid := []string{
		// Managed catalog with an engine block.
		"apiVersion: datascape.io/v1alpha1\nkind: Catalog\nmetadata: {name: c}\n" +
			"spec:\n  engine: nessie\n  providerRef: {name: svc}\n  nessie: {defaultBranch: main}\n",
		// External catalog through a connection.
		"apiVersion: datascape.io/v1alpha1\nkind: Catalog\nmetadata: {name: c2}\n" +
			"spec:\n  engine: glue\n  external: true\n  connectionRef: {name: conn}\n",
		// Managed connection (stable entrypoint).
		"apiVersion: datascape.io/v1alpha1\nkind: Connection\nmetadata: {name: n}\n" +
			"spec:\n  providerRef: {name: edge}\n  port: 15999\n  target: db.internal:5432\n  secretRef: {name: creds}\n",
		// External connection (plain address record).
		"apiVersion: datascape.io/v1alpha1\nkind: Connection\nmetadata: {name: n2}\n" +
			"spec:\n  external: true\n  host: db.corp.internal\n  port: 5432\n",
	}
	for i, doc := range valid {
		if err := loadOne(t, doc); err != nil {
			t.Errorf("valid manifest %d rejected: %v", i, err)
		}
	}

	invalid := []string{
		// Catalog without an engine.
		"apiVersion: datascape.io/v1alpha1\nkind: Catalog\nmetadata: {name: c}\n" +
			"spec:\n  providerRef: {name: svc}\n",
		// Managed connection without a target.
		"apiVersion: datascape.io/v1alpha1\nkind: Connection\nmetadata: {name: n}\n" +
			"spec:\n  providerRef: {name: edge}\n  port: 15999\n",
		// External connection without a host.
		"apiVersion: datascape.io/v1alpha1\nkind: Connection\nmetadata: {name: n}\n" +
			"spec:\n  external: true\n  port: 5432\n",
		// Connection smuggling inline credentials.
		"apiVersion: datascape.io/v1alpha1\nkind: Connection\nmetadata: {name: n}\n" +
			"spec:\n  external: true\n  host: h\n  port: 5432\n  password: hunter2\n",
	}
	for i, doc := range invalid {
		if err := loadOne(t, doc); err == nil {
			t.Errorf("invalid manifest %d accepted", i)
		}
	}
}
