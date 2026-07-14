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
	doc := "apiVersion: datascape.io/v1alpha1\nkind: EventStream\nmetadata: {name: events}\n" +
		"spec:\n  providerRef: {name: rp}\n  partitions: 3\n  retention: {duration: 7d}\n"
	if err := loadOne(t, doc); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
}
