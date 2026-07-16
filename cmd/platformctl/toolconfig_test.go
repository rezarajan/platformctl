package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

func toolTestState() ([]resource.Envelope, state.State, map[resource.Key]string) {
	cat := resource.Envelope{}
	cat.Kind = "Catalog"
	cat.Metadata.Name = "lakehouse-catalog"
	cat.Spec = map[string]any{"engine": "nessie", "providerRef": map[string]any{"name": "catalog-svc"}}

	minio := resource.Envelope{}
	minio.Kind = "Provider"
	minio.Metadata.Name = "local-minio"
	minio.Spec = map[string]any{"type": "minio", "runtime": map[string]any{"type": "docker"}, "secretRefs": []any{"minio-root"}}

	pg := resource.Envelope{}
	pg.Kind = "Provider"
	pg.Metadata.Name = "local-pg"
	pg.Spec = map[string]any{"type": "postgres", "runtime": map[string]any{"type": "docker"}, "secretRefs": []any{"pg-admin"}}

	src := resource.Envelope{}
	src.Kind = "Source"
	src.Metadata.Name = "orders"
	src.Spec = map[string]any{"engine": "postgres", "providerRef": map[string]any{"name": "local-pg"}, "postgres": map[string]any{"database": "ordersdb"}}

	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		cat.Key(): {Provider: map[string]any{
			"defaultBranch": "main",
			"endpoints": []any{
				map[string]any{"name": "iceberg-rest", "scheme": "http", "host": "http://127.0.0.1:19120/iceberg", "internal": "http://catalog-svc:19120/iceberg", "insecure": true},
			},
		}},
		minio.Key(): {Provider: map[string]any{
			"endpoints": []any{
				map[string]any{"name": "s3", "scheme": "http", "host": "http://127.0.0.1:19010", "internal": "http://local-minio:9000", "insecure": true},
			},
		}},
		pg.Key(): {Provider: map[string]any{
			"endpoints": []any{
				map[string]any{"name": "postgres", "scheme": "postgres", "host": "127.0.0.1:15432", "internal": "local-pg:5432", "insecure": true},
			},
		}},
		src.Key(): {},
	}}
	creds := map[resource.Key]string{minio.Key(): "minio-root", pg.Key(): "pg-admin"}
	return []resource.Envelope{cat, minio, pg, src}, st, creds
}

// TestToolConfigViews guards docs/planning/07 §2.3: inventory --for renders
// the exact endpoints from state into paste-ready tool config — and never
// renders a secret value, only SecretReference names.
func TestToolConfigViews(t *testing.T) {
	envs, st, creds := toolTestState()
	f := gatherToolFacts(envs, st, creds)

	cases := []struct {
		tool string
		want []string
	}{
		{"spark", []string{"http://127.0.0.1:19120/iceberg", "spark.sql.catalog.lakehouse.ref                  main", "fs.s3a.endpoint", "http://127.0.0.1:19010", `"minio-root"`}},
		{"trino", []string{"connector.name=iceberg", "iceberg.rest-catalog.uri=http://127.0.0.1:19120/iceberg", "s3.endpoint=http://127.0.0.1:19010"}},
		{"dbt", []string{"type: postgres", "host: 127.0.0.1", "port: 15432", "dbname: ordersdb", `"pg-admin"`}},
		{"psql", []string{"psql -h 127.0.0.1 -p 15432", "ordersdb"}},
		{"s3", []string{"--endpoint-url http://127.0.0.1:19010", `"minio-root"`}},
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		if err := renderToolConfig(&buf, tc.tool, f); err != nil {
			t.Fatalf("%s: %v", tc.tool, err)
		}
		out := buf.String()
		for _, want := range tc.want {
			if !strings.Contains(out, want) {
				t.Errorf("%s output missing %q:\n%s", tc.tool, want, out)
			}
		}
	}
}

func TestToolConfigUnknownTool(t *testing.T) {
	if err := renderToolConfig(&bytes.Buffer{}, "excel", toolFacts{}); err == nil {
		t.Fatal("unknown tool accepted")
	}
}

func TestToolConfigEmptyStateSelfDescribes(t *testing.T) {
	var buf bytes.Buffer
	if err := renderToolConfig(&buf, "spark", toolFacts{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "apply the platform first") {
		t.Errorf("empty-state output does not self-describe:\n%s", buf.String())
	}
}
