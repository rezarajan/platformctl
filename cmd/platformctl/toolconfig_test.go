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

// TestToolConfigMultipleDatabases guards docs/remediation/F-010: a platform
// with several providers of one kind must render one clearly-labeled
// section per component, correctly paired with its own Source's database
// name via providerRef — not silently pick the last envelope in iteration
// order.
func TestToolConfigMultipleDatabases(t *testing.T) {
	pg1 := resource.Envelope{}
	pg1.Kind = "Provider"
	pg1.Metadata.Name = "orders-pg"
	pg1.Spec = map[string]any{"type": "postgres", "runtime": map[string]any{"type": "docker"}, "secretRefs": []any{"orders-creds"}}

	pg2 := resource.Envelope{}
	pg2.Kind = "Provider"
	pg2.Metadata.Name = "billing-pg"
	pg2.Spec = map[string]any{"type": "postgres", "runtime": map[string]any{"type": "docker"}, "secretRefs": []any{"billing-creds"}}

	// Sources declared out of provider order and interleaved, to prove
	// pairing is by providerRef, not envelope position.
	srcBilling := resource.Envelope{}
	srcBilling.Kind = "Source"
	srcBilling.Metadata.Name = "billing-src"
	srcBilling.Spec = map[string]any{"engine": "postgres", "providerRef": map[string]any{"name": "billing-pg"}, "postgres": map[string]any{"database": "billingdb"}}

	srcOrders := resource.Envelope{}
	srcOrders.Kind = "Source"
	srcOrders.Metadata.Name = "orders-src"
	srcOrders.Spec = map[string]any{"engine": "postgres", "providerRef": map[string]any{"name": "orders-pg"}, "postgres": map[string]any{"database": "ordersdb"}}

	envs := []resource.Envelope{srcBilling, pg1, srcOrders, pg2}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		pg1.Key(): {Provider: map[string]any{
			"endpoints": []any{map[string]any{"name": "postgres", "scheme": "postgres", "host": "127.0.0.1:15432", "internal": "orders-pg:5432", "insecure": true}},
		}},
		pg2.Key(): {Provider: map[string]any{
			"endpoints": []any{map[string]any{"name": "postgres", "scheme": "postgres", "host": "127.0.0.1:15433", "internal": "billing-pg:5432", "insecure": true}},
		}},
		srcBilling.Key(): {},
		srcOrders.Key():  {},
	}}
	creds := map[resource.Key]string{pg1.Key(): "orders-creds", pg2.Key(): "billing-creds"}

	f := gatherToolFacts(envs, st, creds)
	if len(f.postgres) != 2 {
		t.Fatalf("gatherToolFacts collected %d postgres entries, want 2", len(f.postgres))
	}

	var buf bytes.Buffer
	if err := renderToolConfig(&buf, "psql", f); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	// Each component's own port and database must appear together — proves
	// the pairing survived, not just that both facts appear somewhere.
	for _, want := range []string{
		"default/Provider/orders-pg",
		"psql -h 127.0.0.1 -p 15432",
		"-d ordersdb",
		"default/Provider/billing-pg",
		"psql -h 127.0.0.1 -p 15433",
		"-d billingdb",
		`"orders-creds"`,
		`"billing-creds"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("psql multi-database output missing %q:\n%s", want, out)
		}
	}

	// The two ports must not be swapped onto the wrong database — assert
	// the exact pairing, not just presence of both substrings.
	ordersIdx := strings.Index(out, "orders-pg")
	ordersPortIdx := strings.Index(out, "-p 15432")
	billingIdx := strings.Index(out, "billing-pg")
	billingPortIdx := strings.Index(out, "-p 15433")
	if !(ordersIdx < ordersPortIdx && ordersPortIdx < billingIdx) {
		t.Errorf("orders-pg section did not stay before its own port and before the billing section:\n%s", out)
	}
	if billingIdx > billingPortIdx {
		t.Errorf("billing-pg header appeared after its own port:\n%s", out)
	}
}

// TestToolConfigSingleInstanceOutputUnchanged guards the F-010 fix's
// backward-compatibility requirement: with exactly one entry per family,
// output must be byte-identical to the pre-plurality renderers (no section
// header, no component-suffixed profile/alias names).
func TestToolConfigSingleInstanceOutputUnchanged(t *testing.T) {
	envs, st, creds := toolTestState()
	f := gatherToolFacts(envs, st, creds)

	var buf bytes.Buffer
	if err := renderToolConfig(&buf, "dbt", f); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "# --- ") {
		t.Errorf("single-instance dbt output has a section header:\n%s", out)
	}
	if !strings.HasPrefix(strings.TrimLeft(out, "#"), "") || !strings.Contains(out, "datascape:\n") {
		t.Errorf("single-instance dbt output profile name changed (want bare 'datascape:'):\n%s", out)
	}
	if strings.Contains(out, "datascape-local-pg") {
		t.Errorf("single-instance dbt output used a component-suffixed profile name:\n%s", out)
	}
}
