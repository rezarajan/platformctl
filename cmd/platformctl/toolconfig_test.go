package main

import (
	"bufio"
	"bytes"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/rezarajan/platformctl/internal/adapters/providers/prometheus"
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

	rp := resource.Envelope{}
	rp.Kind = "Provider"
	rp.Metadata.Name = "local-redpanda"
	rp.Spec = map[string]any{"type": "redpanda", "runtime": map[string]any{"type": "docker"}}

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
		rp.Key(): {Provider: map[string]any{
			"endpoints": []any{
				map[string]any{"name": "kafka", "scheme": "kafka", "host": "127.0.0.1:19092", "internal": "local-redpanda:9092", "insecure": true},
				map[string]any{"name": "metrics", "scheme": "http", "host": "http://127.0.0.1:19399/public_metrics", "internal": "http://local-redpanda:9644/public_metrics", "insecure": true},
			},
		}},
		src.Key(): {},
	}}
	creds := map[resource.Key]string{minio.Key(): "minio-root", pg.Key(): "pg-admin"}
	return []resource.Envelope{cat, minio, pg, rp, src}, st, creds
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
		{"kafka", []string{"bootstrap.servers=127.0.0.1:19092"}},
		{"dagster", []string{`"host": "127.0.0.1"`, `"port": 15432`, `"database": "ordersdb"`, "EnvVar(\"DB_USER\")", `endpoint_url="http://127.0.0.1:19010"`, `"bootstrap_servers": "127.0.0.1:19092"`, `"pg-admin"`, `"minio-root"`}},
		{"flink", []string{"CREATE CATALOG lakehouse WITH", "'uri'          = 'http://127.0.0.1:19120/iceberg'", "'ref'          = 'main'", "'s3.endpoint'            = 'http://127.0.0.1:19010'", "'properties.bootstrap.servers' = '127.0.0.1:19092'", `"minio-root"`}},
		{"metabase", []string{"MB_DB_TYPE=postgres", "MB_DB_DBNAME=ordersdb", "MB_DB_PORT=15432", "MB_DB_HOST=127.0.0.1", `"pg-admin"`}},
		{"superset", []string{"postgresql://DB_USER:DB_PASSWORD@127.0.0.1:15432/ordersdb", `"pg-admin"`}},
		{"prometheus", []string{"job_name: local-redpanda", "127.0.0.1:19399", "metrics_path: /public_metrics"}},
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
	if !(ordersIdx < ordersPortIdx && ordersPortIdx < billingIdx) { //nolint:staticcheck // QF1001: De Morgan's form reads worse for this "stays in order" assertion
		t.Errorf("orders-pg section did not stay before its own port and before the billing section:\n%s", out)
	}
	if billingIdx > billingPortIdx {
		t.Errorf("billing-pg header appeared after its own port:\n%s", out)
	}
}

// TestRenderDagsterGolden is an exact-string test: dagster's resource
// config is Python, which has no stdlib-cheap parser in Go, so the E3
// accept criterion ("parse where a parser is stdlib-cheap, golden
// otherwise") is met with a golden comparison instead.
func TestRenderDagsterGolden(t *testing.T) {
	envs, st, creds := toolTestState()
	f := gatherToolFacts(envs, st, creds)

	var buf bytes.Buffer
	if err := renderToolConfig(&buf, "dagster", f); err != nil {
		t.Fatal(err)
	}

	want := `# dagster resources — postgres/S3/Kafka resource config for Definitions(resources=...).
# Credentials come from the named SecretReference's env keys; never inline them.
from dagster import EnvVar
from dagster_aws.s3 import S3Resource

postgres_resource = {
    "host": "127.0.0.1",
    "port": 15432,
    "database": "ordersdb",
    "user": EnvVar("DB_USER"),
    "password": EnvVar("DB_PASSWORD"),
}
# DB_USER/DB_PASSWORD: the "pg-admin" SecretReference (username/password keys)
s3_resource = S3Resource(
    endpoint_url="http://127.0.0.1:19010",
    aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
)
# AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY: the "minio-root" SecretReference (username/password keys)
kafka_resource = {
    "bootstrap_servers": "127.0.0.1:19092",
}
`
	if buf.String() != want {
		t.Errorf("dagster output mismatch:\n--- got ---\n%s\n--- want ---\n%s", buf.String(), want)
	}
}

// TestRenderFlinkParses guards flink's SQL catalog + connector properties
// with a regexp — cheap enough (stdlib) to be a real parse rather than a
// substring check, per the E3 accept criterion.
func TestRenderFlinkParses(t *testing.T) {
	envs, st, creds := toolTestState()
	f := gatherToolFacts(envs, st, creds)

	var buf bytes.Buffer
	if err := renderToolConfig(&buf, "flink", f); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	if !strings.Contains(out, "CREATE CATALOG lakehouse WITH (") || !strings.Contains(out, "\n);") {
		t.Fatalf("flink output is not a CREATE CATALOG statement:\n%s", out)
	}

	props := map[string]string{}
	re := regexp.MustCompile(`'([\w.-]+)'\s*=\s*'([^']*)'`)
	for _, m := range re.FindAllStringSubmatch(out, -1) {
		props[m[1]] = m[2]
	}

	want := map[string]string{
		"type":                         "iceberg",
		"catalog-type":                 "rest",
		"uri":                          "http://127.0.0.1:19120/iceberg",
		"ref":                          "main",
		"s3.endpoint":                  "http://127.0.0.1:19010",
		"s3.path-style-access":         "true",
		"connector":                    "kafka",
		"properties.bootstrap.servers": "127.0.0.1:19092",
		"format":                       "json",
	}
	for k, v := range want {
		if got := props[k]; got != v {
			t.Errorf("flink property %q = %q, want %q\nfull output:\n%s", k, got, v, out)
		}
	}
}

// TestRenderMetabaseParses parses metabase's KEY=VALUE env-var output with
// bufio.Scanner (stdlib-cheap), per the E3 accept criterion.
func TestRenderMetabaseParses(t *testing.T) {
	envs, st, creds := toolTestState()
	f := gatherToolFacts(envs, st, creds)

	var buf bytes.Buffer
	if err := renderToolConfig(&buf, "metabase", f); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	env := map[string]string{}
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("metabase output line is not KEY=VALUE: %q\nfull output:\n%s", line, out)
		}
		env[key] = val
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		"MB_DB_TYPE":   "postgres",
		"MB_DB_DBNAME": "ordersdb",
		"MB_DB_PORT":   "15432",
		"MB_DB_HOST":   "127.0.0.1",
	}
	for k, v := range want {
		if got := env[k]; got != v {
			t.Errorf("metabase env %q = %q, want %q\nfull output:\n%s", k, got, v, out)
		}
	}
}

// TestRenderSupersetParses parses superset's SQLAlchemy URI with
// net/url.Parse (stdlib-cheap), per the E3 accept criterion, and asserts no
// secret value is embedded — only the DB_USER/DB_PASSWORD placeholder
// tokens named in the note beneath it.
func TestRenderSupersetParses(t *testing.T) {
	envs, st, creds := toolTestState()
	f := gatherToolFacts(envs, st, creds)

	var buf bytes.Buffer
	if err := renderToolConfig(&buf, "superset", f); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	var uriLine string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "postgresql://") {
			uriLine = line
			break
		}
	}
	if uriLine == "" {
		t.Fatalf("superset output has no SQLAlchemy URI line:\n%s", out)
	}

	u, err := url.Parse(uriLine)
	if err != nil {
		t.Fatalf("superset URI does not parse: %v\nline: %q", err, uriLine)
	}
	if u.Scheme != "postgresql" {
		t.Errorf("superset URI scheme = %q, want postgresql", u.Scheme)
	}
	if u.Host != "127.0.0.1:15432" {
		t.Errorf("superset URI host = %q, want 127.0.0.1:15432", u.Host)
	}
	if u.Path != "/ordersdb" {
		t.Errorf("superset URI path = %q, want /ordersdb", u.Path)
	}
	if u.User.Username() != "DB_USER" {
		t.Errorf("superset URI user = %q, want placeholder DB_USER", u.User.Username())
	}
	if pw, _ := u.User.Password(); pw != "DB_PASSWORD" {
		t.Errorf("superset URI password = %q, want placeholder DB_PASSWORD", pw)
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

// TestPrometheusInventorySnippetIsValidYAML guards docs/planning/08 C9's
// `inventory --for prometheus` accept criterion: the rendered snippet must
// parse as valid YAML (gopkg.in/yaml.v3, already a repo dependency) and
// decode into a scrape config carrying exactly the metrics endpoint facts
// recorded in state — the note lines above it (rendered as "# ..." YAML
// comments by note()) must not break the parse.
func TestPrometheusInventorySnippetIsValidYAML(t *testing.T) {
	envs, st, creds := toolTestState()
	f := gatherToolFacts(envs, st, creds)

	var buf bytes.Buffer
	if err := renderToolConfig(&buf, "prometheus", f); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	var parsed struct {
		Global struct {
			ScrapeInterval string `yaml:"scrape_interval"`
		} `yaml:"global"`
		ScrapeConfigs []struct {
			JobName       string `yaml:"job_name"`
			MetricsPath   string `yaml:"metrics_path"`
			StaticConfigs []struct {
				Targets []string `yaml:"targets"`
			} `yaml:"static_configs"`
		} `yaml:"scrape_configs"`
	}
	if err := yaml.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("inventory --for prometheus did not render valid YAML: %v\noutput:\n%s", err, out)
	}
	if len(parsed.ScrapeConfigs) != 1 {
		t.Fatalf("scrape_configs = %+v, want exactly one job (the redpanda metrics fact)", parsed.ScrapeConfigs)
	}
	job := parsed.ScrapeConfigs[0]
	if job.JobName != "local-redpanda" {
		t.Errorf("job_name = %q, want %q", job.JobName, "local-redpanda")
	}
	if job.MetricsPath != "/public_metrics" {
		t.Errorf("metrics_path = %q, want %q", job.MetricsPath, "/public_metrics")
	}
	if len(job.StaticConfigs) != 1 || len(job.StaticConfigs[0].Targets) != 1 || job.StaticConfigs[0].Targets[0] != "127.0.0.1:19399" {
		t.Errorf("static_configs = %+v, want a single target 127.0.0.1:19399", job.StaticConfigs)
	}

	// Also round-trips through the same package the managed provider itself
	// uses (ParseScrapeConfig), independent of the anonymous struct above.
	if _, err := prometheus.ParseScrapeConfig([]byte(out)); err != nil {
		t.Errorf("prometheus.ParseScrapeConfig rejected the rendered snippet: %v", err)
	}
}

// TestGatherToolFactsFallsBackToInternalForMetrics covers docs/planning/08
// C9 completion: the postgres/mysql exporter sidecars publish their
// "metrics" endpoint fact as Audience: internal — no ep.Host at all, by
// design (the exporter is never host-published). gatherToolFacts's
// blanket "skip when ep.Host == \"\"" rule (every other endpoint kind)
// would otherwise silently drop them from `inventory --for prometheus`;
// the metrics case falls back to ep.Internal instead, so a bring-your-own
// Prometheus joined to the same runtime network can still be configured
// against them.
func TestGatherToolFactsFallsBackToInternalForMetrics(t *testing.T) {
	pgExporter := resource.Envelope{}
	pgExporter.Kind = "Provider"
	pgExporter.Metadata.Name = "local-pg"
	pgExporter.Spec = map[string]any{"type": "postgres", "runtime": map[string]any{"type": "docker"}}

	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		pgExporter.Key(): {Provider: map[string]any{
			"endpoints": []any{
				// No "host" key at all — the exact shape an Audience:
				// internal-only fact publishes (endpoint.List.ToState()
				// omits empty Host entirely).
				map[string]any{"name": "metrics", "scheme": "http", "internal": "http://local-pg-exporter:9187/metrics", "insecure": true},
			},
		}},
	}}

	f := gatherToolFacts([]resource.Envelope{pgExporter}, st, nil)
	if len(f.metrics) != 1 {
		t.Fatalf("metrics facts = %+v, want exactly 1 (fell back to ep.Internal)", f.metrics)
	}
	if f.metrics[0].host != "local-pg-exporter:9187" {
		t.Errorf("metrics[0].host = %q, want %q", f.metrics[0].host, "local-pg-exporter:9187")
	}
	if f.metrics[0].path != "/metrics" {
		t.Errorf("metrics[0].path = %q, want %q", f.metrics[0].path, "/metrics")
	}

	var buf bytes.Buffer
	if err := renderToolConfig(&buf, "prometheus", f); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "local-pg-exporter:9187") {
		t.Errorf("rendered scrape config missing the internal-only exporter target:\n%s", buf.String())
	}
}
