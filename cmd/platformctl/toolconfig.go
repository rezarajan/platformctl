package main

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/adapters/providers/prometheus"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// dbEndpoint pairs one database-shaped Provider's observed endpoint with
// the database name a Source declares against it (matched by providerRef,
// not by iteration order — see gatherToolFacts) and the SecretReference
// name holding its credentials.
type dbEndpoint struct {
	component resource.Key
	host      string // host:port
	db        string
	credsRef  string
}

// simpleEndpoint is a component with just a host and optional credentials
// (s3, kafka) — no per-Source database pairing.
type simpleEndpoint struct {
	component resource.Key
	host      string
	credsRef  string
}

// catalogEndpoint pairs a Catalog's observed iceberg-rest endpoint with its
// default branch.
type catalogEndpoint struct {
	component resource.Key
	host      string
	branch    string
}

// metricsEndpoint is a Provider's observed "metrics" endpoint fact
// (docs/planning/08 C9), split into a bare "host:port" dial target and the
// technology's metrics path — the same shape prometheus.ScrapeTarget takes,
// so renderPrometheus feeds it straight into the same renderer the managed
// `prometheus` provider itself uses to generate its own scrape config
// ("rendered from the same facts").
type metricsEndpoint struct {
	component resource.Key
	host      string // host:port, no scheme
	path      string
}

// toolFacts is everything the config views draw on, gathered once from
// applied state: the exact endpoints and catalog facts a tool needs, plus
// the SecretReference *names* holding credentials — values are never
// rendered (docs/planning/07 §2.3: inventory answers "what exact config do
// I paste into my tool?" without leaking secrets). Every field is a slice,
// in envelope order, because the resource model allows any number of
// providers of one type (docs/remediation/F-010): a platform with two
// postgres Providers must render one paste-ready section per component,
// not silently pick one.
type toolFacts struct {
	catalogs []catalogEndpoint
	s3       []simpleEndpoint
	kafka    []simpleEndpoint
	postgres []dbEndpoint
	mysql    []dbEndpoint
	metrics  []metricsEndpoint
	// trino is populated from a "trino"-named endpoint fact (the trino
	// provider's coordinator, docs/planning/08 D10) — present only once a
	// trino Provider has been applied. renderTrino's live-endpoint branch
	// checks this before falling back to the bring-your-own paste snippet.
	trino []simpleEndpoint
}

func gatherToolFacts(envelopes []resource.Envelope, st state.State, creds map[resource.Key]string) toolFacts {
	f := toolFacts{}
	// First pass: Providers and Catalogs, so every database-shaped
	// component has a slot before Sources are matched against it by
	// providerRef in the second pass — Source and Provider envelopes are
	// not guaranteed to appear in reference order.
	dbIndex := map[resource.Key]int{} // component Key -> index in f.postgres or f.mysql
	dbFamily := map[resource.Key]string{}

	for _, e := range envelopes {
		rs, ok := st.Resources[e.Key()]
		if !ok {
			continue
		}
		switch e.Kind {
		case "Catalog":
			branch, _ := rs.Provider["defaultBranch"].(string)
			for _, ep := range endpoint.FromState(rs.Provider[endpoint.Key]) {
				if ep.Name == "iceberg-rest" && ep.Host != "" {
					f.catalogs = append(f.catalogs, catalogEndpoint{component: e.Key(), host: ep.Host, branch: branch})
				}
			}
		case "Provider":
			for _, ep := range endpoint.FromState(rs.Provider[endpoint.Key]) {
				if ep.Host == "" {
					continue
				}
				switch ep.Name {
				case "s3":
					f.s3 = append(f.s3, simpleEndpoint{component: e.Key(), host: ep.Host, credsRef: creds[e.Key()]})
				case "kafka":
					f.kafka = append(f.kafka, simpleEndpoint{component: e.Key(), host: ep.Host, credsRef: creds[e.Key()]})
				case "trino":
					f.trino = append(f.trino, simpleEndpoint{component: e.Key(), host: ep.Host})
				case "postgres":
					dbIndex[e.Key()] = len(f.postgres)
					dbFamily[e.Key()] = "postgres"
					f.postgres = append(f.postgres, dbEndpoint{component: e.Key(), host: ep.Host, credsRef: creds[e.Key()]})
				case "mysql":
					dbIndex[e.Key()] = len(f.mysql)
					dbFamily[e.Key()] = "mysql"
					f.mysql = append(f.mysql, dbEndpoint{component: e.Key(), host: ep.Host, credsRef: creds[e.Key()]})
				case "metrics":
					// ep.Host carries a full URL (scheme + host:port + path,
					// unlike every other endpoint name above's bare
					// "host:port") — split it into prometheus.ScrapeTarget's
					// shape.
					if u, err := url.Parse(ep.Host); err == nil && u.Host != "" {
						path := u.Path
						if path == "" {
							path = "/metrics"
						}
						f.metrics = append(f.metrics, metricsEndpoint{component: e.Key(), host: u.Host, path: path})
					}
				}
			}
		}
	}

	// Second pass: Sources, matched to their Provider by providerRef so the
	// right database name lands on the right component even when several
	// providers of the same engine exist.
	for _, e := range envelopes {
		if e.Kind != "Source" {
			continue
		}
		engine, _ := e.Spec["engine"].(string)
		block, ok := e.Spec[engine].(map[string]any)
		if !ok {
			continue
		}
		db, _ := block["database"].(string)
		if db == "" {
			continue
		}
		provKey := resource.RefFromSpec(e.Spec, "providerRef").Key(e.Metadata.Namespace, "Provider")
		idx, ok := dbIndex[provKey]
		if !ok {
			continue
		}
		switch dbFamily[provKey] {
		case "postgres":
			f.postgres[idx].db = db
		case "mysql":
			f.mysql[idx].db = db
		}
	}
	return f
}

// knownTools maps --for values to their renderers, so help text and
// dispatch cannot drift apart.
var knownTools = map[string]func(io.Writer, toolFacts){
	"spark":      renderSpark,
	"trino":      renderTrino,
	"dbt":        renderDBT,
	"psql":       renderPsql,
	"s3":         renderS3,
	"kafka":      renderKafka,
	"dagster":    renderDagster,
	"flink":      renderFlink,
	"metabase":   renderMetabase,
	"superset":   renderSuperset,
	"prometheus": renderPrometheus,
}

func toolNames() string {
	names := make([]string, 0, len(knownTools))
	for n := range knownTools {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, "|")
}

func renderToolConfig(w io.Writer, tool string, f toolFacts) error {
	render, ok := knownTools[tool]
	if !ok {
		return fmt.Errorf("unknown tool %q (supported: %s)", tool, toolNames())
	}
	render(w, f)
	return nil
}

// renderToolConfigString is renderToolConfig into an in-memory buffer, for
// callers that need the rendered snippet as a value (e.g. embedding in a
// structured -o json|yaml payload) rather than writing it directly.
func renderToolConfigString(tool string, f toolFacts) (string, error) {
	var buf bytes.Buffer
	if err := renderToolConfig(&buf, tool, f); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func note(w io.Writer, lines ...string) {
	for _, l := range lines {
		fmt.Fprintln(w, "# "+l)
	}
}

// forEachSection renders one block per entry. With exactly one entry the
// output is just that block (byte-identical to the pre-plurality renderers,
// for the single-instance platforms every shipped example is). With more
// than one, each block gets a "# --- component ---" header and a blank
// line separates blocks, so a platform with e.g. two postgres Providers
// gets one clearly-labeled section per component instead of one renderer
// silently picking the last one (docs/remediation/F-010).
func forEachSection[T any](w io.Writer, entries []T, keyOf func(T) resource.Key, render func(io.Writer, T)) {
	for i, e := range entries {
		if len(entries) > 1 {
			fmt.Fprintf(w, "# --- %s ---\n", keyOf(e).String())
		}
		render(w, e)
		if len(entries) > 1 && i < len(entries)-1 {
			fmt.Fprintln(w)
		}
	}
}

func renderSpark(w io.Writer, f toolFacts) {
	note(w, "spark-defaults.conf — Iceberg REST catalog + S3A warehouse access.",
		"Credentials come from the named SecretReference's env keys; never inline them.")
	if len(f.catalogs) == 0 && len(f.s3) == 0 {
		note(w, "no catalog or object-store endpoints recorded — apply the platform first")
		return
	}
	fmt.Fprintln(w, "spark.jars.packages                              org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
	fmt.Fprintln(w, "spark.sql.extensions                             org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
	forEachSection(w, f.catalogs, func(c catalogEndpoint) resource.Key { return c.component }, func(w io.Writer, c catalogEndpoint) {
		fmt.Fprintln(w, "spark.sql.catalog.lakehouse                      org.apache.iceberg.spark.SparkCatalog")
		fmt.Fprintln(w, "spark.sql.catalog.lakehouse.type                 rest")
		fmt.Fprintf(w, "spark.sql.catalog.lakehouse.uri                  %s\n", c.host)
		if c.branch != "" {
			fmt.Fprintf(w, "spark.sql.catalog.lakehouse.ref                  %s\n", c.branch)
		}
	})
	forEachSection(w, f.s3, func(s simpleEndpoint) resource.Key { return s.component }, func(w io.Writer, s simpleEndpoint) {
		fmt.Fprintf(w, "spark.hadoop.fs.s3a.endpoint                     %s\n", s.host)
		fmt.Fprintln(w, "spark.hadoop.fs.s3a.path.style.access            true")
		if s.credsRef != "" {
			note(w, fmt.Sprintf("access/secret key: the %q SecretReference (username/password keys)", s.credsRef))
		}
	})
}

// renderTrino covers two cases (docs/planning/08 D10): a live `trino`
// Provider applied to the platform ("it's already running" per
// docs/adr/006-compute-engines.md — render its coordinator's JDBC URL/UI
// address, alongside the paste-ready snippet below, which stays relevant
// for the bring-your-own case even when a managed coordinator also exists),
// or none applied (only the pre-existing paste-ready snippet, unchanged).
func renderTrino(w io.Writer, f toolFacts) {
	if len(f.trino) > 0 {
		note(w, "Managed trino Provider — live coordinator.")
		forEachSection(w, f.trino, func(t simpleEndpoint) resource.Key { return t.component }, func(w io.Writer, t simpleEndpoint) {
			if t.host == "" {
				note(w, "coordinator applied but no host binding observed (Kubernetes in-cluster access mode?)")
				return
			}
			// t.host is a full "http://host:port" URL (the endpoint fact's
			// own convention, matching s3/nessie/prometheus); a JDBC URL
			// has no http:// scheme, so it's stripped for that line only.
			fmt.Fprintf(w, "jdbc:trino://%s\n", strings.TrimPrefix(t.host, "http://"))
			fmt.Fprintf(w, "UI: %s/ui\n", t.host)
		})
		fmt.Fprintln(w)
	}
	note(w, "etc/catalog/lakehouse.properties — Iceberg REST connector (bring-your-own coordinator).")
	if len(f.catalogs) == 0 {
		note(w, "no catalog endpoint recorded — apply the platform first")
		return
	}
	forEachSection(w, f.catalogs, func(c catalogEndpoint) resource.Key { return c.component }, func(w io.Writer, c catalogEndpoint) {
		fmt.Fprintln(w, "connector.name=iceberg")
		fmt.Fprintln(w, "iceberg.catalog.type=rest")
		fmt.Fprintf(w, "iceberg.rest-catalog.uri=%s\n", c.host)
	})
	forEachSection(w, f.s3, func(s simpleEndpoint) resource.Key { return s.component }, func(w io.Writer, s simpleEndpoint) {
		// s3.region: found live wiring D10's trino provider against real
		// Trino — without it, Trino's S3 filesystem factory falls back to
		// the AWS SDK's default region-provider chain, which spends
		// minutes exhausting every provider (env var, profile, EC2
		// metadata) before failing catalog init outright. MinIO ignores
		// the value; any syntactically valid region satisfies the SDK.
		fmt.Fprintf(w, "fs.native-s3.enabled=true\ns3.endpoint=%s\ns3.region=us-east-1\ns3.path-style-access=true\n", s.host)
		if s.credsRef != "" {
			note(w, fmt.Sprintf("s3.aws-access-key/s3.aws-secret-key: the %q SecretReference", s.credsRef))
		}
	})
}

func renderDBT(w io.Writer, f toolFacts) {
	note(w, "profiles.yml — postgres target against the platform database.")
	if len(f.postgres) == 0 {
		note(w, "no postgres endpoint recorded — apply the platform first")
		return
	}
	forEachSection(w, f.postgres, func(p dbEndpoint) resource.Key { return p.component }, func(w io.Writer, p dbEndpoint) {
		profile := "datascape"
		if len(f.postgres) > 1 {
			profile = "datascape-" + p.component.Name
		}
		host, port, _ := strings.Cut(p.host, ":")
		fmt.Fprintf(w, `%s:
  target: dev
  outputs:
    dev:
      type: postgres
      host: %s
      port: %s
      dbname: %s
      user: "{{ env_var('DB_USER') }}"
      password: "{{ env_var('DB_PASSWORD') }}"
      schema: public
`, profile, host, port, orPlaceholder(p.db, "<database>"))
		if p.credsRef != "" {
			note(w, fmt.Sprintf("DB_USER/DB_PASSWORD: the %q SecretReference (username/password keys)", p.credsRef))
		}
	})
}

func renderPsql(w io.Writer, f toolFacts) {
	note(w, "psql — connect from this machine.")
	if len(f.postgres) == 0 {
		note(w, "no postgres endpoint recorded — apply the platform first")
		return
	}
	forEachSection(w, f.postgres, func(p dbEndpoint) resource.Key { return p.component }, func(w io.Writer, p dbEndpoint) {
		host, port, _ := strings.Cut(p.host, ":")
		fmt.Fprintf(w, "psql -h %s -p %s -U \"$DB_USER\" -d %s\n", host, port, orPlaceholder(p.db, "<database>"))
		if p.credsRef != "" {
			note(w, fmt.Sprintf("DB_USER/PGPASSWORD: the %q SecretReference (username/password keys)", p.credsRef))
		}
	})
}

func renderS3(w io.Writer, f toolFacts) {
	note(w, "AWS CLI / mc — S3-compatible access from this machine.")
	if len(f.s3) == 0 {
		note(w, "no object-store endpoint recorded — apply the platform first")
		return
	}
	forEachSection(w, f.s3, func(s simpleEndpoint) resource.Key { return s.component }, func(w io.Writer, s simpleEndpoint) {
		alias := "datascape"
		if len(f.s3) > 1 {
			alias = "datascape-" + s.component.Name
		}
		fmt.Fprintf(w, "aws s3 ls --endpoint-url %s\n", s.host)
		fmt.Fprintf(w, "mc alias set %s %s \"$AWS_ACCESS_KEY_ID\" \"$AWS_SECRET_ACCESS_KEY\"\n", alias, s.host)
		if s.credsRef != "" {
			note(w, fmt.Sprintf("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY: the %q SecretReference (username/password keys)", s.credsRef))
		}
	})
}

func renderKafka(w io.Writer, f toolFacts) {
	note(w, "Kafka clients — bootstrap from this machine.")
	if len(f.kafka) == 0 {
		note(w, "no kafka endpoint recorded — apply the platform first")
		return
	}
	forEachSection(w, f.kafka, func(k simpleEndpoint) resource.Key { return k.component }, func(w io.Writer, k simpleEndpoint) {
		fmt.Fprintf(w, "bootstrap.servers=%s\n", k.host)
	})
}

// renderPrometheus renders a scrape_configs snippet for a bring-your-own
// Prometheus (docs/planning/08 C9) — one job per Provider's published
// "metrics" endpoint, from a host-published address (this Prometheus is
// assumed to run outside the platform's own runtime network, the same
// audience every other renderer in this file targets). It calls the exact
// same prometheus.RenderScrapeConfig the managed `prometheus` provider
// itself uses to generate its own in-network scrape config — "the same
// facts" means literally the same renderer, not two hand-synced templates.
func renderPrometheus(w io.Writer, f toolFacts) {
	note(w, "prometheus.yml scrape_configs — bring-your-own Prometheus.",
		"Targets are host-published addresses, reachable from wherever this Prometheus runs; the managed `prometheus` provider (gate MonitoringStackProvider) generates the in-network equivalent of this same config from the same facts.")
	if len(f.metrics) == 0 {
		note(w, "no metrics endpoints recorded — apply the platform first")
		return
	}
	targets := make([]prometheus.ScrapeTarget, 0, len(f.metrics))
	for _, m := range f.metrics {
		targets = append(targets, prometheus.ScrapeTarget{Job: m.component.Name, Target: m.host, Path: m.path})
	}
	cfg, err := prometheus.RenderScrapeConfig(targets, "")
	if err != nil {
		note(w, fmt.Sprintf("error rendering scrape config: %v", err))
		return
	}
	w.Write(cfg) //nolint:errcheck
}

func renderDagster(w io.Writer, f toolFacts) {
	note(w, "dagster resources — postgres/S3/Kafka resource config for Definitions(resources=...).",
		"Credentials come from the named SecretReference's env keys; never inline them.")
	if len(f.postgres) == 0 && len(f.s3) == 0 && len(f.kafka) == 0 {
		note(w, "no postgres, s3, or kafka endpoints recorded — apply the platform first")
		return
	}
	fmt.Fprintln(w, "from dagster import EnvVar")
	if len(f.s3) > 0 {
		fmt.Fprintln(w, "from dagster_aws.s3 import S3Resource")
	}
	fmt.Fprintln(w)
	forEachSection(w, f.postgres, func(p dbEndpoint) resource.Key { return p.component }, func(w io.Writer, p dbEndpoint) {
		name := "postgres_resource"
		if len(f.postgres) > 1 {
			name = "postgres_" + pythonIdent(p.component.Name) + "_resource"
		}
		host, port, _ := strings.Cut(p.host, ":")
		fmt.Fprintf(w, "%s = {\n    \"host\": %q,\n    \"port\": %s,\n    \"database\": %q,\n    \"user\": EnvVar(\"DB_USER\"),\n    \"password\": EnvVar(\"DB_PASSWORD\"),\n}\n",
			name, host, port, orPlaceholder(p.db, "<database>"))
		if p.credsRef != "" {
			note(w, fmt.Sprintf("DB_USER/DB_PASSWORD: the %q SecretReference (username/password keys)", p.credsRef))
		}
	})
	forEachSection(w, f.s3, func(s simpleEndpoint) resource.Key { return s.component }, func(w io.Writer, s simpleEndpoint) {
		name := "s3_resource"
		if len(f.s3) > 1 {
			name = "s3_" + pythonIdent(s.component.Name) + "_resource"
		}
		fmt.Fprintf(w, "%s = S3Resource(\n    endpoint_url=%q,\n    aws_access_key_id=EnvVar(\"AWS_ACCESS_KEY_ID\"),\n    aws_secret_access_key=EnvVar(\"AWS_SECRET_ACCESS_KEY\"),\n)\n", name, s.host)
		if s.credsRef != "" {
			note(w, fmt.Sprintf("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY: the %q SecretReference (username/password keys)", s.credsRef))
		}
	})
	forEachSection(w, f.kafka, func(k simpleEndpoint) resource.Key { return k.component }, func(w io.Writer, k simpleEndpoint) {
		name := "kafka_resource"
		if len(f.kafka) > 1 {
			name = "kafka_" + pythonIdent(k.component.Name) + "_resource"
		}
		fmt.Fprintf(w, "%s = {\n    \"bootstrap_servers\": %q,\n}\n", name, k.host)
	})
}

func renderFlink(w io.Writer, f toolFacts) {
	note(w, "Flink SQL — Iceberg REST catalog + Kafka connector properties.",
		"Credentials come from the named SecretReference's env keys; never inline them.")
	if len(f.catalogs) == 0 && len(f.s3) == 0 && len(f.kafka) == 0 {
		note(w, "no catalog, s3, or kafka endpoints recorded — apply the platform first")
		return
	}
	forEachSection(w, f.catalogs, func(c catalogEndpoint) resource.Key { return c.component }, func(w io.Writer, c catalogEndpoint) {
		name := "lakehouse"
		if len(f.catalogs) > 1 {
			name = "lakehouse_" + pythonIdent(c.component.Name)
		}
		fmt.Fprintf(w, "CREATE CATALOG %s WITH (\n  'type'         = 'iceberg',\n  'catalog-type' = 'rest',\n  'uri'          = '%s'", name, c.host)
		if c.branch != "" {
			fmt.Fprintf(w, ",\n  'ref'          = '%s'", c.branch)
		}
		fmt.Fprintln(w, "\n);")
	})
	forEachSection(w, f.s3, func(s simpleEndpoint) resource.Key { return s.component }, func(w io.Writer, s simpleEndpoint) {
		note(w, "add to the catalog's WITH (...) clause above:")
		fmt.Fprintf(w, "  's3.endpoint'            = '%s',\n  's3.path-style-access'   = 'true'\n", s.host)
		if s.credsRef != "" {
			note(w, fmt.Sprintf("s3.access-key-id/s3.secret-access-key: the %q SecretReference", s.credsRef))
		}
	})
	forEachSection(w, f.kafka, func(k simpleEndpoint) resource.Key { return k.component }, func(w io.Writer, k simpleEndpoint) {
		note(w, "Kafka connector properties, for a CREATE TABLE ... WITH (...) clause:")
		fmt.Fprintf(w, "  'connector'                     = 'kafka',\n  'properties.bootstrap.servers' = '%s',\n  'format'                        = 'json'\n", k.host)
	})
}

func renderMetabase(w io.Writer, f toolFacts) {
	note(w, "Metabase — external application database env vars (docker-compose .env).",
		"Credentials come from the named SecretReference's env keys; never inline them.")
	if len(f.postgres) == 0 {
		note(w, "no postgres endpoint recorded — apply the platform first")
		return
	}
	forEachSection(w, f.postgres, func(p dbEndpoint) resource.Key { return p.component }, func(w io.Writer, p dbEndpoint) {
		host, port, _ := strings.Cut(p.host, ":")
		fmt.Fprintln(w, "MB_DB_TYPE=postgres")
		fmt.Fprintf(w, "MB_DB_DBNAME=%s\n", orPlaceholder(p.db, "<database>"))
		fmt.Fprintf(w, "MB_DB_PORT=%s\n", port)
		fmt.Fprintf(w, "MB_DB_HOST=%s\n", host)
		if p.credsRef != "" {
			note(w, fmt.Sprintf("MB_DB_USER/MB_DB_PASS: the %q SecretReference (username/password keys)", p.credsRef))
		}
	})
}

func renderSuperset(w io.Writer, f toolFacts) {
	note(w, "Superset — Settings > Database Connections > + Database, SQLAlchemy URI.",
		"Credentials come from the named SecretReference's env keys; never inline them.")
	if len(f.postgres) == 0 {
		note(w, "no postgres endpoint recorded — apply the platform first")
		return
	}
	forEachSection(w, f.postgres, func(p dbEndpoint) resource.Key { return p.component }, func(w io.Writer, p dbEndpoint) {
		fmt.Fprintf(w, "postgresql://DB_USER:DB_PASSWORD@%s/%s\n", p.host, orPlaceholder(p.db, "<database>"))
		if p.credsRef != "" {
			note(w, fmt.Sprintf("DB_USER/DB_PASSWORD placeholders: the %q SecretReference (username/password keys)", p.credsRef))
		}
	})
}

func orPlaceholder(s, placeholder string) string {
	if s == "" {
		return placeholder
	}
	return s
}

// pythonIdent makes a resource name safe to splice into a Python (or SQL
// bare-word) identifier — component names may contain hyphens, which are
// legal in resource.Key.Name but not in an identifier.
func pythonIdent(s string) string {
	return strings.ReplaceAll(s, "-", "_")
}
