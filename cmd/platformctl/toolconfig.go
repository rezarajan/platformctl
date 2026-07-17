package main

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// toolFacts is everything the config views draw on, gathered once from
// applied state: the exact endpoints and catalog facts a tool needs, plus
// the SecretReference *names* holding credentials — values are never
// rendered (docs/planning/07 §2.3: inventory answers "what exact config do
// I paste into my tool?" without leaking secrets).
type toolFacts struct {
	icebergRestHost string // catalog REST endpoint reachable from the host
	catalogBranch   string
	s3Host          string // http://host:port
	s3CredsRef      string
	kafkaHost       string
	postgresHost    string // host:port
	postgresDB      string
	postgresCreds   string
	mysqlHost       string
	mysqlDB         string
	mysqlCreds      string
}

func gatherToolFacts(envelopes []resource.Envelope, st state.State, creds map[resource.Key]string) toolFacts {
	f := toolFacts{}
	for _, e := range envelopes {
		rs, ok := st.Resources[e.Key()]
		if !ok {
			continue
		}
		switch e.Kind {
		case "Catalog":
			if v, ok := rs.Provider["defaultBranch"].(string); ok {
				f.catalogBranch = v
			}
			for _, ep := range endpoint.FromState(rs.Provider[endpoint.Key]) {
				if ep.Name == "iceberg-rest" && ep.Host != "" {
					f.icebergRestHost = ep.Host
				}
			}
		case "Provider":
			for _, ep := range endpoint.FromState(rs.Provider[endpoint.Key]) {
				if ep.Host == "" {
					continue
				}
				switch ep.Name {
				case "s3":
					f.s3Host = ep.Host
					f.s3CredsRef = creds[e.Key()]
				case "kafka":
					f.kafkaHost = ep.Host
				case "postgres":
					f.postgresHost = ep.Host
					f.postgresCreds = creds[e.Key()]
				case "mysql":
					f.mysqlHost = ep.Host
					f.mysqlCreds = creds[e.Key()]
				}
			}
		case "Source":
			engine, _ := e.Spec["engine"].(string)
			if block, ok := e.Spec[engine].(map[string]any); ok {
				if db, _ := block["database"].(string); db != "" {
					switch engine {
					case "postgres":
						f.postgresDB = db
					case "mysql", "mariadb":
						f.mysqlDB = db
					}
				}
			}
		}
	}
	return f
}

// knownTools maps --for values to their renderers, so help text and
// dispatch cannot drift apart.
var knownTools = map[string]func(io.Writer, toolFacts){
	"spark": renderSpark,
	"trino": renderTrino,
	"dbt":   renderDBT,
	"psql":  renderPsql,
	"s3":    renderS3,
	"kafka": renderKafka,
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

func renderSpark(w io.Writer, f toolFacts) {
	note(w, "spark-defaults.conf — Iceberg REST catalog + S3A warehouse access.",
		"Credentials come from the named SecretReference's env keys; never inline them.")
	if f.icebergRestHost == "" && f.s3Host == "" {
		note(w, "no catalog or object-store endpoints recorded — apply the platform first")
		return
	}
	fmt.Fprintln(w, "spark.jars.packages                              org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
	fmt.Fprintln(w, "spark.sql.extensions                             org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
	if f.icebergRestHost != "" {
		fmt.Fprintln(w, "spark.sql.catalog.lakehouse                      org.apache.iceberg.spark.SparkCatalog")
		fmt.Fprintln(w, "spark.sql.catalog.lakehouse.type                 rest")
		fmt.Fprintf(w, "spark.sql.catalog.lakehouse.uri                  %s\n", f.icebergRestHost)
		if f.catalogBranch != "" {
			fmt.Fprintf(w, "spark.sql.catalog.lakehouse.ref                  %s\n", f.catalogBranch)
		}
	}
	if f.s3Host != "" {
		fmt.Fprintf(w, "spark.hadoop.fs.s3a.endpoint                     %s\n", f.s3Host)
		fmt.Fprintln(w, "spark.hadoop.fs.s3a.path.style.access            true")
		if f.s3CredsRef != "" {
			note(w, fmt.Sprintf("access/secret key: the %q SecretReference (username/password keys)", f.s3CredsRef))
		}
	}
}

func renderTrino(w io.Writer, f toolFacts) {
	note(w, "etc/catalog/lakehouse.properties — Iceberg REST connector.")
	if f.icebergRestHost == "" {
		note(w, "no catalog endpoint recorded — apply the platform first")
		return
	}
	fmt.Fprintln(w, "connector.name=iceberg")
	fmt.Fprintln(w, "iceberg.catalog.type=rest")
	fmt.Fprintf(w, "iceberg.rest-catalog.uri=%s\n", f.icebergRestHost)
	if f.s3Host != "" {
		fmt.Fprintf(w, "fs.native-s3.enabled=true\ns3.endpoint=%s\ns3.path-style-access=true\n", f.s3Host)
		if f.s3CredsRef != "" {
			note(w, fmt.Sprintf("s3.aws-access-key/s3.aws-secret-key: the %q SecretReference", f.s3CredsRef))
		}
	}
}

func renderDBT(w io.Writer, f toolFacts) {
	note(w, "profiles.yml — postgres target against the platform database.")
	if f.postgresHost == "" {
		note(w, "no postgres endpoint recorded — apply the platform first")
		return
	}
	host, port, _ := strings.Cut(f.postgresHost, ":")
	fmt.Fprintf(w, `datascape:
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
`, host, port, orPlaceholder(f.postgresDB, "<database>"))
	if f.postgresCreds != "" {
		note(w, fmt.Sprintf("DB_USER/DB_PASSWORD: the %q SecretReference (username/password keys)", f.postgresCreds))
	}
}

func renderPsql(w io.Writer, f toolFacts) {
	note(w, "psql — connect from this machine.")
	if f.postgresHost == "" {
		note(w, "no postgres endpoint recorded — apply the platform first")
		return
	}
	host, port, _ := strings.Cut(f.postgresHost, ":")
	fmt.Fprintf(w, "psql -h %s -p %s -U \"$DB_USER\" -d %s\n", host, port, orPlaceholder(f.postgresDB, "<database>"))
	if f.postgresCreds != "" {
		note(w, fmt.Sprintf("DB_USER/PGPASSWORD: the %q SecretReference (username/password keys)", f.postgresCreds))
	}
}

func renderS3(w io.Writer, f toolFacts) {
	note(w, "AWS CLI / mc — S3-compatible access from this machine.")
	if f.s3Host == "" {
		note(w, "no object-store endpoint recorded — apply the platform first")
		return
	}
	fmt.Fprintf(w, "aws s3 ls --endpoint-url %s\n", f.s3Host)
	fmt.Fprintf(w, "mc alias set datascape %s \"$AWS_ACCESS_KEY_ID\" \"$AWS_SECRET_ACCESS_KEY\"\n", f.s3Host)
	if f.s3CredsRef != "" {
		note(w, fmt.Sprintf("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY: the %q SecretReference (username/password keys)", f.s3CredsRef))
	}
}

func renderKafka(w io.Writer, f toolFacts) {
	note(w, "Kafka clients — bootstrap from this machine.")
	if f.kafkaHost == "" {
		note(w, "no kafka endpoint recorded — apply the platform first")
		return
	}
	fmt.Fprintf(w, "bootstrap.servers=%s\n", f.kafkaHost)
}

func orPlaceholder(s, placeholder string) string {
	if s == "" {
		return placeholder
	}
	return s
}
