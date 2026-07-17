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
				case "postgres":
					dbIndex[e.Key()] = len(f.postgres)
					dbFamily[e.Key()] = "postgres"
					f.postgres = append(f.postgres, dbEndpoint{component: e.Key(), host: ep.Host, credsRef: creds[e.Key()]})
				case "mysql":
					dbIndex[e.Key()] = len(f.mysql)
					dbFamily[e.Key()] = "mysql"
					f.mysql = append(f.mysql, dbEndpoint{component: e.Key(), host: ep.Host, credsRef: creds[e.Key()]})
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

func renderTrino(w io.Writer, f toolFacts) {
	note(w, "etc/catalog/lakehouse.properties — Iceberg REST connector.")
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
		fmt.Fprintf(w, "fs.native-s3.enabled=true\ns3.endpoint=%s\ns3.path-style-access=true\n", s.host)
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

func orPlaceholder(s, placeholder string) string {
	if s == "" {
		return placeholder
	}
	return s
}
