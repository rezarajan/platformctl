// Package jdbcsink reconciles a Kafka Connect worker carrying Confluent's
// kafka-connect-jdbc sink plugin and registers/updates JDBC sink connectors
// realizing Binding(mode: sink, sourceRef: EventStream, targetRef: Source):
// an EventStream's topic is written into a relational database — the
// sink-into-Source pairing ADR 001 modeled and ADR 009 left as a seam with
// no shipped provider. Implements DatabaseSinkCapableProvider
// (docs/planning/08 D3).
//
// Target resolution mirrors debezium's buildDesiredConnector (its SOURCE
// side, the origin of a cdc-mode Binding) exactly, but for the TARGET side
// of a sink-mode Binding: the database address/credentials come from the
// target Source's realizing Provider (managed) or its Connection (external
// Sources), never from the Dataset-style Provider-level-only credential
// convention s3sink uses (a Source, unlike a Dataset, may already carry its
// own Connection with its own secretRef).
//
// IMPORTANT technical constraint (verified against kafka-connect-jdbc
// v10.9.6 source, sink/metadata/FieldsMetadata.java): the JDBC sink
// connector extracts value columns only from a Struct-typed (schema-
// carrying) record value, and pk.mode=record_key requires a Struct-typed
// key schema — a fully schemaless (Map-typed) record contributes zero
// columns and record_key throws outright. Debezium's own json path
// (debezium.applyConverterConfig) hardcodes schemas.enable=false
// unconditionally, so a CDC-sourced topic only carries schema-carrying
// records when the cdc Binding itself declares options.format: avro (or
// protobuf) — the D1/D2 registry-backed converters. Consequently this
// provider requires a schema-carrying spec.options.format at validate time
// (ValidateBindingOptions rejects unset/"json"); this is a deliberate,
// stronger-than-s3sink constraint, not an oversight — see that method's
// doc comment.
package jdbcsink

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/kafkaconnect"
	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/eventstream"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// The stock Debezium/Connect images ship no JDBC sink plugin, so there is no
// usable default image — spec.configuration.image is required and must
// contain Confluent's kafka-connect-jdbc connector on the plugin path
// (mirrors s3sink's identical requirement; reference build:
// cmd/platformctl/testdata/jdbcsink-image/Dockerfile, version-pinned).
const connectorClass = "io.confluent.connect.jdbc.JdbcSinkConnector"

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request.
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "jdbcsink" }

// SupportedSinkEngines implements DatabaseSinkCapableProvider — exactly the
// engines with shipped Source-realizing providers (postgres, mysql); mariadb
// is a mysql-provider-type variant, not a distinct Source.spec.engine this
// provider declares support for (docs/planning/08 D3's own scoping).
func (p *Provider) SupportedSinkEngines() []string {
	return []string{"postgres", "mysql"}
}

func connectPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "connectPort")
}

// connectPorts mirrors debezium's/s3sink's identical helper — see either's
// doc comment for the full ADR 004/017 reasoning (workers > 1 leaves
// HostPort auto-assigned; ValidateSpec refuses a connectPort pin combined
// with workers).
func connectPorts(cfg provider.Provider, name string, workers int) []runtime.PortBinding {
	if workers > 1 {
		return []runtime.PortBinding{{ContainerPort: 8083, Audience: runtime.AudienceHost}}
	}
	return []runtime.PortBinding{{HostPort: connectPort(cfg, name), ContainerPort: 8083, Audience: runtime.AudienceHost}}
}

// workersDeclared mirrors debezium's/s3sink's identical helper (docs/planning/08 C3).
func workersDeclared(cfg provider.Provider) (int, bool) {
	v, ok := cfg.Configuration["workers"]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		return int(n), true
	}
	return 0, true
}

// workerURLs mirrors debezium's/s3sink's identical helper.
func workerURLs(ctx context.Context, rt runtime.ContainerRuntime, name string, cfg provider.Provider) ([]string, func() error, error) {
	n, _ := workersDeclared(cfg)
	return providerkit.ReachableURLs(ctx, rt, name, 8083, n)
}

func (p *Provider) Reconcile(ctx context.Context, req reconciler.Request) (status.Status, error) {
	switch req.Resource.Kind {
	case "Provider":
		return p.reconcileWorker(ctx, req)
	case "Binding":
		return p.reconcileConnector(ctx, req)
	default:
		return status.Status{}, fmt.Errorf("jdbcsink provider cannot reconcile kind %s", req.Resource.Kind)
	}
}

func (p *Provider) reconcileWorker(ctx context.Context, req reconciler.Request) (status.Status, error) {
	rt := req.Runtime
	st := status.Status{}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	name := naming.RuntimeObjectName(req.Provider)
	image, _ := cfg.Configuration["image"].(string)
	if image == "" {
		return st, fmt.Errorf("Provider %q (type: jdbcsink): spec.configuration.image is required (a Connect image carrying the JDBC sink plugin)", name)
	}
	bootstrap, _ := cfg.Configuration["bootstrapServers"].(string)
	if bootstrap == "" {
		// Graph-inferred (docs/planning/08 E2), mirroring debezium/s3sink.
		bootstrap = req.KafkaBootstrapServers
	}
	if bootstrap == "" {
		return st, fmt.Errorf("Provider %q (type: jdbcsink): spec.configuration.bootstrapServers is required (declare it, or wire a Binding on this Provider to an EventStream whose Provider publishes a Kafka bootstrap address)", name)
	}
	workers, workersDecl := workersDeclared(cfg)
	if workersDecl && workers < 1 {
		return st, fmt.Errorf("Provider %q (type: jdbcsink): spec.configuration.workers must be a positive integer, got %v", name, cfg.Configuration["workers"])
	}
	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
			Image:    image,
			Replicas: workers,
			Env: map[string]string{
				"BOOTSTRAP_SERVERS":                      bootstrap,
				"GROUP_ID":                               name,
				"CONFIG_STORAGE_TOPIC":                   name + "-configs",
				"OFFSET_STORAGE_TOPIC":                   name + "-offsets",
				"STATUS_STORAGE_TOPIC":                   name + "-status",
				"CONFIG_STORAGE_REPLICATION_FACTOR":      "1",
				"OFFSET_STORAGE_REPLICATION_FACTOR":      "1",
				"STATUS_STORAGE_REPLICATION_FACTOR":      "1",
				"KEY_CONVERTER":                          "org.apache.kafka.connect.json.JsonConverter",
				"VALUE_CONVERTER":                        "org.apache.kafka.connect.json.JsonConverter",
				"CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE":   "false",
				"CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE": "false",
				// Same rationale as s3sink: sink connectors commit on offset
				// flush, and the 60s default makes every reconcile-and-verify
				// cycle glacial.
				"OFFSET_FLUSH_INTERVAL_MS": "5000",
				// topics.regex subscriptions (this provider's own
				// desiredConnector, mirroring s3sink's identical comment)
				// only discover topics created after connector registration
				// on consumer metadata refresh — the 5min default would
				// stall a CDC per-table topic that only appears on the
				// source table's first captured event, well past this
				// provider's own reconcile-verify window. Found live
				// against a real Debezium+jdbcsink pipeline (this task's own
				// integration test) — the exact gotcha s3sink's identical
				// setting already documents.
				"CONNECT_CONSUMER_METADATA_MAX_AGE_MS": "10000",
			},
			Ports: connectPorts(cfg, name, workers),
			// CA trust files (docs/planning/08 I2) — mirrors debezium's
			// identical worker-level mount; see providerkit.CATrustDir.
			Files: providerkit.CATrustFileMounts(cfg, req.Secrets),
			HealthCheck: &runtime.HealthCheck{
				Test:     []string{"CMD-SHELL", "curl -sf http://localhost:8083/connectors || exit 1"},
				Interval: 3 * time.Second,
				Timeout:  5 * time.Second,
				Retries:  40,
			},
		},
		WaitTimeout: 180 * time.Second,
	})
	if err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectWorkerHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	hostAddr := ctrState.HostAddr(8083)
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	providerState := map[string]any{
		endpoint.Key: endpoint.List{
			{Name: "connect-rest", Scheme: "http", Host: hostURL, Internal: fmt.Sprintf("http://%s:8083", name), Insecure: true},
		}.ToState(),
		"bootstrapServers": bootstrap,
	}
	if workersDecl {
		providerState["workers"] = workers
	}
	st.ProviderState = providerState
	return st, nil
}

// desiredConnector is the manifest-derived truth about one JDBC sink
// connector: its name, full Connect config, and the preflight endpoint.
// Mirrors debezium.desiredConnector's shape exactly (same field set, same
// preflight dance) but resolved for the Binding's TARGET Source instead of
// its source Source.
type desiredConnector struct {
	name                    string
	config                  map[string]string
	engine                  string
	dbName                  string
	preflightHost           string
	preflightPort           int
	preflightConnectionName string
	credsUser               string
	credsPass               string
	// tlsPosture is the target Source's Connection's outbound TLS posture
	// (docs/planning/08 I2) — nil preserves the pre-I2 plaintext preflight
	// dial byte-for-byte; jdbcURL's own TLS query params are set directly on
	// connection.url instead (the live Connect worker's JDBC driver reads
	// them from there, not from this Go process's own dial).
	tlsPosture *providerkit.DatabaseTLS
}

// enginePortFor returns the JDBC dialect's conventional default port —
// mirrors debezium.connectorFor's port half; this provider needs no
// connector-class table since one connector class (JdbcSinkConnector)
// serves every dialect via connection.url's own JDBC scheme.
func enginePortFor(engine string) (int, error) {
	switch engine {
	case "postgres":
		return 5432, nil
	case "mysql", "mariadb":
		return 3306, nil
	default:
		return 0, fmt.Errorf("no JDBC dialect mapping for sink engine %q", engine)
	}
}

// jdbcURL builds the connection.url property, appending the outbound TLS
// query params tlsPosture calls for (docs/planning/08 I2) — nil appends
// nothing, the pre-I2 plaintext behavior.
func jdbcURL(engine, host string, port int, dbName string, tlsPosture *providerkit.DatabaseTLS) (string, error) {
	switch engine {
	case "postgres":
		return fmt.Sprintf("jdbc:postgresql://%s:%d/%s", host, port, dbName) + postgresJDBCTLSParams(tlsPosture), nil
	case "mysql", "mariadb":
		return fmt.Sprintf("jdbc:mysql://%s:%d/%s", host, port, dbName) + mysqlJDBCTLSParams(tlsPosture), nil
	default:
		return "", fmt.Errorf("no JDBC dialect mapping for sink engine %q", engine)
	}
}

// postgresJDBCTLSParams builds the "?sslmode=...&sslrootcert=..." suffix for
// a pgjdbc connection.url — pgjdbc accepts the same sslmode vocabulary
// libpq does, matching connection.TLSModeRequire/VerifyCA/VerifyFull
// exactly. The CA bundle a worker-level reconcile already mounted
// (providerkit.CATrustFileMounts) is referenced by its fixed, deterministic
// path.
func postgresJDBCTLSParams(tlsPosture *providerkit.DatabaseTLS) string {
	if tlsPosture == nil {
		return ""
	}
	q := url.Values{}
	q.Set("sslmode", tlsPosture.Mode)
	if tlsPosture.CASecretRefName != "" {
		q.Set("sslrootcert", providerkit.CAFilePath(tlsPosture.CASecretRefName))
	}
	return "?" + q.Encode()
}

// mysqlJDBCTLSParams builds the Connector/J TLS query suffix. Unlike
// Debezium's own MySQL binlog client (debezium.applyTLSConfig's MySQL
// half, which needs a Java truststore Datascape does not build), Connector/J
// — the actual JDBC driver the Kafka Connect JDBC sink connector uses —
// accepts a raw PEM CA directly via trustCertificateKeyStoreType=PEM (since
// Connector/J 8.0.22), so full CA verification is supported here.
func mysqlJDBCTLSParams(tlsPosture *providerkit.DatabaseTLS) string {
	if tlsPosture == nil {
		return ""
	}
	q := url.Values{}
	q.Set("sslMode", mysqlJDBCSSLMode(tlsPosture.Mode))
	if tlsPosture.CASecretRefName != "" {
		q.Set("trustCertificateKeyStoreType", "PEM")
		q.Set("trustCertificateKeyStoreUrl", "file:"+providerkit.CAFilePath(tlsPosture.CASecretRefName))
	}
	return "?" + q.Encode()
}

// mysqlJDBCSSLMode maps our libpq-derived mode vocabulary to Connector/J's
// own sslMode enum.
func mysqlJDBCSSLMode(mode string) string {
	switch mode {
	case connection.TLSModeRequire:
		return "REQUIRED"
	case connection.TLSModeVerifyCA:
		return "VERIFY_CA"
	case connection.TLSModeVerifyFull:
		return "VERIFY_IDENTITY"
	default:
		return "PREFERRED"
	}
}

// buildDesiredConnector resolves the manifest-derived JDBC sink connector
// config for a Binding(mode: sink, sourceRef: EventStream, targetRef:
// Source). Target address/credential resolution mirrors debezium.
// buildDesiredConnector's SOURCE-side resolution (see that function's doc
// comment for the full preference-order rationale) applied to the TARGET
// side: the target Source's Connection (external Sources) wins, then its
// realizing Provider's own container name (managed Sources), then a
// Binding-level options override.
func buildDesiredConnector(req reconciler.Request) (desiredConnector, error) {
	res := req.Resource
	d := desiredConnector{}
	b, err := binding.FromEnvelope(res)
	if err != nil {
		return d, err
	}
	if b.Mode != binding.ModeSink {
		return d, fmt.Errorf("Binding %q: jdbcsink realizes mode \"sink\" only, got %q", res.Metadata.Name, b.Mode)
	}

	srcRef := resource.RefFromSpec(res.Spec, "sourceRef")
	if _, ok := req.Resources[srcRef.Key(res.Metadata.Namespace, "EventStream")]; !ok {
		return d, fmt.Errorf("Binding %q: sourceRef %q not found", res.Metadata.Name, b.SourceRef)
	}

	tgtRef := resource.RefFromSpec(res.Spec, "targetRef")
	tgtEnv, ok := req.Resources[tgtRef.Key(res.Metadata.Namespace, "Source")]
	if !ok {
		return d, fmt.Errorf("Binding %q: targetRef %q not found", res.Metadata.Name, b.TargetRef)
	}
	tgt, err := source.FromEnvelope(tgtEnv)
	if err != nil {
		return d, err
	}
	dbName, _ := tgt.EngineConfig["database"].(string)

	enginePort, err := enginePortFor(tgt.Engine)
	if err != nil {
		return d, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}

	// The target database address, same preference order as debezium's
	// SOURCE-side resolution: the target Source's Connection (external
	// targets — a managed Connection answers at its own name on the shared
	// network, an external one at its declared host), the target Source's
	// Provider container name, then explicit options overrides. Shared
	// byte-for-byte with debezium via providerkit.ResolveEndpoint
	// (docs/planning/08 I5).
	ep, ok := providerkit.ResolveEndpoint(req, tgt, tgtEnv, enginePort, b.Options)
	if !ok {
		return d, fmt.Errorf("Binding %q: cannot determine target database hostname (no providerRef or Connection on Source %q, and no options.databaseHostname)", res.Metadata.Name, b.TargetRef)
	}
	dbHost, dbPort := ep.Host, ep.Port
	d.preflightHost, d.preflightPort = ep.PreflightHost, ep.PreflightPort
	d.preflightConnectionName = ep.PreflightConnectionName

	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return d, err
	}

	// Credentials: the target Source's Connection secretRef when declared
	// (and this provider lists it in spec.secretRefs so the engine resolved
	// it), else the provider-level credentialsSecretRef — mirroring
	// debezium's replicationSecretRef fallback, named credentialsSecretRef
	// to match s3sink's own convention (task instruction: secrets via the
	// Source's/Connection's existing secretRef plumbing).
	creds, ok := providerkit.ResolveEndpointCredentials(req, cfg, ep.ConnectionSecretRef, "credentialsSecretRef")
	if !ok {
		return d, fmt.Errorf("Binding %q: jdbcsink Provider %q has no resolved credentials — declare the target Source's Connection secretRef or configuration.credentialsSecretRef in spec.secretRefs", res.Metadata.Name, naming.RuntimeObjectName(req.Provider))
	}

	// Outbound TLS posture (docs/planning/08 I2): nil when the target
	// Source's Connection is managed or declares no spec.tls — the pre-I2
	// plaintext preflight/connection.url byte-for-byte unchanged.
	tlsPosture, err := providerkit.ResolveDatabaseTLS(req, cfg, ep)
	if err != nil {
		return d, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}
	d.tlsPosture = tlsPosture

	url, err := jdbcURL(tgt.Engine, dbHost, dbPort, dbName, tlsPosture)
	if err != nil {
		return d, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}

	config := map[string]string{
		"connector.class":     connectorClass,
		"tasks.max":           "1",
		"connection.url":      url,
		"connection.user":     creds["username"],
		"connection.password": creds["password"],
		// CDC traffic arrives on per-table topics prefixed with the
		// EventStream name (<stream>.<schema>.<table>), not the bare
		// EventStream/topic name itself — a literal "topics" list would
		// silently subscribe to nothing for a CDC-sourced stream (found
		// live: the Debezium leg of this task's own integration test).
		// topics.regex matches the stream's own bare topic name and any
		// prefixed one, mirroring s3sink.desiredConnectorConfig's identical
		// topics.regex exactly (the name is regex-quoted so a topic name
		// containing regex metacharacters, e.g. a '.', matches literally
		// instead of as a wildcard — docs/planning/07 §2.2).
		"topics.regex": "^" + regexp.QuoteMeta(b.SourceRef) + "(\\..*)?$",
	}

	// insert.mode (docs/planning/08 D3): insert (default) | upsert, already
	// shape-validated by ValidateBindingOptions.
	mode, _ := b.Options["mode"].(string)
	if mode == "" {
		mode = "insert"
	}
	config["insert.mode"] = mode
	if mode == "upsert" {
		if pkFields := stringList(b.Options["pkFields"]); len(pkFields) > 0 {
			config["pk.mode"] = "record_value"
			config["pk.fields"] = strings.Join(pkFields, ",")
		} else {
			// No explicit pk.fields: use every field of the record key as the
			// primary key — the natural fit for a CDC-sourced topic, whose
			// key IS the source table's primary key (Debezium's own
			// convention).
			config["pk.mode"] = "record_key"
		}
	}

	// Table name derivation (docs/planning/08 D3): an explicit options.table
	// wins; otherwise the connector's own default (table.name.format:
	// "${topic}") applies — the target table is named after the source
	// EventStream/topic. Since a topic name may contain hyphens (illegal in
	// most unquoted SQL identifiers), an explicit options.table is the
	// documented, recommended path whenever the EventStream's name isn't
	// already a valid bare SQL identifier.
	if t, _ := b.Options["table"].(string); t != "" {
		config["table.name.format"] = t
	}

	if v, ok := b.Options["autoCreate"].(bool); ok {
		config["auto.create"] = strconv.FormatBool(v)
	}
	if v, ok := b.Options["autoEvolve"].(bool); ok {
		config["auto.evolve"] = strconv.FormatBool(v)
	}

	// unwrap (docs/planning/08 D3, necessary plumbing beyond the task's own
	// options list): a CDC-sourced topic carries Debezium's own envelope
	// (before/after/source/op/ts_ms), not a flat row — writing that envelope
	// verbatim into a relational table is never what's wanted. Debezium's
	// own unwrap SMT (bundled in every debezium/connect-based image,
	// including this provider's own required worker image) extracts the
	// "after" state as the record's new value before the JDBC sink binds it
	// to columns; false (default) passes records through unmodified, for
	// non-CDC-sourced topics whose values are already flat.
	if v, ok := b.Options["unwrap"].(bool); ok && v {
		config["transforms"] = "unwrap"
		config["transforms.unwrap.type"] = "io.debezium.transforms.ExtractNewRecordState"
		config["transforms.unwrap.drop.tombstones"] = "true"
	}

	format, _ := b.Options["format"].(string)
	converterOverride, _ := b.Options["converter"].(string)
	if err := applyConverterConfig(config, format, converterOverride, req.SchemaRegistryURL); err != nil {
		return d, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}

	if b.DeadLetter != nil {
		applyDeadLetterConfig(config, b.DeadLetter, req, res.Metadata.Namespace)
	}

	d.name = res.Metadata.Name
	d.config = config
	d.engine = tgt.Engine
	d.dbName = dbName
	d.credsUser = creds["username"]
	d.credsPass = creds["password"]
	return d, nil
}

// stringList decodes a []any of strings (JSON-decoded option lists) into a
// []string, skipping non-string entries.
func stringList(v any) []string {
	list, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

// applyDeadLetterConfig mirrors s3sink.applyDeadLetterConfig verbatim
// (docs/planning/08 D6): sink Bindings share the identical Kafka Connect
// DLQ config shape regardless of connector, so both providers translate
// spec.options.deadLetter the same way. Not extracted to providerkit —
// three near-identical provider-local copies (this, s3sink's, and the
// applyConverterConfig pattern below) stay under docs/planning/08 G1's
// extraction bar (a helper with more parameters than lines saved is worse
// than the duplication); see that function's doc comment for the full
// replication-factor-resolution rationale.
func applyDeadLetterConfig(config map[string]string, dl *binding.DeadLetter, req reconciler.Request, namespace string) {
	config["errors.tolerance"] = dl.Tolerance
	config["errors.deadletterqueue.topic.name"] = dl.Stream
	config["errors.deadletterqueue.context.headers.enable"] = "true"
	replication := 1
	dlqKey := resource.Key{Namespace: resource.NormalizeNamespace(namespace), Kind: "EventStream", Name: dl.Stream}
	if dlqEnv, ok := req.Resources[dlqKey]; ok {
		if es, err := eventstream.FromEnvelope(dlqEnv); err == nil {
			replication = es.ReplicationFactor()
		}
	}
	config["errors.deadletterqueue.topic.replication.factor"] = strconv.Itoa(replication)
}

// applyConverterConfig mirrors debezium's/s3sink's identical helper verbatim
// (docs/planning/08 D1/D2) — not extracted to providerkit for the same
// reason applyDeadLetterConfig isn't (see its doc comment): a third
// provider-local copy stays under the G1 extraction bar. See either
// original's doc comment for the full registry-resolution rationale.
func applyConverterConfig(config map[string]string, format, converterOverride, registryURL string) error {
	switch format {
	case "", "json":
		class := "org.apache.kafka.connect.json.JsonConverter"
		if converterOverride != "" {
			class = converterOverride
		}
		config["key.converter"] = class
		config["value.converter"] = class
		config["key.converter.schemas.enable"] = "false"
		config["value.converter.schemas.enable"] = "false"
	case "avro", "protobuf":
		if registryURL == "" {
			return fmt.Errorf("stream format %q requires a schema registry endpoint, but none was resolved from the EventStream's Provider (has it been applied since configuration.schemaRegistry: enabled was set?)", format)
		}
		class := defaultConverterClass(format)
		if converterOverride != "" {
			class = converterOverride
		}
		config["key.converter"] = class
		config["value.converter"] = class
		config["key.converter.schema.registry.url"] = registryURL
		config["value.converter.schema.registry.url"] = registryURL
	default:
		return fmt.Errorf("options.format %q is not supported (must be one of: avro, protobuf)", format)
	}
	return nil
}

func defaultConverterClass(format string) string {
	switch format {
	case "avro":
		return "io.confluent.connect.avro.AvroConverter"
	case "protobuf":
		return "io.confluent.connect.protobuf.ProtobufConverter"
	default:
		return ""
	}
}

// reconcileConnector registers or updates the JDBC sink connector realizing
// a Binding(mode: sink, targetRef: Source), then verifies it reaches
// RUNNING. The preflight dial before registration mirrors debezium's
// reconcileConnector exactly (same two shapes: an external Connection's
// declared address dialed directly, or a managed Connection resolved
// through runtime.EnsureReachable) — a JDBC sink registered against an
// unreachable database fails just as unhelpfully as a CDC source connector
// would.
func (p *Provider) reconcileConnector(ctx context.Context, req reconciler.Request) (status.Status, error) {
	st := status.Status{}
	d, err := buildDesiredConnector(req)
	if err != nil {
		return st, err
	}
	config := d.config
	connectorName := d.name
	res, rt := req.Resource, req.Runtime
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}

	if d.preflightHost != "" {
		if err := providerkit.VerifyDatabaseConnection(ctx, d.engine, d.preflightHost, d.preflightPort, d.dbName, d.credsUser, d.credsPass, d.tlsPosture); err != nil {
			return st, fmt.Errorf("Binding %q: target database connection preflight failed before registering connector: %w", res.Metadata.Name, err)
		}
	} else if d.preflightConnectionName != "" {
		addr, closeAddr, err := rt.EnsureReachable(ctx, d.preflightConnectionName, d.preflightPort)
		if err != nil {
			return st, fmt.Errorf("Binding %q: resolve reachable address for Connection %q: %w", res.Metadata.Name, d.preflightConnectionName, err)
		}
		host, port, ok := hostPort(addr)
		if ok {
			err = providerkit.VerifyDatabaseConnection(ctx, d.engine, host, port, d.dbName, d.credsUser, d.credsPass, d.tlsPosture)
		}
		closeAddr()
		if !ok {
			return st, fmt.Errorf("Binding %q: reachable address %q for Connection %q is not a valid host:port", res.Metadata.Name, addr, d.preflightConnectionName)
		}
		if err != nil {
			return st, fmt.Errorf("Binding %q: target database connection preflight failed before registering connector: %w", res.Metadata.Name, err)
		}
	}

	name := naming.RuntimeObjectName(req.Provider)
	urls, closeURLs, err := workerURLs(ctx, rt, name, cfg)
	if err != nil {
		return st, err
	}
	defer closeURLs()
	if err := kafkaconnect.PutConnectorConfig(ctx, urls, connectorName, config); err != nil {
		return st, err
	}

	state, err := kafkaconnect.WaitConnectorRunning(ctx, urls, connectorName, 90*time.Second)
	now := time.Now()
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorNotRunning, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
		return st, err
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectorRunning}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"connector":  connectorName,
		"state":      state,
		"insertMode": config["insert.mode"],
	}
	return st, nil
}

func hostPort(address string) (string, int, bool) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, false
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, false
	}
	return host, port, true
}

func (p *Provider) Destroy(ctx context.Context, req reconciler.Request) error {
	res, rt := req.Resource, req.Runtime
	name := naming.RuntimeObjectName(req.Provider)
	switch res.Kind {
	case "Provider":
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return err
		}
		if err := rt.Remove(ctx, name); err != nil {
			return err
		}
		_ = rt.RemoveNetwork(ctx, providerkit.Network(cfg))
		return nil
	case "Binding":
		// A dead Connect worker takes its connectors with it (mirrors
		// s3sink/debezium's identical destroy-tolerance reasoning).
		if ctr, found, err := rt.Inspect(ctx, name); err != nil || !found || !ctr.Running {
			return err
		}
		cfg, err := provider.FromEnvelope(req.Provider)
		if err != nil {
			return err
		}
		urls, closeURLs, err := workerURLs(ctx, rt, name, cfg)
		if err != nil {
			return err
		}
		defer closeURLs()
		return kafkaconnect.DeleteConnector(ctx, urls, res.Metadata.Name)
	default:
		return fmt.Errorf("jdbcsink provider cannot destroy kind %s", res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, req reconciler.Request) (status.Status, error) {
	res, rt := req.Resource, req.Runtime
	st := status.Status{}
	now := time.Now()
	name := naming.RuntimeObjectName(req.Provider)
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return st, err
	}
	switch res.Kind {
	case "Provider":
		if n, declared := workersDeclared(cfg); declared && n > 1 {
			return providerkit.ProbeConnectWorkerSet(ctx, rt, name, n, now)
		}
		ctrState, found, err := rt.Inspect(ctx, name)
		if err != nil {
			return st, err
		}
		if found && ctrState.Healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectWorkerHealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectWorkerUnhealthy}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectWorkerUnhealthy}, now)
		}
		return st, nil
	case "Binding":
		urls, closeURLs, err := workerURLs(ctx, rt, name, cfg)
		if err != nil {
			return st, err
		}
		defer closeURLs()
		state, err := kafkaconnect.ConnectorState(ctx, urls, res.Metadata.Name)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorMissing, Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectorMissing}, now)
			return st, nil
		}
		if state != "RUNNING" {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorState + state}, now)
			st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
			return st, nil
		}
		if drifted := connectorConfigDrift(ctx, req, urls); len(drifted) > 0 {
			msg := "connector config differs from manifest at: " + strings.Join(drifted, ", ")
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorConfigDrift, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectorConfigDrift, Message: msg}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectorRunning}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	default:
		return st, fmt.Errorf("jdbcsink provider cannot probe kind %s", res.Kind)
	}
}

// connectorConfigDrift mirrors s3sink's/debezium's identical pattern:
// diffs the live connector config against the manifest-derived one,
// returning drifted key *names* only (values may carry credentials).
func connectorConfigDrift(ctx context.Context, req reconciler.Request, urls []string) []string {
	d, err := buildDesiredConnector(req)
	if err != nil {
		return []string{"(desired config unresolvable: " + err.Error() + ")"}
	}
	actual, err := kafkaconnect.GetConnectorConfig(ctx, urls, d.name)
	if err != nil {
		return []string{"(live config unreadable: " + err.Error() + ")"}
	}
	var drifted []string
	for k, want := range d.config {
		if actual[k] != want {
			drifted = append(drifted, k)
		}
	}
	sort.Strings(drifted)
	return drifted
}

// ValidateSpec implements SpecValidator: mirrors debezium's/s3sink's split
// between gate-independent shape checks here and HighAvailability gate
// enforcement in application/registry's runtime decorator.
// credentialsSecretRef is intentionally optional here (unlike s3sink's
// unconditional requirement): a target Source with its own external
// Connection secretRef needs no provider-level fallback at all — mirrors
// debezium's identical treatment of replicationSecretRef.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if v, _ := cfg.Configuration["image"].(string); v == "" {
		return fmt.Errorf("spec.configuration.image is required (a Connect image carrying the JDBC sink plugin; no stock image ships one)")
	}
	if v, _ := cfg.Configuration["bootstrapServers"].(string); v == "" {
		return fmt.Errorf("spec.configuration.bootstrapServers is required (the Kafka address the Connect worker joins)")
	}
	if ref, _ := cfg.Configuration["credentialsSecretRef"].(string); ref != "" && !cfg.HasSecretRef(ref) {
		return fmt.Errorf("configuration.credentialsSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
	}
	if v, declared := cfg.Configuration["workers"]; declared {
		n, ok := -1, false
		switch t := v.(type) {
		case int:
			n, ok = t, true
		case float64:
			if t == float64(int(t)) {
				n, ok = int(t), true
			}
		}
		if !ok || n < 1 {
			return fmt.Errorf("spec.configuration.workers must be a positive integer, got %v", v)
		}
		if _, pinned := cfg.Configuration["connectPort"]; pinned {
			return fmt.Errorf("spec.configuration.connectPort cannot be combined with spec.configuration.workers: each worker's host port is auto-assigned")
		}
	}
	return nil
}

// ValidateBindingOptions implements reconciler.BindingOptionsValidator.
//
// options.format is REQUIRED to be a schema-carrying value (avro or
// protobuf) — not merely accepted as an enhancement the way s3sink/debezium
// treat it. This is a stronger constraint than every other provider in this
// codebase and is deliberate: kafka-connect-jdbc's FieldsMetadata.extract
// (v10.9.6) only derives value columns from a Struct-typed record value and
// requires a Struct-typed key schema for pk.mode=record_key — a fully
// schemaless (Map-typed, json-format) record contributes zero columns and
// upsert mode throws outright. Rejecting json/unset format here at validate
// time (ADR 011: a manifest that validates must not half-apply) is more
// honest than letting the connector fail cryptically at registration.
func (p *Provider) ValidateBindingOptions(_ string, options map[string]any) error {
	format, _ := options["format"].(string)
	switch format {
	case "avro", "protobuf":
	default:
		return fmt.Errorf("options.format %q is not supported: jdbcsink requires a schema-carrying format (avro or protobuf) — the JDBC sink connector cannot derive column names/types from schemaless json records (must be one of: avro, protobuf)", format)
	}
	if v, ok := options["converter"]; ok {
		if s, _ := v.(string); s == "" {
			return fmt.Errorf("options.converter must be a non-empty string when set")
		}
	}
	if v, ok := options["mode"]; ok {
		mode, _ := v.(string)
		if mode != "insert" && mode != "upsert" {
			return fmt.Errorf("options.mode %q is not supported (must be one of: insert, upsert)", mode)
		}
	}
	if v, ok := options["table"]; ok {
		if s, _ := v.(string); s == "" {
			return fmt.Errorf("options.table must be a non-empty string when set")
		}
	}
	if v, ok := options["pkFields"]; ok {
		list, ok := v.([]any)
		if !ok || len(list) == 0 {
			return fmt.Errorf("options.pkFields must be a non-empty list of column names")
		}
		for _, f := range list {
			s, ok := f.(string)
			if !ok || s == "" {
				return fmt.Errorf("options.pkFields entries must be non-empty strings, got %v", f)
			}
		}
	}
	if v, ok := options["databaseHostname"]; ok {
		if s, _ := v.(string); s == "" {
			return fmt.Errorf("options.databaseHostname must be a non-empty string when set")
		}
	}
	if v, ok := options["databasePort"]; ok {
		switch n := v.(type) {
		case int:
			if n < 1 || n > 65535 {
				return fmt.Errorf("options.databasePort %d out of range 1-65535", n)
			}
		case float64:
			if n != float64(int(n)) || n < 1 || n > 65535 {
				return fmt.Errorf("options.databasePort %v must be an integer in 1-65535", n)
			}
		default:
			return fmt.Errorf("options.databasePort must be an integer, got %T", v)
		}
	}
	for _, key := range []string{"autoCreate", "autoEvolve", "unwrap"} {
		if v, ok := options[key]; ok {
			if _, ok := v.(bool); !ok {
				return fmt.Errorf("options.%s must be a boolean, got %T", key, v)
			}
		}
	}
	return nil
}
