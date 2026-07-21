// Package debezium reconciles a Kafka Connect (Debezium) container and
// registers/updates CDC connectors against Bindings via the Connect REST API.
// Implements CDCCapableProvider and LineageAware (Phase 3).
package debezium

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	// Registers the MySQL database/sql driver for connector preflight.
	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/rezarajan/platformctl/internal/adapters/kafkaconnect"
	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Debezium container images are published on quay.io (the Docker Hub
// mirror stopped receiving 2.x tags).
const defaultImage = "quay.io/debezium/connect:2.7@sha256:f062d06e19be455ebf43cca662747f2ab6efbe4678954e7d64ac06055b8c7aff"

// Provider holds no cross-call state (docs/planning/08 F5): every method
// receives what it needs via reconciler.Request. ConfigureLineage in
// particular used to rely on a lastConnector field set by a just-completed
// Reconcile on the same instance — now it re-derives the connector name
// from the same Request instead (desiredConnector is deterministic).
type Provider struct{}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "debezium" }

// SupportedSourceEngines implements CDCCapableProvider.
func (p *Provider) SupportedSourceEngines() []string {
	return []string{"postgres", "mysql", "mariadb", "mongodb"}
}

func connectPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "connectPort")
}

// connectPorts builds the worker container's Ports declaration: workers <=
// 1 (undeclared) keeps the exact pre-C3 single-container behavior — a
// concrete, deterministically-derived (or pinned) HostPort via
// connectPort. workers > 1 deliberately leaves HostPort unset (0) so
// Docker/Kubernetes auto-assign a *distinct* port per ordinal — mirroring
// redpanda's reconcileBrokerSet (docs/adr/004's known limitation: "a fixed
// host-audience HostPort cannot be combined with Replicas > 1" — every
// ordinal would otherwise inherit the identical connectPort(cfg, name)
// value, since ordinalContainerSpec copies Ports verbatim, and ordinal 1's
// create would fail with a port-already-allocated error). ValidateSpec
// refuses a connectPort pin combined with workers, closing this the same
// way ADR 017 §a.4 closes it for redpanda's brokers.
func connectPorts(cfg provider.Provider, name string, workers int) []runtime.PortBinding {
	if workers > 1 {
		return []runtime.PortBinding{{ContainerPort: 8083, Audience: runtime.AudienceHost}}
	}
	return []runtime.PortBinding{{HostPort: connectPort(cfg, name), ContainerPort: 8083, Audience: runtime.AudienceHost}}
}

// reachableURL returns an "http://host:port" this process can dial right
// now for the Connect worker's REST API, plus a close func that must always
// be called. Kafka Connect's REST API is stateless HTTP with no
// broker-style redirect protocol, so — unlike redpanda's Kafka admin
// connection — the resolved address can be used directly for one call, no
// placeholder/dialer-interception trick needed.
func reachableURL(ctx context.Context, rt runtime.ContainerRuntime, name string) (string, func() error, error) {
	return providerkit.ReachableURL(ctx, rt, name, 8083)
}

// workersDeclared reads spec.configuration.workers (docs/planning/08 C3).
// declared=false (the key absent) selects the pre-C3 single-container
// shape, byte-for-byte; declared=true (any value >= 1, validated by
// ValidateSpec) opts into the `ContainerSpec.Replicas: N, StableIdentity:
// false` shape — Connect workers are natively distributed (group.id +
// internal topics) and hold no per-worker durable state, so unlike
// redpanda's brokers (docs/adr/017) no stable per-ordinal identity is
// needed; the workers rebalance connectors/tasks among themselves via
// Kafka's own consumer-group protocol.
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

// workerURLs resolves every currently-reachable Connect worker's REST base
// URL for this Provider (docs/planning/08 C3) — the input to
// kafkaconnect's multi-address failover. workers <= 1 (undeclared) is the
// pre-C3 single-address reachableURL path, unchanged; workers > 1 iterates
// ordinals via providerkit.ReachableURLs, skipping any that don't currently
// resolve (a killed worker just isn't offered as a failover candidate).
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
		return status.Status{}, fmt.Errorf("debezium provider cannot reconcile kind %s", req.Resource.Kind)
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
		image = defaultImage
	}
	bootstrap, _ := cfg.Configuration["bootstrapServers"].(string)
	if bootstrap == "" {
		// Graph-inferred (docs/planning/08 E2): the engine already resolved
		// this from the Binding(s) wired to this worker's target/source
		// EventStream, when unambiguous — req.KafkaBootstrapServers.
		bootstrap = req.KafkaBootstrapServers
	}
	if bootstrap == "" {
		return st, fmt.Errorf("Provider %q (type: debezium): spec.configuration.bootstrapServers is required (declare it, or wire a Binding on this Provider to an EventStream whose Provider publishes a Kafka bootstrap address)", name)
	}
	workers, workersDecl := workersDeclared(cfg)
	if workersDecl && workers < 1 {
		return st, fmt.Errorf("Provider %q (type: debezium): spec.configuration.workers must be a positive integer, got %v", name, cfg.Configuration["workers"])
	}
	ctrState, err := providerkit.EnsureInstance(ctx, rt, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
			Image: image,
			// Replicas/StableIdentity (docs/planning/08 C3, docs/adr/004):
			// workers undeclared -> ReplicaCount()==1, byte-for-byte the
			// pre-C3 single-container shape. StableIdentity is always
			// false — Connect workers hold no per-worker durable state and
			// rebalance connectors/tasks via Kafka's own consumer-group
			// protocol, so no per-ordinal volume/hostname identity is
			// needed (the D10/Trino-shaped branch of ADR 004, this
			// Provider's first real consumer).
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
			},
			Ports: connectPorts(cfg, name, workers),
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
	// Observed binding, not configured intent (docs/planning/09 F1: no
	// domain-layer address guess is kept around for this).
	hostAddr := ctrState.HostAddr(8083)
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	providerState := map[string]any{
		endpoint.Key: endpoint.List{
			{Name: "connect-rest", Scheme: "http", Host: hostURL, Internal: fmt.Sprintf("http://%s:8083", name), Insecure: true},
		}.ToState(),
		// The effective bootstrapServers this worker was actually started
		// with — declared or graph-inferred (docs/planning/08 E2) — so an
		// inferred default is as visible as an explicit one via
		// `platformctl state inspect`, not silently baked in.
		"bootstrapServers": bootstrap,
	}
	if workersDecl {
		// Echoed for operators (docs/planning/08 C3), mirroring redpanda's
		// "brokers" providerState field (docs/adr/017 question b) — the
		// declared worker count, not a per-ordinal breakdown (per-ordinal
		// liveness is Probe-time observation, never persisted, same rule).
		providerState["workers"] = workers
	}
	st.ProviderState = providerState
	return st, nil
}

// desiredConnector is the manifest-derived truth about one CDC connector:
// its name, full Connect config, and the preflight endpoint. Built
// identically by reconcile (to register) and Probe (to diff against the
// live config — docs/planning/07 §2.1: RUNNING with the wrong topic, table
// filter, or credentials is drift, not health).
type desiredConnector struct {
	name   string
	config map[string]string
	engine string
	dbName string
	// preflightHost/Port dial an external Connection's declared address
	// directly — no runtime involved, since it's outside platformctl's
	// management entirely. preflightConnectionName/Port instead name a
	// managed Connection's own forwarder container (the proxy provider
	// names its Connection-realizing container after the Connection, not
	// after itself — see proxy.reconcileConnection) + port, resolved
	// through runtime.EnsureReachable at reconcile time (docs/planning/08
	// B8, docs/planning/09 F1): the forwarder's actual reachable address,
	// like every other provider's admin connection, cannot be a
	// domain-layer loopback-address guess.
	preflightHost           string
	preflightPort           int
	preflightConnectionName string
	credsUser               string
	credsPass               string
}

func buildDesiredConnector(req reconciler.Request) (desiredConnector, error) {
	res := req.Resource
	d := desiredConnector{}
	b, err := binding.FromEnvelope(res)
	if err != nil {
		return d, err
	}
	if b.Mode != binding.ModeCDC {
		return d, fmt.Errorf("Binding %q: debezium realizes mode \"cdc\" only, got %q", res.Metadata.Name, b.Mode)
	}

	srcRef := resource.RefFromSpec(res.Spec, "sourceRef")
	srcEnv, ok := req.Resources[srcRef.Key(res.Metadata.Namespace, "Source")]
	if !ok {
		return d, fmt.Errorf("Binding %q: sourceRef %q not found", res.Metadata.Name, b.SourceRef)
	}
	src, err := source.FromEnvelope(srcEnv)
	if err != nil {
		return d, err
	}
	dbName, _ := src.EngineConfig["database"].(string)
	schema, _ := src.EngineConfig["schema"].(string)
	if schema == "" {
		schema = "public"
	}

	connectorClass, enginePort, err := connectorFor(src.Engine)
	if err != nil {
		return d, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}

	// The database address, in preference order: the Source's Connection
	// (external sources — a managed Connection answers at its own name on
	// the shared network, an external one at its declared host), the
	// Source's Provider container name, then explicit options overrides.
	dbHost := ""
	dbPort := enginePort
	connSecretRef := ""
	if src.ProviderRef != nil {
		dbHost = *src.ProviderRef
	}
	if src.External && src.ConnectionRef != nil {
		connRef := resource.RefFromSpec(srcEnv.Spec, "connectionRef")
		if connEnv, ok := req.Resources[connRef.Key(srcEnv.Metadata.Namespace, "Connection")]; ok {
			conn, err := connection.FromEnvelope(connEnv)
			if err != nil {
				return d, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
			}
			dbHost, dbPort = conn.Endpoint(naming.RuntimeObjectName(connEnv))
			if conn.External {
				if addr, ok := conn.ExternalAddress(); ok {
					if host, port, ok := hostPort(addr); ok {
						d.preflightHost, d.preflightPort = host, port
					}
				}
			} else {
				d.preflightConnectionName, d.preflightPort = naming.RuntimeObjectName(connEnv), conn.Port
			}
			if conn.SecretRef != nil {
				connSecretRef = *conn.SecretRef
			}
		}
	}
	if h, ok := b.Options["databaseHostname"].(string); ok && h != "" {
		dbHost = h // explicit override
	}
	if v, ok := b.Options["databasePort"]; ok {
		switch n := v.(type) {
		case int:
			dbPort = n
		case float64:
			dbPort = int(n)
		}
	}
	if dbHost == "" {
		return d, fmt.Errorf("Binding %q: cannot determine database hostname (no providerRef or Connection on Source, and no options.databaseHostname)", res.Metadata.Name)
	}

	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return d, err
	}

	// Credentials: the Connection's secretRef when the Source declares one
	// (and this provider lists it in spec.secretRefs so the engine resolved
	// it), else the provider-level replicationSecretRef.
	replRefName, _ := cfg.Configuration["replicationSecretRef"].(string)
	creds, ok := req.Secrets[connSecretRef]
	if !ok {
		creds, ok = req.Secrets[replRefName]
	}
	if !ok {
		return d, fmt.Errorf("Binding %q: debezium Provider %q has no resolved credentials — declare the Connection's secretRef or configuration.replicationSecretRef in spec.secretRefs", res.Metadata.Name, naming.RuntimeObjectName(req.Provider))
	}

	topicPrefix := b.TargetRef // topics become <EventStream name>.<schema>.<table>
	connectorName := res.Metadata.Name

	config := map[string]string{
		"connector.class":   connectorClass,
		"database.hostname": dbHost,
		"database.port":     strconv.Itoa(dbPort),
		"database.user":     creds["username"],
		"database.password": creds["password"],
		"topic.prefix":      topicPrefix,
		// Redpanda does not auto-create topics by default; let Connect create
		// per-table CDC topics itself (single-node replication).
		"topic.creation.default.replication.factor": "1",
		"topic.creation.default.partitions":         "1",
	}
	format, _ := b.Options["format"].(string)
	converterOverride, _ := b.Options["converter"].(string)
	if err := applyConverterConfig(config, format, converterOverride, req.SchemaRegistryURL); err != nil {
		return d, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}
	if src.Engine == "postgres" {
		config["database.dbname"] = dbName
		config["plugin.name"] = "pgoutput"
	} else {
		// MySQL/MariaDB: the connector filters by database, not dbname, and
		// needs a unique server id per connector.
		config["database.include.list"] = dbName
		config["database.server.id"] = strconv.FormatUint(uint64(serverID(connectorName)), 10)
		workerBootstrap, _ := cfg.Configuration["bootstrapServers"].(string)
		if workerBootstrap == "" {
			// Same graph-inferred fallback reconcileWorker used to start
			// this worker's own BOOTSTRAP_SERVERS (docs/planning/08 E2) —
			// the schema-history client must join the identical broker.
			workerBootstrap = req.KafkaBootstrapServers
		}
		config["schema.history.internal.kafka.bootstrap.servers"] = workerBootstrap
		config["schema.history.internal.kafka.topic"] = topicPrefix + ".schema-history"
	}
	if tables, ok := b.Options["tables"].([]any); ok && len(tables) > 0 {
		qualifier := schema
		if src.Engine != "postgres" {
			qualifier = dbName // MySQL/MariaDB qualify tables by database
		}
		qualified := make([]string, 0, len(tables))
		for _, t := range tables {
			if s, ok := t.(string); ok {
				qualified = append(qualified, qualifier+"."+s)
			}
		}
		config["table.include.list"] = strings.Join(qualified, ",")
	}
	if mode, ok := b.Options["snapshotMode"].(string); ok && mode != "" {
		config["snapshot.mode"] = mode
	}

	d.name = connectorName
	d.config = config
	d.engine = src.Engine
	d.dbName = dbName
	d.credsUser = creds["username"]
	d.credsPass = creds["password"]
	return d, nil
}

// reconcileConnector registers or updates the Debezium connector realizing a
// Binding(mode: cdc), then verifies it reaches RUNNING.
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
		if err := verifyDatabaseConnection(ctx, d.engine, d.preflightHost, d.preflightPort, d.dbName, d.credsUser, d.credsPass); err != nil {
			return st, fmt.Errorf("Binding %q: database connection preflight failed before registering connector: %w", res.Metadata.Name, err)
		}
	} else if d.preflightConnectionName != "" {
		addr, closeAddr, err := rt.EnsureReachable(ctx, d.preflightConnectionName, d.preflightPort)
		if err != nil {
			return st, fmt.Errorf("Binding %q: resolve reachable address for Connection %q: %w", res.Metadata.Name, d.preflightConnectionName, err)
		}
		host, port, ok := hostPort(addr)
		if ok {
			err = verifyDatabaseConnection(ctx, d.engine, host, port, d.dbName, d.credsUser, d.credsPass)
		}
		closeAddr()
		if !ok {
			return st, fmt.Errorf("Binding %q: reachable address %q for Connection %q is not a valid host:port", res.Metadata.Name, addr, d.preflightConnectionName)
		}
		if err != nil {
			return st, fmt.Errorf("Binding %q: database connection preflight failed before registering connector: %w", res.Metadata.Name, err)
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
		// ReasonConnectorState is a prefix: the observed live connector
		// state is appended so the reason names the exact state without a
		// separate Message (docs/planning/08 G4).
		st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
		return st, err
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonConnectorRunning}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"connector": connectorName,
		"state":     state,
		// Which tables the connector actually captures — empty means every
		// table in the publication. Surfaced so `status -o json` answers
		// "why isn't my table streaming" without a Connect API call.
		"tableIncludeList": config["table.include.list"],
	}
	return st, nil
}

// ConfigureLineage implements LineageAware: forwards the endpoint into
// Debezium's native OpenLineage integration by updating the connector
// config. Re-derives the connector's desired config from req (rather than
// relying on state a just-completed Reconcile left behind) so the method is
// self-contained per call (docs/planning/08 F5) — a no-op when req.Resource
// is the Provider's own worker-level reconcile, since no connector exists
// to update yet; the endpoint applies at the Binding's next reconcile.
func (p *Provider) ConfigureLineage(ctx context.Context, req reconciler.Request, ep lineage.LineageEndpoint) error {
	if req.Resource.Kind != "Binding" {
		return nil
	}
	d, err := buildDesiredConnector(req)
	if err != nil {
		return err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return err
	}
	name := naming.RuntimeObjectName(req.Provider)
	urls, closeURLs, err := workerURLs(ctx, req.Runtime, name, cfg)
	if err != nil {
		return err
	}
	defer closeURLs()
	current, err := kafkaconnect.GetConnectorConfig(ctx, urls, d.name)
	if err != nil {
		return err
	}
	applyLineage(current, ep)
	return kafkaconnect.PutConnectorConfig(ctx, urls, d.name, current)
}

// applyConverterConfig sets the key/value converter config for a Binding's
// spec.options.format (docs/planning/08 D1): json (default) needs no
// registry; avro/protobuf require registryURL, resolved by the engine from
// the EventStream's own realizing Provider — compatibility.Check already
// refused an avro/protobuf format against a registry-less provider chain at
// validate time, so an empty registryURL reaching here means the upstream
// Provider hasn't reconciled yet in this run (defensive, not expected to
// trigger given dependency-graph ordering). converterOverride is an advanced
// escape hatch: an explicit converter class wins over the format-derived
// default for both key and value converters (e.g. a non-Confluent-compatible
// Avro/Protobuf converter implementation).
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
			return fmt.Errorf("options.format %q requires a schema registry endpoint, but none was resolved from the EventStream's Provider (has it been applied since configuration.schemaRegistry: enabled was set?)", format)
		}
		class := defaultConverterClass(format)
		if converterOverride != "" {
			class = converterOverride
		}
		config["key.converter"] = class
		config["value.converter"] = class
		config["key.converter.schema.registry.url"] = registryURL
		config["value.converter.schema.registry.url"] = registryURL
		// Debezium derives Avro record namespaces from the topic prefix —
		// which is this platform's EventStream name, a DNS label that may
		// legally contain hyphens. Hyphens are illegal in Avro names, so
		// without sanitization every hyphenated resource name makes the
		// registry reject the schema (422 "Invalid namespace") and the
		// task FAILs after registration. Debezium's own adjustment modes
		// rewrite illegal characters to underscores; topic and subject
		// names are unaffected (they permit hyphens).
		config["schema.name.adjustment.mode"] = "avro"
		config["field.name.adjustment.mode"] = "avro"
	default:
		return fmt.Errorf("options.format %q is not supported (must be one of: json, avro, protobuf)", format)
	}
	return nil
}

// defaultConverterClass maps a schema-carrying format to the Confluent
// Connect converter class Redpanda's built-in registry is compatible with.
func defaultConverterClass(format string) string {
	switch format {
	case "avro":
		return "io.confluent.connect.avro.AvroConverter"
	case "protobuf":
		return "io.confluent.connect.protobuf.ProtobufConverter"
	default:
		return "org.apache.kafka.connect.json.JsonConverter"
	}
}

func applyLineage(config map[string]string, ep lineage.LineageEndpoint) {
	config["openlineage.integration.enabled"] = "true"
	config["openlineage.integration.config.transport.type"] = "http"
	config["openlineage.integration.config.transport.url"] = ep.URL
	if ep.Namespace != "" {
		config["openlineage.integration.job.namespace"] = ep.Namespace
	}
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

func verifyDatabaseConnection(ctx context.Context, engine, host string, port int, dbName, user, pass string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	switch engine {
	case "postgres":
		u := url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(user, pass),
			Host:   host + ":" + strconv.Itoa(port),
			Path:   "/" + dbName,
		}
		q := u.Query()
		q.Set("sslmode", "disable")
		u.RawQuery = q.Encode()
		conn, err := pgx.Connect(ctx, u.String())
		if err != nil {
			return fmt.Errorf("connect to postgres %s:%d/%s as %q: %w", host, port, dbName, user, err)
		}
		defer conn.Close(ctx)
		return nil
	case "mysql", "mariadb":
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=10s", user, pass, host, port, dbName))
		if err != nil {
			return fmt.Errorf("connect to mysql %s:%d/%s as %q: %w", host, port, dbName, user, err)
		}
		defer db.Close()
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("connect to mysql %s:%d/%s as %q: %w", host, port, dbName, user, err)
		}
		return nil
	default:
		return nil
	}
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
		// A dead Connect worker takes its connectors with it; requiring a
		// live REST API here would make destroy unable to converge after
		// out-of-band failures. Inspect(name) covers both shapes: the
		// literal container (workers undeclared) or the replica-set
		// aggregate (docs/adr/004) — "at least one member running" for the
		// latter.
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
		return fmt.Errorf("debezium provider cannot destroy kind %s", res.Kind)
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
		healthy := found && ctrState.Healthy
		if healthy {
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
			// Declared state is a RUNNING connector; anything else is drift.
			// ReasonConnectorState is a prefix: the observed live connector
			// state is appended so the reason names the exact state without
			// a separate Message (docs/planning/08 G4).
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonConnectorState + state}, now)
			st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonConnectorState + state}, now)
			return st, nil
		}
		// RUNNING is not enough (docs/planning/07 §2.1): the live config
		// must still match the manifest-derived one. Drifted key *names*
		// only — values may carry credentials and must never leak into
		// conditions.
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
		return st, fmt.Errorf("debezium provider cannot probe kind %s", res.Kind)
	}
}

// connectorFor resolves the Debezium connector class and default port for a
// Source engine. SupportedSourceEngines and this table must stay in step.
func connectorFor(engine string) (class string, port int, err error) {
	switch engine {
	case "postgres":
		return "io.debezium.connector.postgresql.PostgresConnector", 5432, nil
	case "mysql", "mariadb":
		return "io.debezium.connector.mysql.MySqlConnector", 3306, nil
	case "mongodb":
		return "io.debezium.connector.mongodb.MongoDbConnector", 27017, nil
	default:
		return "", 0, fmt.Errorf("no Debezium connector mapping for source engine %q", engine)
	}
}

// ValidateSpec implements SpecValidator: a mis-wired worker fails at
// validate, never as a half-applied platform.
func (p *Provider) ValidateSpec(cfg provider.Provider) error {
	if v, _ := cfg.Configuration["bootstrapServers"].(string); v == "" {
		return fmt.Errorf("spec.configuration.bootstrapServers is required (the Kafka address the Connect worker joins)")
	}
	if ref, _ := cfg.Configuration["replicationSecretRef"].(string); ref != "" && !cfg.HasSecretRef(ref) {
		return fmt.Errorf("configuration.replicationSecretRef %q must also be listed in spec.secretRefs for the engine to resolve it", ref)
	}
	// workers > 1 (docs/planning/08 C3) requires the HighAvailability gate
	// (enforced at validate by cmd/platformctl's checkHighAvailabilityGate,
	// the same mechanism as redpanda's brokers — docs/adr/017 §a.8); this
	// check only guards the value's own shape, mirroring redpanda's
	// ValidateSpec split between gate-independent shape checks here and
	// gate enforcement in loadAndValidate.
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
		// Host-port pin cannot be combined with the replica-set shape:
		// every ordinal's host port is auto-assigned (connectPorts,
		// mirroring docs/adr/017 §a.4's identical refusal for redpanda's
		// brokers).
		if _, pinned := cfg.Configuration["connectPort"]; pinned {
			return fmt.Errorf("spec.configuration.connectPort cannot be combined with spec.configuration.workers: each worker's host port is auto-assigned")
		}
	}
	return nil
}

// connectorConfigDrift diffs the live connector config against the
// manifest-derived one and returns the drifted key names (sorted), or nil
// when equivalent. Lineage keys (openlineage.*) are engine-managed after
// registration and deliberately excluded; extra live keys beyond the
// desired set are Connect-added defaults, not drift.
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

// ValidateBindingOptions implements reconciler.BindingOptionsValidator:
// every option this provider consumes at reconcile time is checked at
// validate time, so a typo'd snapshot mode or malformed table list fails
// before any infrastructure is touched.
func (p *Provider) ValidateBindingOptions(_ string, options map[string]any) error {
	if v, ok := options["tables"]; ok {
		list, ok := v.([]any)
		if !ok || len(list) == 0 {
			return fmt.Errorf("options.tables must be a non-empty list of table names")
		}
		for _, t := range list {
			s, ok := t.(string)
			if !ok || s == "" {
				return fmt.Errorf("options.tables entries must be non-empty strings, got %v", t)
			}
		}
	}
	if v, ok := options["snapshotMode"]; ok {
		mode, _ := v.(string)
		// The union of modes Debezium's postgres and mysql connectors accept.
		valid := map[string]bool{
			"always": true, "initial": true, "initial_only": true, "no_data": true,
			"never": true, "when_needed": true, "schema_only": true, "schema_only_recovery": true,
		}
		if !valid[mode] {
			return fmt.Errorf("options.snapshotMode %q is not a Debezium snapshot mode (e.g. initial, never, when_needed, no_data)", mode)
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
	// format/converter (docs/planning/08 D1): shape only — whether avro/
	// protobuf actually has a registry to talk to is a compatibility.Check
	// concern (it needs the EventStream's Provider resolved), not this
	// provider's own option-shape validation.
	if v, ok := options["format"]; ok {
		format, _ := v.(string)
		switch format {
		case "json", "avro", "protobuf":
		default:
			return fmt.Errorf("options.format %q is not supported (must be one of: json, avro, protobuf)", format)
		}
	}
	if v, ok := options["converter"]; ok {
		if s, _ := v.(string); s == "" {
			return fmt.Errorf("options.converter must be a non-empty string when set")
		}
	}
	return nil
}

// serverID derives a stable, effectively-unique MySQL replication server id
// from the connector name. MySQL requires every replication client on a
// server to carry a distinct non-zero server_id; the previous formula
// (184000 + len(connectorClass)) was constant per engine, so two MySQL
// connectors against the same server would kick each other's binlog session
// off (docs/planning/07 §2.2). FNV-1a over the name is deterministic (plan
// stays reproducible, NFR-1) and collisions between the handful of
// connectors a deployment runs are negligible. Range: [100000, 2^32).
func serverID(connectorName string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(connectorName))
	const floor = 100000
	v := h.Sum32()
	if v < floor {
		v += floor
	}
	return v
}
