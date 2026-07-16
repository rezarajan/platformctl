// Package debezium reconciles a Kafka Connect (Debezium) container and
// registers/updates CDC connectors against Bindings via the Connect REST API.
// Implements CDCCapableProvider and LineageAware (Phase 3).
package debezium

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	// Registers the MySQL database/sql driver for connector preflight.
	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/rezarajan/platformctl/internal/adapters/kafkaconnect"
	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Debezium container images are published on quay.io (the Docker Hub
// mirror stopped receiving 2.x tags).
const defaultImage = "quay.io/debezium/connect:2.7"

type Provider struct {
	providerRes resource.Envelope
	cfg         provider.Provider
	secrets     map[string]map[string]string
	resources   map[resource.Key]resource.Envelope
	lineage     *lineage.LineageEndpoint
	// lastConnector remembers the connector registered by the most recent
	// Reconcile so ConfigureLineage (called by the engine afterwards) can
	// update that connector's configuration in place.
	lastConnector string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Type() string { return "debezium" }

// SupportedSourceEngines implements CDCCapableProvider.
func (p *Provider) SupportedSourceEngines() []string {
	return []string{"postgres", "mysql", "mariadb", "mongodb"}
}

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) SetSecrets(secrets map[string]map[string]string) { p.secrets = secrets }

func (p *Provider) SetResourceSet(byKey map[resource.Key]resource.Envelope) { p.resources = byKey }

func (p *Provider) containerName() string { return p.providerRes.Metadata.Name }

func (p *Provider) connectPort() int {
	configured := 0
	if v, ok := p.cfg.Configuration["connectPort"]; ok {
		switch n := v.(type) {
		case int:
			configured = n
		case float64:
			configured = int(n)
		}
	}
	return hostport.Resolve(configured, p.containerName())
}

func (p *Provider) connectURL() string { return "http://127.0.0.1:" + strconv.Itoa(p.connectPort()) }

func (p *Provider) network() string {
	if n, ok := p.cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}

func (p *Provider) Reconcile(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	switch res.Kind {
	case "Provider":
		return p.reconcileWorker(ctx, rt)
	case "Binding":
		return p.reconcileConnector(ctx, res)
	default:
		return status.Status{}, fmt.Errorf("debezium provider cannot reconcile kind %s", res.Kind)
	}
}

func (p *Provider) reconcileWorker(ctx context.Context, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	name := p.containerName()
	image, _ := p.cfg.Configuration["image"].(string)
	if image == "" {
		image = defaultImage
	}
	bootstrap, _ := p.cfg.Configuration["bootstrapServers"].(string)
	if bootstrap == "" {
		return st, fmt.Errorf("Provider %q (type: debezium): spec.configuration.bootstrapServers is required", name)
	}
	labels := runtime.ManagedLabels(p.providerRes.Metadata.Namespace, "Provider", name, name)

	if err := rt.EnsureNetwork(ctx, runtime.NetworkSpec{Name: p.network(), Labels: labels}); err != nil {
		return st, err
	}
	_, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name:  name,
		Image: image,
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
		Networks: []string{p.network()},
		Ports:    []runtime.PortBinding{{HostPort: p.connectPort(), ContainerPort: 8083}},
		HealthCheck: &runtime.HealthCheck{
			Test:     []string{"CMD-SHELL", "curl -sf http://localhost:8083/connectors || exit 1"},
			Interval: 3 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  40,
		},
		Labels: labels,
	})
	if err != nil {
		return st, err
	}
	if err := rt.WaitHealthy(ctx, name, 180*time.Second); err != nil {
		return st, err
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ConnectWorkerHealthy"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	st.ProviderState = map[string]any{
		"connectUrl": p.connectURL(),
		endpoint.Key: endpoint.List{
			{Name: "connect-rest", Scheme: "http", Host: p.connectURL(), Internal: fmt.Sprintf("http://%s:8083", p.containerName())},
		}.ToState(),
	}
	return st, nil
}

// reconcileConnector registers or updates the Debezium connector realizing a
// Binding(mode: cdc), then verifies it reaches RUNNING.
func (p *Provider) reconcileConnector(ctx context.Context, res resource.Envelope) (status.Status, error) {
	st := status.Status{}
	b, err := binding.FromEnvelope(res)
	if err != nil {
		return st, err
	}
	if b.Mode != binding.ModeCDC {
		return st, fmt.Errorf("Binding %q: debezium realizes mode \"cdc\" only, got %q", res.Metadata.Name, b.Mode)
	}

	srcRef := resource.RefFromSpec(res.Spec, "sourceRef")
	srcEnv, ok := p.resources[srcRef.Key(res.Metadata.Namespace, "Source")]
	if !ok {
		return st, fmt.Errorf("Binding %q: sourceRef %q not found", res.Metadata.Name, b.SourceRef)
	}
	src, err := source.FromEnvelope(srcEnv)
	if err != nil {
		return st, err
	}
	dbName, _ := src.EngineConfig["database"].(string)
	schema, _ := src.EngineConfig["schema"].(string)
	if schema == "" {
		schema = "public"
	}

	connectorClass, enginePort, err := connectorFor(src.Engine)
	if err != nil {
		return st, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
	}

	// The database address, in preference order: the Source's Connection
	// (external sources — a managed Connection answers at its own name on
	// the shared network, an external one at its declared host), the
	// Source's Provider container name, then explicit options overrides.
	dbHost := ""
	dbPort := enginePort
	preflightHost := ""
	preflightPort := 0
	connSecretRef := ""
	if src.ProviderRef != nil {
		dbHost = *src.ProviderRef
	}
	if src.External && src.ConnectionRef != nil {
		connRef := resource.RefFromSpec(srcEnv.Spec, "connectionRef")
		if connEnv, ok := p.resources[connRef.Key(srcEnv.Metadata.Namespace, "Connection")]; ok {
			conn, err := connection.FromEnvelope(connEnv)
			if err != nil {
				return st, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
			}
			dbHost, dbPort = conn.Endpoint(connEnv.Metadata.Name)
			if host, port, ok := hostPort(conn.DialAddress()); ok {
				preflightHost, preflightPort = host, port
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
		return st, fmt.Errorf("Binding %q: cannot determine database hostname (no providerRef or Connection on Source, and no options.databaseHostname)", res.Metadata.Name)
	}

	// Credentials: the Connection's secretRef when the Source declares one
	// (and this provider lists it in spec.secretRefs so the engine resolved
	// it), else the provider-level replicationSecretRef.
	replRefName, _ := p.cfg.Configuration["replicationSecretRef"].(string)
	creds, ok := p.secrets[connSecretRef]
	if !ok {
		creds, ok = p.secrets[replRefName]
	}
	if !ok {
		return st, fmt.Errorf("Binding %q: debezium Provider %q has no resolved credentials — declare the Connection's secretRef or configuration.replicationSecretRef in spec.secretRefs", res.Metadata.Name, p.containerName())
	}

	topicPrefix := b.TargetRef // topics become <EventStream name>.<schema>.<table>
	connectorName := res.Metadata.Name

	config := map[string]string{
		"connector.class":                connectorClass,
		"database.hostname":              dbHost,
		"database.port":                  strconv.Itoa(dbPort),
		"database.user":                  creds["username"],
		"database.password":              creds["password"],
		"topic.prefix":                   topicPrefix,
		"key.converter":                  "org.apache.kafka.connect.json.JsonConverter",
		"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
		"key.converter.schemas.enable":   "false",
		"value.converter.schemas.enable": "false",
		// Redpanda does not auto-create topics by default; let Connect create
		// per-table CDC topics itself (single-node replication).
		"topic.creation.default.replication.factor": "1",
		"topic.creation.default.partitions":         "1",
	}
	if src.Engine == "postgres" {
		config["database.dbname"] = dbName
		config["plugin.name"] = "pgoutput"
	} else {
		// MySQL/MariaDB: the connector filters by database, not dbname, and
		// needs a unique server id per connector.
		config["database.include.list"] = dbName
		config["database.server.id"] = strconv.Itoa(184000 + len(connectorClass)) //nolint:mnd
		config["schema.history.internal.kafka.bootstrap.servers"], _ = p.cfg.Configuration["bootstrapServers"].(string)
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
	p.applyLineage(config)

	if preflightHost != "" {
		if err := verifyDatabaseConnection(ctx, src.Engine, preflightHost, preflightPort, dbName, creds["username"], creds["password"]); err != nil {
			return st, fmt.Errorf("Binding %q: database connection preflight failed before registering connector: %w", res.Metadata.Name, err)
		}
	}
	if err := kafkaconnect.PutConnectorConfig(ctx, p.connectURL(), connectorName, config); err != nil {
		return st, err
	}
	p.lastConnector = connectorName

	state, err := kafkaconnect.WaitConnectorRunning(ctx, p.connectURL(), connectorName, 90*time.Second)
	now := time.Now()
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorNotRunning", Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: "ConnectorState" + state}, now)
		return st, err
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ConnectorRunning"}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
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
// Debezium's native OpenLineage integration by updating the connector config.
func (p *Provider) ConfigureLineage(ctx context.Context, endpoint lineage.LineageEndpoint) error {
	p.lineage = &endpoint
	if p.lastConnector == "" {
		return nil // worker-level reconcile; endpoint applies at next connector registration
	}
	current, err := kafkaconnect.GetConnectorConfig(ctx, p.connectURL(), p.lastConnector)
	if err != nil {
		return err
	}
	p.applyLineage(current)
	return kafkaconnect.PutConnectorConfig(ctx, p.connectURL(), p.lastConnector, current)
}

func (p *Provider) applyLineage(config map[string]string) {
	if p.lineage == nil {
		return
	}
	config["openlineage.integration.enabled"] = "true"
	config["openlineage.integration.config.transport.type"] = "http"
	config["openlineage.integration.config.transport.url"] = p.lineage.URL
	if p.lineage.Namespace != "" {
		config["openlineage.integration.job.namespace"] = p.lineage.Namespace
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

func (p *Provider) Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error {
	switch res.Kind {
	case "Provider":
		if err := rt.Remove(ctx, p.containerName()); err != nil {
			return err
		}
		_ = rt.RemoveNetwork(ctx, p.network())
		return nil
	case "Binding":
		// A dead Connect worker takes its connectors with it; requiring a
		// live REST API here would make destroy unable to converge after
		// out-of-band failures.
		if ctr, found, err := rt.Inspect(ctx, p.containerName()); err != nil || !found || !ctr.Running {
			return err
		}
		return kafkaconnect.DeleteConnector(ctx, p.connectURL(), res.Metadata.Name)
	default:
		return fmt.Errorf("debezium provider cannot destroy kind %s", res.Kind)
	}
}

func (p *Provider) Probe(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	switch res.Kind {
	case "Provider":
		ctrState, found, err := rt.Inspect(ctx, p.containerName())
		if err != nil {
			return st, err
		}
		healthy := found && ctrState.Healthy
		if healthy {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ConnectWorkerHealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		} else {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectWorkerUnhealthy"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectWorkerUnhealthy"}, now)
		}
		return st, nil
	case "Binding":
		state, err := kafkaconnect.ConnectorState(ctx, p.connectURL(), res.Metadata.Name)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorMissing", Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectorMissing"}, now)
			return st, nil
		}
		if state == "RUNNING" {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ConnectorRunning"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		} else {
			// Declared state is a RUNNING connector; anything else is drift.
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorState" + state}, now)
			st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: "ConnectorState" + state}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectorState" + state}, now)
		}
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
	return nil
}
