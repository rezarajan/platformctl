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
const defaultImage = "quay.io/debezium/connect:2.7@sha256:f062d06e19be455ebf43cca662747f2ab6efbe4678954e7d64ac06055b8c7aff"

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

// connectURL is the Connect worker's configured-intent address — used only
// for the informational ProviderState field surfaced by reconcileWorker, not
// for actual REST calls (docs/planning/08 B8: those go through
// reachableAddr/EnsureReachable, since this "127.0.0.1:port" guess is wrong
// on Kubernetes).
func (p *Provider) connectURL() string { return "http://127.0.0.1:" + strconv.Itoa(p.connectPort()) }

// reachableURL returns an "http://host:port" this process can dial right
// now for the Connect worker's REST API, plus a close func that must always
// be called. Kafka Connect's REST API is stateless HTTP with no
// broker-style redirect protocol, so — unlike redpanda's Kafka admin
// connection — the resolved address can be used directly for one call, no
// placeholder/dialer-interception trick needed.
func (p *Provider) reachableURL(ctx context.Context, rt runtime.ContainerRuntime) (string, func() error, error) {
	addr, closeAddr, err := rt.EnsureReachable(ctx, p.containerName(), 8083)
	if err != nil {
		return "", nil, err
	}
	return "http://" + addr, closeAddr, nil
}

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
		return p.reconcileConnector(ctx, res, rt)
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
	ctrState, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
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
	// Observed binding, not intent (connectURL() stays intent-based for the
	// provider's own REST calls, a documented Docker-mode assumption).
	hostAddr := ctrState.HostAddr(8083)
	hostURL := ""
	if hostAddr != "" {
		hostURL = "http://" + hostAddr
	}
	st.ProviderState = map[string]any{
		"connectUrl": p.connectURL(),
		endpoint.Key: endpoint.List{
			{Name: "connect-rest", Scheme: "http", Host: hostURL, Internal: fmt.Sprintf("http://%s:8083", p.containerName()), Insecure: true},
		}.ToState(),
	}
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
	// B8): the forwarder's actual reachable address, like every other
	// provider's admin connection, cannot be a domain-layer
	// "127.0.0.1:port" guess.
	preflightHost           string
	preflightPort           int
	preflightConnectionName string
	credsUser               string
	credsPass               string
}

func (p *Provider) desiredConnector(res resource.Envelope) (desiredConnector, error) {
	d := desiredConnector{}
	b, err := binding.FromEnvelope(res)
	if err != nil {
		return d, err
	}
	if b.Mode != binding.ModeCDC {
		return d, fmt.Errorf("Binding %q: debezium realizes mode \"cdc\" only, got %q", res.Metadata.Name, b.Mode)
	}

	srcRef := resource.RefFromSpec(res.Spec, "sourceRef")
	srcEnv, ok := p.resources[srcRef.Key(res.Metadata.Namespace, "Source")]
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
		if connEnv, ok := p.resources[connRef.Key(srcEnv.Metadata.Namespace, "Connection")]; ok {
			conn, err := connection.FromEnvelope(connEnv)
			if err != nil {
				return d, fmt.Errorf("Binding %q: %w", res.Metadata.Name, err)
			}
			dbHost, dbPort = conn.Endpoint(connEnv.Metadata.Name)
			if conn.External {
				if host, port, ok := hostPort(conn.DialAddress()); ok {
					d.preflightHost, d.preflightPort = host, port
				}
			} else {
				d.preflightConnectionName, d.preflightPort = connEnv.Metadata.Name, conn.Port
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

	// Credentials: the Connection's secretRef when the Source declares one
	// (and this provider lists it in spec.secretRefs so the engine resolved
	// it), else the provider-level replicationSecretRef.
	replRefName, _ := p.cfg.Configuration["replicationSecretRef"].(string)
	creds, ok := p.secrets[connSecretRef]
	if !ok {
		creds, ok = p.secrets[replRefName]
	}
	if !ok {
		return d, fmt.Errorf("Binding %q: debezium Provider %q has no resolved credentials — declare the Connection's secretRef or configuration.replicationSecretRef in spec.secretRefs", res.Metadata.Name, p.containerName())
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
		config["database.server.id"] = strconv.FormatUint(uint64(serverID(connectorName)), 10)
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
func (p *Provider) reconcileConnector(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) (status.Status, error) {
	st := status.Status{}
	d, err := p.desiredConnector(res)
	if err != nil {
		return st, err
	}
	config := d.config
	connectorName := d.name
	p.applyLineage(config)

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
	url, closeURL, err := p.reachableURL(ctx, rt)
	if err != nil {
		return st, err
	}
	defer closeURL()
	if err := kafkaconnect.PutConnectorConfig(ctx, url, connectorName, config); err != nil {
		return st, err
	}
	p.lastConnector = connectorName

	state, err := kafkaconnect.WaitConnectorRunning(ctx, url, connectorName, 90*time.Second)
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
func (p *Provider) ConfigureLineage(ctx context.Context, endpoint lineage.LineageEndpoint, rt runtime.ContainerRuntime) error {
	p.lineage = &endpoint
	if p.lastConnector == "" {
		return nil // worker-level reconcile; endpoint applies at next connector registration
	}
	url, closeURL, err := p.reachableURL(ctx, rt)
	if err != nil {
		return err
	}
	defer closeURL()
	current, err := kafkaconnect.GetConnectorConfig(ctx, url, p.lastConnector)
	if err != nil {
		return err
	}
	p.applyLineage(current)
	return kafkaconnect.PutConnectorConfig(ctx, url, p.lastConnector, current)
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
		url, closeURL, err := p.reachableURL(ctx, rt)
		if err != nil {
			return err
		}
		defer closeURL()
		return kafkaconnect.DeleteConnector(ctx, url, res.Metadata.Name)
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
		url, closeURL, err := p.reachableURL(ctx, rt)
		if err != nil {
			return st, err
		}
		defer closeURL()
		state, err := kafkaconnect.ConnectorState(ctx, url, res.Metadata.Name)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorMissing", Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectorMissing"}, now)
			return st, nil
		}
		if state != "RUNNING" {
			// Declared state is a RUNNING connector; anything else is drift.
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorState" + state}, now)
			st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: "ConnectorState" + state}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectorState" + state}, now)
			return st, nil
		}
		// RUNNING is not enough (docs/planning/07 §2.1): the live config
		// must still match the manifest-derived one. Drifted key *names*
		// only — values may carry credentials and must never leak into
		// conditions.
		if drifted := p.connectorConfigDrift(ctx, res, url); len(drifted) > 0 {
			msg := "connector config differs from manifest at: " + strings.Join(drifted, ", ")
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorConfigDrift", Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ConnectorConfigDrift", Message: msg}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ConnectorRunning"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
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

// connectorConfigDrift diffs the live connector config against the
// manifest-derived one and returns the drifted key names (sorted), or nil
// when equivalent. Lineage keys (openlineage.*) are engine-managed after
// registration and deliberately excluded; extra live keys beyond the
// desired set are Connect-added defaults, not drift.
func (p *Provider) connectorConfigDrift(ctx context.Context, res resource.Envelope, url string) []string {
	d, err := p.desiredConnector(res)
	if err != nil {
		return []string{"(desired config unresolvable: " + err.Error() + ")"}
	}
	actual, err := kafkaconnect.GetConnectorConfig(ctx, url, d.name)
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
