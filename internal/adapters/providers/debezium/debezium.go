// Package debezium reconciles a Kafka Connect (Debezium) container and
// registers/updates CDC connectors against Bindings via the Connect REST API.
// Implements CDCCapableProvider and LineageAware (Phase 3).
package debezium

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/kafkaconnect"
	"github.com/rezarajan/platformctl/internal/domain/binding"
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
func (p *Provider) SupportedSourceEngines() []string { return []string{"postgres", "mysql", "mongodb"} }

func (p *Provider) SetProviderResource(env resource.Envelope) {
	p.providerRes = env
	p.cfg, _ = provider.FromEnvelope(env)
}

func (p *Provider) SetSecrets(secrets map[string]map[string]string) { p.secrets = secrets }

func (p *Provider) SetResourceSet(byKey map[resource.Key]resource.Envelope) { p.resources = byKey }

func (p *Provider) containerName() string { return p.providerRes.Metadata.Name }

func (p *Provider) connectPort() int {
	if v, ok := p.cfg.Configuration["connectPort"]; ok {
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		}
	}
	return 8083
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
	labels := map[string]string{
		runtime.LabelManagedBy:  runtime.ManagedByValue,
		runtime.LabelGeneration: name,
	}

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
	st.ProviderState = map[string]any{"connectUrl": p.connectURL()}
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

	srcEnv, ok := p.resources[resource.Key{Kind: "Source", Name: b.SourceRef}]
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

	// The database host is the Source's Provider container name on the shared
	// network; External sources use configuration from their connectionRef.
	dbHost := ""
	dbPort := 5432
	if src.ProviderRef != nil {
		dbHost = *src.ProviderRef
	}
	if h, ok := b.Options["databaseHostname"].(string); ok && h != "" {
		dbHost = h // explicit override (external sources)
	}
	if dbHost == "" {
		return st, fmt.Errorf("Binding %q: cannot determine database hostname (no providerRef on Source and no options.databaseHostname)", res.Metadata.Name)
	}

	replRefName, _ := p.cfg.Configuration["replicationSecretRef"].(string)
	creds, ok := p.secrets[replRefName]
	if !ok {
		return st, fmt.Errorf("Binding %q: debezium Provider %q needs configuration.replicationSecretRef naming a declared secretRef", res.Metadata.Name, p.containerName())
	}

	topicPrefix := b.TargetRef // topics become <EventStream name>.<schema>.<table>
	connectorName := res.Metadata.Name

	config := map[string]string{
		"connector.class":                "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname":              dbHost,
		"database.port":                  strconv.Itoa(dbPort),
		"database.user":                  creds["username"],
		"database.password":              creds["password"],
		"database.dbname":                dbName,
		"topic.prefix":                   topicPrefix,
		"plugin.name":                    "pgoutput",
		"key.converter":                  "org.apache.kafka.connect.json.JsonConverter",
		"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
		"key.converter.schemas.enable":   "false",
		"value.converter.schemas.enable": "false",
		// Redpanda does not auto-create topics by default; let Connect create
		// per-table CDC topics itself (single-node replication).
		"topic.creation.default.replication.factor": "1",
		"topic.creation.default.partitions":         "1",
	}
	if tables, ok := b.Options["tables"].([]any); ok && len(tables) > 0 {
		qualified := make([]string, 0, len(tables))
		for _, t := range tables {
			if s, ok := t.(string); ok {
				qualified = append(qualified, schema+"."+s)
			}
		}
		config["table.include.list"] = strings.Join(qualified, ",")
	}
	if mode, ok := b.Options["snapshotMode"].(string); ok && mode != "" {
		config["snapshot.mode"] = mode
	}
	p.applyLineage(config)

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

func (p *Provider) Destroy(ctx context.Context, res resource.Envelope, rt runtime.ContainerRuntime) error {
	switch res.Kind {
	case "Provider":
		if err := rt.Remove(ctx, p.containerName()); err != nil {
			return err
		}
		_ = rt.RemoveNetwork(ctx, p.network())
		return nil
	case "Binding":
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
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ConnectorState" + state}, now)
			st.SetCondition(status.Condition{Type: status.Degraded, Status: status.True, Reason: "ConnectorState" + state}, now)
		}
		return st, nil
	default:
		return st, fmt.Errorf("debezium provider cannot probe kind %s", res.Kind)
	}
}
