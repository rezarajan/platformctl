package compatibility

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/domain/versionprofile"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

type stubProvider struct{ typeName string }

func (s stubProvider) Type() string { return s.typeName }
func (s stubProvider) Reconcile(context.Context, resource.Envelope, runtime.ContainerRuntime) (status.Status, error) {
	return status.Status{}, nil
}
func (s stubProvider) Destroy(context.Context, resource.Envelope, runtime.ContainerRuntime) error {
	return nil
}
func (s stubProvider) Probe(context.Context, resource.Envelope, runtime.ContainerRuntime) (status.Status, error) {
	return status.Status{}, nil
}

type cdcStub struct{ stubProvider }

func (cdcStub) SupportedSourceEngines() []string { return []string{"postgres", "mysql", "mongodb"} }

type sinkStub struct{ stubProvider }

func (sinkStub) SupportedSinkFormats() []string { return []string{"parquet", "json"} }

type externalConfigStub struct{ stubProvider }

func (externalConfigStub) ConfigureExternal(context.Context, resource.Envelope, runtime.ContainerRuntime) (status.Status, error) {
	return status.Status{}, nil
}

// versionedStub is a local double for reconciler.VersionedProvider — it
// exercises compatibility's use of VersionCatalog() without importing a
// concrete technology adapter (docs/planning/07 §layering invariant,
// docs/remediation/F-004). The real postgres catalog stays covered by the
// CDC/lakehouse integration suites.
type versionedStub struct {
	stubProvider
	catalog versionprofile.Catalog
}

func (v versionedStub) VersionCatalog() versionprofile.Catalog { return v.catalog }

func envelope(kind, name string, spec map[string]any) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = kind
	e.Metadata.Name = name
	e.Spec = spec
	return e
}

func cdcManifests(engine string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "postgres-cdc", map[string]any{
			"type":    "debezium",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-postgres", map[string]any{
			"type":    "postgres",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Source", "student-database", map[string]any{
			"engine":      engine,
			"providerRef": map[string]any{"name": "local-postgres"},
		}),
		envelope("EventStream", "attendance-events", map[string]any{
			"providerRef": map[string]any{"name": "postgres-cdc"},
		}),
		envelope("Binding", "student-db-to-events", map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "student-database"},
			"targetRef":   map[string]any{"name": "attendance-events"},
			"providerRef": map[string]any{"name": "postgres-cdc"},
		}),
	}
}

func resolver(impl reconciler.Provider) ProviderResolver {
	return func(string) (reconciler.Provider, error) { return impl, nil }
}

// TestUnsupportedEngineErrorFormat covers the Phase 3 exit criterion: the
// validate-time error matches the documented shape exactly
// (docs/planning/02-architecture.md §5.2) — on the character, not in spirit.
func TestUnsupportedEngineErrorFormat(t *testing.T) {
	err := Check(cdcManifests("sqlite"), resolver(cdcStub{stubProvider{"debezium"}}))
	if err == nil {
		t.Fatal("validate accepted an unsupported source engine")
	}
	want := `Binding "student-db-to-events": Provider "postgres-cdc" (type: debezium)
does not support source engine "sqlite" (supported: mongodb, mysql, postgres)`
	if err.Error() != want {
		t.Errorf("error format mismatch\ngot:\n%s\nwant:\n%s", err.Error(), want)
	}
}

// TestNonCDCCapableProviderRejected: a Binding referencing a Provider that
// does not implement CDCCapableProvider fails at validate, not apply.
func TestNonCDCCapableProviderRejected(t *testing.T) {
	err := Check(cdcManifests("postgres"), resolver(stubProvider{"redpanda"}))
	if err == nil {
		t.Fatal("validate accepted a non-CDC-capable provider behind a cdc Binding")
	}
	if !strings.Contains(err.Error(), `does not support mode "cdc"`) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSupportedEngineAccepted(t *testing.T) {
	if err := Check(cdcManifests("postgres"), resolver(cdcStub{stubProvider{"debezium"}})); err != nil {
		t.Fatalf("valid CDC binding rejected: %v", err)
	}
}

func sinkManifests(format string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "s3-sink", map[string]any{
			"type":    "s3sink",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-minio", map[string]any{
			"type":    "minio",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("EventStream", "attendance-events", map[string]any{
			"providerRef": map[string]any{"name": "s3-sink"},
		}),
		envelope("Dataset", "attendance-raw", map[string]any{
			"providerRef": map[string]any{"name": "local-minio"},
			"bucket":      "raw-events",
			"format":      format,
		}),
		envelope("Binding", "attendance-events-to-lake", map[string]any{
			"mode":        "sink",
			"sourceRef":   map[string]any{"name": "attendance-events"},
			"targetRef":   map[string]any{"name": "attendance-raw"},
			"providerRef": map[string]any{"name": "s3-sink"},
		}),
	}
}

// TestUnsupportedSinkFormatErrorFormat covers the Phase 4 exit criterion:
// a Binding(mode: sink) whose Dataset format the provider cannot write fails
// at validate with the documented error shape
// (docs/planning/02-architecture.md §5.2).
func TestUnsupportedSinkFormatErrorFormat(t *testing.T) {
	err := Check(sinkManifests("avro"), resolver(sinkStub{stubProvider{"s3sink"}}))
	if err == nil {
		t.Fatal("validate accepted an unsupported sink format")
	}
	want := `Binding "attendance-events-to-lake": Provider "s3-sink" (type: s3sink)
does not support sink format "avro" (supported: json, parquet)`
	if err.Error() != want {
		t.Errorf("error format mismatch\ngot:\n%s\nwant:\n%s", err.Error(), want)
	}
}

// TestNonSinkCapableProviderRejected: a Binding(mode: sink) referencing a
// Provider that does not implement SinkCapableProvider fails at validate,
// not apply.
func TestNonSinkCapableProviderRejected(t *testing.T) {
	err := Check(sinkManifests("json"), resolver(stubProvider{"redpanda"}))
	if err == nil {
		t.Fatal("validate accepted a non-sink-capable provider behind a sink Binding")
	}
	if !strings.Contains(err.Error(), `does not support mode "sink"`) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSupportedSinkFormatAccepted(t *testing.T) {
	if err := Check(sinkManifests("json"), resolver(sinkStub{stubProvider{"s3sink"}})); err != nil {
		t.Fatalf("valid sink binding rejected: %v", err)
	}
}

type dbSinkStub struct{ stubProvider }

func (dbSinkStub) SupportedSinkEngines() []string { return []string{"postgres"} }

type ingestStub struct{ stubProvider }

func (ingestStub) SupportedIngestFormats() []string { return []string{"json", "parquet"} }

func dbSinkManifests(engine string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "jdbc-sink", map[string]any{
			"type":    "jdbcsink",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-postgres", map[string]any{
			"type":    "postgres",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("EventStream", "attendance-events", map[string]any{
			"providerRef": map[string]any{"name": "jdbc-sink"},
		}),
		envelope("Source", "reporting-db", map[string]any{
			"engine":      engine,
			"providerRef": map[string]any{"name": "local-postgres"},
		}),
		envelope("Binding", "events-to-reporting-db", map[string]any{
			"mode":        "sink",
			"sourceRef":   map[string]any{"name": "attendance-events"},
			"targetRef":   map[string]any{"name": "reporting-db"},
			"providerRef": map[string]any{"name": "jdbc-sink"},
		}),
	}
}

// TestDatabaseAsSink: a Source (an engine-backed database) is a legitimate
// sink-mode target — the taxonomy encodes direction in the Binding, not the
// noun. Capability gates which providers can realize it.
func TestDatabaseAsSink(t *testing.T) {
	if err := Check(dbSinkManifests("postgres"), resolver(dbSinkStub{stubProvider{"jdbcsink"}})); err != nil {
		t.Fatalf("sink Binding into a Source rejected despite capability: %v", err)
	}
	err := Check(dbSinkManifests("postgres"), resolver(sinkStub{stubProvider{"s3sink"}}))
	if err == nil || !strings.Contains(err.Error(), "database-sink capability") {
		t.Errorf("provider without DatabaseSinkCapableProvider accepted, err = %v", err)
	}
	err = Check(dbSinkManifests("mysql"), resolver(dbSinkStub{stubProvider{"jdbcsink"}}))
	if err == nil || !strings.Contains(err.Error(), `does not support sink engine "mysql"`) {
		t.Errorf("unsupported sink engine accepted, err = %v", err)
	}
}

func ingestManifests() []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "s3-ingest", map[string]any{
			"type":    "s3source",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-minio", map[string]any{
			"type":    "minio",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Dataset", "landed-files", map[string]any{
			"providerRef": map[string]any{"name": "local-minio"},
			"bucket":      "raw",
			"format":      "json",
		}),
		envelope("EventStream", "replayed-events", map[string]any{
			"providerRef": map[string]any{"name": "s3-ingest"},
		}),
		envelope("Binding", "files-to-events", map[string]any{
			"mode":        "ingest",
			"sourceRef":   map[string]any{"name": "landed-files"},
			"targetRef":   map[string]any{"name": "replayed-events"},
			"providerRef": map[string]any{"name": "s3-ingest"},
		}),
	}
}

// TestObjectStoreAsSource: a Dataset is a legitimate ingest-mode origin.
func TestObjectStoreAsSource(t *testing.T) {
	if err := Check(ingestManifests(), resolver(ingestStub{stubProvider{"s3source"}})); err != nil {
		t.Fatalf("ingest Binding from a Dataset rejected despite capability: %v", err)
	}
	err := Check(ingestManifests(), resolver(stubProvider{"redpanda"}))
	if err == nil || !strings.Contains(err.Error(), "ingest capability") {
		t.Errorf("provider without IngestCapableProvider accepted, err = %v", err)
	}
}

// TestDisallowedPairingListsAlternatives: nonsense pairings name what the
// mode actually connects.
func TestDisallowedPairingListsAlternatives(t *testing.T) {
	ms := ingestManifests()
	// ingest with the refs swapped: EventStream -> Dataset is not an ingest pairing.
	ms[4] = envelope("Binding", "files-to-events", map[string]any{
		"mode":        "ingest",
		"sourceRef":   map[string]any{"name": "replayed-events"},
		"targetRef":   map[string]any{"name": "landed-files"},
		"providerRef": map[string]any{"name": "s3-ingest"},
	})
	err := Check(ms, resolver(ingestStub{stubProvider{"s3source"}}))
	if err == nil || !strings.Contains(err.Error(), "allowed pairings: Dataset->EventStream") {
		t.Errorf("swapped ingest refs accepted or unclear, err = %v", err)
	}
}

type catalogStub struct{ stubProvider }

func (catalogStub) SupportedCatalogEngines() []string { return []string{"nessie"} }

type connStub struct{ stubProvider }

func (connStub) SupportedConnectionSchemes() []string { return []string{"tcp"} }

func catalogManifests(engine string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "catalog-svc", map[string]any{
			"type":    "nessie",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Catalog", "lakehouse-catalog", map[string]any{
			"engine":      engine,
			"providerRef": map[string]any{"name": "catalog-svc"},
		}),
	}
}

// TestCatalogEngineCapability: a Catalog names an engine its provider must
// declare — the Catalog kind stays provider-agnostic, capability gates it.
func TestCatalogEngineCapability(t *testing.T) {
	if err := Check(catalogManifests("nessie"), resolver(catalogStub{stubProvider{"nessie"}})); err != nil {
		t.Fatalf("valid catalog rejected: %v", err)
	}
	err := Check(catalogManifests("hive"), resolver(catalogStub{stubProvider{"nessie"}}))
	if err == nil {
		t.Fatal("validate accepted an unsupported catalog engine")
	}
	want := `Catalog "lakehouse-catalog": Provider "catalog-svc" (type: nessie)
does not support catalog engine "hive" (supported: nessie)`
	if err.Error() != want {
		t.Errorf("error format mismatch\ngot:\n%s\nwant:\n%s", err.Error(), want)
	}
	if err := Check(catalogManifests("nessie"), resolver(stubProvider{"redpanda"})); err == nil {
		t.Fatal("validate accepted a non-catalog-capable provider behind a Catalog")
	}
}

func connectionManifests(scheme string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "edge", map[string]any{
			"type":    "proxy",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Connection", "orders-db", map[string]any{
			"providerRef": map[string]any{"name": "edge"},
			"scheme":      scheme,
			"port":        15999,
			"target":      "db.internal:5432",
		}),
	}
}

// TestConnectionSchemeCapability: a managed Connection's provider must
// declare its transport scheme; external Connections are skipped (nothing
// realizes them).
func TestConnectionSchemeCapability(t *testing.T) {
	if err := Check(connectionManifests("tcp"), resolver(connStub{stubProvider{"proxy"}})); err != nil {
		t.Fatalf("valid connection rejected: %v", err)
	}
	err := Check(connectionManifests("udp"), resolver(connStub{stubProvider{"proxy"}}))
	if err == nil {
		t.Fatal("validate accepted an unsupported connection scheme")
	}
	if !strings.Contains(err.Error(), `does not support connection scheme "udp" (supported: tcp)`) {
		t.Errorf("unexpected error: %v", err)
	}
	if err := Check(connectionManifests("tcp"), resolver(stubProvider{"redpanda"})); err == nil {
		t.Fatal("validate accepted a non-connection-capable provider behind a Connection")
	}
	external := []resource.Envelope{
		envelope("Connection", "prod-db", map[string]any{
			"external": true,
			"host":     "db.corp.internal",
			"port":     5432,
		}),
	}
	if err := Check(external, resolver(stubProvider{"redpanda"})); err != nil {
		t.Fatalf("external Connection must skip capability checks: %v", err)
	}
}

type specValidatingStub struct{ stubProvider }

func (specValidatingStub) ValidateSpec(cfg provider.Provider) error {
	if v, _ := cfg.Configuration["bootstrapServers"].(string); v == "" {
		return errors.New("spec.configuration.bootstrapServers is required")
	}
	return nil
}

// TestProviderSpecValidatedAtValidate: a provider's own configuration
// requirements surface at validate — a developer can never reach apply with
// a mis-wired Provider.
func TestProviderSpecValidatedAtValidate(t *testing.T) {
	manifests := []resource.Envelope{
		envelope("Provider", "worker", map[string]any{
			"type":    "debezium",
			"runtime": map[string]any{"type": "fake"},
		}),
	}
	err := Check(manifests, resolver(specValidatingStub{stubProvider{"debezium"}}))
	if err == nil {
		t.Fatal("validate accepted a Provider failing its own spec validation")
	}
	if !strings.Contains(err.Error(), `Provider "worker" (type: debezium)`) || !strings.Contains(err.Error(), "bootstrapServers") {
		t.Errorf("unexpected error: %v", err)
	}

	manifests[0].Spec["configuration"] = map[string]any{"bootstrapServers": "broker:9092"}
	if err := Check(manifests, resolver(specValidatingStub{stubProvider{"debezium"}})); err != nil {
		t.Fatalf("valid provider spec rejected: %v", err)
	}
}

// TestConnectionRefTargetKind: connectionRef must resolve to a Connection or
// SecretReference — pointing it at anything else fails at validate.
func TestConnectionRefTargetKind(t *testing.T) {
	manifests := []resource.Envelope{
		envelope("Provider", "not-a-connection", map[string]any{
			"type":    "postgres",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Source", "prod-db", map[string]any{
			"engine":        "postgres",
			"external":      true,
			"connectionRef": map[string]any{"name": "not-a-connection"},
		}),
	}
	err := Check(manifests, resolver(stubProvider{"postgres"}))
	if err == nil {
		t.Fatal("validate accepted a connectionRef pointing at a Provider")
	}
	if !strings.Contains(err.Error(), "must resolve to a Connection or SecretReference") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestConnectionSecretRefTargetKind: a Connection's secretRef must resolve
// to a SecretReference.
func TestConnectionSecretRefTargetKind(t *testing.T) {
	manifests := []resource.Envelope{
		envelope("Connection", "orders-db", map[string]any{
			"external":  true,
			"host":      "db.corp.internal",
			"port":      5432,
			"secretRef": map[string]any{"name": "missing-creds"},
		}),
	}
	err := Check(manifests, resolver(stubProvider{"proxy"}))
	if err == nil {
		t.Fatal("validate accepted a Connection secretRef resolving to nothing")
	}
	if !strings.Contains(err.Error(), `secretRef "missing-creds" must resolve to a SecretReference`) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExternalProviderRefRequiresConfigurer(t *testing.T) {
	manifests := []resource.Envelope{
		envelope("Provider", "object-store", map[string]any{
			"type":    "minio",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Dataset", "raw", map[string]any{
			"external":    true,
			"providerRef": map[string]any{"name": "object-store"},
			"bucket":      "raw",
			"format":      "json",
		}),
	}
	err := Check(manifests, resolver(stubProvider{"minio"}))
	if err == nil {
		t.Fatal("validate accepted an external providerRef without ExternalConfigurer")
	}
	if !strings.Contains(err.Error(), "ExternalConfigurer") {
		t.Fatalf("error does not name ExternalConfigurer: %v", err)
	}
	if err := Check(manifests, resolver(externalConfigStub{stubProvider{"minio"}})); err != nil {
		t.Fatalf("external configurer rejected: %v", err)
	}
}

// TestExternalProviderRefRequiresConfigurerPerKind guards docs/planning/07
// §0.3 / docs/planning/08 A6: every kind whose schema accepts
// `spec.external: true` alongside a `providerRef` must be refused at
// validate time, not silently accepted, when the resolved provider does not
// implement ExternalConfigurer — audited kind by kind rather than assumed
// from one example (TestExternalProviderRefRequiresConfigurer already covers
// Dataset). Provider itself is excluded: its schema has no providerRef field
// (a Provider cannot reference itself), so an external Provider always takes
// the connection-resolvable-only path, never this check.
func TestExternalProviderRefRequiresConfigurerPerKind(t *testing.T) {
	prov := envelope("Provider", "svc", map[string]any{
		"type":    "svc",
		"runtime": map[string]any{"type": "fake"},
	})
	creds := envelope("SecretReference", "creds", map[string]any{
		"backend": "env",
		"keys":    []any{"password"},
	})
	cases := []struct {
		kind string
		spec map[string]any
	}{
		{"Source", map[string]any{
			"engine":      "postgres",
			"external":    true,
			"providerRef": map[string]any{"name": "svc"},
		}},
		{"Catalog", map[string]any{
			"engine":        "nessie",
			"external":      true,
			"providerRef":   map[string]any{"name": "svc"},
			"connectionRef": map[string]any{"name": "creds"},
		}},
		{"Connection", map[string]any{
			"port":        5432,
			"host":        "db.example.com",
			"external":    true,
			"providerRef": map[string]any{"name": "svc"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			manifests := []resource.Envelope{prov, creds, envelope(tc.kind, "target", tc.spec)}
			err := Check(manifests, resolver(stubProvider{"svc"}))
			if err == nil {
				t.Fatalf("%s: validate accepted an external providerRef without ExternalConfigurer", tc.kind)
			}
			if !strings.Contains(err.Error(), "ExternalConfigurer") {
				t.Fatalf("%s: error does not name ExternalConfigurer: %v", tc.kind, err)
			}
			if err := Check(manifests, resolver(externalConfigStub{stubProvider{"svc"}})); err != nil {
				t.Fatalf("%s: external configurer rejected: %v", tc.kind, err)
			}
		})
	}
}

// TestVersionedProviderValidation: a versioned provider's configuration is
// checked at validate — unknown version rejected, image-without-version
// rejected, valid version accepted. Uses the real registry resolver via a
// stub that returns the postgres provider.
func TestVersionedProviderValidation(t *testing.T) {
	pg := func(cfg map[string]any) []resource.Envelope {
		return []resource.Envelope{
			envelope("Provider", "db", map[string]any{
				"type":          "postgres",
				"runtime":       map[string]any{"type": "docker"},
				"configuration": cfg,
				"secretRefs":    []any{"creds"},
			}),
			envelope("SecretReference", "creds", map[string]any{"backend": "env", "keys": []any{"username", "password"}}),
		}
	}
	stub := versionedStub{
		stubProvider: stubProvider{"postgres"},
		catalog: versionprofile.Catalog{
			Default: "18",
			Profiles: map[string]versionprofile.Profile{
				"16": {Version: "16", Image: "postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20", DataMount: "/var/lib/postgresql/data"},
				"18": {Version: "18", Image: "postgres:18@sha256:32ca0af8e77bfb8c6610c488e4691f83f972a3e9e64d3b02facf3ab111ad5500", DataMount: "/var/lib/postgresql"},
			},
		},
	}
	resolvePG := func(string) (reconciler.Provider, error) { return stub, nil }

	// Valid version.
	if err := Check(pg(map[string]any{"version": "18"}), resolvePG); err != nil {
		t.Errorf("valid version rejected: %v", err)
	}
	// Unknown version.
	if err := Check(pg(map[string]any{"version": "99"}), resolvePG); err == nil {
		t.Error("unknown postgres version accepted")
	}
	// Image without version — the reported instability.
	err := Check(pg(map[string]any{"image": "postgres:18@sha256:32ca0af8e77bfb8c6610c488e4691f83f972a3e9e64d3b02facf3ab111ad5500"}), resolvePG)
	if err == nil || !strings.Contains(err.Error(), "without configuration.version") {
		t.Errorf("image-without-version not rejected: %v", err)
	}
}

type optionsValidatingStub struct{ stubProvider }

func (optionsValidatingStub) SupportedSourceEngines() []string { return []string{"postgres"} }
func (optionsValidatingStub) ValidateBindingOptions(_ string, options map[string]any) error {
	if v, ok := options["snapshotMode"]; ok {
		if s, _ := v.(string); s == "bogus" {
			return errors.New(`options.snapshotMode "bogus" is not a Debezium snapshot mode`)
		}
	}
	return nil
}

// TestBindingOptionsValidated: a provider implementing
// BindingOptionsValidator gets its Binding options checked at validate time
// (docs/planning/07 §2.2) — a bad option block fails before apply.
func TestBindingOptionsValidated(t *testing.T) {
	manifests := cdcManifests("postgres")
	// Inject a bad option block into the Binding.
	for i := range manifests {
		if manifests[i].Kind == "Binding" {
			manifests[i].Spec["options"] = map[string]any{"snapshotMode": "bogus"}
		}
	}
	err := Check(manifests, resolver(optionsValidatingStub{stubProvider{"debezium"}}))
	if err == nil {
		t.Fatal("validate accepted a Binding option the provider rejects")
	}
	if !strings.Contains(err.Error(), "snapshotMode") {
		t.Fatalf("error does not name the bad option: %v", err)
	}

	// The same set with a good option block passes.
	for i := range manifests {
		if manifests[i].Kind == "Binding" {
			manifests[i].Spec["options"] = map[string]any{"snapshotMode": "initial"}
		}
	}
	if err := Check(manifests, resolver(optionsValidatingStub{stubProvider{"debezium"}})); err != nil {
		t.Fatalf("valid option block rejected: %v", err)
	}
}
