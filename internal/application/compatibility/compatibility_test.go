package compatibility

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/domain/versionprofile"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

type stubProvider struct{ typeName string }

func (s stubProvider) Type() string { return s.typeName }
func (s stubProvider) Reconcile(context.Context, reconciler.Request) (status.Status, error) {
	return status.Status{}, nil
}
func (s stubProvider) Destroy(context.Context, reconciler.Request) error {
	return nil
}
func (s stubProvider) Probe(context.Context, reconciler.Request) (status.Status, error) {
	return status.Status{}, nil
}

type cdcStub struct{ stubProvider }

func (cdcStub) SupportedSourceEngines() []string { return []string{"postgres", "mysql", "mongodb"} }

type sinkStub struct{ stubProvider }

func (sinkStub) SupportedSinkFormats() []string { return []string{"parquet", "json"} }

type externalConfigStub struct{ stubProvider }

func (externalConfigStub) ConfigureExternal(context.Context, reconciler.Request) (status.Status, error) {
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

func (v versionedStub) VersionCatalog(provider.Provider) versionprofile.Catalog { return v.catalog }

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
	t.Parallel()
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
	t.Parallel()
	err := Check(cdcManifests("postgres"), resolver(stubProvider{"redpanda"}))
	if err == nil {
		t.Fatal("validate accepted a non-CDC-capable provider behind a cdc Binding")
	}
	if !strings.Contains(err.Error(), `does not support mode "cdc"`) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSupportedEngineAccepted(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	err := Check(sinkManifests("json"), resolver(stubProvider{"redpanda"}))
	if err == nil {
		t.Fatal("validate accepted a non-sink-capable provider behind a sink Binding")
	}
	if !strings.Contains(err.Error(), `does not support mode "sink"`) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSupportedSinkFormatAccepted(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
				"18": {Version: "18", Image: "postgres:18@sha256:3a82e1f56c8f0f5616a11103ac3d47e632c3938698946a7ad26da0df1334744a", DataMount: "/var/lib/postgresql"},
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
	err := Check(pg(map[string]any{"image": "postgres:18@sha256:3a82e1f56c8f0f5616a11103ac3d47e632c3938698946a7ad26da0df1334744a"}), resolvePG)
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
	t.Parallel()
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

// multiResolver dispatches by provider type — needed once a test has more
// than one distinct Provider type in play (docs/planning/08 D1's
// schema-format check resolves the EventStream's own Provider, which is a
// different type from the Binding's providerRef in a realistic CDC set).
func multiResolver(byType map[string]reconciler.Provider) ProviderResolver {
	return func(typeName string) (reconciler.Provider, error) {
		impl, ok := byType[typeName]
		if !ok {
			return nil, fmt.Errorf("unknown provider type %q", typeName)
		}
		return impl, nil
	}
}

// schemaRegistryStub is a local double for reconciler.SchemaRegistryCapableProvider
// (docs/planning/08 D1) — stands in for the redpanda adapter without
// importing it (CLAUDE.md's application-test layering exception).
type schemaRegistryStub struct {
	stubProvider
	formats []string
}

func (s schemaRegistryStub) SupportedSchemaFormats(provider.Provider) []string { return s.formats }

// schemaFormatManifests builds a cdc-mode Binding whose EventStream's own
// Provider (kafka-cluster, type redpanda) is distinct from the Binding's own
// providerRef (postgres-cdc, type debezium) — the schema-format check
// resolves the former, never the latter.
func schemaFormatManifests(format string) []resource.Envelope {
	bindingEnv := envelope("Binding", "student-db-to-events", map[string]any{
		"mode":        "cdc",
		"sourceRef":   map[string]any{"name": "student-database"},
		"targetRef":   map[string]any{"name": "attendance-events"},
		"providerRef": map[string]any{"name": "postgres-cdc"},
	})
	if format != "" {
		bindingEnv.Spec["options"] = map[string]any{"format": format}
	}
	return []resource.Envelope{
		envelope("Provider", "postgres-cdc", map[string]any{
			"type":    "debezium",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-postgres", map[string]any{
			"type":    "postgres",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "kafka-cluster", map[string]any{
			"type":    "redpanda",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Source", "student-database", map[string]any{
			"engine":      "postgres",
			"providerRef": map[string]any{"name": "local-postgres"},
		}),
		envelope("EventStream", "attendance-events", map[string]any{
			"providerRef": map[string]any{"name": "kafka-cluster"},
		}),
		bindingEnv,
	}
}

// TestSchemaFormatWithoutRegistryErrorFormat covers the D1 accept criterion:
// a Binding declaring a schema-carrying format against a provider chain
// without a registry endpoint fails at validate with the standard
// capability-error shape (docs/planning/02-architecture.md §5.2), naming the
// EventStream's own Provider — the one whose configuration actually decides
// registry availability — not the Binding's providerRef.
func TestSchemaFormatWithoutRegistryErrorFormat(t *testing.T) {
	t.Parallel()
	resolve := multiResolver(map[string]reconciler.Provider{
		"debezium": cdcStub{stubProvider{"debezium"}},
		"postgres": stubProvider{"postgres"},
		"redpanda": schemaRegistryStub{stubProvider{"redpanda"}, []string{"json"}}, // registry disabled
	})
	err := Check(schemaFormatManifests("avro"), resolve)
	if err == nil {
		t.Fatal("validate accepted a schema-carrying format against a registry-less provider chain")
	}
	want := `Binding "student-db-to-events": Provider "kafka-cluster" (type: redpanda)
does not support format "avro" (supported: json)`
	if err.Error() != want {
		t.Errorf("error format mismatch\ngot:\n%s\nwant:\n%s", err.Error(), want)
	}
}

// TestSchemaFormatWithRegistryAccepted: the same Binding validates once the
// EventStream's Provider declares the format supported (registry enabled).
func TestSchemaFormatWithRegistryAccepted(t *testing.T) {
	t.Parallel()
	resolve := multiResolver(map[string]reconciler.Provider{
		"debezium": cdcStub{stubProvider{"debezium"}},
		"postgres": stubProvider{"postgres"},
		"redpanda": schemaRegistryStub{stubProvider{"redpanda"}, []string{"avro", "json", "protobuf"}},
	})
	if err := Check(schemaFormatManifests("avro"), resolve); err != nil {
		t.Fatalf("valid schema-carrying format rejected: %v", err)
	}
}

// TestSchemaFormatUnsetOrJSONNeedsNoRegistry: the common (unset/json) case
// never touches the EventStream's Provider capability at all — a provider
// that doesn't implement SchemaRegistryCapableProvider is still a valid
// target for a plain json-format (or format-unset) Binding.
func TestSchemaFormatUnsetOrJSONNeedsNoRegistry(t *testing.T) {
	t.Parallel()
	resolve := multiResolver(map[string]reconciler.Provider{
		"debezium": cdcStub{stubProvider{"debezium"}},
		"postgres": stubProvider{"postgres"},
		"redpanda": stubProvider{"redpanda"}, // no SchemaRegistryCapableProvider at all
	})
	for _, format := range []string{"", "json"} {
		if err := Check(schemaFormatManifests(format), resolve); err != nil {
			t.Errorf("format %q rejected: %v", format, err)
		}
	}
}

// parquetSinkManifests builds a sink-mode Binding to a Dataset of the given
// format whose EventStream's own Provider (kafka-cluster, type redpanda) is
// distinct from the Binding's providerRef (s3-sink, type s3sink) — the D2
// parquet check resolves the former, never the latter.
func parquetSinkManifests(format string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "s3-sink", map[string]any{
			"type":    "s3sink",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "kafka-cluster", map[string]any{
			"type":    "redpanda",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "local-minio", map[string]any{
			"type":    "minio",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("EventStream", "attendance-events", map[string]any{
			"providerRef": map[string]any{"name": "kafka-cluster"},
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

// TestParquetSinkWithoutRegistryErrorFormat covers the D2 accept criterion:
// a parquet Dataset behind a sink Binding whose EventStream Provider chain
// lacks a registry endpoint fails at validate with the standard
// capability-error shape (docs/planning/02 §5.2 / ADR 009), naming the
// EventStream's own Provider — the resource whose configuration decides
// registry availability — not the Binding's providerRef.
func TestParquetSinkWithoutRegistryErrorFormat(t *testing.T) {
	t.Parallel()
	resolve := multiResolver(map[string]reconciler.Provider{
		"s3sink":   sinkStub{stubProvider{"s3sink"}},
		"minio":    stubProvider{"minio"},
		"redpanda": schemaRegistryStub{stubProvider{"redpanda"}, []string{"json"}}, // registry disabled
	})
	err := Check(parquetSinkManifests("parquet"), resolve)
	if err == nil {
		t.Fatal("validate accepted a parquet Dataset against a registry-less provider chain")
	}
	want := `Binding "attendance-events-to-lake": Provider "kafka-cluster" (type: redpanda)
does not support format "parquet" (supported: json)`
	if err.Error() != want {
		t.Errorf("error format mismatch\ngot:\n%s\nwant:\n%s", err.Error(), want)
	}
}

// TestParquetSinkNoCapabilityFallsBackToJSON: an EventStream Provider with
// no SchemaRegistryCapableProvider implementation at all is refused the same
// way as an explicitly registry-less one.
func TestParquetSinkNoCapabilityFallsBackToJSON(t *testing.T) {
	t.Parallel()
	resolve := multiResolver(map[string]reconciler.Provider{
		"s3sink":   sinkStub{stubProvider{"s3sink"}},
		"minio":    stubProvider{"minio"},
		"redpanda": stubProvider{"redpanda"},
	})
	if err := Check(parquetSinkManifests("parquet"), resolve); err == nil {
		t.Fatal("validate accepted a parquet Dataset against a provider with no schema-registry capability")
	}
}

// TestParquetSinkWithRegistryAccepted: the same set validates once the
// EventStream's Provider supports avro (registry enabled) — parquet's
// schema-carrying requirement is satisfied by the Avro converter chain.
func TestParquetSinkWithRegistryAccepted(t *testing.T) {
	t.Parallel()
	resolve := multiResolver(map[string]reconciler.Provider{
		"s3sink":   sinkStub{stubProvider{"s3sink"}},
		"minio":    stubProvider{"minio"},
		"redpanda": schemaRegistryStub{stubProvider{"redpanda"}, []string{"avro", "json", "protobuf"}},
	})
	if err := Check(parquetSinkManifests("parquet"), resolve); err != nil {
		t.Fatalf("parquet Dataset with a registry-enabled chain rejected: %v", err)
	}
}

// TestNonParquetSinkNeedsNoRegistry: json/jsonl/csv Datasets stay on the
// schemaless path — no registry demand is made of the EventStream's
// Provider (gate-off manifests are behavior-identical to pre-D2).
func TestNonParquetSinkNeedsNoRegistry(t *testing.T) {
	t.Parallel()
	resolve := multiResolver(map[string]reconciler.Provider{
		"s3sink":   allFormatSinkStub{stubProvider{"s3sink"}},
		"minio":    stubProvider{"minio"},
		"redpanda": stubProvider{"redpanda"}, // no registry capability at all
	})
	for _, format := range []string{"json", "jsonl", "csv"} {
		if err := Check(parquetSinkManifests(format), resolve); err != nil {
			t.Errorf("schemaless sink format %q rejected: %v", format, err)
		}
	}
}

// allFormatSinkStub mirrors the real s3sink adapter's format list.
type allFormatSinkStub struct{ stubProvider }

func (allFormatSinkStub) SupportedSinkFormats() []string {
	return []string{"json", "jsonl", "csv", "parquet"}
}

// TestSchemaFormatNoCapabilityFallsBackToJSON: an EventStream Provider that
// doesn't implement SchemaRegistryCapableProvider at all falls back to
// "supported: json" — the same message shape as an explicitly registry-less
// one, since from the Binding's perspective the two are indistinguishable.
func TestSchemaFormatNoCapabilityFallsBackToJSON(t *testing.T) {
	t.Parallel()
	resolve := multiResolver(map[string]reconciler.Provider{
		"debezium": cdcStub{stubProvider{"debezium"}},
		"postgres": stubProvider{"postgres"},
		"redpanda": stubProvider{"redpanda"},
	})
	err := Check(schemaFormatManifests("protobuf"), resolve)
	if err == nil {
		t.Fatal("validate accepted a schema-carrying format against a provider with no schema-registry capability")
	}
	want := `Binding "student-db-to-events": Provider "kafka-cluster" (type: redpanda)
does not support format "protobuf" (supported: json)`
	if err.Error() != want {
		t.Errorf("error format mismatch\ngot:\n%s\nwant:\n%s", err.Error(), want)
	}
}

// streamReplicationStub is a local double for
// reconciler.StreamReplicationValidator (docs/adr/017 §a.7) — it exercises
// compatibility's EventStream replication check without importing the
// redpanda adapter (CLAUDE.md's layering test exception).
type streamReplicationStub struct {
	stubProvider
	maxReplication int
}

func (s streamReplicationStub) ValidateStreamReplication(_ provider.Provider, replication int) error {
	if replication > s.maxReplication {
		return fmt.Errorf("spec.replication %d exceeds the configured broker count %d", replication, s.maxReplication)
	}
	return nil
}

func replicationManifests(replication any) []resource.Envelope {
	spec := map[string]any{
		"providerRef": map[string]any{"name": "kafka-cluster"},
		"partitions":  3,
	}
	if replication != nil {
		spec["replication"] = replication
	}
	return []resource.Envelope{
		envelope("Provider", "kafka-cluster", map[string]any{
			"type":          "redpanda",
			"runtime":       map[string]any{"type": "fake"},
			"configuration": map[string]any{"brokers": 2},
		}),
		envelope("EventStream", "attendance-events", spec),
	}
}

// TestCheckEventStreamReplicationExceedsBrokers: an EventStream declaring
// more replicas than its realizing Provider can host fails at validate with
// an error naming the resource, the provider, and both numbers
// (docs/adr/017 §a.7).
func TestCheckEventStreamReplicationExceedsBrokers(t *testing.T) {
	t.Parallel()
	err := Check(replicationManifests(3), resolver(streamReplicationStub{stubProvider{"redpanda"}, 2}))
	if err == nil {
		t.Fatal("Check accepted replication 3 against a 2-broker provider")
	}
	for _, want := range []string{"attendance-events", "kafka-cluster", "3", "2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

// TestCheckEventStreamReplicationWithinBrokers: replication at or under the
// broker count validates; so does an EventStream that never declares
// replication against a provider without the capability (the pre-C2 path,
// unchanged).
func TestCheckEventStreamReplicationWithinBrokers(t *testing.T) {
	t.Parallel()
	if err := Check(replicationManifests(2), resolver(streamReplicationStub{stubProvider{"redpanda"}, 2})); err != nil {
		t.Errorf("Check rejected replication 2 against a 2-broker provider: %v", err)
	}
	if err := Check(replicationManifests(nil), resolver(stubProvider{"redpanda"})); err != nil {
		t.Errorf("Check rejected an EventStream without replication against a capability-less provider: %v", err)
	}
	// replication: 1 is the explicit default — no capability required.
	if err := Check(replicationManifests(1), resolver(stubProvider{"redpanda"})); err != nil {
		t.Errorf("Check rejected replication 1 against a capability-less provider: %v", err)
	}
}

// sinkManifestsWithDeadLetter is sinkManifests("json") with
// spec.options.deadLetter declared on the sink Binding, plus (when
// includeDLQStream) a second EventStream realizing the named DLQ topic —
// docs/planning/08 D6.
func sinkManifestsWithDeadLetter(dlqStream string, includeDLQStream bool) []resource.Envelope {
	manifests := sinkManifests("json")
	for i, e := range manifests {
		if e.Kind == "Binding" {
			spec := make(map[string]any, len(e.Spec)+1)
			for k, v := range e.Spec {
				spec[k] = v
			}
			spec["options"] = map[string]any{
				"deadLetter": map[string]any{"stream": dlqStream},
			}
			manifests[i].Spec = spec
		}
	}
	if includeDLQStream {
		manifests = append(manifests, envelope("EventStream", dlqStream, map[string]any{
			"providerRef": map[string]any{"name": "s3-sink"},
		}))
	}
	return manifests
}

// TestDeadLetterQueueExistingStreamAccepted covers docs/planning/08 D6: a
// deadLetter.stream naming an EventStream present in the manifest set
// validates cleanly.
func TestDeadLetterQueueExistingStreamAccepted(t *testing.T) {
	t.Parallel()
	err := Check(sinkManifestsWithDeadLetter("dlq-events", true), resolver(sinkStub{stubProvider{"s3sink"}}))
	if err != nil {
		t.Fatalf("valid deadLetter.stream rejected: %v", err)
	}
}

// TestDeadLetterQueueMissingStreamRejected covers the D6 Accept item
// verbatim: "validate rejects a deadLetter naming a missing EventStream."
func TestDeadLetterQueueMissingStreamRejected(t *testing.T) {
	t.Parallel()
	err := Check(sinkManifestsWithDeadLetter("dlq-events", false), resolver(sinkStub{stubProvider{"s3sink"}}))
	if err == nil {
		t.Fatal("Check accepted a deadLetter.stream naming a missing EventStream")
	}
	for _, want := range []string{"attendance-events-to-lake", "dlq-events", "does not resolve to an EventStream"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

// TestDeadLetterQueueOnCDCBindingRejected: deadLetter is a sink-mode
// concept (docs/planning/08 D6); binding.FromEnvelope refuses it on any
// other mode, and Check surfaces that refusal at validate.
func TestDeadLetterQueueOnCDCBindingRejected(t *testing.T) {
	t.Parallel()
	manifests := cdcManifests("postgres")
	for i, e := range manifests {
		if e.Kind == "Binding" {
			spec := make(map[string]any, len(e.Spec)+1)
			for k, v := range e.Spec {
				spec[k] = v
			}
			spec["options"] = map[string]any{
				"deadLetter": map[string]any{"stream": "attendance-events"},
			}
			manifests[i].Spec = spec
		}
	}
	if err := Check(manifests, resolver(cdcStub{stubProvider{"debezium"}})); err == nil {
		t.Fatal("Check accepted options.deadLetter on a cdc-mode Binding")
	}
}

type tunnelStub struct{ stubProvider }

func (tunnelStub) SupportsTunnelChaining() []string { return []string{"tcp"} }

// viaStub is a ConnectionCapableProvider that also implements
// reconciler.ViaConsumingProvider (docs/planning/08 I1) — stands in for
// `proxy`, the realizing provider that actually consumes spec.via.
type viaStub struct{ connStub }

func (viaStub) ConsumesVia() bool { return true }

func viaManifests(viaTarget string) []resource.Envelope {
	return []resource.Envelope{
		envelope("Provider", "edge", map[string]any{
			"type":    "proxy",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Provider", "vpc-tunnel", map[string]any{
			"type":    "wireguard",
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Connection", "private-db", map[string]any{
			"providerRef": map[string]any{"name": "edge"},
			"scheme":      "tcp",
			"port":        15999,
			"target":      "10.8.0.10:5432",
			"via":         map[string]any{"name": viaTarget},
		}),
	}
}

func typedResolver(byType map[string]reconciler.Provider) ProviderResolver {
	return func(typ string) (reconciler.Provider, error) {
		impl, ok := byType[typ]
		if !ok {
			return nil, fmt.Errorf("no provider registered for type %q", typ)
		}
		return impl, nil
	}
}

// TestConnectionViaRealized: spec.via passes the structural, tunnel-
// capability, and pairing checks when the Connection's realizing provider
// implements reconciler.ViaConsumingProvider (docs/planning/08 I1, closing
// docs/adr/023's Scope deviation) — validate accepts it, since apply will
// actually route the forwarder's egress through the named tunnel rather
// than realizing an unconsumed, silently-inert field.
func TestConnectionViaRealized(t *testing.T) {
	t.Parallel()
	impls := map[string]reconciler.Provider{
		"proxy":     viaStub{connStub{stubProvider{"proxy"}}},
		"wireguard": tunnelStub{stubProvider{"wireguard"}},
	}
	if err := Check(viaManifests("vpc-tunnel"), typedResolver(impls)); err != nil {
		t.Fatalf("validate rejected a Connection whose via-capable Provider is consumed by a via-consuming realizing provider: %v", err)
	}

	// The pairing check: a realizing provider that is connection-capable
	// but NOT via-consuming fails with the documented message shape, even
	// though the named tunnel itself is perfectly valid.
	impls["proxy"] = connStub{stubProvider{"proxy"}}
	err := Check(viaManifests("vpc-tunnel"), typedResolver(impls))
	if err == nil || !strings.Contains(err.Error(), "does not support spec.via (provider implements no via-consuming capability)") {
		t.Errorf("want via-consuming pairing error, got: %v", err)
	}

	// A via naming a non-tunnel-capable Provider fails the capability
	// check first, with the documented message shape.
	impls["proxy"] = viaStub{connStub{stubProvider{"proxy"}}}
	impls["wireguard"] = connStub{stubProvider{"wireguard"}}
	err = Check(viaManifests("vpc-tunnel"), typedResolver(impls))
	if err == nil || !strings.Contains(err.Error(), "does not support tunnel chaining") {
		t.Errorf("want tunnel-capability error, got: %v", err)
	}

	// A via that resolves to nothing fails structurally.
	impls["wireguard"] = tunnelStub{stubProvider{"wireguard"}}
	err = Check(viaManifests("no-such-provider"), typedResolver(impls))
	if err == nil || !strings.Contains(err.Error(), "does not resolve to a Provider") {
		t.Errorf("want resolution error, got: %v", err)
	}
}

// TestDomainCoherenceWithProvider pins the docs/adr/022 addendum: a
// dependent explicitly declaring a domain different from its realizing
// Provider's is refused at validate (runtime objects live in the
// provider's domain); an undeclared domain inherits silently.
func TestDomainCoherenceWithProvider(t *testing.T) {
	t.Parallel()
	prov := envelope("Provider", "broker", map[string]any{
		"type": "redpanda", "runtime": map[string]any{"type": "fake"},
	})
	prov.Metadata.Domain = "infra"
	es := envelope("EventStream", "events", map[string]any{
		"providerRef": map[string]any{"name": "broker"},
	})
	es.Metadata.Domain = "analytics"
	err := Check([]resource.Envelope{prov, es}, resolver(stubProvider{"redpanda"}))
	if err == nil || !strings.Contains(err.Error(), "does not match realizing Provider") {
		t.Fatalf("want domain-coherence refusal, got: %v", err)
	}
	// Undeclared inherits — no error from coherence.
	es.Metadata.Domain = ""
	if err := Check([]resource.Envelope{prov, es}, resolver(stubProvider{"redpanda"})); err != nil && strings.Contains(err.Error(), "does not match realizing Provider") {
		t.Fatalf("undeclared domain must inherit, got refusal: %v", err)
	}
	// Matching explicit declaration passes coherence.
	es.Metadata.Domain = "infra"
	if err := Check([]resource.Envelope{prov, es}, resolver(stubProvider{"redpanda"})); err != nil && strings.Contains(err.Error(), "does not match realizing Provider") {
		t.Fatalf("matching domain refused: %v", err)
	}
}
