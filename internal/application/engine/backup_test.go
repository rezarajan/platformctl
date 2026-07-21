package engine

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/noop"
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	envsecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/env"
	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/clock"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// fakeBackupProvider is a local BackupCapableProvider double (CLAUDE.md's
// documented application-test exception: noop is an allowed test double;
// technology adapters like postgres/mysql/s3 are not importable here) that
// records every call so tests can assert dispatch, refusal, and
// call-avoidance without touching real infrastructure.
type fakeBackupProvider struct {
	noop.Provider
	backupCalls  int
	restoreCalls int
	gotDest      backup.Location
	gotSrc       backup.Location
	manifest     backup.Manifest
	err          error
}

func (f *fakeBackupProvider) Type() string { return "fakebackup" }

func (f *fakeBackupProvider) Backup(_ context.Context, _ reconciler.Request, dest backup.Location) (backup.Manifest, error) {
	f.backupCalls++
	f.gotDest = dest
	if f.err != nil {
		return backup.Manifest{}, f.err
	}
	m := f.manifest
	m.Destination = backup.RefOf(dest, "generated-key.sql")
	return m, nil
}

func (f *fakeBackupProvider) Restore(_ context.Context, _ reconciler.Request, src backup.Location) error {
	f.restoreCalls++
	f.gotSrc = src
	return f.err
}

func newBackupTestEngine(t *testing.T, prov reconciler.Provider, providerType string) (*Engine, []resource.Envelope) {
	t.Helper()
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	reg.RegisterProvider(providerType, func() reconciler.Provider { return prov }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	eng := &Engine{
		Registry:   reg,
		StateStore: localfile.New(filepath.Join(t.TempDir(), "state.json")),
		Clock:      &clock.Fake{T: time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)},
	}
	envelopes := []resource.Envelope{
		envelope("Provider", "db", map[string]any{
			"type":    providerType,
			"runtime": map[string]any{"type": "fake"},
		}),
		envelope("Source", "orders", map[string]any{
			"providerRef": map[string]any{"name": "db"},
			"engine":      "postgres",
		}),
	}
	return eng, envelopes
}

func TestBackupDispatchesToCapableProvider(t *testing.T) {
	prov := &fakeBackupProvider{}
	eng, envelopes := newBackupTestEngine(t, prov, "fakebackup")
	dest := backup.Location{Endpoint: "http://s3:9000", Bucket: "backups", Prefix: "orders", AccessKey: "AKIA", SecretKey: "shh"}

	key := resource.Key{Namespace: "default", Kind: "Source", Name: "orders"}
	m, err := eng.Backup(context.Background(), envelopes, key, dest)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if prov.backupCalls != 1 {
		t.Fatalf("backupCalls = %d, want 1", prov.backupCalls)
	}
	if prov.gotDest != dest {
		t.Fatalf("provider received dest %+v, want %+v", prov.gotDest, dest)
	}
	if m.Destination.Bucket != "backups" {
		t.Fatalf("manifest destination bucket = %q, want backups", m.Destination.Bucket)
	}
}

func TestBackupRefusesForNonCapableProvider(t *testing.T) {
	eng, envelopes := newBackupTestEngine(t, noop.New(), "noop-typed")
	// Point the Source at a provider whose type isn't BackupCapableProvider.
	envelopes[0].Spec["type"] = "noop-typed"
	key := resource.Key{Namespace: "default", Kind: "Source", Name: "orders"}
	_, err := eng.Backup(context.Background(), envelopes, key, backup.Location{})
	if err == nil {
		t.Fatal("Backup: expected an error for a non-BackupCapableProvider, got nil")
	}
}

func TestRestoreRefusesWithoutAllowOverwrite(t *testing.T) {
	prov := &fakeBackupProvider{}
	eng, envelopes := newBackupTestEngine(t, prov, "fakebackup")
	eng.AllowOverwrite = false

	key := resource.Key{Namespace: "default", Kind: "Source", Name: "orders"}
	err := eng.Restore(context.Background(), envelopes, key, backup.Location{Bucket: "backups", Prefix: "orders/dump.sql"})
	if err == nil {
		t.Fatal("Restore: expected a refusal error when AllowOverwrite is false, got nil")
	}
	if prov.restoreCalls != 0 {
		t.Fatalf("provider.Restore was called %d time(s); the engine must refuse before any provider call, touching zero infrastructure", prov.restoreCalls)
	}
}

func TestRestoreCallsProviderWhenAllowed(t *testing.T) {
	prov := &fakeBackupProvider{}
	eng, envelopes := newBackupTestEngine(t, prov, "fakebackup")
	eng.AllowOverwrite = true
	src := backup.Location{Bucket: "backups", Prefix: "orders/dump.sql", AccessKey: "AKIA", SecretKey: "shh"}

	key := resource.Key{Namespace: "default", Kind: "Source", Name: "orders"}
	if err := eng.Restore(context.Background(), envelopes, key, src); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if prov.restoreCalls != 1 {
		t.Fatalf("restoreCalls = %d, want 1", prov.restoreCalls)
	}
	if prov.gotSrc != src {
		t.Fatalf("provider received src %+v, want %+v", prov.gotSrc, src)
	}
}

func newLocationTestEngine(t *testing.T) *Engine {
	t.Helper()
	gates := featuregate.NewRegistry()
	reg := registry.New(gates)
	reg.RegisterProvider("s3", func() reconciler.Provider { return noop.New() }, "")
	reg.RegisterRuntime("fake", func(_ map[string]any) (runtime.ContainerRuntime, error) {
		return fakeruntime.New(), nil
	})
	return &Engine{
		Registry:    reg,
		StateStore:  localfile.New(filepath.Join(t.TempDir(), "state.json")),
		SecretStore: envsecrets.New(),
		Clock:       &clock.Fake{T: time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)},
	}
}

// TestResolveDatasetLocation covers --to/--from's Dataset form: a Dataset's
// own s3/minio Provider supplies the endpoint (its internal DNS name and
// fixed API port, matching s3.go's own convention) and credentials (its
// rootSecretRef, resolved exactly like any other capability call).
func TestResolveDatasetLocation(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_STORE_ROOT_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_STORE_ROOT_PASSWORD", "s3cr3t")
	eng := newLocationTestEngine(t)
	envelopes := []resource.Envelope{
		envelope("Provider", "minio-a", map[string]any{
			"type":          "s3",
			"runtime":       map[string]any{"type": "fake", "network": "custom-net"},
			"configuration": map[string]any{"rootSecretRef": "store-root"},
			"secretRefs":    []any{"store-root"},
		}),
		envelope("SecretReference", "store-root", map[string]any{"backend": "env", "keys": []any{"username", "password"}}),
		envelope("Dataset", "warehouse", map[string]any{
			"providerRef": map[string]any{"name": "minio-a"},
			"bucket":      "wh",
			"prefix":      "backups",
			"format":      "parquet",
		}),
	}
	loc, err := eng.ResolveObjectStoreLocation(context.Background(), envelopes, "Dataset/warehouse", "", "orders.sql", "default")
	if err != nil {
		t.Fatalf("ResolveObjectStoreLocation: %v", err)
	}
	if loc.Endpoint != "http://minio-a:9000" {
		t.Errorf("Endpoint = %q, want http://minio-a:9000", loc.Endpoint)
	}
	if loc.Bucket != "wh" || loc.Prefix != "backups/orders.sql" {
		t.Errorf("Bucket/Prefix = %q/%q, want wh/backups/orders.sql", loc.Bucket, loc.Prefix)
	}
	if loc.AccessKey != "admin" || loc.SecretKey != "s3cr3t" {
		t.Errorf("AccessKey/SecretKey = %q/%q, want admin/s3cr3t", loc.AccessKey, loc.SecretKey)
	}
	if loc.Network != "custom-net" {
		t.Errorf("Network = %q, want custom-net", loc.Network)
	}
}

func TestResolveDatasetLocationRejectsNonS3Provider(t *testing.T) {
	eng := newLocationTestEngine(t)
	eng.Registry.RegisterProvider("postgres", func() reconciler.Provider { return noop.New() }, "")
	envelopes := []resource.Envelope{
		envelope("Provider", "pg", map[string]any{"type": "postgres", "runtime": map[string]any{"type": "fake"}}),
		envelope("Dataset", "not-really", map[string]any{
			"providerRef": map[string]any{"name": "pg"},
			"bucket":      "wh",
			"format":      "parquet",
		}),
	}
	_, err := eng.ResolveObjectStoreLocation(context.Background(), envelopes, "Dataset/not-really", "", "", "default")
	if err == nil {
		t.Fatal("expected an error resolving a Dataset backed by a non-s3 Provider")
	}
}

// TestResolveURLLocation covers --to/--from's raw-URL form: credentials
// come from a SecretReference named by --credentials-secret-ref, never from
// the URL itself.
func TestResolveURLLocation(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_URL_CREDS_ACCESSKEY", "AKIAEXAMPLE")
	t.Setenv("DATASCAPE_SECRET_URL_CREDS_SECRETKEY", "topsecret")
	eng := newLocationTestEngine(t)
	envelopes := []resource.Envelope{
		envelope("SecretReference", "url-creds", map[string]any{"backend": "env", "keys": []any{"accessKey", "secretKey"}}),
	}
	loc, err := eng.ResolveObjectStoreLocation(context.Background(), envelopes, "http://minio.example:9000/external-bucket/backups", "url-creds", "", "default")
	if err != nil {
		t.Fatalf("ResolveObjectStoreLocation: %v", err)
	}
	if loc.Endpoint != "http://minio.example:9000" || loc.Bucket != "external-bucket" || loc.Prefix != "backups" {
		t.Errorf("got endpoint=%q bucket=%q prefix=%q", loc.Endpoint, loc.Bucket, loc.Prefix)
	}
	if loc.AccessKey != "AKIAEXAMPLE" || loc.SecretKey != "topsecret" {
		t.Errorf("got accessKey=%q secretKey=%q", loc.AccessKey, loc.SecretKey)
	}
	if !loc.Insecure {
		t.Error("http:// URL should resolve Insecure = true")
	}
	if loc.Network != "" {
		t.Errorf("Network = %q, want empty for a raw URL destination", loc.Network)
	}
}

func TestResolveURLLocationRequiresCredentialsSecretRef(t *testing.T) {
	eng := newLocationTestEngine(t)
	_, err := eng.ResolveObjectStoreLocation(context.Background(), nil, "http://minio.example:9000/bucket/prefix", "", "", "default")
	if err == nil || !strings.Contains(err.Error(), "credentials-secret-ref") {
		t.Fatalf("expected an error naming --credentials-secret-ref, got %v", err)
	}
}
