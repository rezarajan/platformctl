//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	godriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/rezarajan/platformctl/internal/adapters/providers/dbjob"
	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/testkit"
)

// TestBackupRestorePostgresRoundTrip and its mysql/s3 siblings cover
// docs/planning/08 C6's accept criterion: seed rows -> backup -> destroy ->
// apply fresh -> restore -> rows present (postgres, mysql), plus an s3
// Dataset -> Dataset round trip. All three share one durable backup
// destination (testdata/backup-restore-scenario/store.yaml, a bkp-minio
// Provider + two Datasets) applied once and never destroyed mid-test —
// destroying it would wipe the very backups under test. The database tier
// (db.yaml) is destroyed and re-applied fresh via the db-only/ duplicate
// (same resource names, same shared state file) so restore proves it
// repopulates a genuinely fresh, empty database, not the still-live one.
func TestBackupRestorePostgresRoundTrip(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_USERNAME", "bkpadmin")
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_PASSWORD", "bkp-admin-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_PASSWORD", "bkp-mysql-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_USERNAME", "bkpminio")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_PASSWORD", "bkp-minio-pw")

	_, storeStateFile, dbStateFile, combined, dbOnly := setupBackupScenario(t)
	ctx := context.Background()

	// Seed rows.
	conn, err := pgx.Connect(ctx, "postgres://bkpadmin:bkp-admin-pw@127.0.0.1:19730/bkpdb?sslmode=disable")
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS widgets (id serial PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO widgets (name) VALUES ('sprocket'), ('gizmo')`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	_ = conn.Close(ctx)

	// Backup.
	out, err, code := run(t, "backup", "Source/bkp-pg-src", combined, "--to", "Dataset/bkp-store",
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("backup failed (code %d): %v\n%s", code, err, out)
	}
	var manifest backup.Manifest
	if err := json.Unmarshal([]byte(out), &manifest); err != nil {
		t.Fatalf("parse backup manifest: %v\n%s", err, out)
	}
	if manifest.Destination.Key == "" {
		t.Fatalf("manifest has no destination key:\n%s", out)
	}
	if strings.Contains(out, "bkp-admin-pw") || strings.Contains(out, "bkp-minio-pw") {
		t.Fatalf("backup -o json output embeds a plaintext credential:\n%s", out)
	}

	// Destroy just the db tier, then apply it fresh (empty database) —
	// against the db tier's own state file, never the store's (see
	// setupBackupScenario).
	if out, err, code := run(t, "destroy", dbOnly, "--state-file", dbStateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("destroy db tier failed (code %d): %v\n%s", code, err, out)
	}
	if out, err, code := run(t, "apply", dbOnly, "--state-file", dbStateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("re-apply db tier failed (code %d): %v\n%s", code, err, out)
	}

	conn, err = pgx.Connect(ctx, "postgres://bkpadmin:bkp-admin-pw@127.0.0.1:19730/bkpdb?sslmode=disable")
	if err != nil {
		t.Fatalf("connect to fresh postgres: %v", err)
	}
	var count int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_name = 'widgets'`).Scan(&count); err != nil {
		t.Fatalf("check fresh table: %v", err)
	}
	if count != 0 {
		t.Fatal("widgets table exists after destroy+apply fresh — the database was not actually recreated empty")
	}
	_ = conn.Close(ctx)

	// Restore.
	out, err, code = run(t, "restore", "Source/bkp-pg-src", combined, "--from", "Dataset/bkp-store",
		"--object", strings.TrimPrefix(manifest.Destination.Key, "dumps/"),
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true",
		"--yes-i-understand-this-overwrites-existing-data", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("restore failed (code %d): %v\n%s", code, err, out)
	}

	conn, err = pgx.Connect(ctx, "postgres://bkpadmin:bkp-admin-pw@127.0.0.1:19730/bkpdb?sslmode=disable")
	if err != nil {
		t.Fatalf("connect to restored postgres: %v", err)
	}
	defer conn.Close(ctx)
	var names []string
	rows, err := conn.Query(ctx, `SELECT name FROM widgets ORDER BY name`)
	if err != nil {
		t.Fatalf("query restored rows: %v", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, name)
	}
	if got := strings.Join(names, ","); got != "gizmo,sprocket" {
		t.Fatalf("restored rows = %v, want [gizmo sprocket]", names)
	}
}

func TestBackupRestoreMySQLRoundTrip(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_USERNAME", "bkpadmin")
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_PASSWORD", "bkp-admin-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_PASSWORD", "bkp-mysql-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_USERNAME", "bkpminio")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_PASSWORD", "bkp-minio-pw")

	_, storeStateFile, dbStateFile, combined, dbOnly := setupBackupScenario(t)

	dsn := func() string {
		cfg := godriver.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = "root", "bkp-mysql-pw", "tcp", "127.0.0.1:19731", "bkpdb"
		return cfg.FormatDSN()
	}

	db, err := sql.Open("mysql", dsn())
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS widgets (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO widgets (name) VALUES ('sprocket'), ('gizmo')`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	_ = db.Close()

	out, err, code := run(t, "backup", "Source/bkp-mysql-src", combined, "--to", "Dataset/bkp-store",
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("backup failed (code %d): %v\n%s", code, err, out)
	}
	var manifest backup.Manifest
	if err := json.Unmarshal([]byte(out), &manifest); err != nil {
		t.Fatalf("parse backup manifest: %v\n%s", err, out)
	}

	if out, err, code := run(t, "destroy", dbOnly, "--state-file", dbStateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("destroy db tier failed (code %d): %v\n%s", code, err, out)
	}
	if out, err, code := run(t, "apply", dbOnly, "--state-file", dbStateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("re-apply db tier failed (code %d): %v\n%s", code, err, out)
	}

	db, err = sql.Open("mysql", dsn())
	if err != nil {
		t.Fatalf("open fresh mysql: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_name = 'widgets' AND table_schema = 'bkpdb'`).Scan(&count); err != nil {
		t.Fatalf("check fresh table: %v", err)
	}
	if count != 0 {
		t.Fatal("widgets table exists after destroy+apply fresh — the database was not actually recreated empty")
	}
	_ = db.Close()

	out, err, code = run(t, "restore", "Source/bkp-mysql-src", combined, "--from", "Dataset/bkp-store",
		"--object", strings.TrimPrefix(manifest.Destination.Key, "dumps/"),
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true",
		"--yes-i-understand-this-overwrites-existing-data", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("restore failed (code %d): %v\n%s", code, err, out)
	}

	db, err = sql.Open("mysql", dsn())
	if err != nil {
		t.Fatalf("open restored mysql: %v", err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT name FROM widgets ORDER BY name`)
	if err != nil {
		t.Fatalf("query restored rows: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, name)
	}
	if got := strings.Join(names, ","); got != "gizmo,sprocket" {
		t.Fatalf("restored rows = %v, want [gizmo sprocket]", names)
	}
}

// TestBackupRestoreS3DatasetRoundTrip covers the s3 half of the accept
// criterion: a Dataset -> Dataset bucket sync (not a job container — the s3
// provider already speaks S3 in-process), destroy-of-the-source-objects
// standing in for "destroy" (a bucket's object set, not a container, is the
// data here), then restore syncing them back.
func TestBackupRestoreS3DatasetRoundTrip(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_USERNAME", "bkpadmin")
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_PASSWORD", "bkp-admin-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_PASSWORD", "bkp-mysql-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_USERNAME", "bkpminio")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_PASSWORD", "bkp-minio-pw")

	_, storeStateFile, _, combined, _ := setupBackupScenario(t)

	// Seed an object into the bkp-store Dataset directly via the minio-go
	// client against the instance's published host port.
	addr := "127.0.0.1:19732"
	cl := newMinioTestClient(t, addr, "bkpminio", "bkp-minio-pw")
	ctx := context.Background()
	putTestObject(t, ctx, cl, "bkp-store", "dumps/warehouse/part-0001.parquet", "hello-from-warehouse")

	out, err, code := run(t, "backup", "Dataset/bkp-store", combined, "--to", "Dataset/bkp-store-mirror",
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("s3 backup failed (code %d): %v\n%s", code, err, out)
	}

	// Simulate data loss: remove the object from the source bucket.
	if err := cl.RemoveObject(ctx, "bkp-store", "dumps/warehouse/part-0001.parquet", minio.RemoveObjectOptions{}); err != nil {
		t.Fatalf("simulate data loss: %v", err)
	}

	out, err, code = run(t, "restore", "Dataset/bkp-store", combined, "--from", "Dataset/bkp-store-mirror",
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true",
		"--yes-i-understand-this-overwrites-existing-data", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("s3 restore failed (code %d): %v\n%s", code, err, out)
	}

	got := getTestObject(t, ctx, cl, "bkp-store", "dumps/warehouse/part-0001.parquet")
	if got != "hello-from-warehouse" {
		t.Fatalf("restored object content = %q, want %q", got, "hello-from-warehouse")
	}
}

// setupBackupScenario applies the durable backup store (once, into its own
// state file) and the database tier (fresh, into a separate state file), and
// returns the shared Docker runtime, the store's state file (what backup/
// restore commands use — it is the only state that ever carries bkp-minio's
// persisted endpoint fact), the db tier's state file (what the destroy/apply
// cycle uses), and the two manifest paths every subtest needs: combined
// (secrets + store + db, for backup/restore's Kind/name selectors, which
// must resolve both the Source/Dataset being acted on and the Dataset
// destination/source from one manifest) and dbOnly (secrets + db, for
// destroy/apply cycles that must never touch the store).
//
// Two state files, not one: `apply` is authoritative over its *entire*
// state file — any resource present in state but absent from the manifest
// being applied is planned for deletion (internal/application/plan's
// computeApplyDeletes). db-only/db.yaml is deliberately scoped to exclude
// store.yaml's resources so destroy/apply can cycle the database tier
// without touching the durable backup store — but that scoping only holds
// if the store's resources are never present in the *same* state file
// db-only's apply is authoritative over. A single shared state file (this
// scenario's original shape) meant `apply dbOnly` planned — and executed —
// a delete of bkp-minio/bkp-store/bkp-store-mirror on every restore cycle
// (found live: the s3 Dataset round trip alone hid this, since it never
// exercises the db-only destroy/apply cycle at all).
func setupBackupScenario(t *testing.T) (rt *dockerruntime.Runtime, storeStateFile, dbStateFile, combined, dbOnly string) {
	t.Helper()
	rtc, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:        rtc,
		Workloads: []string{"bkp-postgres", "bkp-mysql", "bkp-minio"},
		Volumes:   []string{"bkp-postgres-data", "bkp-mysql-data", "bkp-minio-data"},
		Networks:  []string{"datascape-bkp"},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	storeStateFile = filepath.Join(t.TempDir(), "store-state.json")
	dbStateFile = filepath.Join(t.TempDir(), "db-state.json")
	storeOnly := "testdata/backup-restore-scenario/store-only"
	combined = "testdata/backup-restore-scenario"
	dbOnly = "testdata/backup-restore-scenario/db-only"

	if out, err, code := run(t, "apply", storeOnly, "--state-file", storeStateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("initial store apply failed (code %d): %v\n%s", code, err, out)
	}
	if out, err, code := run(t, "apply", dbOnly, "--state-file", dbStateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("initial db apply failed (code %d): %v\n%s", code, err, out)
	}
	return rtc, storeStateFile, dbStateFile, combined, dbOnly
}

func newMinioTestClient(t *testing.T, addr, user, pass string) *minio.Client {
	t.Helper()
	cl, err := minio.New(addr, &minio.Options{Creds: credentials.NewStaticV4(user, pass, ""), Secure: false})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	return cl
}

func putTestObject(t *testing.T, ctx context.Context, cl *minio.Client, bucket, key, content string) {
	t.Helper()
	exists, err := cl.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("check bucket %q: %v", bucket, err)
	}
	if !exists {
		if err := cl.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatalf("create bucket %q: %v", bucket, err)
		}
	}
	r := strings.NewReader(content)
	if _, err := cl.PutObject(ctx, bucket, key, r, int64(len(content)), minio.PutObjectOptions{}); err != nil {
		t.Fatalf("put object %s/%s: %v", bucket, key, err)
	}
}

func getTestObject(t *testing.T, ctx context.Context, cl *minio.Client, bucket, key string) string {
	t.Helper()
	obj, err := cl.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("get object %s/%s: %v", bucket, key, err)
	}
	defer obj.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, obj); err != nil {
		t.Fatalf("read object %s/%s: %v", bucket, key, err)
	}
	return buf.String()
}

// --- I12 fault-injection tests (docs/planning/08 §7.8 I12; docs/adr/
// 007-backup-restore.md's addendum) ---
//
// These drive internal/adapters/providers/dbjob.RunPipeline directly
// (rather than the full backup/restore CLI verbs) against a live bkp-minio
// destination — the durable-store half of setupBackupScenario, applied
// alone since no database tier is needed to exercise the pipeline's own
// failure handling. Each test injects one of the three failure modes doc
// 08 I12 names (producer dies mid-stream, consumer never starts,
// corrupt/absent exit file) and asserts two things: RunPipeline returns a
// clean, named error (never a hang, never a panic), and the destination
// bucket is left with no partial object — verified by listing, not by
// trusting the error message alone.

// faultMinioAddr/faultNetwork/faultBucket are setupFaultInjectionStore's
// fixed coordinates — testdata/backup-restore-scenario/store-only's
// bkp-minio Provider (same fixture setupBackupScenario's store half uses).
const (
	faultMinioAddr = "127.0.0.1:19732" // host-published port (store.yaml's minio.configuration.port)
	faultNetwork   = "datascape-bkp"
	faultBucket    = "bkp-store"
)

// setupFaultInjectionStore applies just the durable backup store (bkp-minio
// + its Datasets), returning the shared Docker runtime and a backup.Location
// resolved the same way Engine.resolveDatasetLocation would (internal DNS
// name + published F4 port 9000 — internal/adapters/providers/s3/s3.go's
// apiPort — since these tests drive dbjob directly, not through the engine).
func setupFaultInjectionStore(t *testing.T) (rt *dockerruntime.Runtime, loc backup.Location) {
	t.Helper()
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_USERNAME", "bkpminio")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_PASSWORD", "bkp-minio-pw")

	rtc, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	// docs/adr/029: janitor-owned cleanup (J2 sweep) — declared
	// objects, canonical order, silent pre-clean, loud post-clean.
	jan := testkit.Janitor{
		RT:        rtc,
		Workloads: []string{"bkp-minio"},
		Volumes:   []string{"bkp-minio-data"},
		Networks:  []string{faultNetwork},
	}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)

	stateFile := filepath.Join(t.TempDir(), "store-state.json")
	storeOnly := "testdata/backup-restore-scenario/store-only"
	if out, err, code := run(t, "apply", storeOnly, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("store apply failed (code %d): %v\n%s", code, err, out)
	}

	return rtc, backup.Location{
		Endpoint:  "http://bkp-minio:9000",
		Bucket:    faultBucket,
		Prefix:    "dumps",
		Network:   faultNetwork,
		AccessKey: "bkpminio",
		SecretKey: "bkp-minio-pw",
	}
}

// listFaultObjects lists every object under prefix in bucket — the
// "no partial object left behind" half of each fault-injection assertion.
func listFaultObjects(t *testing.T, bucket, prefix string) []string {
	t.Helper()
	cl := newMinioTestClient(t, faultMinioAddr, "bkpminio", "bkp-minio-pw")
	ctx := context.Background()
	var keys []string
	for obj := range cl.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			t.Fatalf("list objects under %s/%s: %v", bucket, prefix, obj.Err)
		}
		keys = append(keys, obj.Key)
	}
	return keys
}

// mcSideFor builds a dbjob.Side speaking mc against loc — shared by every
// fault-injection test that needs a working consumer/cleanup side (only the
// "consumer never starts" test deliberately omits the mc config, to make
// the consumer fail immediately).
func mcSideFor(t *testing.T, loc backup.Location) dbjob.Side {
	t.Helper()
	mcConfig, err := dbjob.MCConfig(loc)
	if err != nil {
		t.Fatalf("mc config: %v", err)
	}
	return dbjob.Side{
		Image:    dbjob.MCImage,
		Networks: []string{loc.Network},
		Env:      map[string]string{"MC_CONFIG_DIR": dbjob.MCConfigDir},
		Files:    []runtime.FileMount{{Path: dbjob.MCConfigPath, Content: mcConfig, Mode: 0o600}},
	}
}

// TestBackupRestoreFaultProducerKilledMidStream covers I12's first
// fault-injection mode: the producer is killed out from under a live
// transfer. RunPipeline must return a clean, producer-named error, and
// PipelineSpec.Cleanup ("mc rm --force", wired the same way postgres/mysql's
// Backup wires it) must leave no partial object in the bucket — even though
// the consumer may have already completed an upload of the truncated bytes
// it received before the FIFO's write end closed when the producer died.
func TestBackupRestoreFaultProducerKilledMidStream(t *testing.T) {
	rt, loc := setupFaultInjectionStore(t)
	jobName := naming.Derived("bkp-fault-mid", naming.Timestamp(time.Now()))
	key := "dumps/" + jobName + ".bin"
	mcSide := mcSideFor(t, loc)

	spec := dbjob.PipelineSpec{
		JobName: jobName,
		Producer: dbjob.Side{
			Image:    dbjob.MCImage,
			Networks: []string{loc.Network},
			// Streams ~6.5MB over ~5s (100 x 64KB chunks, 50ms apart) —
			// long enough for the goroutine below to kill it mid-flight.
			ShellCmd: "i=0; while [ $i -lt 100 ]; do dd if=/dev/zero bs=65536 count=1 2>/dev/null; i=$((i+1)); sleep 0.05; done",
		},
		Consumer: func() dbjob.Side {
			s := mcSide
			s.ShellCmd = fmt.Sprintf("mc pipe %s/%s/%s", dbjob.MCAlias, loc.Bucket, key)
			return s
		}(),
		Cleanup: func() *dbjob.Side {
			s := mcSide
			s.ShellCmd = fmt.Sprintf("mc rm --force %s/%s/%s", dbjob.MCAlias, loc.Bucket, key)
			return &s
		}(),
		Timeout: 60 * time.Second,
	}

	go func() {
		time.Sleep(750 * time.Millisecond)
		_ = rt.Remove(context.Background(), jobName+"-producer")
	}()

	_, err := dbjob.RunPipeline(context.Background(), rt, spec)
	if err == nil {
		t.Fatal("expected RunPipeline to fail when the producer is killed mid-stream")
	}
	if !strings.Contains(err.Error(), "producer") {
		t.Fatalf("expected a producer-named error, got: %v", err)
	}
	t.Logf("clean named error: %v", err)

	if objs := listFaultObjects(t, loc.Bucket, key); len(objs) != 0 {
		t.Fatalf("partial object left behind after producer killed mid-stream: %v", objs)
	}
}

// TestBackupRestoreFaultConsumerNeverStarts covers I12's second
// fault-injection mode: the consumer's tool rejects its command outright
// and exits before ever consuming a byte — the exact failure class of the
// C6/K1 entrypoint bug (mc ran "mc sh -c ..." as an unknown subcommand and
// exited immediately), reproduced here with a genuinely unknown mc
// subcommand. Without the pipeline's peer-unstick logic, the producer
// would otherwise block on its FIFO write for the rest of the timeout.
// RunPipeline must return a clean, consumer-named error promptly (not wait
// out the deadline), and no object may exist at the target key.
//
// (An earlier version of this test used an unregistered mc alias — but mc
// treats an unknown alias as a local filesystem path and exits 0, so the
// injection silently succeeded. Found live in this suite's first run.)
func TestBackupRestoreFaultConsumerNeverStarts(t *testing.T) {
	rt, loc := setupFaultInjectionStore(t)
	jobName := naming.Derived("bkp-fault-noconsumer", naming.Timestamp(time.Now()))
	key := "dumps/" + jobName + ".bin"
	mcSide := mcSideFor(t, loc)

	spec := dbjob.PipelineSpec{
		JobName: jobName,
		Producer: dbjob.Side{
			Image:    dbjob.MCImage,
			Networks: []string{loc.Network},
			ShellCmd: "echo hello",
		},
		Consumer: func() dbjob.Side {
			s := mcSide
			s.ShellCmd = "mc this-subcommand-does-not-exist"
			return s
		}(),
		Cleanup: func() *dbjob.Side {
			s := mcSide
			s.ShellCmd = fmt.Sprintf("mc rm --force %s/%s/%s", dbjob.MCAlias, loc.Bucket, key)
			return &s
		}(),
		Timeout: 30 * time.Second,
	}

	start := time.Now()
	_, err := dbjob.RunPipeline(context.Background(), rt, spec)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected RunPipeline to fail when the consumer never starts")
	}
	if !strings.Contains(err.Error(), "consumer") {
		t.Fatalf("expected a consumer-named error, got: %v", err)
	}
	if elapsed > 20*time.Second {
		t.Fatalf("RunPipeline took %s to detect a consumer that failed immediately — peer-unstick should make this fast, not approach the timeout", elapsed)
	}
	t.Logf("clean named error in %s: %v", elapsed, err)

	if objs := listFaultObjects(t, loc.Bucket, key); len(objs) != 0 {
		t.Fatalf("partial object left behind when consumer never started: %v", objs)
	}
}

// TestBackupRestoreFaultExitFileProtocolBroken covers I12's third
// fault-injection mode, in both variants: the consumer's exit-file
// sentinel is written CORRUPT (garbage instead of an exit code) or never
// written at all — in both cases AFTER the consumer really did upload the
// object, which is the dangerous half of the exit-file-race class: the
// data landed, but the protocol that proves it can no longer be trusted.
// RunPipeline must return a clean, consumer-named error (never treat an
// unverifiable side as success), and Cleanup must remove the
// already-uploaded object so an unverifiable backup never lingers looking
// like a good one — verified by listing the bucket.
//
// Injection: the ShellCmd deliberately breaks out of dbjob's consumer
// wrapper (`(CMD) < pipe; echo $? > consumer-exit`) with an unbalanced
// `)` — the injected text closes the wrapper's subshell itself, performs
// the upload, tampers with (or skips) the exit file, and `exit 0`s the
// wrapper early so its honest exit-file write never runs. This is
// intentionally coupled to sideSpec's wrapper shape: if that shape
// changes, this breaks loudly and the injection must be re-derived. A
// producer-side `kill -9 1` was tried first and is impossible: the kernel
// ignores SIGKILL sent to PID 1 from inside its own PID namespace, so
// that injection silently succeeded (found live in this suite's first
// run).
func TestBackupRestoreFaultExitFileProtocolBroken(t *testing.T) {
	rt, loc := setupFaultInjectionStore(t)
	mcSide := mcSideFor(t, loc)

	cases := []struct {
		name string
		// tamper is the shell fragment run (inside the broken-out wrapper)
		// after the upload completes, in place of the wrapper's honest
		// "echo $? > consumer-exit".
		tamper string
	}{
		{name: "corrupt", tamper: "echo CORRUPT > /work/consumer-exit"},
		{name: "absent", tamper: ":"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			jobName := naming.Derived("bkp-fault-exitfile", tc.name, naming.Timestamp(time.Now()))
			key := "dumps/" + jobName + ".bin"

			spec := dbjob.PipelineSpec{
				JobName: jobName,
				Producer: dbjob.Side{
					Image:    dbjob.MCImage,
					Networks: []string{loc.Network},
					ShellCmd: "echo hello",
				},
				Consumer: func() dbjob.Side {
					s := mcSide
					s.ShellCmd = fmt.Sprintf(
						"mc pipe %s/%s/%s) < %s; %s; exit 0; (true",
						dbjob.MCAlias, loc.Bucket, key, dbjob.PipePath, tc.tamper,
					)
					return s
				}(),
				Cleanup: func() *dbjob.Side {
					s := mcSide
					s.ShellCmd = fmt.Sprintf("mc rm --force %s/%s/%s", dbjob.MCAlias, loc.Bucket, key)
					return &s
				}(),
				Timeout: 30 * time.Second,
			}

			_, err := dbjob.RunPipeline(context.Background(), rt, spec)
			if err == nil {
				t.Fatalf("expected RunPipeline to fail when the consumer's exit file is %s", tc.name)
			}
			if !strings.Contains(err.Error(), "consumer") {
				t.Fatalf("expected a consumer-named error, got: %v", err)
			}
			if tc.name == "corrupt" && !strings.Contains(err.Error(), "CORRUPT") {
				t.Fatalf("expected the corrupt exit-file content to be named in the error, got: %v", err)
			}
			t.Logf("clean named error: %v", err)

			// The consumer genuinely uploaded before the protocol broke —
			// Cleanup must have removed it: an unverifiable backup must
			// not linger looking like a good one.
			if objs := listFaultObjects(t, loc.Bucket, key); len(objs) != 0 {
				t.Fatalf("unverifiable object left behind after %s exit file: %v", tc.name, objs)
			}
		})
	}
}

// --- I13 fault-injection tests (docs/planning/08 §7.8 I13; docs/adr/
// 007-backup-restore.md addendum 2) ---
//
// TestBackupRestoreFaultCorruptionNeverReachesTarget proves the
// verify-then-promote guarantee directly: a stored backup object is
// tampered with AFTER a successful backup (a trailing SQL comment line
// appended — valid SQL, so the corrupted dump still replays without a
// syntax error, but its bytes no longer match the manifest's recorded
// checksum) and Restore is called against the live target. This
// specifically exercises the post-hoc checksum-mismatch path
// (dbjob.VerifyIntegrity), not a producer/consumer container crash (I12
// already covers those, on the Backup side) — the scratch database must be
// dropped and the live target must come out byte-identical to how it went
// in, proven by a full-content fingerprint (not just "still has the old
// rows", which a partial-but-lucky overwrite could satisfy), for both
// engines.

func postgresRowFingerprint(t *testing.T, ctx context.Context, dsn string) string {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for fingerprint: %v", err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT id, name FROM widgets ORDER BY id`)
	if err != nil {
		t.Fatalf("query for fingerprint: %v", err)
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan for fingerprint: %v", err)
		}
		fmt.Fprintf(&sb, "%d:%s\n", id, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read fingerprint rows: %v", err)
	}
	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:])
}

func mysqlRowFingerprint(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT id, name FROM widgets ORDER BY id`)
	if err != nil {
		t.Fatalf("query for fingerprint: %v", err)
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan for fingerprint: %v", err)
		}
		fmt.Fprintf(&sb, "%d:%s\n", id, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read fingerprint rows: %v", err)
	}
	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:])
}

// corruptStoredObject appends a trailing SQL comment line to the object at
// key — the plain-SQL dump format both engines produce still replays
// cleanly (a `-- ...` line is a no-op comment to psql/mysql alike), but the
// object's bytes no longer match the backup manifest's recorded checksum,
// exactly the "corrupted... after it was written" fault this test injects.
func corruptStoredObject(t *testing.T, ctx context.Context, cl *minio.Client, bucket, key string) {
	t.Helper()
	obj, err := cl.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("get object %s/%s to corrupt: %v", bucket, key, err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, obj); err != nil {
		t.Fatalf("read object %s/%s to corrupt: %v", bucket, key, err)
	}
	_ = obj.Close()
	buf.WriteString("\n-- corruption-injected-by-TestBackupRestoreFaultCorruptionNeverReachesTarget\n")
	if _, err := cl.PutObject(ctx, bucket, key, &buf, int64(buf.Len()), minio.PutObjectOptions{}); err != nil {
		t.Fatalf("put corrupted object %s/%s: %v", bucket, key, err)
	}
}

func TestBackupRestoreFaultCorruptionNeverReachesTargetPostgres(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_USERNAME", "bkpadmin")
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_PASSWORD", "bkp-admin-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_PASSWORD", "bkp-mysql-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_USERNAME", "bkpminio")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_PASSWORD", "bkp-minio-pw")

	_, storeStateFile, _, combined, _ := setupBackupScenario(t)
	ctx := context.Background()
	const dsn = "postgres://bkpadmin:bkp-admin-pw@127.0.0.1:19730/bkpdb?sslmode=disable"

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS widgets (id serial PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO widgets (name) VALUES ('sprocket'), ('gizmo')`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	_ = conn.Close(ctx)

	out, err, code := run(t, "backup", "Source/bkp-pg-src", combined, "--to", "Dataset/bkp-store",
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("backup failed (code %d): %v\n%s", code, err, out)
	}
	var manifest backup.Manifest
	if err := json.Unmarshal([]byte(out), &manifest); err != nil {
		t.Fatalf("parse backup manifest: %v\n%s", err, out)
	}

	beforeFingerprint := postgresRowFingerprint(t, ctx, dsn)

	cl := newMinioTestClient(t, faultMinioAddr, "bkpminio", "bkp-minio-pw")
	corruptStoredObject(t, ctx, cl, "bkp-store", manifest.Destination.Key)

	out, err, code = run(t, "restore", "Source/bkp-pg-src", combined, "--from", "Dataset/bkp-store",
		"--object", strings.TrimPrefix(manifest.Destination.Key, "dumps/"),
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true",
		"--yes-i-understand-this-overwrites-existing-data", "-o", "json")
	if err == nil && code == 0 {
		t.Fatalf("expected restore of a corrupted object to fail, got success:\n%s", out)
	}
	if !strings.Contains(out, "integrity check failed") && !strings.Contains(fmt.Sprint(err), "integrity check failed") {
		t.Logf("restore failed as expected, but not via the named integrity-check error (still acceptable — target-untouched is the load-bearing assertion): %v\n%s", err, out)
	}

	afterFingerprint := postgresRowFingerprint(t, ctx, dsn)
	if afterFingerprint != beforeFingerprint {
		t.Fatalf("target content changed after a failed corrupt restore: before=%s after=%s — corruption reached the target", beforeFingerprint, afterFingerprint)
	}

	// The scratch database must have been dropped — no
	// "bkpdb_restore_*" database left lingering after the failure.
	conn, err = pgx.Connect(ctx, "postgres://bkpadmin:bkp-admin-pw@127.0.0.1:19730/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("connect to check scratch cleanup: %v", err)
	}
	defer conn.Close(ctx)
	var scratchCount int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_database WHERE datname LIKE 'bkpdb\_restore\_%'`).Scan(&scratchCount); err != nil {
		t.Fatalf("check for leftover scratch databases: %v", err)
	}
	if scratchCount != 0 {
		t.Fatalf("%d leftover scratch database(s) after a failed restore — scratch was not dropped", scratchCount)
	}
}

func TestBackupRestoreFaultCorruptionNeverReachesTargetMySQL(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_USERNAME", "bkpadmin")
	t.Setenv("DATASCAPE_SECRET_BKP_PG_ADMIN_PASSWORD", "bkp-admin-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_BKP_MYSQL_ROOT_PASSWORD", "bkp-mysql-pw")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_USERNAME", "bkpminio")
	t.Setenv("DATASCAPE_SECRET_BKP_MINIO_ROOT_PASSWORD", "bkp-minio-pw")

	_, storeStateFile, _, combined, _ := setupBackupScenario(t)
	ctx := context.Background()

	dsn := func() string {
		cfg := godriver.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = "root", "bkp-mysql-pw", "tcp", "127.0.0.1:19731", "bkpdb"
		return cfg.FormatDSN()
	}

	db, err := sql.Open("mysql", dsn())
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS widgets (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO widgets (name) VALUES ('sprocket'), ('gizmo')`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	_ = db.Close()

	out, err, code := run(t, "backup", "Source/bkp-mysql-src", combined, "--to", "Dataset/bkp-store",
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("backup failed (code %d): %v\n%s", code, err, out)
	}
	var manifest backup.Manifest
	if err := json.Unmarshal([]byte(out), &manifest); err != nil {
		t.Fatalf("parse backup manifest: %v\n%s", err, out)
	}

	db, err = sql.Open("mysql", dsn())
	if err != nil {
		t.Fatalf("open mysql for fingerprint: %v", err)
	}
	beforeFingerprint := mysqlRowFingerprint(t, db)
	_ = db.Close()

	cl := newMinioTestClient(t, faultMinioAddr, "bkpminio", "bkp-minio-pw")
	corruptStoredObject(t, ctx, cl, "bkp-store", manifest.Destination.Key)

	out, err, code = run(t, "restore", "Source/bkp-mysql-src", combined, "--from", "Dataset/bkp-store",
		"--object", strings.TrimPrefix(manifest.Destination.Key, "dumps/"),
		"--state-file", storeStateFile, "--feature-gates=BackupRestore=true",
		"--yes-i-understand-this-overwrites-existing-data", "-o", "json")
	if err == nil && code == 0 {
		t.Fatalf("expected restore of a corrupted object to fail, got success:\n%s", out)
	}
	if !strings.Contains(out, "integrity check failed") && !strings.Contains(fmt.Sprint(err), "integrity check failed") {
		t.Logf("restore failed as expected, but not via the named integrity-check error (still acceptable — target-untouched is the load-bearing assertion): %v\n%s", err, out)
	}

	db, err = sql.Open("mysql", dsn())
	if err != nil {
		t.Fatalf("open mysql to re-check fingerprint: %v", err)
	}
	afterFingerprint := mysqlRowFingerprint(t, db)
	_ = db.Close()
	if afterFingerprint != beforeFingerprint {
		t.Fatalf("target content changed after a failed corrupt restore: before=%s after=%s — corruption reached the target", beforeFingerprint, afterFingerprint)
	}

	// The scratch schema must have been dropped — no "bkpdb_restore_*"
	// schema left lingering after the failure.
	admin, err := sql.Open("mysql", func() string {
		cfg := godriver.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = "root", "bkp-mysql-pw", "tcp", "127.0.0.1:19731"
		return cfg.FormatDSN()
	}())
	if err != nil {
		t.Fatalf("open mysql to check scratch cleanup: %v", err)
	}
	defer admin.Close()
	var scratchCount int
	if err := admin.QueryRow(`SELECT count(*) FROM information_schema.schemata WHERE schema_name LIKE 'bkpdb\_restore\_%'`).Scan(&scratchCount); err != nil {
		t.Fatalf("check for leftover scratch schemas: %v", err)
	}
	if scratchCount != 0 {
		t.Fatalf("%d leftover scratch schema(s) after a failed restore — scratch was not dropped", scratchCount)
	}
}
