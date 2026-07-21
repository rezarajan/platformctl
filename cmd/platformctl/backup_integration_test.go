//go:build integration

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"

	godriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/domain/backup"
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

	_, stateFile, combined, dbOnly := setupBackupScenario(t)
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
		"--state-file", stateFile, "--feature-gates=BackupRestore=true", "-o", "json")
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

	// Destroy just the db tier, then apply it fresh (empty database).
	if out, err, code := run(t, "destroy", dbOnly, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("destroy db tier failed (code %d): %v\n%s", code, err, out)
	}
	if out, err, code := run(t, "apply", dbOnly, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
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
		"--state-file", stateFile, "--feature-gates=BackupRestore=true",
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

	_, stateFile, combined, dbOnly := setupBackupScenario(t)

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
		"--state-file", stateFile, "--feature-gates=BackupRestore=true", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("backup failed (code %d): %v\n%s", code, err, out)
	}
	var manifest backup.Manifest
	if err := json.Unmarshal([]byte(out), &manifest); err != nil {
		t.Fatalf("parse backup manifest: %v\n%s", err, out)
	}

	if out, err, code := run(t, "destroy", dbOnly, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("destroy db tier failed (code %d): %v\n%s", code, err, out)
	}
	if out, err, code := run(t, "apply", dbOnly, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
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
		"--state-file", stateFile, "--feature-gates=BackupRestore=true",
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

	_, stateFile, combined, _ := setupBackupScenario(t)

	// Seed an object into the bkp-store Dataset directly via the minio-go
	// client against the instance's published host port.
	addr := "127.0.0.1:19732"
	cl := newMinioTestClient(t, addr, "bkpminio", "bkp-minio-pw")
	ctx := context.Background()
	putTestObject(t, ctx, cl, "bkp-store", "dumps/warehouse/part-0001.parquet", "hello-from-warehouse")

	out, err, code := run(t, "backup", "Dataset/bkp-store", combined, "--to", "Dataset/bkp-store-mirror",
		"--state-file", stateFile, "--feature-gates=BackupRestore=true", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("s3 backup failed (code %d): %v\n%s", code, err, out)
	}

	// Simulate data loss: remove the object from the source bucket.
	if err := cl.RemoveObject(ctx, "bkp-store", "dumps/warehouse/part-0001.parquet", minio.RemoveObjectOptions{}); err != nil {
		t.Fatalf("simulate data loss: %v", err)
	}

	out, err, code = run(t, "restore", "Dataset/bkp-store", combined, "--from", "Dataset/bkp-store-mirror",
		"--state-file", stateFile, "--feature-gates=BackupRestore=true",
		"--yes-i-understand-this-overwrites-existing-data", "-o", "json")
	if err != nil || code != 0 {
		t.Fatalf("s3 restore failed (code %d): %v\n%s", code, err, out)
	}

	got := getTestObject(t, ctx, cl, "bkp-store", "dumps/warehouse/part-0001.parquet")
	if got != "hello-from-warehouse" {
		t.Fatalf("restored object content = %q, want %q", got, "hello-from-warehouse")
	}
}

// setupBackupScenario applies the durable backup store (once) and the
// database tier (fresh), and returns the shared Docker runtime, the shared
// state file, and the two manifest paths every subtest needs: combined
// (secrets + store + db, for backup/restore) and dbOnly (secrets + db, for
// destroy/apply cycles that must never touch the store).
func setupBackupScenario(t *testing.T) (rt *dockerruntime.Runtime, stateFile, combined, dbOnly string) {
	t.Helper()
	rtc, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()
	cleanup := func() {
		for _, c := range []string{"bkp-postgres", "bkp-mysql", "bkp-minio"} {
			_ = rtc.Remove(ctx, c)
		}
		for _, v := range []string{"bkp-postgres-data", "bkp-mysql-data", "bkp-minio-data"} {
			_ = rtc.RemoveVolume(ctx, v)
		}
		_ = rtc.RemoveNetwork(ctx, "datascape-bkp")
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile = filepath.Join(t.TempDir(), "state.json")
	combined = "testdata/backup-restore-scenario"
	dbOnly = "testdata/backup-restore-scenario/db-only"

	if out, err, code := run(t, "apply", combined, "--state-file", stateFile, "--auto-approve"); err != nil || code != 0 {
		t.Fatalf("initial apply failed (code %d): %v\n%s", code, err, out)
	}
	return rtc, stateFile, combined, dbOnly
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
