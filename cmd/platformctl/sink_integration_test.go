//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
)

const (
	sinkConnectURL = "http://localhost:18186"
	sinkMinioAddr  = "localhost:19101"
	sinkPGConn     = "postgres://datascape_admin:admin-secret-pw@localhost:15545/attendance?sslmode=disable"
)

// TestSinkEndToEnd covers the Phase 4 exit criteria: the Phase 3 manifest set
// extended with a minio Provider, a Dataset, and a Binding(mode: sink), with
// real CDC traffic landing as objects in MinIO. The sink capability check at
// validate is covered in application/compatibility.
func TestSinkEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_SINK_PG_ADMIN_USERNAME", "datascape_admin")
	t.Setenv("DATASCAPE_SECRET_SINK_PG_ADMIN_PASSWORD", "admin-secret-pw")
	t.Setenv("DATASCAPE_SECRET_SINK_PG_REPL_USERNAME", "datascape_repl")
	t.Setenv("DATASCAPE_SECRET_SINK_PG_REPL_PASSWORD", "repl-secret-pw")
	t.Setenv("DATASCAPE_SECRET_SINK_MINIO_ROOT_USERNAME", "datascape_minio")
	t.Setenv("DATASCAPE_SECRET_SINK_MINIO_ROOT_PASSWORD", "minio-secret-pw")

	buildSinkConnectImage(t)

	rt, err := dockerruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	containers := []string{"datascape-sink-s3", "datascape-sink-minio", "datascape-sink-dbz", "datascape-sink-pg", "datascape-sink-rp"}
	cleanup := func() {
		for _, c := range containers {
			_ = rt.Remove(ctx, c)
		}
		for _, v := range []string{"datascape-sink-pg-data", "datascape-sink-rp-data", "datascape-sink-minio-data"} {
			_ = rt.RemoveVolume(ctx, v)
		}
		_ = rt.RemoveNetwork(ctx, "datascape-sink-net")
	}
	cleanup()
	t.Cleanup(cleanup)

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/sink-scenario"

	// Exit criterion: the extended manifest set reaches Ready end-to-end.
	start := time.Now()
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("apply from empty state took %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after apply: %s", line)
		}
	}

	// Real CDC traffic: create the captured table and insert rows; Debezium
	// streams them to the per-table topic, the sink lands them in MinIO.
	insertRows(t, ctx)
	obj := waitForObject(t, ctx, "raw-events", "attendance/", 180*time.Second)
	if !strings.Contains(obj, "alice") {
		t.Errorf("landed object does not contain inserted row 'alice':\n%s", obj)
	}

	minioBefore, found, err := rt.Inspect(ctx, "datascape-sink-minio")
	if err != nil || !found {
		t.Fatalf("minio container not found after apply: %v", err)
	}
	pgBefore, _, _ := rt.Inspect(ctx, "datascape-sink-pg")
	rpBefore, _, _ := rt.Inspect(ctx, "datascape-sink-rp")

	// Exit criterion: idempotent re-apply across all newly-added resources.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("re-apply did not report 'no changes':\n%s", out)
	}

	// Exit criterion: changing Dataset.spec.format updates the connector
	// without recreating the broker, database, or object store.
	changed := filepath.Join(t.TempDir(), "changed")
	if err := os.MkdirAll(changed, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(manifests, "manifests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	bumped := strings.Replace(string(data), "format: json", "format: jsonl", 1)
	if bumped == string(data) {
		t.Fatal("format replacement did not change the manifest")
	}
	if err := os.WriteFile(filepath.Join(changed, "manifests.yaml"), []byte(bumped), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err, code = run(t, "apply", changed, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("format-change apply failed (code %d): %v\n%s", code, err, out)
	}
	cfg := sinkConnectorConfig(t, "sink-events-to-lake")
	if got, want := cfg["format.output.type"], "jsonl"; got != want {
		t.Errorf("format.output.type = %q, want %q", got, want)
	}
	for name, before := range map[string]string{
		"datascape-sink-minio": minioBefore.ID,
		"datascape-sink-pg":    pgBefore.ID,
		"datascape-sink-rp":    rpBefore.ID,
	} {
		after, found, err := rt.Inspect(ctx, name)
		if err != nil || !found {
			t.Fatalf("%s missing after format update: %v", name, err)
		}
		if after.ID != before {
			t.Errorf("%s was recreated (ID %s -> %s); a Dataset format change must not touch it", name, before, after.ID)
		}
	}

	// Exit criterion: destroy tears down the sink connector, the object
	// store, and its data cleanly.
	out, err, code = run(t, "destroy", changed, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, c := range containers {
		if _, found, _ := rt.Inspect(ctx, c); found {
			t.Errorf("container %s still present after destroy", c)
		}
	}
	managed, err := rt.ListManaged(ctx)
	if err != nil {
		t.Fatalf("list managed: %v", err)
	}
	for _, m := range managed {
		if strings.HasPrefix(m.Name, "datascape-sink-") {
			t.Errorf("orphaned managed container after destroy: %s", m.Name)
		}
	}
}

// buildSinkConnectImage builds the Connect worker image carrying the Aiven
// S3 sink plugin (stock Connect images ship none); cached across runs.
func buildSinkConnectImage(t *testing.T) {
	t.Helper()
	build := exec.Command("docker", "build", "-t", "datascape-s3sink-connect:test", "testdata/s3sink-image")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sink connect image: %v\n%s", err, out)
	}
}

func insertRows(t *testing.T, ctx context.Context) {
	t.Helper()
	conn, err := pgx.Connect(ctx, sinkPGConn)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS students (id serial PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO students (name) VALUES ('alice'), ('bob')`); err != nil {
		t.Fatalf("insert rows: %v", err)
	}
}

// waitForObject polls the bucket until an object appears under prefix and
// returns its contents.
func waitForObject(t *testing.T, ctx context.Context, bucket, prefix string, timeout time.Duration) string {
	t.Helper()
	cl, err := minio.New(sinkMinioAddr, &minio.Options{
		Creds:  credentials.NewStaticV4("datascape_minio", "minio-secret-pw", ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	deadline := time.Now().Add(timeout)
	for {
		for obj := range cl.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if obj.Err != nil {
				break // bucket may not exist yet
			}
			r, err := cl.GetObject(ctx, bucket, obj.Key, minio.GetObjectOptions{})
			if err != nil {
				t.Fatalf("get object %s: %v", obj.Key, err)
			}
			body, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				t.Fatalf("read object %s: %v", obj.Key, err)
			}
			t.Logf("object landed: %s/%s (%d bytes)", bucket, obj.Key, len(body))
			return string(body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("no object appeared under %s/%s within %s (sink connector state: %s)", bucket, prefix, timeout, sinkConnectorState(t))
		}
		time.Sleep(3 * time.Second)
	}
}

func sinkConnectorConfig(t *testing.T, name string) map[string]string {
	t.Helper()
	var cfg map[string]string
	getJSON(t, fmt.Sprintf("%s/connectors/%s/config", sinkConnectURL, name), &cfg)
	return cfg
}

func sinkConnectorState(t *testing.T) string {
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			State string `json:"state"`
			Trace string `json:"trace"`
		} `json:"tasks"`
	}
	getJSON(t, fmt.Sprintf("%s/connectors/%s/status", sinkConnectURL, "sink-events-to-lake"), &body)
	states := body.Connector.State
	for _, task := range body.Tasks {
		states += " task:" + task.State
		if task.Trace != "" {
			states += " trace: " + task.Trace
		}
	}
	return states
}
