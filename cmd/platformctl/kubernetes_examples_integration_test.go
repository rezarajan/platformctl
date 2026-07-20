//go:build integration

package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// rewriteExampleForKubernetes reads every *.yaml file in srcDir, rewrites
// every Provider's spec.runtime to {type: kubernetes, network: ns, access},
// and writes the combined result as one manifests.yaml in a fresh temp dir
// (docs/planning/08 B8's own suggestion: "a runtime-parameterized manifest
// fixture" — this parameterizes the literal example manifests rather than
// hand-maintaining a second, driftable copy of them).
// loadImageIntoCluster copies a locally-built image onto the test cluster's
// node(s) so a Deployment referencing it (with imagePullPolicy other than
// Always) can start without a registry. The CI cluster is kind
// (helm/kind-action, see .github/workflows/ci.yml), while local runs may use
// minikube — dispatch on whichever is actually present rather than assuming
// one. kind is tried first: `kind get clusters` exits 0 with empty output
// when no kind cluster exists, so an empty list cleanly falls through to the
// minikube path.
func loadImageIntoCluster(t *testing.T, image string) {
	t.Helper()
	if out, err := exec.Command("kind", "get", "clusters").Output(); err == nil {
		clusters := strings.Fields(strings.TrimSpace(string(out)))
		if len(clusters) > 0 {
			for _, name := range clusters {
				if out, err := exec.Command("kind", "load", "docker-image", image, "--name", name).CombinedOutput(); err != nil {
					t.Fatalf("kind load docker-image into %q: %v\n%s", name, err, out)
				}
			}
			return
		}
	}
	if out, err := exec.Command("minikube", "image", "load", image).CombinedOutput(); err != nil {
		t.Fatalf("minikube image load: %v\n%s", err, out)
	}
}

func rewriteExampleForKubernetes(t *testing.T, srcDir, ns, access string) string {
	t.Helper()
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatalf("read dir %s: %v", srcDir, err)
	}
	var docs []map[string]any
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(srcDir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		dec := yaml.NewDecoder(bytes.NewReader(data))
		for {
			var doc map[string]any
			if err := dec.Decode(&doc); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("parse %s: %v", e.Name(), err)
			}
			if doc == nil {
				continue
			}
			if doc["kind"] == "Provider" {
				if spec, ok := doc["spec"].(map[string]any); ok {
					spec["runtime"] = map[string]any{"type": "kubernetes", "network": ns, "access": access}
				}
			}
			docs = append(docs, doc)
		}
	}
	var buf bytes.Buffer
	for i, doc := range docs {
		if i > 0 {
			buf.WriteString("---\n")
		}
		data, err := yaml.Marshal(doc)
		if err != nil {
			t.Fatalf("encode doc %d: %v", i, err)
		}
		buf.Write(data)
	}
	outDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(outDir, "manifests.yaml"), buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write manifests: %v", err)
	}
	return outDir
}

// TestCDCAttendanceExampleOnKubernetes covers the Stage B goal's literal
// exit criterion #1 (docs/planning/08 §4): the full examples/cdc-attendance/
// scenario applies to Ready on a real cluster, with platformctl itself
// running outside it, data actually flows end to end (a row inserted into
// Postgres lands as an object in MinIO through the real Debezium+Redpanda+
// S3-sink pipeline), and destroy tears down cleanly. access: node-port
// throughout so every provider's CLI-side admin call (the B8 fix) resolves
// a real, externally-dialable address.
func TestCDCAttendanceExampleOnKubernetes(t *testing.T) {
	requireK8s(t)
	t.Setenv("DATASCAPE_SECRET_POSTGRES_ADMIN_CREDS_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_POSTGRES_ADMIN_CREDS_PASSWORD", "admin-pw")
	t.Setenv("DATASCAPE_SECRET_POSTGRES_REPLICATION_CREDS_USERNAME", "repl")
	t.Setenv("DATASCAPE_SECRET_POSTGRES_REPLICATION_CREDS_PASSWORD", "repl-pw")
	t.Setenv("DATASCAPE_SECRET_MINIO_ROOT_CREDS_USERNAME", "minioadmin")
	t.Setenv("DATASCAPE_SECRET_MINIO_ROOT_CREDS_PASSWORD", "minioadmin-pw")

	// The s3sink connector image has no public registry copy (built
	// on-demand, same as the Docker acceptance test) — the cluster's node
	// needs its own copy since it doesn't share the host's Docker image
	// cache the way `docker run` does.
	build := exec.Command("docker", "build", "-t", "datascape-s3sink-connect:local", "../../examples/cdc-attendance/s3sink-image")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sink connect image: %v\n%s", err, out)
	}
	loadImageIntoCluster(t, "datascape-s3sink-connect:local")

	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	const ns = "datascape-cdc-example-test"
	cleanup := func() { _ = rt.RemoveNetwork(ctx, ns) }
	cleanup()
	t.Cleanup(cleanup)

	manifests := rewriteExampleForKubernetes(t, "../../examples/cdc-attendance", ns, "node-port")
	stateFile := filepath.Join(t.TempDir(), "state.json")
	const gateVal = "KubernetesRuntime=true"

	out, err, code := run(t, "validate", manifests, "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("validate failed (code %d): %v\n%s", code, err, out)
	}

	start := time.Now()
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("cdc-attendance applied to kubernetes in %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready: %s", line)
		}
	}

	// Data actually flows: insert a row into the real Postgres (reached via
	// EnsureReachable, not a hardcoded port) and confirm it lands as an
	// object in the real MinIO, through Debezium -> Redpanda -> s3sink.
	pgAddr, closePG, err := rt.EnsureReachable(ctx, "local-postgres", 5432)
	if err != nil {
		t.Fatalf("EnsureReachable(local-postgres): %v", err)
	}
	defer closePG()
	insertCDCAttendanceRow(t, ctx, pgAddr)

	minioAddr, closeMinio, err := rt.EnsureReachable(ctx, "local-minio", 9000)
	if err != nil {
		t.Fatalf("EnsureReachable(local-minio): %v", err)
	}
	defer closeMinio()
	obj := waitForObjectAt(t, ctx, minioAddr, "minioadmin", "minioadmin-pw", "raw-events", "attendance/", 180*time.Second)
	if !strings.Contains(obj, "k8s-acceptance-alice") {
		t.Errorf("landed object missing inserted row:\n%.500s", obj)
	}

	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, name := range []string{"local-redpanda", "local-postgres", "postgres-cdc", "local-minio", "s3-sink"} {
		if _, found, err := rt.Inspect(ctx, name); err != nil {
			t.Errorf("Inspect(%s) after destroy: %v", name, err)
		} else if found {
			t.Errorf("deployment %q still present after destroy", name)
		}
	}
}

func insertCDCAttendanceRow(t *testing.T, ctx context.Context, addr string) {
	t.Helper()
	connStr := fmt.Sprintf("postgres://admin:admin-pw@%s/studentdb?sslmode=disable", addr)
	deadline := time.Now().Add(60 * time.Second)
	var conn *pgx.Conn
	var err error
	for {
		conn, err = pgx.Connect(ctx, connStr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect to postgres at %s: %v", addr, err)
		}
		time.Sleep(1 * time.Second)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS students (id serial PRIMARY KEY, name text NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO students (name) VALUES ('k8s-acceptance-alice')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
}

// pingLakehouseHTTP is a small helper mirroring the Docker lakehouse test's
// inline HTTP checks, parameterized on address instead of a hardcoded port.
func pingLakehouseHTTP(t *testing.T, url string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Errorf("GET %s: %v", url, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
}

func pingLakehouseMySQL(t *testing.T, addr string) error {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("root:mysql-root-pw@tcp(%s)/eventsdb?timeout=10s", addr))
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Ping()
}

// TestLakehouseExampleOnKubernetes covers the Stage B goal's literal exit
// criterion #2 (docs/planning/08 §4): the full examples/lakehouse/ scenario
// — Catalog + Connection kinds included — applies to Ready on a real
// cluster from outside it, and destroys cleanly. The Connection's target
// ("external-orders-db:5432", catalog-and-connections.yaml) is stood up as
// a real, unmanaged-by-platformctl Kubernetes Deployment in the same
// namespace first, mirroring the Docker test's out-of-band `docker run` —
// the proxy Provider's forwarder must reach it via cluster DNS exactly like
// it reaches everything else it manages.
func TestLakehouseExampleOnKubernetes(t *testing.T) {
	requireK8s(t)
	t.Setenv("DATASCAPE_SECRET_LAKE_MINIO_ROOT_USERNAME", "minioadmin")
	t.Setenv("DATASCAPE_SECRET_LAKE_MINIO_ROOT_PASSWORD", "minioadmin-pw")
	t.Setenv("DATASCAPE_SECRET_LAKE_PG_ADMIN_USERNAME", "admin")
	t.Setenv("DATASCAPE_SECRET_LAKE_PG_ADMIN_PASSWORD", "admin-pw")
	t.Setenv("DATASCAPE_SECRET_LAKE_MYSQL_ROOT_USERNAME", "root")
	t.Setenv("DATASCAPE_SECRET_LAKE_MYSQL_ROOT_PASSWORD", "mysql-root-pw")
	t.Setenv("DATASCAPE_SECRET_EXT_ORDERS_CREDS_USERNAME", "orders_ro")
	t.Setenv("DATASCAPE_SECRET_EXT_ORDERS_CREDS_PASSWORD", "orders-pw")

	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	const ns = "datascape-lakehouse-example-test"
	// external-orders-db is stood up out-of-band below and is never part of
	// platformctl's managed lifecycle, so RemoveNetwork now refuses to delete
	// the namespace while it (a workload) is still present — the same "network
	// in use" refusal Docker gives. Tear it down explicitly first so cleanup
	// can actually drop the namespace and leave nothing behind.
	cleanup := func() {
		_ = rt.Remove(ctx, "external-orders-db")
		_ = rt.RemoveNetwork(ctx, ns)
	}
	cleanup()
	t.Cleanup(cleanup)

	labels := map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue}
	if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}
	// The external database — out-of-band, never platformctl-managed
	// lifecycle, but a real object on the cluster so the proxy's forwarder
	// (running in-cluster) can actually reach it by name. A real
	// HealthCheck (pg_isready), not just "pod Running": several providers
	// in this same suite (nessie, openlineage) turned out to declare no
	// in-container healthcheck and paid for it in flaky CLI-side races —
	// this container isn't part of that fix, so it needs its own real one.
	if _, err := rt.EnsureContainer(ctx, runtimeport.ContainerSpec{
		Name:  "external-orders-db",
		Image: "postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20",
		Cmd:   []string{"postgres", "-c", "wal_level=logical"},
		Env: map[string]string{
			"POSTGRES_USER":     "orders_ro",
			"POSTGRES_PASSWORD": "orders-pw",
			"POSTGRES_DB":       "orders",
		},
		Networks: []string{ns},
		Ports:    []runtimeport.PortBinding{{ContainerPort: 5432}},
		HealthCheck: &runtimeport.HealthCheck{
			Test:     []string{"CMD-SHELL", "pg_isready -h 127.0.0.1 -U orders_ro"},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
		Labels: labels,
	}); err != nil {
		t.Fatalf("stand up external-orders-db: %v", err)
	}
	if err := rt.WaitHealthy(ctx, "external-orders-db", 60*time.Second); err != nil {
		t.Fatalf("external-orders-db WaitHealthy: %v", err)
	}
	// Also confirm it's actually dialable through a fresh tunnel before
	// proceeding — the same belt-and-suspenders EnsureReachable retry as
	// everywhere else in this file, since WaitHealthy alone (no readiness
	// probe backing it on Kubernetes) only proves the pod is Running.
	{
		_, closeConn := dialLakehouseConnectionWithRetry(t, ctx, rt, "external-orders-db", 5432, "orders_ro", "orders-pw", "orders", 60*time.Second)
		closeConn()
	}

	manifests := rewriteExampleForKubernetes(t, "../../examples/lakehouse", ns, "node-port")
	stateFile := filepath.Join(t.TempDir(), "state.json")
	const gateVal = "KubernetesRuntime=true"

	out, err, code := run(t, "validate", manifests, "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("validate failed (code %d): %v\n%s", code, err, out)
	}

	start := time.Now()
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	t.Logf("lakehouse applied to kubernetes in %s", time.Since(start).Round(time.Second))

	out, err, code = run(t, "status", manifests, "--state-file", stateFile, "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready: %s", line)
		}
	}

	nessieAddr, closeNessie, err := rt.EnsureReachable(ctx, "catalog-svc", 19120)
	if err != nil {
		t.Fatalf("EnsureReachable(catalog-svc): %v", err)
	}
	pingLakehouseHTTP(t, "http://"+nessieAddr+"/api/v2/config")
	pingLakehouseHTTP(t, "http://"+nessieAddr+"/api/v2/trees/main")
	closeNessie()

	marquezAddr, closeMarquez, err := rt.EnsureReachable(ctx, "lake-lineage", 5000)
	if err != nil {
		t.Fatalf("EnsureReachable(lake-lineage): %v", err)
	}
	pingLakehouseHTTP(t, "http://"+marquezAddr+"/api/v1/namespaces")
	closeMarquez()

	mysqlAddr, closeMySQL, err := rt.EnsureReachable(ctx, "lake-mysql", 3306)
	if err != nil {
		t.Fatalf("EnsureReachable(lake-mysql): %v", err)
	}
	if err := pingLakehouseMySQL(t, mysqlAddr); err != nil {
		t.Errorf("mysql source not reachable: %v", err)
	}
	closeMySQL()

	// The Connection is a working entrypoint to the external database,
	// reached exactly the way any other client would: through the proxy's
	// forwarder, resolved via EnsureReachable rather than a hardcoded port.
	// A fresh tunnel is opened per attempt, not reused across retries — a
	// port-forward tunnel opened while the forwarder pod's own upstream
	// dial is still warming up can end up silently dead even once it's
	// ready (see nessie/openlineage's waitAPIReady doc for the same
	// finding in production code).
	// 15999 is the Connection's own declared spec.port (what consumers
	// dial), not the target's 5432 — the forwarder listens on the former
	// and connects onward to the latter (catalog-and-connections.yaml).
	pgConn, closePGConn := dialLakehouseConnectionWithRetry(t, ctx, rt, "orders-db", 15999, "orders_ro", "orders-pw", "orders", 60*time.Second)
	if _, err := pgConn.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (id serial PRIMARY KEY, sku text);
		INSERT INTO orders (sku) VALUES ('sku-1')`); err != nil {
		t.Fatalf("write through the Connection entrypoint: %v", err)
	}
	closePGConn()

	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gateVal)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for _, name := range []string{"orders-cdc", "lake-redpanda", "orders-db", "lake-lineage", "lake-lineage-db", "catalog-svc", "lake-mysql", "lake-postgres", "lake-minio"} {
		if _, found, err := rt.Inspect(ctx, name); err != nil {
			t.Errorf("Inspect(%s) after destroy: %v", name, err)
		} else if found {
			t.Errorf("deployment %q still present after destroy", name)
		}
	}
	// external-orders-db was never platformctl-managed; destroy must not
	// have touched it.
	if _, found, err := rt.Inspect(ctx, "external-orders-db"); err != nil || !found {
		t.Errorf("external-orders-db missing after destroy (found=%v err=%v); destroy must not touch unmanaged infrastructure", found, err)
	}
}

// dialLakehouseConnectionWithRetry connects to name:port via a freshly
// resolved EnsureReachable tunnel, retrying with a brand-new tunnel each
// attempt until one succeeds or timeout elapses. The returned close func
// tears down both the pgx connection and its tunnel — the tunnel must
// stay open for the connection's whole lifetime, not just the connect
// call, so it is deliberately not closed until the caller is done.
func dialLakehouseConnectionWithRetry(t *testing.T, ctx context.Context, rt *k8sruntime.Runtime, name string, port int, user, pass, db string, timeout time.Duration) (*pgx.Conn, func()) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		addr, closeAddr, err := rt.EnsureReachable(ctx, name, port)
		if err == nil {
			connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, pass, addr, db)
			conn, cerr := pgx.Connect(ctx, connStr)
			if cerr == nil {
				return conn, func() {
					_ = conn.Close(ctx)
					_ = closeAddr()
				}
			}
			lastErr = cerr
			closeAddr()
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect to %s:%d within %s: %v", name, port, timeout, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}
