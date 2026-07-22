//go:build integration

package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minlifecycle "github.com/minio/minio-go/v7/pkg/lifecycle"
	"github.com/twmb/franz-go/pkg/kgo"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// This file covers docs/planning/08 C4 (object-store production posture:
// external S3 + distributed MinIO) and D7 (Dataset lifecycle/retention),
// bundled because both land in the s3 provider and share fixtures/helpers
// with sink_integration_test.go (buildSinkConnectImage) and
// shared_state_integration_test.go's out-of-band-MinIO pattern.

// --- C4 half 1: external object store ---------------------------------

const (
	s3extMinioContainer = "datascape-s3ext-minio"
	s3extMinioNet       = "datascape-s3ext-net"
	s3extMinioHostPort  = "19199"
	s3extMinioUser      = "datascape-s3ext-user"
	s3extMinioPass      = "datascape-s3ext-pw"
)

// startExternalMinio stands up a plain MinIO container out-of-band (`docker
// run`, never through platformctl) to simulate a cloud bucket: platformctl
// only ever talks to it via Connection/credentials, and creates/deletes
// nothing about the container itself (docs/planning/08 C4). It is attached
// to s3extMinioNet (pre-created here) so the in-network sink Binding can
// also reach it by container name — the same network the manifest's other,
// managed Providers join via their own EnsureNetwork call.
func startExternalMinio(t *testing.T) {
	t.Helper()
	// Pre-create the network carrying platformctl's own ownership label
	// (runtime.LabelManagedBy/ManagedByValue) so the manifest's managed
	// Providers' own EnsureNetwork call (first-mover on this network name)
	// accepts it instead of refusing to reuse an "unmanaged" network — the
	// same ownership guard that protects a real deployment's networks
	// (docs/planning/07 §0.4) live-caught here: the network must exist
	// before the external container joins it, but only platformctl's own
	// label makes that pre-existence legitimate.
	if out, err := exec.Command("docker", "network", "create",
		"--label", runtime.LabelManagedBy+"="+runtime.ManagedByValue,
		s3extMinioNet).CombinedOutput(); err != nil {
		t.Fatalf("create external network: %v\n%s", err, out)
	}
	if out, err := exec.Command("docker", "run", "-d", "--name", s3extMinioContainer,
		"--network", s3extMinioNet,
		"-p", s3extMinioHostPort+":9000",
		"-e", "MINIO_ROOT_USER="+s3extMinioUser,
		"-e", "MINIO_ROOT_PASSWORD="+s3extMinioPass,
		"minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e",
		"server", "/data").CombinedOutput(); err != nil {
		t.Fatalf("start external minio: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", s3extMinioContainer).Run()
		_ = exec.Command("docker", "network", "rm", s3extMinioNet).Run()
	})

	cl, err := minio.New("127.0.0.1:"+s3extMinioHostPort, &minio.Options{
		Creds: credentials.NewStaticV4(s3extMinioUser, s3extMinioPass, ""),
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := cl.ListBuckets(context.Background()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("external minio did not become ready in time")
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func s3extClient(t *testing.T) *minio.Client {
	t.Helper()
	cl, err := minio.New("127.0.0.1:"+s3extMinioHostPort, &minio.Options{
		Creds: credentials.NewStaticV4(s3extMinioUser, s3extMinioPass, ""),
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	return cl
}

// TestS3ExternalDatasetEndToEnd covers docs/planning/08 C4 half 1 and D7's
// external contract: Provider(type: s3, external: true) + Connection
// against a real (test-simulated) external endpoint, a Dataset (with
// spec.lifecycle) and an s3sink Binding working with zero containers
// platformctl itself created for the object store, lifecycle rule/
// versioning visible via the S3 API after apply, and an out-of-band
// lifecycle change detected as drift and healed.
func TestS3ExternalDatasetEndToEnd(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_S3EXT_CREDS_USERNAME", s3extMinioUser)
	t.Setenv("DATASCAPE_SECRET_S3EXT_CREDS_PASSWORD", s3extMinioPass)

	buildSinkConnectImage(t)
	startExternalMinio(t)

	rt := requireDocker(t)
	ctx := context.Background()
	managedContainers := []string{"datascape-s3ext-rp", "datascape-s3ext-sink"}
	cleanup := registerDockerCleanup(t, rt, managedContainers, []string{"datascape-s3ext-rp-data"}, "")
	cleanup()

	stateFile := t.TempDir() + "/state.json"
	manifests := "testdata/s3-external-scenario"

	// Accept: apply succeeds with zero managed object-store containers — the
	// external Provider's own kind_handler path (isExternalNoProvider) never
	// creates one, and the Dataset/Binding reconcile against the real
	// endpoint through spec.connectionRef / options.endpoint.
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	if _, found, _ := rt.Inspect(ctx, "datascape-s3ext-store"); found {
		t.Error("a container named after the external s3 Provider exists — external must create nothing")
	}
	exists, err := s3extClient(t).BucketExists(ctx, "s3ext-raw")
	if err != nil || !exists {
		t.Fatalf("bucket s3ext-raw does not exist on the external store after apply: found=%v err=%v", exists, err)
	}

	out, err, code = run(t, "status", manifests, "--state-file", stateFile)
	if err != nil || code != 0 {
		t.Fatalf("status failed (code %d): %v\n%s", code, err, out)
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n")[1:] {
		if !strings.Contains(line, "True") {
			t.Errorf("resource not Ready after apply: %s", line)
		}
	}

	// Accept (D7): the managed lifecycle rule and versioning are visible via
	// the S3 API.
	cl := s3extClient(t)
	lc, err := cl.GetBucketLifecycle(ctx, "s3ext-raw")
	if err != nil {
		t.Fatalf("get bucket lifecycle: %v", err)
	}
	ruleID := "datascape-s3ext-raw"
	rule := findLiveRule(lc, ruleID)
	if rule == nil || int(rule.Expiration.Days) != 14 {
		t.Fatalf("lifecycle rule %q = %+v, want Expiration.Days=14", ruleID, rule)
	}
	vcfg, err := cl.GetBucketVersioning(ctx, "s3ext-raw")
	if err != nil || vcfg.Status != minio.Enabled {
		t.Fatalf("bucket versioning = %+v, err=%v; want Enabled", vcfg, err)
	}

	// Accept: real sink traffic through the Kafka Connect S3 sink connector,
	// landing in the external store, with zero managed object-store
	// containers.
	produceTo(t, "127.0.0.1:19197", "s3ext-events", "external-before")
	waitForObjectAt(t, ctx, "127.0.0.1:"+s3extMinioHostPort, s3extMinioUser, s3extMinioPass, "s3ext-raw", "", 180*time.Second)

	// Accept (D7): an out-of-band lifecycle change is detected as drift...
	changed := minlifecycle.NewConfiguration()
	changed.Rules = []minlifecycle.Rule{{
		ID: ruleID, Status: "Enabled",
		Expiration: minlifecycle.Expiration{Days: minlifecycle.ExpirationDays(3)},
	}}
	if err := cl.SetBucketLifecycle(ctx, "s3ext-raw", changed); err != nil {
		t.Fatalf("out-of-band lifecycle change: %v", err)
	}
	report, code := runDrift(t, manifests, stateFile)
	if code == 0 {
		t.Fatal("drift reported clean after an out-of-band lifecycle change")
	}
	r := report["Dataset/s3ext-raw"]
	if r.Drift != "True" || !strings.Contains(r.Reason, "LifecycleRuleDrift") {
		t.Fatalf("Dataset drift = %+v, want LifecycleRuleDrift", r)
	}

	// ...and re-apply heals it.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	lc, err = cl.GetBucketLifecycle(ctx, "s3ext-raw")
	if err != nil {
		t.Fatalf("get bucket lifecycle after heal: %v", err)
	}
	healed := findLiveRule(lc, ruleID)
	if healed == nil || int(healed.Expiration.Days) != 14 {
		t.Fatalf("lifecycle rule after heal = %+v, want Expiration.Days=14", healed)
	}

	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve")
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	// The external store's bucket must survive destroy (Datascape never
	// deletes an external system, and Dataset destroy is retain-by-default).
	if exists, err := cl.BucketExists(ctx, "s3ext-raw"); err != nil || !exists {
		t.Errorf("external bucket s3ext-raw removed by destroy (must be retained): exists=%v err=%v", exists, err)
	}
}

func findLiveRule(cfg *minlifecycle.Configuration, id string) *minlifecycle.Rule {
	if cfg == nil {
		return nil
	}
	for i := range cfg.Rules {
		if cfg.Rules[i].ID == id {
			return &cfg.Rules[i]
		}
	}
	return nil
}

// produceTo produces one record to topic via a plain kgo client seeded at a
// single, directly-dialable broker address (a legacy non-StableIdentity
// redpanda Provider always publishes its host port directly — no ordinal
// dial-map redirect needed, unlike the multi-broker HA suite). The value is
// wrapped as a JSON object (`{"marker": value}`): the s3sink Binding's
// default converter (Dataset format "json", no CDC/schema envelope) is
// Kafka Connect's JsonConverter with schemas.enable=false, which still
// requires each record's value to itself be parseable JSON — a bare string
// fails deserialization silently (the connector task errors, no object
// ever lands) — a live-caught finding building this test.
func produceTo(t *testing.T, seedBroker, topic, marker string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seedBroker))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()
	value := []byte(`{"marker":"` + marker + `"}`)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value}).FirstErr(); err != nil {
		t.Fatalf("produce %q to %q: %v", marker, topic, err)
	}
}

// --- C4 half 2: distributed MinIO (nodes) ------------------------------

const (
	minioHABase    = "datascape-minioha-s3"
	minioHANet     = "datascape-minioha-net"
	minioHANodes   = 4
	minioHAGates   = "HighAvailability=true"
	minioHABucket  = "minioha-raw"
	minioHATopic   = "minioha-events"
	minioHABrokers = "127.0.0.1:19196"
)

// minioHALiveOrdinalAddr returns the host-published S3 API address of any
// currently-running ordinal (skipping a killed one), mirroring the HA
// redpanda suite's "proceed against the survivors" pattern — MinIO's
// distributed mode serves the whole bucket namespace from any live node.
func minioHALiveOrdinalAddr(t *testing.T, rt *dockerruntime.Runtime) string {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < minioHANodes; i++ {
		ord := runtime.OrdinalName(minioHABase, i)
		st, found, err := rt.Inspect(ctx, ord)
		if err != nil || !found {
			continue
		}
		if addr := st.HostAddr(9000); addr != "" {
			return addr
		}
	}
	t.Fatal("no live MinIO node-set ordinal with an observable host address")
	return ""
}

// TestS3DistributedMinIONodeKill covers docs/planning/08 C4 half 2's accept
// criterion literally: a 4-node distributed MinIO Provider (spec.
// configuration.nodes: 4) reaches Ready, sink traffic (a real Binding(mode:
// sink) Kafka Connect S3 connector) lands objects while all four nodes are
// up, survives one node being killed out-of-band with sink traffic still
// flowing, reports the missing node as drift, and heals on re-apply — plus
// D7's lifecycle rule visible via the S3 API on the same node set.
func TestS3DistributedMinIONodeKill(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_MINIOHA_ROOT_USERNAME", "datascape-minioha-user")
	t.Setenv("DATASCAPE_SECRET_MINIOHA_ROOT_PASSWORD", "datascape-minioha-pw")

	buildSinkConnectImage(t)
	rt := requireDocker(t)
	ctx := context.Background()

	containers := []string{"datascape-minioha-rp", "datascape-minioha-sink"}
	volumes := []string{"datascape-minioha-rp-data"}
	for i := 0; i < minioHANodes; i++ {
		containers = append(containers, runtime.OrdinalName(minioHABase, i))
	}
	cleanup := registerDockerCleanup(t, rt, containers, volumes, minioHANet)
	cleanup()
	// The node set's per-ordinal volumes are adapter-named (docs/adr/004);
	// sweep them by label after the containers are gone (mirrors the
	// provider's own Destroy, and the redpanda HA suite's pattern) rather
	// than guess Docker's exact per-ordinal volume-naming convention here.
	t.Cleanup(func() {
		vols, err := rt.ListManagedVolumes(context.Background())
		if err != nil {
			return
		}
		for _, v := range vols {
			if strings.HasPrefix(v.Name, minioHABase+"-data") {
				_ = rt.RemoveVolume(context.Background(), v.Name)
			}
		}
	})

	stateFile := t.TempDir() + "/state.json"
	manifests := "testdata/minio-ha-scenario"

	// Accept: the 4-node cluster reaches Ready.
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", minioHAGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	for i := 0; i < minioHANodes; i++ {
		ord := runtime.OrdinalName(minioHABase, i)
		st, found, err := rt.Inspect(ctx, ord)
		if err != nil || !found || !st.Running {
			t.Fatalf("node ordinal %s not running after apply: found=%v err=%v", ord, found, err)
		}
	}

	// D7: the lifecycle rule and versioning are visible via the S3 API.
	addr := minioHALiveOrdinalAddr(t, rt)
	cl, err := minio.New(addr, &minio.Options{
		Creds: credentials.NewStaticV4("datascape-minioha-user", "datascape-minioha-pw", ""),
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	lc, err := cl.GetBucketLifecycle(ctx, minioHABucket)
	if err != nil {
		t.Fatalf("get bucket lifecycle: %v", err)
	}
	if r := findLiveRule(lc, "datascape-minioha-raw"); r == nil || int(r.Expiration.Days) != 30 {
		t.Fatalf("lifecycle rule = %+v, want Expiration.Days=30", r)
	}

	// Accept: sink traffic lands with all four nodes up.
	produceTo(t, minioHABrokers, minioHATopic, "before-kill")
	waitForObjectAt(t, ctx, minioHALiveOrdinalAddr(t, rt), "datascape-minioha-user", "datascape-minioha-pw", minioHABucket, "", 180*time.Second)

	// Kill one node out-of-band (not via platformctl state).
	killed := runtime.OrdinalName(minioHABase, 1)
	if err := rt.Remove(ctx, killed); err != nil {
		t.Fatalf("out-of-band node kill: %v", err)
	}

	// Accept: sink traffic keeps flowing with one of four nodes gone —
	// MinIO's erasure-coded pool tolerates it, and any surviving node
	// serves the whole bucket namespace.
	produceTo(t, minioHABrokers, minioHATopic, "during-kill")
	waitForObjectAt(t, ctx, minioHALiveOrdinalAddr(t, rt), "datascape-minioha-user", "datascape-minioha-pw", minioHABucket, "", 180*time.Second)

	// Accept: drift reports the missing node by name.
	report, code := runDrift(t, manifests, stateFile, "--feature-gates", minioHAGates)
	if code == 0 {
		t.Fatal("drift reported clean with a node missing")
	}
	r := report["Provider/"+minioHABase]
	if r.Drift != "True" || !strings.Contains(r.Reason, "NodeMissing") || !strings.Contains(r.Reason, killed) {
		t.Fatalf("Provider drift = %+v, want NodeMissing naming %s", r, killed)
	}

	// Accept: re-apply heals the missing node.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", minioHAGates)
	if err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	if st, found, err := rt.Inspect(ctx, killed); err != nil || !found || !st.Running {
		t.Fatalf("killed node %s not healed by re-apply: found=%v err=%v", killed, found, err)
	}
	report, code = runDrift(t, manifests, stateFile, "--feature-gates", minioHAGates)
	if code != 0 {
		t.Fatalf("drift still dirty after healing apply: %+v", report)
	}

	// Accept: idempotent re-apply, zero changes.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", minioHAGates)
	if err != nil || code != 0 {
		t.Fatalf("idempotent re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("idempotent re-apply did not report 'no changes':\n%s", out)
	}

	// Accept: destroy tears down every ordinal and their volumes cleanly.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", minioHAGates)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for i := 0; i < minioHANodes; i++ {
		ord := runtime.OrdinalName(minioHABase, i)
		if _, found, _ := rt.Inspect(ctx, ord); found {
			t.Errorf("node ordinal %s still present after destroy", ord)
		}
	}
	vols, err := rt.ListManagedVolumes(ctx)
	if err != nil {
		t.Fatalf("list managed volumes: %v", err)
	}
	for _, v := range vols {
		if strings.HasPrefix(v.Name, minioHABase+"-data") {
			t.Errorf("volume %s still present after destroy", v.Name)
		}
	}
}
