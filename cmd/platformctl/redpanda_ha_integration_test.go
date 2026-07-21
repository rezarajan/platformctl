//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	dockerruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/docker"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	rpHABase    = "datascape-rp-ha-test"
	rpHANet     = "datascape-rp-ha-test-net"
	rpHATopic   = "datascape-ha-events"
	rpHABrokers = 3
	rpHAGates   = "HighAvailability=true"
)

// rpHAClient builds a kgo client against the live ordinals of the HA test
// cluster using the same token→dialable-address redirect the provider's own
// admin client uses (docs/adr/017 §a.4): seeds and broker identities are the
// per-ordinal advertised tokens ("<ordinal>:9092"), and a custom dialer maps
// each token to that ordinal's observed host address. Ordinals with no
// observable host address (killed) are left out — the client proceeds
// against the survivors, exactly what a produce/consume-during-broker-loss
// scenario needs.
func rpHAClient(t *testing.T, rt *dockerruntime.Runtime, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	ctx := context.Background()
	dialMap := map[string]string{}
	var seeds []string
	for i := 0; i < rpHABrokers; i++ {
		ord := runtime.OrdinalName(rpHABase, i)
		st, found, err := rt.Inspect(ctx, ord)
		if err != nil || !found {
			continue
		}
		addr := st.HostAddr(9092)
		if addr == "" {
			continue
		}
		token := fmt.Sprintf("%s:%d", ord, 9092)
		dialMap[token] = addr
		seeds = append(seeds, token)
	}
	if len(seeds) == 0 {
		t.Fatal("no live broker ordinal with an observable host address")
	}
	dial := func(ctx context.Context, network, host string) (net.Conn, error) {
		if mapped, ok := dialMap[host]; ok {
			host = mapped
		}
		var d net.Dialer
		return d.DialContext(ctx, network, host)
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(seeds...), kgo.Dialer(dial)}, opts...)...)
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	return cl
}

// rpHAProduceConsume proves the cluster is live end-to-end: produce one
// record and consume it back.
func rpHAProduceConsume(t *testing.T, rt *dockerruntime.Runtime, marker string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prod := rpHAClient(t, rt)
	defer prod.Close()
	if err := prod.ProduceSync(ctx, &kgo.Record{Topic: rpHATopic, Value: []byte(marker)}).FirstErr(); err != nil {
		t.Fatalf("produce %q: %v", marker, err)
	}

	cons := rpHAClient(t, rt,
		kgo.ConsumeTopics(rpHATopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cons.Close()
	for {
		fetches := cons.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("consume %q: %v", marker, err)
		}
		found := false
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Value) == marker {
				found = true
			}
		})
		if found {
			return
		}
	}
}

func rpHADescribeTopic(t *testing.T, rt *dockerruntime.Runtime) (partitions, replication int) {
	t.Helper()
	cl := rpHAClient(t, rt)
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	details, err := adm.ListTopics(ctx, rpHATopic)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	if !details.Has(rpHATopic) {
		t.Fatalf("topic %q does not exist", rpHATopic)
	}
	detail := details[rpHATopic]
	for _, p := range detail.Partitions {
		return len(detail.Partitions), len(p.Replicas)
	}
	t.Fatalf("topic %q has no partitions", rpHATopic)
	return 0, 0
}

// runDriftGated is runDrift with the HighAvailability gate enabled —
// `drift` runs the full validate pipeline first, and this scenario's
// manifest declares brokers: 3.
func runDriftGated(t *testing.T, manifests, stateFile string) (map[string]driftReport, int) {
	t.Helper()
	out, _, code := run(t, "drift", manifests, "--state-file", stateFile, "-o", "json", "--feature-gates", rpHAGates)
	var payload struct {
		Resources []driftReport `json:"resources"`
	}
	if err := json.NewDecoder(strings.NewReader(out)).Decode(&payload); err != nil {
		t.Fatalf("decode drift output: %v\n%s", err, out)
	}
	byResource := make(map[string]driftReport, len(payload.Resources))
	for _, r := range payload.Resources {
		byResource[r.Resource] = r
		if trimmed := strings.TrimPrefix(r.Resource, "default/"); trimmed != r.Resource {
			byResource[trimmed] = r
		}
	}
	return byResource, code
}

// TestRedpandaHAEndToEnd covers docs/planning/08 C2's Accept list on Docker
// (docs/adr/017): a 3-broker cluster reaches Ready with replication-factor-3
// topics; produce/consume keeps working while one broker is killed
// out-of-band; drift names the missing broker; re-apply heals it; re-apply
// is idempotent; destroy is clean.
func TestRedpandaHAEndToEnd(t *testing.T) {
	rt := requireDocker(t)
	ctx := context.Background()

	containers := []string{rpHABase}
	volumes := []string{}
	for i := 0; i < rpHABrokers; i++ {
		containers = append(containers, runtime.OrdinalName(rpHABase, i))
		volumes = append(volumes, runtime.OrdinalName(rpHABase+"-data", i))
	}
	cleanup := registerDockerCleanup(t, rt, containers, volumes, rpHANet)
	cleanup()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/redpanda-ha-scenario"

	// Accept: 3-broker cluster to Ready (the HighAvailability gate's
	// validate-time refusal without the flag is unit-covered in
	// ha_gate_test.go).
	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", rpHAGates)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}
	for i := 0; i < rpHABrokers; i++ {
		ord := runtime.OrdinalName(rpHABase, i)
		st, found, err := rt.Inspect(ctx, ord)
		if err != nil || !found || !st.Running {
			t.Fatalf("broker ordinal %s not running after apply: found=%v err=%v", ord, found, err)
		}
	}

	// Accept: the EventStream lands with replication factor 3, verified via
	// the live admin API.
	partitions, replication := rpHADescribeTopic(t, rt)
	if partitions != 3 {
		t.Errorf("topic partitions = %d, want 3", partitions)
	}
	if replication != 3 {
		t.Errorf("topic replication factor = %d, want 3", replication)
	}

	rpHAProduceConsume(t, rt, "before-kill")

	// Kill one broker out-of-band (not via platformctl state).
	killed := runtime.OrdinalName(rpHABase, 1)
	if err := rt.Remove(ctx, killed); err != nil {
		t.Fatalf("out-of-band broker kill: %v", err)
	}

	// Accept: produce/consume still works with one of three brokers gone.
	rpHAProduceConsume(t, rt, "during-kill")

	// Accept: drift reports the missing broker by name.
	report, code := runDriftGated(t, manifests, stateFile)
	if code == 0 {
		t.Fatal("drift reported clean with a broker missing")
	}
	r := report["Provider/"+rpHABase]
	if r.Drift != "True" || !strings.Contains(r.Reason, "BrokerMissing") || !strings.Contains(r.Reason, killed) {
		t.Fatalf("Provider drift = %+v, want BrokerMissing naming %s", r, killed)
	}

	// Accept: re-apply heals the missing broker (its retained data volume
	// lets it rejoin), and drift goes clean.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", rpHAGates)
	if err != nil || code != 0 {
		t.Fatalf("healing apply failed (code %d): %v\n%s", code, err, out)
	}
	if st, found, err := rt.Inspect(ctx, killed); err != nil || !found || !st.Running {
		t.Fatalf("killed broker %s not healed by re-apply: found=%v err=%v", killed, found, err)
	}
	report, code = runDriftGated(t, manifests, stateFile)
	if code != 0 {
		t.Fatalf("drift still dirty after healing apply: %+v", report)
	}
	rpHAProduceConsume(t, rt, "after-heal")

	// Accept: idempotent re-apply, zero changes.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", rpHAGates)
	if err != nil || code != 0 {
		t.Fatalf("idempotent re-apply failed (code %d): %v\n%s", code, err, out)
	}
	if !strings.Contains(out, "no changes") {
		t.Errorf("idempotent re-apply did not report 'no changes':\n%s", out)
	}

	// Accept: destroy tears down every ordinal, the per-ordinal volumes,
	// and the network cleanly.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", rpHAGates)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	for i := 0; i < rpHABrokers; i++ {
		ord := runtime.OrdinalName(rpHABase, i)
		if _, found, _ := rt.Inspect(ctx, ord); found {
			t.Errorf("broker ordinal %s still present after destroy", ord)
		}
	}
	vols, err := rt.ListManagedVolumes(ctx)
	if err != nil {
		t.Fatalf("list managed volumes: %v", err)
	}
	for _, v := range vols {
		if strings.HasPrefix(v.Name, rpHABase+"-data") {
			t.Errorf("volume %s still present after destroy", v.Name)
		}
	}
}
