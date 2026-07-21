package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeHAManifest writes a redpanda Provider (+ optional EventStream) using
// the fake runtime, for validate-time gate/replication checks that must
// never touch real infrastructure.
func writeHAManifest(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "manifests.yaml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

const haBrokersManifest = `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: rp-ha-gate-test
spec:
  type: redpanda
  runtime:
    type: fake
  configuration:
    brokers: 3
`

// TestValidateRefusesBrokersWithoutHighAvailabilityGate covers docs/adr/017
// §a.8 — the gate-at-validate accept line docs/adr/004 deferred to C2: a
// Provider declaring configuration.brokers > 1 fails `validate` with the
// HighAvailability gate's standard disabled message, never at apply.
func TestValidateRefusesBrokersWithoutHighAvailabilityGate(t *testing.T) {
	dir := writeHAManifest(t, haBrokersManifest)
	_, err, code := run(t, "validate", dir)
	if err == nil {
		t.Fatal("validate accepted brokers: 3 with the HighAvailability gate disabled")
	}
	if code == 0 {
		t.Fatalf("validate exit code = %d, want non-zero", code)
	}
	if !strings.Contains(err.Error(), "HighAvailability") {
		t.Errorf("error does not name the gate: %v", err)
	}
	if !strings.Contains(err.Error(), "brokers") {
		t.Errorf("error does not name the declaring field: %v", err)
	}
}

// TestValidateAcceptsBrokersWithHighAvailabilityGate: the same manifest
// validates once the gate is enabled — the check is the gate, not the field.
func TestValidateAcceptsBrokersWithHighAvailabilityGate(t *testing.T) {
	dir := writeHAManifest(t, haBrokersManifest)
	out, err, code := run(t, "validate", dir, "--feature-gates", "HighAvailability=true")
	if err != nil || code != 0 {
		t.Fatalf("validate failed (code %d): %v\n%s", code, err, out)
	}
}

// TestValidateRefusesReplicationExceedingBrokers covers docs/adr/017 §a.7
// end-to-end through the real redpanda provider's
// StreamReplicationValidator: replication > brokers fails validate with an
// error naming both numbers.
func TestValidateRefusesReplicationExceedingBrokers(t *testing.T) {
	dir := writeHAManifest(t, haBrokersManifest+`---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: rp-ha-gate-events
spec:
  providerRef:
    name: rp-ha-gate-test
  partitions: 3
  replication: 4
`)
	_, err, code := run(t, "validate", dir, "--feature-gates", "HighAvailability=true")
	if err == nil || code == 0 {
		t.Fatal("validate accepted replication 4 against a 3-broker provider")
	}
	for _, want := range []string{"4", "3", "replication"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

// TestValidateRefusesHostPortPinWithBrokers covers docs/adr/017 §a.4's
// validate-time closure of docs/adr/004's known limitation: a fixed host
// port cannot be combined with a replicated set.
func TestValidateRefusesHostPortPinWithBrokers(t *testing.T) {
	dir := writeHAManifest(t, `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: rp-ha-pin-test
spec:
  type: redpanda
  runtime:
    type: fake
  configuration:
    brokers: 3
    kafkaPort: 19192
`)
	_, err, code := run(t, "validate", dir, "--feature-gates", "HighAvailability=true")
	if err == nil || code == 0 {
		t.Fatal("validate accepted a kafkaPort pin combined with brokers")
	}
	if !strings.Contains(err.Error(), "kafkaPort") {
		t.Errorf("error does not name the pinned key: %v", err)
	}
}
