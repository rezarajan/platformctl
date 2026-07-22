package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
)

func externalTLSConnectionEnvelope() resource.Envelope {
	return envelope("Connection", "prod-rds", map[string]any{
		"external": true,
		"host":     "prod-db.rds.amazonaws.com",
		"port":     float64(5432),
		"tls":      map[string]any{"mode": "require"},
	})
}

// TestExternalDatabaseTLSGateBlocksUngatedApply covers docs/planning/08 I2's
// gate wiring: a bare external Connection (no providerRef — never reaches
// resolveRequest) declaring spec.tls.mode fails to reconcile — naming the
// ExternalDatabaseTLS gate — when the gate is unregistered or
// registered-but-disabled, and succeeds once it's enabled. Mirrors
// TestTLSTerminationGateBlocksUngatedApply's shape exactly; the two differ
// only in which kind_handler chokepoint applies (resolveRequest for a
// managed https Connection, externalDatabaseTLSGate for a bare external
// one — see that function's doc comment for why).
func TestExternalDatabaseTLSGateBlocksUngatedApply(t *testing.T) {
	conn := externalTLSConnectionEnvelope()
	envelopes := []resource.Envelope{conn}
	connKey := conn.Key()

	// Case 1: ExternalDatabaseTLS never registered.
	eng := newTestEngine(t, registry.New(featuregate.NewRegistry()))
	result := applyTolerant(t, eng, envelopes)
	err, failed := result.Failed[connKey]
	if !failed {
		t.Fatal("expected Connection reconcile to fail with ExternalDatabaseTLS unregistered")
	}
	if !strings.Contains(err.Error(), "ExternalDatabaseTLS") {
		t.Errorf("error = %q, want it to name the ExternalDatabaseTLS gate", err.Error())
	}

	// Case 2: gate registered but disabled.
	gates2 := featuregate.NewRegistry()
	gates2.Register("ExternalDatabaseTLS", featuregate.Alpha, false)
	eng2 := newTestEngine(t, registry.New(gates2))
	result2 := applyTolerant(t, eng2, envelopes)
	err2, failed2 := result2.Failed[connKey]
	if !failed2 || !strings.Contains(err2.Error(), "ExternalDatabaseTLS") {
		t.Fatalf("expected disabled-gate failure naming ExternalDatabaseTLS, got failed=%v err=%v", failed2, err2)
	}

	// Case 3: gate enabled (the shipped default) — reconcile succeeds (the
	// bare external Connection's own reachability probe may still fail in
	// this unit test — there is no real RDS to dial — but it must fail
	// with a reachability reason, never a gate error).
	gates3 := featuregate.NewRegistry()
	gates3.Register("ExternalDatabaseTLS", featuregate.Alpha, true)
	eng3 := newTestEngine(t, registry.New(gates3))
	result3 := applyTolerant(t, eng3, envelopes)
	if err3, failed3 := result3.Failed[connKey]; failed3 && strings.Contains(err3.Error(), "ExternalDatabaseTLS") {
		t.Fatalf("Connection reconcile still names the gate with ExternalDatabaseTLS enabled: %v", err3)
	}
}

// TestExternalConnectionWithoutTLSUnaffectedByGate: a bare external
// Connection with no spec.tls never consults the ExternalDatabaseTLS gate
// at all — the pre-I2 plaintext path is untouched even when the gate is
// unregistered.
func TestExternalConnectionWithoutTLSUnaffectedByGate(t *testing.T) {
	conn := envelope("Connection", "prod-db", map[string]any{
		"external": true,
		"host":     "prod-db.example.com",
		"port":     float64(5432),
	})
	envelopes := []resource.Envelope{conn}
	eng := newTestEngine(t, registry.New(featuregate.NewRegistry()))
	result := applyTolerant(t, eng, envelopes)
	if err, failed := result.Failed[conn.Key()]; failed && strings.Contains(err.Error(), "ExternalDatabaseTLS") {
		t.Fatalf("Connection with no spec.tls must never consult the gate, got: %v", err)
	}
}

// TestExternalDatabaseTLSGateAlsoAppliesAtProbe covers the probe-hook half
// of the wiring (kind_handler.go's ExternalNoProvider.probe): Probe cannot
// return an error, so a disabled gate must degrade to a Ready=False status
// naming the gate, mirroring probeOneAgainstState's own ReasonProbeFailed
// conversion of a resolveRequest gate error.
func TestExternalDatabaseTLSGateAlsoAppliesAtProbe(t *testing.T) {
	conn := externalTLSConnectionEnvelope()
	eng := newTestEngine(t, registry.New(featuregate.NewRegistry()))
	st := eng.probeOne(context.Background(), conn, map[resource.Key]resource.Envelope{conn.Key(): conn}, nil)
	c, ok := st.Condition(status.Ready)
	if !ok || c.Status != status.False {
		t.Fatalf("probe status = %+v, want Ready=False", st)
	}
	if !strings.Contains(c.Message, "ExternalDatabaseTLS") {
		t.Errorf("message = %q, want it to name the ExternalDatabaseTLS gate", c.Message)
	}
}
