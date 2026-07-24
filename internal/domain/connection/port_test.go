package connection

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/naming"
)

// M2 (docs/adr/035 decision 2, docs/planning/08 §7.12): a managed
// Connection's spec.port is optional — omitted, it auto-allocates
// deterministically via the same internal/domain/hostport allocator a
// Provider's own omitted host port already resolves through, keyed on the
// Connection's own runtime object name so the value is stable across
// reconciles; an explicit pin passes through unchanged.

func TestOmittedPortAutoAllocatesDeterministically(t *testing.T) {
	t.Parallel()
	spec := baseManagedSpec()
	delete(spec, "port")
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.Port <= 0 {
		t.Fatalf("Port = %d, want a positive auto-allocated port", c.Port)
	}
	want := hostport.For(naming.RuntimeObjectName(envelope(spec)))
	if c.Port != want {
		t.Errorf("Port = %d, want %d (hostport.For(%q))", c.Port, want, "test-conn")
	}

	// Stable across repeated reconciles: a second FromEnvelope call on the
	// identical manifest must derive the identical port, with no shared
	// state between calls beyond the deterministic name-based hash.
	c2, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope (second call): %v", err)
	}
	if c2.Port != c.Port {
		t.Errorf("second FromEnvelope Port = %d, want %d (stable across reconciles)", c2.Port, c.Port)
	}
}

func TestPinnedPortUnchanged(t *testing.T) {
	t.Parallel()
	spec := baseManagedSpec()
	spec["port"] = float64(15999)
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.Port != 15999 {
		t.Errorf("Port = %d, want 15999 (explicit pin kept byte-identically)", c.Port)
	}
}

func TestExternalConnectionStillRequiresPort(t *testing.T) {
	t.Parallel()
	spec := baseExternalSpec()
	delete(spec, "port")
	if _, err := FromEnvelope(envelope(spec)); err == nil {
		t.Fatal("expected error: spec.port is required on an external connection (no entrypoint to auto-allocate for)")
	}
}

func TestExternalConnectionPinnedPortUnchanged(t *testing.T) {
	t.Parallel()
	spec := baseExternalSpec()
	spec["port"] = float64(5432)
	c, err := FromEnvelope(envelope(spec))
	if err != nil {
		t.Fatalf("FromEnvelope: %v", err)
	}
	if c.Port != 5432 {
		t.Errorf("Port = %d, want 5432 (external connection port is literal, never auto-allocated)", c.Port)
	}
}
