package debezium

import "testing"

// TestServerIDUniquePerConnector guards docs/planning/07 §2.2: two MySQL
// connectors on the same server must not share a replication server id (the
// previous formula was constant per engine, so they kicked each other's
// binlog session off).
func TestServerIDUniquePerConnector(t *testing.T) {
	a := serverID("orders-cdc")
	b := serverID("customers-cdc")
	if a == b {
		t.Fatalf("serverID collided for distinct connectors: %d", a)
	}
	if a < 100000 || b < 100000 {
		t.Errorf("serverID below floor: %d, %d", a, b)
	}
	if a != serverID("orders-cdc") {
		t.Error("serverID not deterministic for the same name")
	}
}
