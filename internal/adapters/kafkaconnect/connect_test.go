package kafkaconnect

import (
	"strings"
	"testing"
)

// TestConnectorPathEscapesName guards docs/planning/07 §2.2: a connector
// name containing path metacharacters must not corrupt the REST path.
func TestConnectorPathEscapesName(t *testing.T) {
	got := connectorPath("http://x:8083", "a/b c?d", "/config")
	if strings.Contains(got, "a/b c") {
		t.Fatalf("connector name not escaped: %q", got)
	}
	if !strings.HasPrefix(got, "http://x:8083/connectors/") || !strings.HasSuffix(got, "/config") {
		t.Errorf("path shape wrong: %q", got)
	}
}

func TestConnectorPathPlainNameUnchanged(t *testing.T) {
	got := connectorPath("http://x:8083", "orders-cdc", "/status")
	want := "http://x:8083/connectors/orders-cdc/status"
	if got != want {
		t.Errorf("connectorPath = %q, want %q", got, want)
	}
}
