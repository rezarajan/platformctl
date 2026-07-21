package kafkaconnect

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

// TestTryEachFirstSuccess covers the common case: the first candidate
// answers, so tryEach never touches the rest.
func TestTryEachFirstSuccess(t *testing.T) {
	calls := 0
	got, err := tryEach([]string{"a", "b", "c"}, func(baseURL string) (string, error) {
		calls++
		return baseURL, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "a" || calls != 1 {
		t.Fatalf("got=%q calls=%d, want got=\"a\" calls=1", got, calls)
	}
}

// TestTryEachFailsOverToLiveCandidate is the docs/planning/08 C3 contract
// test: "connector REST calls go to any live worker" — a dead first
// candidate must not block a live second one.
func TestTryEachFailsOverToLiveCandidate(t *testing.T) {
	tried := []string{}
	got, err := tryEach([]string{"dead-1", "dead-2", "live"}, func(baseURL string) (string, error) {
		tried = append(tried, baseURL)
		if baseURL == "live" {
			return "ok", nil
		}
		return "", errors.New("connection refused")
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "ok" {
		t.Fatalf("got %q, want \"ok\"", got)
	}
	if want := []string{"dead-1", "dead-2", "live"}; !equalStrings(tried, want) {
		t.Fatalf("tried %v, want %v", tried, want)
	}
}

// TestTryEachAllFail joins every candidate's failure into one error naming
// how many were tried, rather than surfacing only the last one.
func TestTryEachAllFail(t *testing.T) {
	_, err := tryEach([]string{"a", "b"}, func(baseURL string) (string, error) {
		return "", errors.New("boom:" + baseURL)
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "2 tried") || !strings.Contains(msg, "boom:a") || !strings.Contains(msg, "boom:b") {
		t.Fatalf("error %q does not name every failed candidate", msg)
	}
}

func TestTryEachEmptyCandidates(t *testing.T) {
	if _, err := tryEach([]string{}, func(string) (string, error) { return "", nil }); err == nil {
		t.Fatal("expected an error for zero candidates")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestConnectorStateFailsOverAcrossRealServers is an end-to-end failover
// check through the real HTTP path (not just tryEach in isolation): a dead
// first worker address plus a live second one still yields the connector's
// state.
func TestConnectorStateFailsOverAcrossRealServers(t *testing.T) {
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}`))
	}))
	defer live.Close()
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close() // closed immediately: every request to it now refuses the connection

	state, err := ConnectorState(context.Background(), []string{deadURL, live.URL}, "orders-cdc")
	if err != nil {
		t.Fatalf("ConnectorState with one dead worker: %v", err)
	}
	if state != "RUNNING" {
		t.Errorf("state = %q, want RUNNING", state)
	}
}

// TestIsTransientConnectErrorRecognizesForwardingFailure guards the exact
// failure caught live while building C3's integration test: one Connect
// worker's attempt to forward a REST request to another worker (the
// framework's own distributed-mode behavior) mid-rebalance/mid-rejoin
// surfaces as an HTTP 500 whose body embeds a plain connection failure, or
// as a bare transport-level connection-reset error.
func TestIsTransientConnectErrorRecognizesForwardingFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"forward-connection-refused-500-body", errors.New(`delete connector "x": HTTP 500: {"error_code":500,"message":"IO Error trying to forward REST request: java.net.ConnectException: Connection refused"}`), true},
		{"connection-reset-by-peer", errors.New(`Delete "http://x/connectors/x": read tcp 127.0.0.1:1->127.0.0.1:2: read: connection reset by peer`), true},
		{"rebalance-409", errors.New("register connector \"x\": HTTP 409: rebalance in progress"), true},
		{"unrelated-error", errors.New("register connector \"x\": HTTP 422: invalid connector config"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isTransientConnectError(c.err); got != c.want {
				t.Errorf("isTransientConnectError(%q) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// TestRetryTransientRetriesThenSucceeds covers the shared retry primitive
// PutConnectorConfig/DeleteConnector both use: a transient failure is
// retried until it clears (or the deadline elapses), a non-transient one
// returns immediately.
func TestRetryTransientRetriesThenSucceeds(t *testing.T) {
	attempts := 0
	err := retryTransient(context.Background(), time.Second, time.Millisecond, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("HTTP 409: rebalance in progress")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryTransient: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestRetryTransientReturnsNonTransientImmediately(t *testing.T) {
	attempts := 0
	err := retryTransient(context.Background(), time.Second, time.Millisecond, func() error {
		attempts++
		return errors.New("HTTP 422: invalid connector config")
	})
	if err == nil {
		t.Fatal("want the non-transient error surfaced")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (no retry for a non-transient error)", attempts)
	}
}

// TestDeleteConnectorFailsOverOnForwardingError: a dead first worker (the
// C3 failover contract) still lets DeleteConnector succeed against the
// live second one.
func TestDeleteConnectorFailsOverOnForwardingError(t *testing.T) {
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer live.Close()
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	if err := DeleteConnector(context.Background(), []string{deadURL, live.URL}, "orders-cdc"); err != nil {
		t.Fatalf("DeleteConnector with one dead worker: %v", err)
	}
}
