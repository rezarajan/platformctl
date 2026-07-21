// Package kafkaconnect wraps the Kafka Connect REST API operations shared by
// every Connect-worker-based provider (debezium, s3sink).
package kafkaconnect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// connectorPath builds /connectors/<name><suffix> with the connector name
// URL-escaped — names derive from resource names today (DNS-label-safe), but
// the REST client must not silently corrupt a path if that ever changes
// (docs/planning/07 §2.2).
func connectorPath(baseURL, name, suffix string) string {
	return baseURL + "/connectors/" + url.PathEscape(name) + suffix
}

var httpClient = &http.Client{Timeout: 15 * time.Second}

// tryEach calls op against each of baseURLs in order and returns the first
// success (docs/planning/08 C3: "connector REST calls go to any live
// worker"). Every candidate is one currently-reachable Connect worker's own
// REST address — Connect's distributed mode makes any worker's REST port a
// valid entry point for any connector regardless of which worker actually
// owns it (the framework forwards internally), so trying the next candidate
// on failure is a correct, cheap failover, not a guess. There is
// deliberately no wait between candidates here — the wait/retry policy
// belongs to the caller's own loop (PutConnectorConfig's transient-error
// retry, WaitConnectorRunning's poll interval); tryEach itself is a single,
// synchronous attempt across the given address set.
//
// Address freshness is the caller's responsibility too: baseURLs is a
// snapshot resolved once per Reconcile/Probe/Destroy call (the provider's
// own EnsureReachable-per-ordinal helper) — the re-resolve-per-attempt
// discipline ADR 015 requires lives at that call site (a fresh runtime
// resolution before each such call), not inside this narrowly-scoped REST
// package, which has no runtime.ContainerRuntime access by design (ADR 008:
// kafkaconnect is an intra-adapter helper, not a port).
//
// Returns a joined error naming every candidate's failure when none
// succeed; baseURLs must be non-empty (a provider with zero reachable
// workers fails earlier, at address resolution, with its own error).
func tryEach[T any](baseURLs []string, op func(baseURL string) (T, error)) (T, error) {
	var zero T
	if len(baseURLs) == 0 {
		return zero, fmt.Errorf("no Kafka Connect worker address given")
	}
	var errs []error
	for _, u := range baseURLs {
		v, err := op(u)
		if err == nil {
			return v, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", u, err))
	}
	return zero, fmt.Errorf("every Kafka Connect worker address failed (%d tried): %w", len(baseURLs), errors.Join(errs...))
}

// retryTransient re-invokes op, bounded by deadline, as long as it keeps
// failing with an isTransientConnectError — the one retry/backoff primitive
// every Connect REST operation that can hit a transient worker-group
// condition shares (docs/planning/08 C3), rather than each duplicating its
// own deadline loop. A single op invocation is expected to already be a
// tryEach across every candidate worker address, so a caller gets both
// forms of resilience: failover across live workers *and* retry through a
// transient condition common to all of them (e.g. mid-rebalance, right
// after a killed worker's replacement just rejoined the group — caught
// live: internal/adapters/kafkaconnect's own conformance history, C3).
func retryTransient(ctx context.Context, deadline time.Duration, interval time.Duration, op func() error) error {
	end := time.Now().Add(deadline)
	for {
		err := op()
		if err == nil || !isTransientConnectError(err) || time.Now().After(end) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// PutConnectorConfig registers or updates a connector; PUT to
// /connectors/<name>/config is an idempotent upsert in the Connect REST API.
// Transient failures are retried with a bounded deadline: worker rebalances
// (HTTP 409) and validation-time backend unavailability (Connect probes the
// source/sink while validating, so a database that came back a moment ago —
// or is still in the worker JVM's negative DNS cache — yields HTTP 400
// "connection attempt failed"). baseURLs is every currently-reachable
// worker's address (docs/planning/08 C3); each outer retry attempt tries
// every candidate (tryEach) before waiting and retrying, so a single dead
// worker among several never delays registration.
func PutConnectorConfig(ctx context.Context, baseURLs []string, name string, config map[string]string) error {
	return retryTransient(ctx, 90*time.Second, 3*time.Second, func() error {
		_, err := tryEach(baseURLs, func(baseURL string) (struct{}, error) {
			return struct{}{}, putConnectorConfigOnce(ctx, baseURL, name, config)
		})
		return err
	})
}

func putConnectorConfigOnce(ctx context.Context, baseURL, name string, config map[string]string) error {
	body, err := json.Marshal(config)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, connectorPath(baseURL, name, "/config"), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("register connector %q: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register connector %q: HTTP %d: %s", name, resp.StatusCode, msg)
	}
	return nil
}

// isTransientConnectError recognizes the failure classes a Connect worker
// group can present only briefly: a rebalance in progress (HTTP 409), a
// backend the connector is validating against that isn't reachable *yet*
// (HTTP 400 "connection attempt failed"), and — the class caught live while
// building C3's own integration test — one worker's attempt to internally
// forward a REST request to another worker mid-rebalance/mid-rejoin, which
// surfaces as a plain connection failure (refused, reset, or a broken
// pipe) either in the HTTP transport error or embedded in a Connect-side
// HTTP 500 body ("IO Error trying to forward REST request:
// java.net.ConnectException: Connection refused").
func isTransientConnectError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "HTTP 409") ||
		strings.Contains(msg, "connection attempt failed") ||
		strings.Contains(msg, "Connection refused") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "EOF")
}

// GetConnectorConfig tries each of baseURLs in turn (see tryEach) and
// returns the first successful read.
func GetConnectorConfig(ctx context.Context, baseURLs []string, name string) (map[string]string, error) {
	return tryEach(baseURLs, func(baseURL string) (map[string]string, error) {
		return getConnectorConfigOnce(ctx, baseURL, name)
	})
}

func getConnectorConfigOnce(ctx context.Context, baseURL, name string) (map[string]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, connectorPath(baseURL, name, "/config"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get connector %q config: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get connector %q config: HTTP %d: %s", name, resp.StatusCode, msg)
	}
	var config map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		return nil, err
	}
	return config, nil
}

// DeleteConnector tries each of baseURLs in turn (see tryEach), retrying
// through a transient worker-group condition (see isTransientConnectError)
// for up to 30s — long enough to ride out a brief mid-rebalance forwarding
// failure without making a routine `destroy` hang anywhere near
// PutConnectorConfig's 90s registration budget.
func DeleteConnector(ctx context.Context, baseURLs []string, name string) error {
	return retryTransient(ctx, 30*time.Second, 3*time.Second, func() error {
		_, err := tryEach(baseURLs, func(baseURL string) (struct{}, error) {
			return struct{}{}, deleteConnectorOnce(ctx, baseURL, name)
		})
		return err
	})
}

func deleteConnectorOnce(ctx context.Context, baseURL, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, connectorPath(baseURL, name, ""), nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete connector %q: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete connector %q: HTTP %d: %s", name, resp.StatusCode, msg)
	}
	return nil
}

// RestartConnector restarts the connector's failed instances and tasks —
// the recovery path for tasks that died against a temporarily-unavailable
// backend, where re-PUTting an identical config is a no-op. Tries each of
// baseURLs in turn (see tryEach).
func RestartConnector(ctx context.Context, baseURLs []string, name string) error {
	_, err := tryEach(baseURLs, func(baseURL string) (struct{}, error) {
		return struct{}{}, restartConnectorOnce(ctx, baseURL, name)
	})
	return err
}

func restartConnectorOnce(ctx context.Context, baseURL, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		connectorPath(baseURL, name, "/restart?includeTasks=true&onlyFailed=true"), nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("restart connector %q: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("restart connector %q: HTTP %d: %s", name, resp.StatusCode, msg)
	}
	return nil
}

// ConnectorState returns the connector's aggregate state (RUNNING, FAILED,
// PAUSED, ...); the connector state and every task's state must agree for
// RUNNING to be reported. Tries each of baseURLs in turn (see tryEach): any
// live worker answers the same distributed-mode status for the connector,
// regardless of which worker owns its tasks.
func ConnectorState(ctx context.Context, baseURLs []string, name string) (string, error) {
	return tryEach(baseURLs, func(baseURL string) (string, error) {
		return connectorStateOnce(ctx, baseURL, name)
	})
}

func connectorStateOnce(ctx context.Context, baseURL, name string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, connectorPath(baseURL, name, "/status"), nil)
	if err != nil {
		return "", err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("connector %q status: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("connector %q status: HTTP %d: %s", name, resp.StatusCode, msg)
	}
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			State string `json:"state"`
			Trace string `json:"trace"`
		} `json:"tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	state := body.Connector.State
	for _, task := range body.Tasks {
		if task.State != "RUNNING" {
			return task.State, nil
		}
	}
	return state, nil
}

// WaitConnectorRunning polls with bounded backoff until the connector and all
// its tasks are RUNNING. Connector registration is inherently async; generous
// documented timeouts, not tight retries (roadmap risk register). Every poll
// tick tries each of baseURLs (see tryEach) — a worker that dies mid-wait is
// simply skipped in favor of a survivor, without waiting out its own
// timeout first.
//
// FAILED is treated as recoverable within the deadline, not terminal: a task
// that died while its backend was briefly unavailable (or still sits in the
// worker JVM's negative DNS cache after a container was replaced) recovers
// after a restart. Failed instances are restarted at most every 10s; a
// connector that stays FAILED to the deadline is reported as such.
func WaitConnectorRunning(ctx context.Context, baseURLs []string, name string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	lastState := "UNKNOWN"
	var lastRestart time.Time
	for {
		state, err := ConnectorState(ctx, baseURLs, name)
		if err == nil {
			lastState = state
			if state == "RUNNING" {
				return state, nil
			}
			if state == "FAILED" && time.Since(lastRestart) > 10*time.Second {
				if err := RestartConnector(ctx, baseURLs, name); err == nil {
					lastRestart = time.Now()
				}
			}
		}
		if time.Now().After(deadline) {
			return lastState, fmt.Errorf("connector %q did not reach RUNNING within %s (last state: %s)", name, timeout, lastState)
		}
		select {
		case <-ctx.Done():
			return lastState, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
