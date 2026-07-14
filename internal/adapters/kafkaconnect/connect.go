// Package kafkaconnect wraps the Kafka Connect REST API operations shared by
// every Connect-worker-based provider (debezium, s3sink).
package kafkaconnect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var httpClient = &http.Client{Timeout: 15 * time.Second}

// PutConnectorConfig registers or updates a connector; PUT to
// /connectors/<name>/config is an idempotent upsert in the Connect REST API.
// Transient failures are retried with a bounded deadline: worker rebalances
// (HTTP 409) and validation-time backend unavailability (Connect probes the
// source/sink while validating, so a database that came back a moment ago —
// or is still in the worker JVM's negative DNS cache — yields HTTP 400
// "connection attempt failed").
func PutConnectorConfig(ctx context.Context, baseURL, name string, config map[string]string) error {
	deadline := time.Now().Add(90 * time.Second)
	for {
		err := putConnectorConfigOnce(ctx, baseURL, name, config)
		if err == nil || !isTransientPutError(err) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func putConnectorConfigOnce(ctx context.Context, baseURL, name string, config map[string]string) error {
	body, err := json.Marshal(config)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, baseURL+"/connectors/"+name+"/config", bytes.NewReader(body))
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

func isTransientPutError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "HTTP 409") ||
		strings.Contains(msg, "connection attempt failed") ||
		strings.Contains(msg, "Connection refused")
}

func GetConnectorConfig(ctx context.Context, baseURL, name string) (map[string]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/connectors/"+name+"/config", nil)
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

func DeleteConnector(ctx context.Context, baseURL, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, baseURL+"/connectors/"+name, nil)
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
// backend, where re-PUTting an identical config is a no-op.
func RestartConnector(ctx context.Context, baseURL, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/connectors/"+name+"/restart?includeTasks=true&onlyFailed=true", nil)
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
// RUNNING to be reported.
func ConnectorState(ctx context.Context, baseURL, name string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/connectors/"+name+"/status", nil)
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
// documented timeouts, not tight retries (roadmap risk register).
//
// FAILED is treated as recoverable within the deadline, not terminal: a task
// that died while its backend was briefly unavailable (or still sits in the
// worker JVM's negative DNS cache after a container was replaced) recovers
// after a restart. Failed instances are restarted at most every 10s; a
// connector that stays FAILED to the deadline is reported as such.
func WaitConnectorRunning(ctx context.Context, baseURL, name string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	lastState := "UNKNOWN"
	var lastRestart time.Time
	for {
		state, err := ConnectorState(ctx, baseURL, name)
		if err == nil {
			lastState = state
			if state == "RUNNING" {
				return state, nil
			}
			if state == "FAILED" && time.Since(lastRestart) > 10*time.Second {
				if err := RestartConnector(ctx, baseURL, name); err == nil {
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
