package debezium

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

var httpClient = &http.Client{Timeout: 15 * time.Second}

// putConnectorConfig registers or updates a connector; PUT to
// /connectors/<name>/config is an idempotent upsert in the Connect REST API.
func putConnectorConfig(ctx context.Context, baseURL, name string, config map[string]string) error {
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

func getConnectorConfig(ctx context.Context, baseURL, name string) (map[string]string, error) {
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

func deleteConnector(ctx context.Context, baseURL, name string) error {
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

// connectorState returns the connector's aggregate state (RUNNING, FAILED,
// PAUSED, ...); the connector state and every task's state must agree for
// RUNNING to be reported.
func connectorState(ctx context.Context, baseURL, name string) (string, error) {
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

// waitConnectorRunning polls with bounded backoff until the connector and all
// its tasks are RUNNING. Connector registration is inherently async; generous
// documented timeouts, not tight retries (roadmap risk register).
func waitConnectorRunning(ctx context.Context, baseURL, name string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	lastState := "UNKNOWN"
	for {
		state, err := connectorState(ctx, baseURL, name)
		if err == nil {
			lastState = state
			if state == "RUNNING" {
				return state, nil
			}
			if state == "FAILED" {
				return state, fmt.Errorf("connector %q entered FAILED state", name)
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
