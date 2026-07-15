// Package vault implements SecretStore against HashiCorp Vault's KV v2
// engine: SecretReference R resolves from <mount>/data/<base>/<R> using
// VAULT_ADDR and VAULT_TOKEN. Mount and base path default to "secret" and
// "datascape" (override via DATASCAPE_VAULT_MOUNT / DATASCAPE_VAULT_BASE).
// Gated by VaultSecretBackend (Phase 6).
package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/secret"
)

type Store struct {
	client *http.Client
}

func New() *Store { return &Store{client: &http.Client{Timeout: 10 * time.Second}} }

func (s *Store) config() (addr, token, mount, base string, err error) {
	addr = os.Getenv("VAULT_ADDR")
	token = os.Getenv("VAULT_TOKEN")
	if addr == "" || token == "" {
		return "", "", "", "", fmt.Errorf("the vault backend requires VAULT_ADDR and VAULT_TOKEN")
	}
	mount = os.Getenv("DATASCAPE_VAULT_MOUNT")
	if mount == "" {
		mount = "secret"
	}
	base = os.Getenv("DATASCAPE_VAULT_BASE")
	if base == "" {
		base = "datascape"
	}
	return addr, token, mount, base, nil
}

func (s *Store) Resolve(ctx context.Context, ref secret.SecretReference) (map[string]string, error) {
	if ref.Backend != secret.BackendVault {
		return nil, fmt.Errorf("SecretReference %q: backend %q not resolvable by the vault store", ref.Name, ref.Backend)
	}
	addr, token, mount, base, err := s.config()
	if err != nil {
		return nil, fmt.Errorf("SecretReference %q: %w", ref.Name, err)
	}
	url := fmt.Sprintf("%s/v1/%s/data/%s/%s", addr, mount, base, ref.Name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", token)
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SecretReference %q: vault request: %w", ref.Name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("SecretReference %q: vault returned HTTP %d for %s: %s", ref.Name, resp.StatusCode, url, msg)
	}
	var body struct {
		Data struct {
			Data map[string]any `json:"data"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("SecretReference %q: decode vault response: %w", ref.Name, err)
	}
	out := make(map[string]string, len(ref.Keys))
	for _, key := range ref.Keys {
		v, ok := body.Data.Data[key]
		if !ok {
			return nil, fmt.Errorf("SecretReference %q: key %q not present at %s/%s/%s", ref.Name, key, mount, base, ref.Name)
		}
		out[key] = fmt.Sprintf("%v", v)
	}
	return out, nil
}

// Preflight resolves ref and discards the values: there is no cheaper
// existence check against KV v2, and a real read also verifies connectivity
// and the token before any infrastructure is touched.
func (s *Store) Preflight(ctx context.Context, ref secret.SecretReference) error {
	_, err := s.Resolve(ctx, ref)
	return err
}
