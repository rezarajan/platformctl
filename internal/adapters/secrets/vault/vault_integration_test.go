//go:build integration

package vault

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/secret"
)

// TestVaultResolve covers the Phase 6 VaultSecretBackend deliverable against
// a real Vault dev server.
func TestVaultResolve(t *testing.T) {
	_ = exec.Command("docker", "rm", "-f", "datascape-vault-test").Run()
	if out, err := exec.Command("docker", "run", "-d", "--name", "datascape-vault-test",
		"-e", "VAULT_DEV_ROOT_TOKEN_ID=test-root-token",
		"-p", "18200:8200", "hashicorp/vault:latest").CombinedOutput(); err != nil {
		t.Fatalf("run vault: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "datascape-vault-test").Run() })

	t.Setenv("VAULT_ADDR", "http://127.0.0.1:18200")
	t.Setenv("VAULT_TOKEN", "test-root-token")

	// Wait for the dev server, then write a KV v2 secret over the API.
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := http.Get("http://127.0.0.1:18200/v1/sys/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("vault dev server did not become ready")
		}
		time.Sleep(time.Second)
	}
	body := bytes.NewBufferString(`{"data":{"username":"vault-user","password":"vault-pw"}}`)
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:18200/v1/secret/data/datascape/db-creds", body)
	req.Header.Set("X-Vault-Token", "test-root-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("write secret: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("write secret: HTTP %d", resp.StatusCode)
	}

	got, err := New().Resolve(context.Background(), secret.SecretReference{
		Name: "db-creds", Backend: secret.BackendVault, Keys: []string{"username", "password"},
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got["username"] != "vault-user" || got["password"] != "vault-pw" {
		t.Errorf("resolved = %v", got)
	}

	// Missing keys fail with the path in the message.
	_, err = New().Resolve(context.Background(), secret.SecretReference{
		Name: "db-creds", Backend: secret.BackendVault, Keys: []string{"nope"},
	})
	if err == nil || !strings.Contains(fmt.Sprint(err), "nope") {
		t.Errorf("missing-key error = %v", err)
	}
}
