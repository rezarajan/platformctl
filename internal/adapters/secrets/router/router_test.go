package router

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	envsecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/env"
	filesecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/file"
	"github.com/rezarajan/platformctl/internal/domain/secret"
)

func TestRouterDispatchesByBackend(t *testing.T) {
	t.Setenv("DATASCAPE_SECRET_CREDS_USERNAME", "alice")
	dir := t.TempDir()
	t.Setenv("DATASCAPE_SECRETS_DIR", dir)
	if err := os.MkdirAll(filepath.Join(dir, "fcreds"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fcreds", "token"), []byte("s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	r := New().
		Register(secret.BackendEnv, envsecrets.New()).
		Register(secret.BackendFile, filesecrets.New())

	got, err := r.Resolve(context.Background(), secret.SecretReference{Name: "creds", Backend: secret.BackendEnv, Keys: []string{"username"}})
	if err != nil || got["username"] != "alice" {
		t.Errorf("env resolve = %v, %v", got, err)
	}

	got, err = r.Resolve(context.Background(), secret.SecretReference{Name: "fcreds", Backend: secret.BackendFile, Keys: []string{"token"}})
	if err != nil || got["token"] != "s3cr3t" {
		t.Errorf("file resolve = %v, %v (trailing newline must be trimmed)", got, err)
	}

	// Declared-but-unavailable backends fail fast with a clear message.
	_, err = r.Resolve(context.Background(), secret.SecretReference{Name: "k", Backend: secret.BackendKubernetes, Keys: []string{"x"}})
	if err == nil || !strings.Contains(err.Error(), "not available") {
		t.Errorf("kubernetes backend error = %v, want not-available", err)
	}
	_, err = r.Resolve(context.Background(), secret.SecretReference{Name: "v", Backend: secret.BackendVault, Keys: []string{"x"}})
	if err == nil || !strings.Contains(err.Error(), "not available") {
		t.Errorf("ungated vault backend error = %v, want not-available", err)
	}
}
