// Package file implements SecretStore against files on disk: key K of
// SecretReference R resolves from $DATASCAPE_SECRETS_DIR/<R>/<K> (trailing
// newline trimmed) — the layout used by container secret mounts.
package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/secret"
)

const dirEnv = "DATASCAPE_SECRETS_DIR"

type Store struct{}

func New() *Store { return &Store{} }

func (s *Store) Resolve(_ context.Context, ref secret.SecretReference) (map[string]string, error) {
	if ref.Backend != secret.BackendFile {
		return nil, fmt.Errorf("SecretReference %q: backend %q not resolvable by the file store", ref.Name, ref.Backend)
	}
	base := os.Getenv(dirEnv)
	if base == "" {
		return nil, fmt.Errorf("SecretReference %q: %s is not set (the file backend reads <dir>/<name>/<key>)", ref.Name, dirEnv)
	}
	out := make(map[string]string, len(ref.Keys))
	for _, key := range ref.Keys {
		p := filepath.Join(base, ref.Name, key)
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("SecretReference %q: key %q not readable at %s: %w", ref.Name, key, p, err)
		}
		out[key] = strings.TrimRight(string(data), "\n")
	}
	return out, nil
}

func (s *Store) Preflight(_ context.Context, ref secret.SecretReference) error {
	if ref.Backend != secret.BackendFile {
		return fmt.Errorf("SecretReference %q: backend %q not resolvable by the file store", ref.Name, ref.Backend)
	}
	base := os.Getenv(dirEnv)
	if base == "" {
		return fmt.Errorf("SecretReference %q: %s is not set (the file backend reads <dir>/<name>/<key>)", ref.Name, dirEnv)
	}
	var missing []string
	for _, key := range ref.Keys {
		p := filepath.Join(base, ref.Name, key)
		if _, err := os.Stat(p); err != nil {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("SecretReference %q: missing key file(s): %s", ref.Name, strings.Join(missing, ", "))
	}
	return nil
}
