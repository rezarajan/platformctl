// Package env implements SecretStore against environment variables.
// A key K of SecretReference R resolves from DATASCAPE_SECRET_<R>_<K>,
// uppercased, with dashes mapped to underscores.
package env

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/secret"
)

type Store struct{}

func New() *Store { return &Store{} }

func (s *Store) Resolve(_ context.Context, ref secret.SecretReference) (map[string]string, error) {
	if ref.Backend != secret.BackendEnv {
		return nil, fmt.Errorf("SecretReference %q: backend %q not resolvable by the env store", ref.Name, ref.Backend)
	}
	out := make(map[string]string, len(ref.Keys))
	for _, key := range ref.Keys {
		envVar := envVarName(ref.Name, key)
		val, ok := os.LookupEnv(envVar)
		if !ok {
			return nil, fmt.Errorf("SecretReference %q: key %q not found (expected env var %s)", ref.Name, key, envVar)
		}
		out[key] = val
	}
	return out, nil
}

func envVarName(refName, key string) string {
	norm := func(s string) string {
		return strings.ToUpper(strings.ReplaceAll(s, "-", "_"))
	}
	return "DATASCAPE_SECRET_" + norm(refName) + "_" + norm(key)
}
