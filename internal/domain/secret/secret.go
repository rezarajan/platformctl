// Package secret defines the SecretReference kind.
// See docs/planning/03-resource-model-reference.md §10.
package secret

import "fmt"

type Backend string

const (
	BackendEnv        Backend = "env"
	BackendFile       Backend = "file"
	BackendKubernetes Backend = "kubernetes" // accepted by schema, resolution not implemented in v1
	BackendVault      Backend = "vault"      // accepted by schema, resolution not implemented in v1
)

type SecretReference struct {
	Name    string
	Backend Backend
	Keys    []string
}

func (s SecretReference) Validate() error {
	switch s.Backend {
	case BackendEnv, BackendFile, BackendKubernetes, BackendVault:
	default:
		return fmt.Errorf("SecretReference %q: unknown backend %q (allowed: env, file, kubernetes, vault)", s.Name, s.Backend)
	}
	if len(s.Keys) == 0 {
		return fmt.Errorf("SecretReference %q: spec.keys must not be empty", s.Name)
	}
	return nil
}
