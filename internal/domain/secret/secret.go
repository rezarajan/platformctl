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
	Name string
	// Namespace is the Datascape namespace (metadata.namespace) the
	// SecretReference resource itself lives in. The env/file/vault backends
	// ignore it; the kubernetes backend uses it as the Kubernetes namespace
	// to look in unless Kubernetes.Namespace overrides it — the same
	// "Datascape namespace doubles as the Kubernetes namespace" convention
	// the runtime adapter uses for Providers.
	Namespace string
	Backend   Backend
	Keys      []string
	// Kubernetes carries backend: kubernetes-specific overrides
	// (spec.kubernetes in the manifest). Both fields are optional; Name
	// defaults to SecretReference.Name and Namespace to
	// SecretReference.Namespace when left empty.
	Kubernetes KubernetesRef
}

// KubernetesRef overrides the Kubernetes Secret object identity a
// backend: kubernetes SecretReference resolves against.
type KubernetesRef struct {
	Name      string
	Namespace string
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
