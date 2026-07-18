// Package kubernetes implements SecretStore against native Kubernetes
// Secrets — the idiomatic backend for a platform running on the kubernetes
// runtime, where operators already manage Secret objects through the same
// cluster (docs/planning/08 B4). Gated by KubernetesSecretBackend
// (Alpha, disabled).
//
// A SecretReference resolves against the Kubernetes Secret named
// spec.kubernetes.name (default: metadata.name) in namespace
// spec.kubernetes.namespace (default: metadata.namespace — the same
// "Datascape namespace doubles as the Kubernetes namespace" convention the
// kubernetes runtime adapter uses for Providers), reading spec.keys as the
// Secret's data keys.
package kubernetes

import (
	"context"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/rezarajan/platformctl/internal/domain/secret"
)

type Store struct {
	// clientset is resolved lazily (on first Resolve/Preflight call) since
	// building it dials nothing by itself, but doing so at construction
	// time would make New() capable of failing before any SecretReference
	// even needs the kubernetes backend — every other secret store in this
	// project only fails when actually asked to resolve something.
	clientsetFor func() (kubernetes.Interface, error)
}

// New builds a Store using the standard kubeconfig loading rules
// (KUBECONFIG env, then ~/.kube/config), or in-cluster config when running
// inside a pod — the same ambient resolution the kubernetes runtime adapter
// uses when a Provider's spec.runtime doesn't override it. A
// SecretReference has no per-reference kubeconfig/context field (unlike a
// Provider's runtime block): the secret backend is a process-wide config
// choice, not a per-resource one.
func New() *Store {
	return &Store{clientsetFor: newAmbientClientset}
}

func newAmbientClientset() (kubernetes.Interface, error) {
	restCfg, err := loadAmbientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	return kubernetes.NewForConfig(restCfg)
}

func loadAmbientConfig() (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

func (s *Store) target(ref secret.SecretReference) (name, namespace string) {
	name = ref.Kubernetes.Name
	if name == "" {
		name = ref.Name
	}
	namespace = ref.Kubernetes.Namespace
	if namespace == "" {
		namespace = ref.Namespace
	}
	if namespace == "" {
		namespace = "default"
	}
	return name, namespace
}

func (s *Store) Resolve(ctx context.Context, ref secret.SecretReference) (map[string]string, error) {
	if ref.Backend != secret.BackendKubernetes {
		return nil, fmt.Errorf("SecretReference %q: backend %q not resolvable by the kubernetes store", ref.Name, ref.Backend)
	}
	clientset, err := s.clientsetFor()
	if err != nil {
		return nil, fmt.Errorf("SecretReference %q: %w", ref.Name, err)
	}
	name, namespace := s.target(ref)
	sec, err := clientset.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("SecretReference %q: kubernetes secret %s/%s not found", ref.Name, namespace, name)
	}
	if err != nil {
		return nil, fmt.Errorf("SecretReference %q: get kubernetes secret %s/%s: %w", ref.Name, namespace, name, err)
	}
	out := make(map[string]string, len(ref.Keys))
	var missing []string
	for _, key := range ref.Keys {
		v, ok := sec.Data[key]
		if !ok {
			missing = append(missing, key)
			continue
		}
		out[key] = string(v)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("SecretReference %q: kubernetes secret %s/%s missing key(s): %s", ref.Name, namespace, name, strings.Join(missing, ", "))
	}
	return out, nil
}

// Preflight aggregates every missing key into one error, same as Resolve
// but never returning the resolved values — Resolve already does the
// cheapest possible existence+completeness check (a single Get), so
// Preflight just discards the values on success.
func (s *Store) Preflight(ctx context.Context, ref secret.SecretReference) error {
	_, err := s.Resolve(ctx, ref)
	return err
}
