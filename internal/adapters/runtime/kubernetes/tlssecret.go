// This file implements the TLS-Secret half of runtime.IngressCapableRuntime
// (docs/planning/08 C8, docs/adr/018 addendum): a plain
// kubernetes.io/tls-shaped Secret (keys "tls.crt"/"tls.key" — the same
// convention `kubectl create secret tls` and cert-manager use), reused for
// two distinct purposes by the ingress provider:
//
//   - a Connection's own leaf certificate (provided via spec.tls.secretRef,
//     or generated for spec.tls.selfSigned) — referenced by
//     IngressSpec.TLSSecretName.
//   - the ingress Provider's own local CA keypair (spec.tls.selfSigned only)
//     — never referenced by any Ingress object, stored purely so it can be
//     read back via GetTLSSecret and reused rather than regenerated (and
//     therefore rotated) on every apply.
//
// No RBAC change needed: deploy/kubernetes/rbac/role.yaml already grants
// get/create/update/delete on secrets cluster-wide (ContainerSpec.Files/
// ImagePullAuth already needed it).
package kubernetes

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	tlsCertKey = "tls.crt"
	tlsKeyKey  = "tls.key"
)

func buildTLSSecret(namespace, name string, certPEM, keyPEM []byte, labels map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    withOwnership(labels),
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			tlsCertKey: certPEM,
			tlsKeyKey:  keyPEM,
		},
	}
}

// EnsureTLSSecret creates or updates the named Secret — idempotent, matching
// every other Ensure* on this port. Refuses to touch a same-name Secret this
// adapter didn't create (the same ownership-label refusal EnsureIngress
// already applies), so this can never clobber a cert-manager-issued Secret
// even if a manifest typo'd secretRef/selfSigned onto a name cert-manager
// also owns.
func (r *Runtime) EnsureTLSSecret(ctx context.Context, namespace, name string, certPEM, keyPEM []byte, labels map[string]string) error {
	desired := buildTLSSecret(namespace, name, certPEM, keyPEM, labels)
	existing, err := r.clientset.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, cerr := r.clientset.CoreV1().Secrets(namespace).Create(ctx, desired, metav1.CreateOptions{}); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			return fmt.Errorf("create tls secret %q: %w", name, cerr)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get tls secret %q: %w", name, err)
	default:
		if existing.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
			return fmt.Errorf("secret %q exists but is not managed by platformctl; refusing to replace it", name)
		}
		desired.ResourceVersion = existing.ResourceVersion
		if _, uerr := r.clientset.CoreV1().Secrets(namespace).Update(ctx, desired, metav1.UpdateOptions{}); uerr != nil {
			return fmt.Errorf("update tls secret %q: %w", name, uerr)
		}
		return nil
	}
}

// GetTLSSecret reads an existing Secret's cert/key material without
// mutating it — used both for drift detection (does the live Secret still
// match the desired cert) and to read back a previously-provisioned local
// CA before deciding whether to regenerate one. found is false when no such
// Secret exists — including a cert-manager-managed spec.tls.secretName not
// yet issued, which is expected to converge, not an error.
func (r *Runtime) GetTLSSecret(ctx context.Context, namespace, name string) (certPEM, keyPEM []byte, found bool, err error) {
	secret, gerr := r.clientset.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(gerr) {
		return nil, nil, false, nil
	}
	if gerr != nil {
		return nil, nil, false, fmt.Errorf("get tls secret %q: %w", name, gerr)
	}
	return secret.Data[tlsCertKey], secret.Data[tlsKeyKey], true, nil
}

// RemoveTLSSecret deletes a Secret this provider created — never called for
// a cert-manager-managed spec.tls.secretName (referencing only, never
// operating cert-manager). A no-op, not an error, if already gone.
func (r *Runtime) RemoveTLSSecret(ctx context.Context, namespace, name string) error {
	if err := r.clientset.CoreV1().Secrets(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete tls secret %q: %w", name, err)
	}
	return nil
}
