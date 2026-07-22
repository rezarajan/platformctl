// This file implements runtime.IngressCapableRuntime (docs/planning/08 C7,
// docs/adr/018): one networking.k8s.io/v1 Ingress object per managed HTTP
// Connection, routing Host(spec.Host) to the existing Service spec.TargetName
// already created by that upstream's own EnsureContainer call. Kubernetes is
// the only adapter that implements this interface — see runtime.go's doc
// comment on IngressCapableRuntime for why Docker realizes HTTP routing
// itself instead of through this port.
package kubernetes

import (
	"context"
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

func buildIngress(spec runtimeport.IngressSpec) *networkingv1.Ingress {
	pathType := networkingv1.PathTypePrefix
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: spec.Namespace,
			Labels:    withOwnership(spec.Labels),
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{
					Host: spec.Host,
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{
									Path:     "/",
									PathType: &pathType,
									Backend: networkingv1.IngressBackend{
										Service: &networkingv1.IngressServiceBackend{
											Name: spec.TargetName,
											Port: networkingv1.ServiceBackendPort{
												Number: int32(spec.TargetPort),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	// TLSSecretName (docs/planning/08 C8): the Secret must already exist —
	// either materialized by this same provider (EnsureTLSSecret) or
	// referenced by name only (a cert-manager-managed spec.tls.secretName).
	// This adapter never creates it as a side effect of EnsureIngress.
	if spec.TLSSecretName != "" {
		ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{spec.Host}, SecretName: spec.TLSSecretName}}
	}
	return ing
}

// EnsureIngress creates or updates the named Ingress — idempotent, matching
// every other Ensure* on this port. Update (not create-if-absent only) so an
// out-of-band-mangled rule (the C7 accept criterion) converges back to the
// declared route on the next reconcile, the same drift-heals-on-reconcile
// contract ensureNetworkPolicies already gives NetworkPolicy objects.
func (r *Runtime) EnsureIngress(ctx context.Context, spec runtimeport.IngressSpec) (runtimeport.IngressState, error) {
	desired := buildIngress(spec)
	existing, err := r.clientset.NetworkingV1().Ingresses(spec.Namespace).Get(ctx, spec.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		created, cerr := r.clientset.NetworkingV1().Ingresses(spec.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		if cerr != nil {
			return runtimeport.IngressState{}, fmt.Errorf("create ingress %q: %w", spec.Name, cerr)
		}
		return ingressState(created), nil
	case err != nil:
		return runtimeport.IngressState{}, fmt.Errorf("get ingress %q: %w", spec.Name, err)
	default:
		if existing.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
			return runtimeport.IngressState{}, fmt.Errorf("ingress %q exists but is not managed by platformctl; refusing to replace it", spec.Name)
		}
		desired.ResourceVersion = existing.ResourceVersion
		updated, uerr := r.clientset.NetworkingV1().Ingresses(spec.Namespace).Update(ctx, desired, metav1.UpdateOptions{})
		if uerr != nil {
			return runtimeport.IngressState{}, fmt.Errorf("update ingress %q: %w", spec.Name, uerr)
		}
		return ingressState(updated), nil
	}
}

// GetIngress reads current state without mutating — the ingress provider's
// Probe drift check.
func (r *Runtime) GetIngress(ctx context.Context, namespace, name string) (runtimeport.IngressState, bool, error) {
	existing, err := r.clientset.NetworkingV1().Ingresses(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return runtimeport.IngressState{}, false, nil
	}
	if err != nil {
		return runtimeport.IngressState{}, false, fmt.Errorf("get ingress %q: %w", name, err)
	}
	return ingressState(existing), true, nil
}

func (r *Runtime) RemoveIngress(ctx context.Context, namespace, name string) error {
	if err := r.clientset.NetworkingV1().Ingresses(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete ingress %q: %w", name, err)
	}
	return nil
}

// ingressState reports the declared Host rule and, best-effort, the
// controller-observed load-balancer address (empty when the cluster's
// ingress controller hasn't published one — e.g. no LoadBalancer support on
// a local cluster; never blocks Ready on this alone, matching every other
// "observed, not assumed" endpoint fact in this codebase).
func ingressState(ing *networkingv1.Ingress) runtimeport.IngressState {
	st := runtimeport.IngressState{}
	if len(ing.Spec.Rules) > 0 {
		rule := ing.Spec.Rules[0]
		st.Host = rule.Host
		if rule.HTTP != nil && len(rule.HTTP.Paths) > 0 {
			backend := rule.HTTP.Paths[0].Backend
			if backend.Service != nil {
				st.TargetName = backend.Service.Name
				st.TargetPort = int(backend.Service.Port.Number)
			}
		}
	}
	for _, lb := range ing.Status.LoadBalancer.Ingress {
		if lb.IP != "" {
			st.Address = lb.IP
			break
		}
		if lb.Hostname != "" {
			st.Address = lb.Hostname
			break
		}
	}
	if len(ing.Spec.TLS) > 0 {
		st.TLSSecretName = ing.Spec.TLS[0].SecretName
	}
	return st
}
