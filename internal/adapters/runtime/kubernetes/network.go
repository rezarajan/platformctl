// This file holds the Namespace/NetworkPolicy seam: EnsureNetwork,
// RemoveNetwork, the isolation-boundary and external-ingress NetworkPolicy
// reconcilers, and ListManagedNetworks (docs/planning/08 §7.6 G3).
package kubernetes

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// EnsureNetwork creates the Namespace and, unless opted out
// (IsolationPolicy: IsolationNone), a default-deny + allow-same-namespace
// NetworkPolicy pair (docs/planning/08 B7) — without it, a Docker network's
// isolation boundary silently weakens to "DNS parity only" on Kubernetes:
// any pod anywhere in the cluster could reach a Service the Namespace
// mapping alone does nothing to stop.
func (r *Runtime) EnsureNetwork(ctx context.Context, spec runtimeport.NetworkSpec) error {
	ns, err := r.clientset.CoreV1().Namespaces().Get(ctx, spec.Name, metav1.GetOptions{})
	switch {
	case err == nil:
		if ns.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
			return fmt.Errorf("namespace %q exists but is not managed by platformctl; refusing to reuse it — choose a dedicated name via the Provider's spec.runtime.network (every object of one platform joins that namespace); every cluster has pre-existing unmanaged namespaces like default/kube-system, so a collision here is expected, not a bug", spec.Name)
		}
	case apierrors.IsNotFound(err):
		if _, err := r.clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Labels: withOwnership(spec.Labels)},
		}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create namespace %q: %w", spec.Name, err)
		}
	default:
		return fmt.Errorf("get namespace %q: %w", spec.Name, err)
	}

	if spec.IsolationPolicy == runtimeport.IsolationNone {
		fmt.Fprintf(os.Stderr, "warning: namespace %q uses networkPolicy: none — no isolation boundary is provisioned; every pod in the cluster can reach it unless something else in the cluster restricts it\n", spec.Name)
		return nil
	}
	return r.ensureNetworkPolicies(ctx, spec.Name, spec.Labels)
}

// ensureNetworkPolicies creates or updates the isolation-boundary
// NetworkPolicy pair. Update (not just create-if-absent) so a namespace
// created before this existed, or one whose policies were edited
// out-of-band, converges back to the declared boundary on the next apply —
// the same drift-heals-on-reconcile behavior every other managed object
// gets.
func (r *Runtime) ensureNetworkPolicies(ctx context.Context, ns string, labels map[string]string) error {
	for _, policy := range buildNetworkPolicies(ns, labels) {
		existing, err := r.clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, policy.Name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			if _, err := r.clientset.NetworkingV1().NetworkPolicies(ns).Create(ctx, policy, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create networkpolicy %q: %w", policy.Name, err)
			}
		case err != nil:
			return fmt.Errorf("get networkpolicy %q: %w", policy.Name, err)
		default:
			policy.ResourceVersion = existing.ResourceVersion
			if _, err := r.clientset.NetworkingV1().NetworkPolicies(ns).Update(ctx, policy, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("update networkpolicy %q: %w", policy.Name, err)
			}
		}
	}
	return nil
}

// ensureExternalIngressPolicy reconciles the per-container NetworkPolicy that
// opens the namespace's default-deny boundary for the ports of a container
// exposed outside it (node-port/load-balancer). It is idempotent at the
// EnsureContainer level: a spec whose hash is unchanged never reaches here,
// and an access-mode change (which changes the hash) converges the policy —
// created when the mode becomes external, deleted when it stops being.
//
// The hole is only provisioned when the namespace actually carries the
// default-deny wall: with no wall there is nothing to punch through, and a
// pod-selecting policy would instead *restrict* an IsolationNone pod to just
// these ports — the opposite of that namespace's declared "no isolation".
func (r *Runtime) ensureExternalIngressPolicy(ctx context.Context, ns string, spec runtimeport.ContainerSpec) error {
	name := externalIngressPolicyName(spec.Name)
	desired := buildExternalIngressPolicy(ns, spec)
	if desired != nil {
		if _, err := r.clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, denyAllIngressPolicyName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			desired = nil // no wall in this namespace; no hole to punch
		} else if err != nil {
			return fmt.Errorf("get default-deny policy in %q: %w", ns, err)
		}
	}
	if desired == nil {
		if err := r.clientset.NetworkingV1().NetworkPolicies(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete external-ingress policy %q: %w", name, err)
		}
		return nil
	}
	existing, err := r.clientset.NetworkingV1().NetworkPolicies(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := r.clientset.NetworkingV1().NetworkPolicies(ns).Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create external-ingress policy %q: %w", name, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get external-ingress policy %q: %w", name, err)
	default:
		desired.ResourceVersion = existing.ResourceVersion
		if _, err := r.clientset.NetworkingV1().NetworkPolicies(ns).Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update external-ingress policy %q: %w", name, err)
		}
		return nil
	}
}

func (r *Runtime) RemoveNetwork(ctx context.Context, name string) error {
	ns, err := r.clientset.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get namespace %q: %w", name, err)
	}
	if ns.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
		return fmt.Errorf("namespace %q is not managed by platformctl; refusing to remove it", name)
	}
	// A Docker network cannot be removed while containers are still attached
	// (NetworkRemove returns "network has active endpoints"). Providers that
	// share one network each best-effort-call RemoveNetwork on Destroy and
	// lean on that refusal: the shared network outlives every member but the
	// last, and no container is ever deleted as a side effect of removing the
	// network. Deleting a Kubernetes Namespace, by contrast, cascades to every
	// object inside it — so without the same "in use" guard, destroying one
	// Provider on a shared namespace would wipe its siblings and any unmanaged
	// workload placed alongside them. Mirror Docker: a Deployment is the
	// container analog, so refuse while any Deployment still lives here and
	// delete the namespace only once it has been emptied of workloads. (Remove
	// blocks until its Deployment is fully gone, so a just-removed member does
	// not linger in this List.)
	deployments, err := r.clientset.AppsV1().Deployments(name).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list deployments in namespace %q: %w", name, err)
	}
	statefulSets, err := r.clientset.AppsV1().StatefulSets(name).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list statefulsets in namespace %q: %w", name, err)
	}
	if n := len(deployments.Items) + len(statefulSets.Items); n > 0 {
		return fmt.Errorf("namespace %q still has %d active workload(s); refusing to remove it", name, n)
	}
	if err := r.clientset.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete namespace %q: %w", name, err)
	}
	return nil
}

// ListManagedNetworks reports every managed Namespace (the Docker network
// analog — EnsureNetwork creates one Namespace per Docker network, see
// EnsureNetwork above), independent of whether any Deployment currently runs
// in it.
func (r *Runtime) ListManagedNetworks(ctx context.Context) ([]runtimeport.ManagedNetwork, error) {
	list, err := r.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{
		LabelSelector: runtimeport.LabelManagedBy + "=" + runtimeport.ManagedByValue,
	})
	if err != nil {
		return nil, fmt.Errorf("list managed namespaces: %w", err)
	}
	out := make([]runtimeport.ManagedNetwork, 0, len(list.Items))
	for _, ns := range list.Items {
		out = append(out, runtimeport.ManagedNetwork{Name: ns.Name, Labels: ns.Labels})
	}
	return out, nil
}
