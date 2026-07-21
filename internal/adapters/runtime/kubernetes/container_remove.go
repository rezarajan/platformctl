// This file holds the container teardown and inspection seam: Remove,
// Inspect, ListManaged, WaitHealthy, and the state-building helpers they
// share (docs/planning/08 §7.6 G3). The create/update half of the same
// EnsureContainer/Remove/Inspect/ListManaged seam lives in container.go.
package kubernetes

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// enrichedState builds a ContainerState from a Deployment and, when its own
// Service has a host-reachable address (NodePort/LoadBalancer — never
// ClusterIP/port-forward), fills in HostIP/HostPort per port so `platformctl
// inventory` reports real, observed exposure instead of always claiming
// cluster-internal-only (docs/planning/08 B2). Best-effort: a Service lookup
// failure just leaves ports cluster-internal, matching the pre-B2 behavior,
// rather than failing the whole Inspect/ListManaged call.
func (r *Runtime) enrichedState(ctx context.Context, ns string, d *appsv1.Deployment) runtimeport.ContainerState {
	st := stateFromDeployment(d)
	if len(st.Ports) == 0 {
		return st
	}
	svc, err := r.clientset.CoreV1().Services(ns).Get(ctx, st.Name, metav1.GetOptions{})
	if err != nil {
		return st
	}
	for i, p := range st.Ports {
		if hostIP, hostPort, ok := r.observedHostAddr(ctx, svc, int32(p.ContainerPort)); ok {
			st.Ports[i].HostIP = hostIP
			st.Ports[i].HostPort = hostPort
		}
	}
	return st
}

// replicaSetReadiness resolves name against the Deployment path, then the
// StatefulSet path (docs/adr/004-replicas-and-identity.md), reporting
// which namespace it lives in, whether either was found, and whether it is
// currently ready ("at least one replica" — the same rule stateFromDeployment/
// stateFromStatefulSet already apply). Shared by WaitHealthy and any other
// method that only needs a yes/no readiness signal rather than the full
// ContainerState.
func (r *Runtime) replicaSetReadiness(ctx context.Context, name string) (ns string, found, ready bool, err error) {
	dns, d, derr := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if derr != nil {
		return "", false, false, fmt.Errorf("get deployment %q: %w", name, derr)
	}
	if d != nil {
		return dns, true, d.Status.ReadyReplicas > 0, nil
	}
	sns, sts, serr := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.StatefulSet, error) {
		return r.clientset.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if serr != nil {
		return "", false, false, fmt.Errorf("get statefulset %q: %w", name, serr)
	}
	if sts != nil {
		return sns, true, sts.Status.ReadyReplicas > 0, nil
	}
	return "", false, false, nil
}

func (r *Runtime) WaitHealthy(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ns, found, ready, err := r.replicaSetReadiness(ctx, name)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("container %q not found", name)
		}
		if ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("container %q did not become healthy within %s%s", name, timeout, r.tailLogs(ctx, ns, name))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Inspect resolves name against the Deployment path, then the StatefulSet
// path, then — since neither top-level object is ever literally named an
// ordinal ("<base>-<i>" is a Pod name StatefulSet assigns natively, never a
// StatefulSet's own name) — a direct Pod lookup, so an ordinal name resolves
// to that specific replica's own state (docs/adr/004's "ordinal hostname
// resolution" conformance subtest).
func (r *Runtime) Inspect(ctx context.Context, name string) (runtimeport.ContainerState, bool, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return runtimeport.ContainerState{}, false, err
	}
	if d != nil {
		return r.enrichedState(ctx, ns, d), true, nil
	}
	_, sts, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.StatefulSet, error) {
		return r.clientset.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return runtimeport.ContainerState{}, false, err
	}
	if sts != nil {
		return stateFromStatefulSet(sts), true, nil
	}
	_, pod, stsName, err := r.findOrdinalPod(ctx, name)
	if err != nil {
		return runtimeport.ContainerState{}, false, err
	}
	if pod == nil {
		return runtimeport.ContainerState{}, false, nil
	}
	return stateFromPod(pod, stsName), true, nil
}

// findOrdinalPod looks up a Pod literally named name (StatefulSet pods are
// named "<StatefulSet name>-<ordinal>" by Kubernetes itself — no adapter
// code manufactures this) across every managed namespace, and returns the
// name of the StatefulSet that owns it (read from the Pod's own
// OwnerReferences, never parsed out of the name string) so callers can
// address the pod's container, which is always named after the StatefulSet,
// not the pod.
func (r *Runtime) findOrdinalPod(ctx context.Context, name string) (ns string, pod *corev1.Pod, statefulSetName string, err error) {
	ns, pod, err = findAcrossNamespaces(ctx, r, func(ns string) (*corev1.Pod, error) {
		return r.clientset.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil || pod == nil {
		return "", nil, "", err
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "StatefulSet" {
			return ns, pod, owner.Name, nil
		}
	}
	return "", nil, "", fmt.Errorf("pod %q exists but is not owned by a StatefulSet; not a valid ordinal replica address", name)
}

// stateFromPod builds a single ordinal replica's own ContainerState — never
// the aggregate ReadyReplicas count, which only has a meaning at the
// replica-set level (Inspect against the StatefulSet's own name, above).
func stateFromPod(pod *corev1.Pod, containerName string) runtimeport.ContainerState {
	st := runtimeport.ContainerState{
		Name:    pod.Name,
		ID:      string(pod.UID),
		Labels:  pod.Labels,
		Running: pod.Status.Phase == corev1.PodRunning,
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			st.Healthy = true
		}
	}
	for _, c := range pod.Spec.Containers {
		if c.Name != containerName {
			continue
		}
		st.Image = c.Image
		env := make(map[string]string, len(c.Env))
		for _, e := range c.Env {
			env[e.Name] = e.Value
		}
		st.Env = env
		for _, p := range c.Ports {
			proto := "tcp"
			if p.Protocol == corev1.ProtocolUDP {
				proto = "udp"
			}
			st.Ports = append(st.Ports, runtimeport.PortBinding{ContainerPort: int(p.ContainerPort), Protocol: proto})
		}
	}
	return st
}

// Remove tears down the Deployment path, then the StatefulSet path
// (docs/adr/004-replicas-and-identity.md) — whichever object exists for
// name. Per-ordinal PersistentVolumeClaims from a StatefulSet's
// VolumeClaimTemplates are deliberately left in place, matching the
// Deployment path's existing behavior of never touching volumes as a side
// effect of Remove.
func (r *Runtime) Remove(ctx context.Context, name string) error {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return err
	}
	if d != nil {
		return r.removeDeployment(ctx, ns, name, d)
	}
	nsSts, sts, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.StatefulSet, error) {
		return r.clientset.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return err
	}
	if sts == nil {
		return nil
	}
	return r.removeStatefulSet(ctx, nsSts, name, sts)
}

func (r *Runtime) removeDeployment(ctx context.Context, ns, name string, d *appsv1.Deployment) error {
	if d.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
		return fmt.Errorf("deployment %q is not managed by platformctl; refusing to remove it", name)
	}
	propagation := metav1.DeletePropagationForeground
	if err := r.clientset.AppsV1().Deployments(ns).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete deployment %q: %w", name, err)
	}
	if err := r.removeCommonContainerObjects(ctx, ns, name); err != nil {
		return err
	}
	// Foreground propagation means the Deployment stays visible (with a
	// deletionTimestamp) until its ReplicaSet/Pods are actually gone.
	// Docker's ContainerRemove(Force: true) is synchronous — callers
	// (engine, conformance suite) expect Remove to mean "gone" when it
	// returns, so wait for that here rather than leaking the async gap.
	return r.waitObjectGone(ctx, func() error {
		_, err := r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		return err
	}, "deployment", name)
}

func (r *Runtime) removeStatefulSet(ctx context.Context, ns, name string, sts *appsv1.StatefulSet) error {
	if sts.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
		return fmt.Errorf("statefulset %q is not managed by platformctl; refusing to remove it", name)
	}
	propagation := metav1.DeletePropagationForeground
	if err := r.clientset.AppsV1().StatefulSets(ns).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete statefulset %q: %w", name, err)
	}
	// The headless governing Service is also named `name`, so it is covered
	// by removeCommonContainerObjects' app=<name> Service query, alongside
	// any alias Services.
	if err := r.removeCommonContainerObjects(ctx, ns, name); err != nil {
		return err
	}
	if err := r.clientset.PolicyV1().PodDisruptionBudgets(ns).Delete(ctx, pdbName(name), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete poddisruptionbudget for %q: %w", name, err)
	}
	return r.waitObjectGone(ctx, func() error {
		_, err := r.clientset.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		return err
	}, "statefulset", name)
}

// removeCommonContainerObjects deletes the Service(s), files Secret, and
// external-ingress NetworkPolicy hole shared by both the Deployment and
// StatefulSet teardown paths.
func (r *Runtime) removeCommonContainerObjects(ctx context.Context, ns, name string) error {
	// Delete every Service addressing this container — its own name plus
	// alias Services, all labeled app=<name> by ensureService/
	// ensureHeadlessService/ensureAliasServices.
	svcs, err := r.clientset.CoreV1().Services(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + name + "," + runtimeport.LabelManagedBy + "=" + runtimeport.ManagedByValue,
	})
	if err != nil {
		return fmt.Errorf("list services for %q: %w", name, err)
	}
	for _, svc := range svcs.Items {
		if err := r.clientset.CoreV1().Services(ns).Delete(ctx, svc.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete service %q: %w", svc.Name, err)
		}
	}
	if err := r.clientset.CoreV1().Secrets(ns).Delete(ctx, filesSecretName(name), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete files secret for %q: %w", name, err)
	}
	// Delete the per-container external-ingress hole (if any) by its
	// deterministic name — the minimal RBAC role grants delete but not list
	// on networkpolicies, so this must not enumerate.
	if err := r.clientset.NetworkingV1().NetworkPolicies(ns).Delete(ctx, externalIngressPolicyName(name), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete external-ingress policy for %q: %w", name, err)
	}
	return nil
}

// waitObjectGone polls getErr until it reports NotFound, matching Docker's
// synchronous ContainerRemove semantics: callers expect Remove to mean
// "gone" when it returns, not "deletion requested."
func (r *Runtime) waitObjectGone(ctx context.Context, getErr func() error, kind, name string) error {
	const removeTimeout = 45 * time.Second // > TerminationGracePeriodSeconds + API/GC overhead
	deadline := time.Now().Add(removeTimeout)
	for {
		err := getErr()
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("waiting for %s %q removal: %w", kind, name, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s %q did not finish terminating within %s", kind, name, removeTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// ListManaged reports one ContainerState per managed Deployment plus one per
// managed StatefulSet (docs/adr/004-replicas-and-identity.md) — the
// aggregate view of a replica set, not its individual ordinal Pods, matching
// the same "one entry per top-level managed object" contract ListManaged has
// always had.
func (r *Runtime) ListManaged(ctx context.Context) ([]runtimeport.ContainerState, error) {
	namespaces, err := r.managedNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	var out []runtimeport.ContainerState
	for _, ns := range namespaces {
		list, err := r.clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{
			LabelSelector: runtimeport.LabelManagedBy + "=" + runtimeport.ManagedByValue,
		})
		if err != nil {
			return nil, fmt.Errorf("list deployments in namespace %q: %w", ns, err)
		}
		for i := range list.Items {
			out = append(out, r.enrichedState(ctx, ns, &list.Items[i]))
		}
		stsList, err := r.clientset.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{
			LabelSelector: runtimeport.LabelManagedBy + "=" + runtimeport.ManagedByValue,
		})
		if err != nil {
			return nil, fmt.Errorf("list statefulsets in namespace %q: %w", ns, err)
		}
		for i := range stsList.Items {
			out = append(out, stateFromStatefulSet(&stsList.Items[i]))
		}
	}
	return out, nil
}
