// Package kubernetes implements ContainerRuntime against a real Kubernetes
// cluster via client-go. It is the second ContainerRuntime adapter (Docker
// being the first) and exists to prove the port boundary in
// internal/ports/runtime is genuinely runtime-agnostic: every provider
// (redpanda, postgres, debezium, s3, ...) reconciles against this adapter
// with zero code changes, exercised by the same conformance suite the
// Docker adapter passes (internal/ports/runtime/conformance).
//
// Mapping decisions (see docs/planning/07-production-grade-docker-runtime-gap-analysis.md
// "Cross-Runtime Portability" and docs/planning/04-roadmap-and-feature-gates.md §10):
//
//   - A Docker "network" (ContainerSpec.Networks / VolumeSpec.Networks) is a
//     shared addressing+isolation domain that lets containers resolve each
//     other by name. A Kubernetes Namespace is the same kind of domain
//     (every object in it gets DNS via a Service name), so EnsureNetwork
//     ensures a Namespace of that name exists, and every container/volume
//     naming that network is placed inside it.
//   - EnsureContainer creates a single-replica Deployment plus a matching
//     ClusterIP Service (same name as the container) so other pods in the
//     namespace can reach it at "<name>:<port>" — the exact addressing
//     style every provider already uses for Docker's embedded DNS. No
//     provider code changes were needed to make this work.
//   - EnsureVolume creates a PersistentVolumeClaim in the namespace derived
//     from VolumeSpec.Networks[0] (PVCs cannot be mounted cross-namespace).
//   - RestartPolicy: Kubernetes Deployments require Pod restartPolicy
//     "Always" — there is no Pod-level "give up after N restarts" the way
//     Docker's on-failure+MaxRetries has. This is a genuine, documented
//     per-runtime difference, not a bug: MaxRetries and non-Always modes
//     are accepted but not enforced by this adapter.
//   - LogConfig (Docker's per-container log driver) has no Kubernetes
//     equivalent (logging is a node/kubelet concern) and is ignored here.
//   - SecurityContext.SecurityOpt is a Docker-specific escape hatch with no
//     generic Kubernetes translation and is ignored here.
package kubernetes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/client-go/util/retry"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// specHashAnnotation carries a fingerprint of the last-applied ContainerSpec
// so EnsureContainer can detect "already matches" — the same role
// Docker's specGenLabel label plays, but stored as an annotation because a
// sha256 hex digest (64 chars) exceeds Kubernetes' 63-character label-value
// limit.
const specHashAnnotation = "io.datascape.spec-hash"

// accessModeAnnotation records the ContainerSpec.AccessMode a Deployment was
// last created/updated with, so EnsureReachable (which only receives a bare
// name, the Docker port's contract) can recover which reachability strategy
// to use without threading the spec through separately.
const accessModeAnnotation = "io.datascape.access-mode"

type Runtime struct {
	clientset kubernetes.Interface
	// restConfig is kept alongside clientset because the pods/exec
	// subresource (ReadFile's live-path fallback, below) needs to build its
	// own SPDY executor directly against the REST transport/auth — there is
	// no exec method on kubernetes.Interface itself.
	restConfig *rest.Config
}

// New connects using the standard kubeconfig loading rules (KUBECONFIG env,
// then ~/.kube/config), or in-cluster config when running inside a pod.
// config["kubeconfig"] overrides the kubeconfig path; config["context"]
// selects a non-current context.
func New(config map[string]any) (*Runtime, error) {
	restCfg, err := loadConfig(config)
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	return &Runtime{clientset: clientset, restConfig: restCfg}, nil
}

func loadConfig(config map[string]any) (*rest.Config, error) {
	kubeconfigPath, _ := config["kubeconfig"].(string)
	contextName, _ := config["context"].(string)

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules.ExplicitPath = kubeconfigPath
	}
	overrides := &clientcmd.ConfigOverrides{}
	if contextName != "" {
		overrides.CurrentContext = contextName
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func withOwnership(labels map[string]string) map[string]string {
	out := map[string]string{runtimeport.LabelManagedBy: runtimeport.ManagedByValue}
	for k, v := range sanitizeLabels(labels) {
		out[k] = v
	}
	return out
}

// sanitizeLabels defends against label values that don't match Kubernetes'
// syntax (alphanumeric, '-', '_', '.', <=63 chars, must start/end
// alphanumeric) — in practice every value platformctl produces already
// complies (docs/planning/07 §0.1's DNS-label name policy is a subset of
// this), but a runtime adapter should not panic against the Kubernetes API
// server if some future label value doesn't.
func sanitizeLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = sanitizeLabelValue(v)
	}
	return out
}

func sanitizeLabelValue(v string) string {
	if v == "" {
		return v
	}
	var b strings.Builder
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-_.")
	if len(out) > 63 {
		out = out[:63]
		out = strings.TrimRight(out, "-_.")
	}
	return out
}

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

// targetNamespace picks the namespace a volume/container belongs to: the
// first named network, since every provider always names exactly one.
func targetNamespace(networks []string) (string, error) {
	if len(networks) == 0 {
		return "", fmt.Errorf("no network specified; the kubernetes runtime requires exactly one (PersistentVolumeClaims and Deployments are namespace-scoped)")
	}
	return networks[0], nil
}

// defaultVolumeSizeBytes preserves this adapter's original hardcoded
// request (10Gi) as the default when VolumeSpec.SizeBytes is unset.
const defaultVolumeSizeBytes int64 = 10 * 1024 * 1024 * 1024

func (r *Runtime) EnsureVolume(ctx context.Context, spec runtimeport.VolumeSpec) error {
	ns, err := targetNamespace(spec.Networks)
	if err != nil {
		return err
	}
	desiredSize := spec.SizeBytes
	if desiredSize <= 0 {
		desiredSize = defaultVolumeSizeBytes
	}
	desiredQty := *resource.NewQuantity(desiredSize, resource.BinarySI)

	pvc, err := r.clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, spec.Name, metav1.GetOptions{})
	if err == nil {
		if pvc.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
			return fmt.Errorf("volume %q exists but is not managed by platformctl; refusing to reuse it", spec.Name)
		}
		currentQty := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		switch desiredQty.Cmp(currentQty) {
		case 0:
			return nil // already matches — no-op
		case -1:
			return fmt.Errorf("volume %q requests %s, smaller than its current %s — Kubernetes does not support shrinking a bound PersistentVolumeClaim; use a new volume name to start over",
				spec.Name, desiredQty.String(), currentQty.String())
		}
		// Increase: a live PVC expansion patch. Succeeds only when the
		// StorageClass has allowVolumeExpansion: true — otherwise the API
		// server rejects it, surfaced to the caller as-is.
		pvc.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: desiredQty}
		if _, err := r.clientset.CoreV1().PersistentVolumeClaims(ns).Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("expand persistentvolumeclaim %q to %s: %w", spec.Name, desiredQty.String(), err)
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get persistentvolumeclaim %q: %w", spec.Name, err)
	}
	desired := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Namespace: ns, Labels: withOwnership(spec.Labels)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: desiredQty},
			},
		},
	}
	// StorageClassName is immutable once a PVC is created — this only ever
	// applies to a fresh volume; changing it on an existing VolumeSpec has
	// no effect, matching Kubernetes' own behavior rather than erroring on
	// something the API itself can't act on.
	if spec.StorageClass != "" {
		desired.Spec.StorageClassName = &spec.StorageClass
	}
	_, err = r.clientset.CoreV1().PersistentVolumeClaims(ns).Create(ctx, desired, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create persistentvolumeclaim %q: %w", spec.Name, err)
	}
	return nil
}

func (r *Runtime) RemoveVolume(ctx context.Context, name string) error {
	ns, pvc, err := findAcrossNamespaces(ctx, r, func(ns string) (*corev1.PersistentVolumeClaim, error) {
		return r.clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return err
	}
	if pvc == nil {
		return nil
	}
	if pvc.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
		return fmt.Errorf("volume %q is not managed by platformctl; refusing to remove it", name)
	}
	if err := r.clientset.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete persistentvolumeclaim %q: %w", name, err)
	}
	return nil
}

// findAcrossNamespaces looks up a namespace-scoped object by name across
// every namespace this adapter manages, since Remove/RemoveVolume/Inspect
// only receive a bare name (the Docker port's contract — volumes and
// containers are addressed globally by name, matching Docker's own
// cluster-global volume/container namespacing).
func findAcrossNamespaces[T any](ctx context.Context, r *Runtime, get func(ns string) (T, error)) (string, T, error) {
	var zero T
	namespaces, err := r.managedNamespaces(ctx)
	if err != nil {
		return "", zero, err
	}
	for _, ns := range namespaces {
		obj, err := get(ns)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", zero, err
		}
		return ns, obj, nil
	}
	return "", zero, nil
}

func (r *Runtime) managedNamespaces(ctx context.Context) ([]string, error) {
	list, err := r.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{
		LabelSelector: runtimeport.LabelManagedBy + "=" + runtimeport.ManagedByValue,
	})
	if err != nil {
		return nil, fmt.Errorf("list managed namespaces: %w", err)
	}
	out := make([]string, 0, len(list.Items))
	for _, ns := range list.Items {
		out = append(out, ns.Name)
	}
	sort.Strings(out)
	return out, nil
}

// EnsureContainer dispatches to the StatefulSet path (Replicas > 1 and
// StableIdentity — C2/C4's shape) or the Deployment path (every other case,
// including a plain Replicas > 1 with StableIdentity: false — D10's shape),
// per docs/design/004-replicas-and-identity.md. Replicas <= 1 reproduces
// this adapter's original single-replica Deployment behavior byte-for-byte.
func (r *Runtime) EnsureContainer(ctx context.Context, spec runtimeport.ContainerSpec) (runtimeport.ContainerState, error) {
	n := spec.ReplicaCount()
	if n > 1 && spec.StableIdentity {
		return r.ensureStatefulSet(ctx, spec, int32(n))
	}
	return r.ensureDeployment(ctx, spec, int32(n))
}

func (r *Runtime) ensureDeployment(ctx context.Context, spec runtimeport.ContainerSpec, replicas int32) (runtimeport.ContainerState, error) {
	ns, err := targetNamespace(spec.Networks)
	if err != nil {
		return runtimeport.ContainerState{}, err
	}
	desiredHash := specHash(spec)

	existing, err := r.clientset.AppsV1().Deployments(ns).Get(ctx, spec.Name, metav1.GetOptions{})
	if err == nil {
		if existing.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
			return runtimeport.ContainerState{}, fmt.Errorf("deployment %q exists but is not managed by platformctl; refusing to replace it", spec.Name)
		}
		if existing.Annotations[specHashAnnotation] == desiredHash {
			return r.enrichedState(ctx, ns, existing), nil // matches — no-op
		}
	} else if !apierrors.IsNotFound(err) {
		return runtimeport.ContainerState{}, fmt.Errorf("get deployment %q: %w", spec.Name, err)
	}

	deploymentNotFound := apierrors.IsNotFound(err)

	deployment, err := buildDeployment(ns, spec, desiredHash, replicas)
	if err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureService(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensurePodDisruptionBudget(ctx, ns, spec, replicas); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureExternalIngressPolicy(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureFilesSecret(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureImagePullSecret(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}

	if deploymentNotFound {
		created, err := r.clientset.AppsV1().Deployments(ns).Create(ctx, deployment, metav1.CreateOptions{})
		if err != nil {
			return runtimeport.ContainerState{}, fmt.Errorf("create deployment %q: %w", spec.Name, err)
		}
		return r.enrichedState(ctx, ns, created), nil
	}
	// The Deployment controller continuously bumps .status (replicas,
	// conditions, observedGeneration) independently of our .spec change, so
	// the ResourceVersion captured by the Get above is often already stale
	// by the time Update runs. Retry on conflict, re-fetching the latest
	// ResourceVersion each attempt, rather than failing the whole apply.
	var updated *appsv1.Deployment
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, getErr := r.clientset.AppsV1().Deployments(ns).Get(ctx, spec.Name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		deployment.ResourceVersion = latest.ResourceVersion
		var updateErr error
		updated, updateErr = r.clientset.AppsV1().Deployments(ns).Update(ctx, deployment, metav1.UpdateOptions{})
		return updateErr
	})
	if err != nil {
		return runtimeport.ContainerState{}, fmt.Errorf("update deployment %q: %w", spec.Name, err)
	}
	return r.enrichedState(ctx, ns, updated), nil
}

// ensureStatefulSet is ensureDeployment's StableIdentity counterpart
// (docs/design/004-replicas-and-identity.md): a headless Service instead of
// a ClusterIP one, and a StatefulSet instead of a Deployment so
// VolumeClaimTemplates and native ordinal pod naming apply. Selector and
// VolumeClaimTemplates are immutable once a StatefulSet is created —
// updates only ever touch the latest object's Labels/Annotations/Replicas/
// Template, never resending a freshly-built value for those two fields
// (Kubernetes' API-server defaulting can make an independently-rebuilt
// VolumeClaimTemplates value differ byte-for-byte from what's stored even
// when semantically unchanged, which the immutability check rejects).
func (r *Runtime) ensureStatefulSet(ctx context.Context, spec runtimeport.ContainerSpec, replicas int32) (runtimeport.ContainerState, error) {
	ns, err := targetNamespace(spec.Networks)
	if err != nil {
		return runtimeport.ContainerState{}, err
	}
	desiredHash := specHash(spec)

	existing, err := r.clientset.AppsV1().StatefulSets(ns).Get(ctx, spec.Name, metav1.GetOptions{})
	if err == nil {
		if existing.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
			return runtimeport.ContainerState{}, fmt.Errorf("statefulset %q exists but is not managed by platformctl; refusing to replace it", spec.Name)
		}
		if existing.Annotations[specHashAnnotation] == desiredHash {
			return stateFromStatefulSet(existing), nil // matches — no-op
		}
	} else if !apierrors.IsNotFound(err) {
		return runtimeport.ContainerState{}, fmt.Errorf("get statefulset %q: %w", spec.Name, err)
	}

	statefulSetNotFound := apierrors.IsNotFound(err)

	sts, err := buildStatefulSet(ns, spec, desiredHash, replicas)
	if err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureHeadlessService(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureAliasServices(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensurePodDisruptionBudget(ctx, ns, spec, replicas); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureExternalIngressPolicy(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureFilesSecret(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureImagePullSecret(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}

	if statefulSetNotFound {
		created, err := r.clientset.AppsV1().StatefulSets(ns).Create(ctx, sts, metav1.CreateOptions{})
		if err != nil {
			return runtimeport.ContainerState{}, fmt.Errorf("create statefulset %q: %w", spec.Name, err)
		}
		return stateFromStatefulSet(created), nil
	}
	var updated *appsv1.StatefulSet
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, getErr := r.clientset.AppsV1().StatefulSets(ns).Get(ctx, spec.Name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		latest.Labels = sts.Labels
		latest.Annotations = sts.Annotations
		latest.Spec.Replicas = sts.Spec.Replicas
		latest.Spec.Template = sts.Spec.Template
		// Selector, ServiceName and VolumeClaimTemplates are immutable on an
		// existing StatefulSet — left as fetched in latest, never resent.
		var updateErr error
		updated, updateErr = r.clientset.AppsV1().StatefulSets(ns).Update(ctx, latest, metav1.UpdateOptions{})
		return updateErr
	})
	if err != nil {
		return runtimeport.ContainerState{}, fmt.Errorf("update statefulset %q: %w", spec.Name, err)
	}
	return stateFromStatefulSet(updated), nil
}

// ensurePodDisruptionBudget reconciles the maxUnavailable:1 PDB applied
// whenever replicas > 1, deleting it when replicas drops back to <= 1 (a
// scale-down complement, mirroring ensureFilesSecret's create/update/
// delete-on-absence shape).
func (r *Runtime) ensurePodDisruptionBudget(ctx context.Context, ns string, spec runtimeport.ContainerSpec, replicas int32) error {
	name := pdbName(spec.Name)
	if replicas <= 1 {
		if err := r.clientset.PolicyV1().PodDisruptionBudgets(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete poddisruptionbudget %q: %w", name, err)
		}
		return nil
	}
	desired := buildPodDisruptionBudget(ns, spec)
	existing, err := r.clientset.PolicyV1().PodDisruptionBudgets(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := r.clientset.PolicyV1().PodDisruptionBudgets(ns).Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create poddisruptionbudget %q: %w", name, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get poddisruptionbudget %q: %w", name, err)
	default:
		desired.ResourceVersion = existing.ResourceVersion
		if _, err := r.clientset.PolicyV1().PodDisruptionBudgets(ns).Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update poddisruptionbudget %q: %w", name, err)
		}
		return nil
	}
}

// ensureHeadlessService reconciles the governing Service a StatefulSet's
// ServiceName requires. Refuses to convert an existing non-headless Service
// of the same name (a container switching StableIdentity: false -> true)
// rather than silently changing its addressing semantics out from under
// whatever already depends on the old ClusterIP address.
func (r *Runtime) ensureHeadlessService(ctx context.Context, ns string, spec runtimeport.ContainerSpec) error {
	desired := buildHeadlessService(ns, spec)
	existing, err := r.clientset.CoreV1().Services(ns).Get(ctx, spec.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := r.clientset.CoreV1().Services(ns).Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create headless service %q: %w", spec.Name, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get service %q: %w", spec.Name, err)
	case existing.Spec.ClusterIP != corev1.ClusterIPNone:
		return fmt.Errorf("service %q exists but is not headless (ClusterIP %q); refusing to convert it — remove it first if switching this container to StableIdentity", spec.Name, existing.Spec.ClusterIP)
	default:
		desired.ResourceVersion = existing.ResourceVersion
		desired.Spec.ClusterIP = existing.Spec.ClusterIP // immutable once set
		if _, err := r.clientset.CoreV1().Services(ns).Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update headless service %q: %w", spec.Name, err)
		}
		return nil
	}
}

// ensureAliasServices ensures a normal ClusterIP Service per declared alias
// (never for spec.Name itself, which is the headless governing Service for
// a StableIdentity set) — aliases remain "any of them" addresses even for a
// stable-identity set, matching the Deployment path's alias handling.
func (r *Runtime) ensureAliasServices(ctx context.Context, ns string, spec runtimeport.ContainerSpec) error {
	for _, alias := range spec.Aliases {
		if err := r.ensureOneService(ctx, ns, alias, spec); err != nil {
			return err
		}
	}
	return nil
}

// ensureService creates or updates the ClusterIP Service backing a
// container's ports. A container with no ports declared gets no Service —
// nothing else in the namespace needs to address it by name.
// ensureFilesSecret creates or updates the Secret backing spec.Files, and
// deletes it when the spec no longer declares files.
func (r *Runtime) ensureFilesSecret(ctx context.Context, ns string, spec runtimeport.ContainerSpec) error {
	name := filesSecretName(spec.Name)
	if len(spec.Files) == 0 {
		if err := r.clientset.CoreV1().Secrets(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete files secret %q: %w", name, err)
		}
		return nil
	}
	desired := buildFilesSecret(ns, spec)
	existing, err := r.clientset.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := r.clientset.CoreV1().Secrets(ns).Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create files secret %q: %w", name, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get files secret %q: %w", name, err)
	default:
		desired.ResourceVersion = existing.ResourceVersion
		if _, err := r.clientset.CoreV1().Secrets(ns).Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update files secret %q: %w", name, err)
		}
		return nil
	}
}

// ensureImagePullSecret creates or updates the dockerconfigjson Secret
// backing spec.ImagePullAuth, and deletes it when the spec no longer
// declares one — the same create/update/delete-on-absence shape as
// ensureFilesSecret above, for the same reason (docs/planning/07 §1.1
// deferral, docs/planning/08 A1).
func (r *Runtime) ensureImagePullSecret(ctx context.Context, ns string, spec runtimeport.ContainerSpec) error {
	name := pullSecretName(spec.Name)
	if spec.ImagePullAuth == nil {
		if err := r.clientset.CoreV1().Secrets(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete image pull secret %q: %w", name, err)
		}
		return nil
	}
	desired, err := buildImagePullSecret(ns, spec)
	if err != nil {
		return fmt.Errorf("build image pull secret %q: %w", name, err)
	}
	existing, err := r.clientset.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := r.clientset.CoreV1().Secrets(ns).Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create image pull secret %q: %w", name, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get image pull secret %q: %w", name, err)
	default:
		desired.ResourceVersion = existing.ResourceVersion
		if _, err := r.clientset.CoreV1().Secrets(ns).Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update image pull secret %q: %w", name, err)
		}
		return nil
	}
}

// ReadFile maps the path back to its Secret key via the annotation
// buildFilesSecret wrote, then returns that key's data.
// ReadFile returns a FileMount's content when the path is one this adapter
// placed itself (the fast, no-exec path every provider's bootstrap-secret
// recovery actually uses); for any other path it falls back to a live
// `cat` inside the pod (docs/planning/08 B3) — content the container's own
// process wrote at runtime, e.g. into a mounted PersistentVolumeClaim. This
// mirrors the Docker adapter's ReadFile, which reads any live path via
// CopyFromContainer without a FileMount-vs-not distinction.
func (r *Runtime) ReadFile(ctx context.Context, name, path string) ([]byte, error) {
	_, secret, err := findAcrossNamespaces(ctx, r, func(ns string) (*corev1.Secret, error) {
		return r.clientset.CoreV1().Secrets(ns).Get(ctx, filesSecretName(name), metav1.GetOptions{})
	})
	if err != nil {
		return nil, err
	}
	if secret != nil {
		var paths map[string]string
		if err := json.Unmarshal([]byte(secret.Annotations[filePathsAnnotation]), &paths); err == nil {
			if key, ok := paths[path]; ok {
				return secret.Data[key], nil
			}
		}
	}
	return r.readFileViaExec(ctx, name, path)
}

// readFileViaExec execs `cat <path>` in the deployment's current running
// pod and returns stdout — the live-filesystem fallback ReadFile uses for
// paths that aren't a FileMount, e.g. content a container's own process
// wrote into a mounted volume. When name is not a Deployment, it may be the
// literal name of a StatefulSet ordinal's own Pod (docs/design/004-replicas-
// and-identity.md) — the aggregate base name of a StableIdentity set is
// deliberately not supported here; callers must address a specific ordinal.
func (r *Runtime) readFileViaExec(ctx context.Context, name, path string) ([]byte, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return nil, err
	}
	if d != nil {
		// buildDeployment always names the (single) container after the
		// Deployment itself — see its ObjectMeta.Name/container.Name — so
		// name doubles as both the pod-selector value and the container to
		// exec into.
		return r.readFileFromSelector(ctx, ns, name, name, path)
	}
	podNS, pod, stsName, err := r.findOrdinalPod(ctx, name)
	if err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, fmt.Errorf("no deployment or replica pod named %q found to read %q from", name, path)
	}
	stdout, stderr, err := r.execInPod(ctx, podNS, pod.Name, stsName, []string{"cat", path})
	if err != nil {
		return nil, fmt.Errorf("read %q from pod %q: %w (stderr: %s)", path, pod.Name, err, strings.TrimSpace(stderr))
	}
	return []byte(stdout), nil
}

// readFileFromSelector execs `cat <path>` in the newest ready pod matching
// app=selectorName — the Deployment-path lookup shared by ReadFile.
func (r *Runtime) readFileFromSelector(ctx context.Context, ns, selectorName, containerName, path string) ([]byte, error) {
	pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=" + selectorName})
	if err != nil {
		return nil, fmt.Errorf("list pods for %q: %w", selectorName, err)
	}
	pod := newestReadyPod(pods.Items)
	if pod == nil {
		return nil, fmt.Errorf("no ready pod for %q to read %q from", selectorName, path)
	}
	stdout, stderr, err := r.execInPod(ctx, ns, pod.Name, containerName, []string{"cat", path})
	if err != nil {
		return nil, fmt.Errorf("read %q from pod %q: %w (stderr: %s)", path, pod.Name, err, strings.TrimSpace(stderr))
	}
	return []byte(stdout), nil
}

// newestReadyPod picks the most recently created Running pod with every
// container ready, or nil if none qualify. A rolling Deployment update can
// transiently leave an old (terminating) pod matching the same selector
// alongside the new one — a bare "first match" is not reliably the current
// generation's pod, which is what broke the first version of exec-based
// ReadFile against a real rollout (found live against minikube, not just in
// a synthetic test).
func newestReadyPod(pods []corev1.Pod) *corev1.Pod {
	var best *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.Status.Phase != corev1.PodRunning || p.DeletionTimestamp != nil {
			continue
		}
		ready := len(p.Status.ContainerStatuses) > 0
		for _, cs := range p.Status.ContainerStatuses {
			if !cs.Ready {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		if best == nil || p.CreationTimestamp.After(best.CreationTimestamp.Time) {
			best = p
		}
	}
	return best
}

// execInPod runs command in the named container of a pod via the
// pods/exec subresource, returning captured stdout/stderr.
func (r *Runtime) execInPod(ctx context.Context, ns, podName, containerName string, command []string) (stdout, stderr string, err error) {
	req := r.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(ns).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(r.restConfig, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("build exec request: %w", err)
	}
	var outBuf, errBuf bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &outBuf, Stderr: &errBuf}); err != nil {
		return outBuf.String(), errBuf.String(), err
	}
	return outBuf.String(), errBuf.String(), nil
}

// ensureService ensures one ClusterIP Service per addressable name: the
// container's own name plus each declared alias (Docker's per-network
// endpoint aliases translated to Kubernetes DNS).
func (r *Runtime) ensureService(ctx context.Context, ns string, spec runtimeport.ContainerSpec) error {
	names := append([]string{spec.Name}, spec.Aliases...)
	for _, svcName := range names {
		if err := r.ensureOneService(ctx, ns, svcName, spec); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) ensureOneService(ctx context.Context, ns, svcName string, spec runtimeport.ContainerSpec) error {
	desired := buildService(ns, svcName, spec)
	existing, err := r.clientset.CoreV1().Services(ns).Get(ctx, svcName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if len(desired.Spec.Ports) == 0 {
			return nil
		}
		if _, err := r.clientset.CoreV1().Services(ns).Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create service %q: %w", svcName, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get service %q: %w", svcName, err)
	case len(desired.Spec.Ports) == 0:
		if err := r.clientset.CoreV1().Services(ns).Delete(ctx, svcName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete service %q: %w", svcName, err)
		}
		return nil
	default:
		desired.ResourceVersion = existing.ResourceVersion
		desired.Spec.ClusterIP = existing.Spec.ClusterIP
		if _, err := r.clientset.CoreV1().Services(ns).Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update service %q: %w", svcName, err)
		}
		return nil
	}
}

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

// observedHostAddr resolves the real, currently-observed host-reachable
// address for a Service port, per the Service's type — nothing for
// ClusterIP (the port-forward/in-cluster access modes have no standing host
// binding at all; EnsureReachable opens one on demand instead).
func (r *Runtime) observedHostAddr(ctx context.Context, svc *corev1.Service, containerPort int32) (string, int, bool) {
	switch svc.Spec.Type {
	case corev1.ServiceTypeNodePort:
		for _, p := range svc.Spec.Ports {
			if p.Port != containerPort {
				continue
			}
			if p.NodePort == 0 {
				return "", 0, false
			}
			nodeIP, err := r.firstNodeAddr(ctx)
			if err != nil || nodeIP == "" {
				return "", 0, false
			}
			return nodeIP, int(p.NodePort), true
		}
	case corev1.ServiceTypeLoadBalancer:
		if len(svc.Status.LoadBalancer.Ingress) == 0 {
			return "", 0, false
		}
		ing := svc.Status.LoadBalancer.Ingress[0]
		addr := ing.IP
		if addr == "" {
			addr = ing.Hostname
		}
		if addr == "" {
			return "", 0, false
		}
		for _, p := range svc.Spec.Ports {
			if p.Port == containerPort {
				return addr, int(p.Port), true
			}
		}
	}
	return "", 0, false
}

// firstNodeAddr picks an address for reaching a NodePort Service: an
// ExternalIP when the cluster has one (real/cloud clusters), falling back to
// InternalIP (local clusters like minikube/kind, where platformctl itself
// typically runs on the same host/network as the node).
func (r *Runtime) firstNodeAddr(ctx context.Context) (string, error) {
	nodes, err := r.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}
	var internal string
	for _, node := range nodes.Items {
		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeExternalIP && addr.Address != "" {
				return addr.Address, nil
			}
			if addr.Type == corev1.NodeInternalIP && internal == "" {
				internal = addr.Address
			}
		}
	}
	return internal, nil
}

// EnsureReachable makes a container's port reachable from this process
// (running outside the cluster), per the AccessMode its Deployment was last
// created/updated with (docs/planning/08 B1):
//
//   - in-cluster: refuses — there is no host-reachable address by design.
//   - node-port/load-balancer: resolves the Service's observed address;
//     close is a no-op (the Service itself is the standing tunnel).
//   - port-forward (default): opens an ephemeral client-go port-forward
//     tunnel to the container's current pod; close tears it down.
//
// When name is not a Deployment, it may be the literal name of a
// StatefulSet ordinal's own Pod (docs/design/004-replicas-and-identity.md):
// only the port-forward access mode is supported for one specific ordinal in
// this iteration — node-port/load-balancer addressing of a single replica
// is a documented "Known limitations" follow-up, not implemented here.
func (r *Runtime) EnsureReachable(ctx context.Context, name string, containerPort int) (string, func() error, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return "", nil, err
	}
	if d != nil {
		accessMode := d.Annotations[accessModeAnnotation]
		switch accessMode {
		case runtimeport.AccessInCluster:
			return "", nil, fmt.Errorf("container %q uses access mode %q; no CLI-side (outside-the-cluster) admin connection is possible — run admin operations from a pod inside the cluster instead", name, runtimeport.AccessInCluster)
		case runtimeport.AccessNodePort, runtimeport.AccessLoadBalancer:
			return r.serviceReachableAddr(ctx, ns, name, containerPort, accessMode)
		default:
			return r.portForwardReachableAddrBySelector(ctx, ns, name, containerPort)
		}
	}
	podNS, pod, _, err := r.findOrdinalPod(ctx, name)
	if err != nil {
		return "", nil, err
	}
	if pod == nil {
		return "", nil, fmt.Errorf("deployment %q not found", name)
	}
	return r.portForwardToPod(ctx, podNS, pod, containerPort)
}

// serviceReachableAddr resolves the node-port/load-balancer address and
// polls until it is actually dialable, not merely assigned. Two distinct
// delays separate "the Service object has an address" from "traffic sent to
// it succeeds": a freshly (re)created LoadBalancer's ingress address can
// take a while to provision, and — found live against minikube, not a
// synthetic test — a NodePort number is allocated by the API server
// synchronously at Service-creation time, before kube-proxy has programmed
// the node's iptables/ipvs rule that actually accepts traffic on it, so a
// dial immediately after the number appears can still see connection
// refused for a brief window. EnsureReachable's contract is "a host:port
// this process can dial right now" — resolving the address without proving
// it's live would silently violate that for callers who dial only once.
func (r *Runtime) serviceReachableAddr(ctx context.Context, ns, name string, containerPort int, accessMode string) (string, func() error, error) {
	const pollTimeout = 60 * time.Second
	deadline := time.Now().Add(pollTimeout)
	for {
		svc, err := r.clientset.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", nil, fmt.Errorf("get service %q: %w", name, err)
		}
		if ip, port, ok := r.observedHostAddr(ctx, svc, int32(containerPort)); ok {
			addr := net.JoinHostPort(ip, strconv.Itoa(port))
			if dialable(addr) {
				return addr, func() error { return nil }, nil
			}
		}
		if time.Now().After(deadline) {
			return "", nil, fmt.Errorf("service %q (access mode %q) did not become dialable for port %d within %s", name, accessMode, containerPort, pollTimeout)
		}
		select {
		case <-ctx.Done():
			return "", nil, ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

// dialable reports whether a TCP connection to addr succeeds right now.
func dialable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// portForwardReachableAddrBySelector resolves the newest ready pod matching
// app=selectorName, then opens a tunnel to it — the Deployment-path lookup
// EnsureReachable's default (port-forward) access mode uses.
func (r *Runtime) portForwardReachableAddrBySelector(ctx context.Context, ns, selectorName string, containerPort int) (string, func() error, error) {
	pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=" + selectorName})
	if err != nil {
		return "", nil, fmt.Errorf("list pods for %q: %w", selectorName, err)
	}
	pod := newestReadyPod(pods.Items)
	if pod == nil {
		return "", nil, fmt.Errorf("no ready pod for %q to port-forward to", selectorName)
	}
	return r.portForwardToPod(ctx, ns, pod, containerPort)
}

// portForwardToPod opens an ephemeral client-go port-forward tunnel to pod
// on an OS-assigned local port, mirroring `kubectl port-forward :containerPort`.
func (r *Runtime) portForwardToPod(ctx context.Context, ns string, pod *corev1.Pod, containerPort int) (string, func() error, error) {
	transport, upgrader, err := spdy.RoundTripperFor(r.restConfig)
	if err != nil {
		return "", nil, fmt.Errorf("build port-forward transport: %w", err)
	}
	req := r.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(ns).
		Name(pod.Name).
		SubResource("portforward")
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", req.URL())

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	var outBuf, errBuf bytes.Buffer
	fw, err := portforward.New(dialer, []string{fmt.Sprintf("0:%d", containerPort)}, stopCh, readyCh, &outBuf, &errBuf)
	if err != nil {
		return "", nil, fmt.Errorf("create port-forwarder to pod %q: %w", pod.Name, err)
	}
	fwErrCh := make(chan error, 1)
	go func() { fwErrCh <- fw.ForwardPorts() }()

	select {
	case <-readyCh:
	case err := <-fwErrCh:
		return "", nil, fmt.Errorf("port-forward to pod %q failed: %w (stderr: %s)", pod.Name, err, strings.TrimSpace(errBuf.String()))
	case <-ctx.Done():
		close(stopCh)
		return "", nil, ctx.Err()
	case <-time.After(15 * time.Second):
		close(stopCh)
		return "", nil, fmt.Errorf("port-forward to pod %q: timed out waiting to become ready", pod.Name)
	}
	ports, err := fw.GetPorts()
	if err != nil || len(ports) == 0 {
		close(stopCh)
		return "", nil, fmt.Errorf("port-forward to pod %q: no local port allocated: %w", pod.Name, err)
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(ports[0].Local)))
	var closeOnce sync.Once
	closeFn := func() error {
		closeOnce.Do(func() { close(stopCh) })
		return nil
	}
	// readyCh only proves the tunnel itself is up (the SPDY stream to the
	// kubelet is established) — not that the container's own process is
	// listening on containerPort yet (the K11 class: a tunnel opened before
	// listen() can look ready forever while carrying no traffic). Per the
	// port contract (docs/planning/08 F3), EnsureReachable must not return
	// an address that isn't currently dialable, so prove it with one direct
	// dial through the tunnel before handing the address back; a caller
	// using runtime.WithReachable (F1) will retry with a fresh tunnel on
	// this error rather than being handed a dead one to discover later.
	if !dialable(addr) {
		closeFn()
		return "", nil, fmt.Errorf("port-forward to pod %q: tunnel is up but port %d is not currently accepting connections", pod.Name, containerPort)
	}
	return addr, closeFn, nil
}

// replicaSetReadiness resolves name against the Deployment path, then the
// StatefulSet path (docs/design/004-replicas-and-identity.md), reporting
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
// to that specific replica's own state (docs/design/004's "ordinal hostname
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
// (docs/design/004-replicas-and-identity.md) — whichever object exists for
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
// managed StatefulSet (docs/design/004-replicas-and-identity.md) — the
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

// ListManagedVolumes reports every managed PersistentVolumeClaim across
// every managed namespace (the Docker volume analog).
func (r *Runtime) ListManagedVolumes(ctx context.Context) ([]runtimeport.ManagedVolume, error) {
	namespaces, err := r.managedNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	var out []runtimeport.ManagedVolume
	for _, ns := range namespaces {
		list, err := r.clientset.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{
			LabelSelector: runtimeport.LabelManagedBy + "=" + runtimeport.ManagedByValue,
		})
		if err != nil {
			return nil, fmt.Errorf("list persistentvolumeclaims in namespace %q: %w", ns, err)
		}
		for _, pvc := range list.Items {
			out = append(out, runtimeport.ManagedVolume{Name: pvc.Name, Labels: pvc.Labels})
		}
	}
	return out, nil
}

// RunsContainerCommands marks this adapter as one whose containers actually
// execute their declared Cmd — see the Docker adapter's identical method
// for why the conformance suite checks for it.
func (r *Runtime) RunsContainerCommands() bool { return true }

// Logs returns the target's log tail. When name is a Deployment, this is the
// newest ready pod matching it; when name is instead the literal name of a
// StatefulSet ordinal's own Pod (docs/design/004-replicas-and-identity.md),
// its logs directly — the aggregate base name of a StableIdentity set is
// deliberately not supported here, matching ReadFile/EnsureReachable.
func (r *Runtime) Logs(ctx context.Context, name string, tail int) (string, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return "", err
	}
	if d != nil {
		return r.podLogsFromSelector(ctx, ns, name, tail)
	}
	podNS, pod, _, err := r.findOrdinalPod(ctx, name)
	if err != nil {
		return "", err
	}
	if pod == nil {
		return "", fmt.Errorf("no deployment or replica pod named %q found", name)
	}
	return r.singlePodLogs(ctx, podNS, pod.Name, tail)
}

// tailLogs mirrors the Docker adapter's failure-message helper: best-effort,
// swallows errors, formatted for inclusion in a "did not become healthy"
// error.
func (r *Runtime) tailLogs(ctx context.Context, ns, name string) string {
	out, err := r.podLogsFromSelector(ctx, ns, name, 10)
	if err != nil || out == "" {
		return ""
	}
	if len(out) > 2000 {
		out = out[len(out)-2000:]
	}
	return "; last log lines:\n" + out
}

// podLogsFromSelector returns the newest matching pod's logs — the
// Deployment-path lookup shared by Logs and tailLogs.
func (r *Runtime) podLogsFromSelector(ctx context.Context, ns, selectorName string, tail int) (string, error) {
	pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + selectorName,
	})
	if err != nil {
		return "", fmt.Errorf("list pods for %q: %w", selectorName, err)
	}
	if len(pods.Items) == 0 {
		return "", nil
	}
	return r.singlePodLogs(ctx, ns, pods.Items[0].Name, tail)
}

// singlePodLogs fetches one exact pod's log tail.
func (r *Runtime) singlePodLogs(ctx context.Context, ns, podName string, tail int) (string, error) {
	if tail <= 0 {
		tail = 200
	}
	tailInt64 := int64(tail)
	rc, err := r.clientset.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{TailLines: &tailInt64}).Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("read logs for pod %q: %w", podName, err)
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return "", fmt.Errorf("read logs for pod %q: %w", podName, err)
	}
	return strings.TrimSpace(buf.String()), nil
}

func specHash(spec runtimeport.ContainerSpec) string {
	data, _ := json.Marshal(spec)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
