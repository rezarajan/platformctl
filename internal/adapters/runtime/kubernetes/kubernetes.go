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
	"os"
	"sort"
	"strings"
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
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/retry"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// specHashAnnotation carries a fingerprint of the last-applied ContainerSpec
// so EnsureContainer can detect "already matches" — the same role
// Docker's specGenLabel label plays, but stored as an annotation because a
// sha256 hex digest (64 chars) exceeds Kubernetes' 63-character label-value
// limit.
const specHashAnnotation = "io.datascape.spec-hash"

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

func (r *Runtime) EnsureContainer(ctx context.Context, spec runtimeport.ContainerSpec) (runtimeport.ContainerState, error) {
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
			return stateFromDeployment(existing), nil // matches — no-op
		}
	} else if !apierrors.IsNotFound(err) {
		return runtimeport.ContainerState{}, fmt.Errorf("get deployment %q: %w", spec.Name, err)
	}

	deploymentNotFound := apierrors.IsNotFound(err)

	deployment, err := buildDeployment(ns, spec, desiredHash)
	if err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureService(ctx, ns, spec); err != nil {
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
		return stateFromDeployment(created), nil
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
	return stateFromDeployment(updated), nil
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
// wrote into a mounted volume.
func (r *Runtime) readFileViaExec(ctx context.Context, name, path string) ([]byte, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, fmt.Errorf("no deployment %q found to read %q from", name, path)
	}
	pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=" + name})
	if err != nil {
		return nil, fmt.Errorf("list pods for %q: %w", name, err)
	}
	pod := newestReadyPod(pods.Items)
	if pod == nil {
		return nil, fmt.Errorf("no ready pod for %q to read %q from", name, path)
	}
	// buildDeployment always names the (single) container after the
	// Deployment itself — see its ObjectMeta.Name/container.Name — so name
	// doubles as both the pod-selector value and the container to exec into.
	stdout, stderr, err := r.execInPod(ctx, ns, pod.Name, name, []string{"cat", path})
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

func (r *Runtime) WaitHealthy(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
			return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		})
		if err != nil {
			return fmt.Errorf("get deployment %q: %w", name, err)
		}
		if d == nil {
			return fmt.Errorf("deployment %q not found", name)
		}
		if d.Status.ReadyReplicas > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("deployment %q did not become healthy within %s%s", name, timeout, r.tailLogs(ctx, ns, name))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (r *Runtime) Inspect(ctx context.Context, name string) (runtimeport.ContainerState, bool, error) {
	_, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return runtimeport.ContainerState{}, false, err
	}
	if d == nil {
		return runtimeport.ContainerState{}, false, nil
	}
	return stateFromDeployment(d), true, nil
}

func (r *Runtime) Remove(ctx context.Context, name string) error {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return err
	}
	if d == nil {
		return nil
	}
	if d.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
		return fmt.Errorf("deployment %q is not managed by platformctl; refusing to remove it", name)
	}
	propagation := metav1.DeletePropagationForeground
	if err := r.clientset.AppsV1().Deployments(ns).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete deployment %q: %w", name, err)
	}
	// Delete every Service addressing this container — its own name plus
	// alias Services, all labeled app=<name> by ensureService.
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
	// Foreground propagation means the Deployment stays visible (with a
	// deletionTimestamp) until its ReplicaSet/Pods are actually gone.
	// Docker's ContainerRemove(Force: true) is synchronous — callers
	// (engine, conformance suite) expect Remove to mean "gone" when it
	// returns, so wait for that here rather than leaking the async gap.
	const removeTimeout = 45 * time.Second // > TerminationGracePeriodSeconds + API/GC overhead
	deadline := time.Now().Add(removeTimeout)
	for {
		_, err := r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("waiting for deployment %q removal: %w", name, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("deployment %q did not finish terminating within %s", name, removeTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

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
			out = append(out, stateFromDeployment(&list.Items[i]))
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

func (r *Runtime) Logs(ctx context.Context, name string, tail int) (string, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return "", err
	}
	if d == nil {
		return "", fmt.Errorf("deployment %q not found", name)
	}
	return r.podLogs(ctx, ns, name, tail)
}

// tailLogs mirrors the Docker adapter's failure-message helper: best-effort,
// swallows errors, formatted for inclusion in a "did not become healthy"
// error.
func (r *Runtime) tailLogs(ctx context.Context, ns, name string) string {
	out, err := r.podLogs(ctx, ns, name, 10)
	if err != nil || out == "" {
		return ""
	}
	if len(out) > 2000 {
		out = out[len(out)-2000:]
	}
	return "; last log lines:\n" + out
}

func (r *Runtime) podLogs(ctx context.Context, ns, name string, tail int) (string, error) {
	if tail <= 0 {
		tail = 200
	}
	pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + name,
	})
	if err != nil {
		return "", fmt.Errorf("list pods for %q: %w", name, err)
	}
	if len(pods.Items) == 0 {
		return "", nil
	}
	tailInt64 := int64(tail)
	rc, err := r.clientset.CoreV1().Pods(ns).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{TailLines: &tailInt64}).Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("read logs for pod %q: %w", pods.Items[0].Name, err)
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return "", fmt.Errorf("read logs for pod %q: %w", pods.Items[0].Name, err)
	}
	return strings.TrimSpace(buf.String()), nil
}

func specHash(spec runtimeport.ContainerSpec) string {
	data, _ := json.Marshal(spec)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
