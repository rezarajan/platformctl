// This file holds the container-creation seam: EnsureContainer and the
// Deployment/StatefulSet ensure paths, plus the Service/PodDisruptionBudget/
// Secret objects they reconcile alongside the workload (docs/planning/08
// §7.6 G3). Teardown and inspection (Remove/Inspect/ListManaged/WaitHealthy)
// live in container_remove.go — the create/update half grew too large for
// one file to stay under the size seam.
package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// EnsureContainer dispatches to the StatefulSet path (StableIdentity at any
// replica count — C2/C4's shape; a 1-replica StatefulSet is what lets a
// stateful cluster scale 1 -> N -> 1 in place, docs/adr/017 §a.2) or the
// Deployment path (every other case, including a plain Replicas > 1 with
// StableIdentity: false — D10's shape), per docs/adr/004-replicas-and-
// identity.md. Replicas <= 1 without StableIdentity reproduces this
// adapter's original single-replica Deployment behavior byte-for-byte.
func (r *Runtime) EnsureContainer(ctx context.Context, spec runtimeport.ContainerSpec) (runtimeport.ContainerState, error) {
	// Refuse loudly, never drop silently (doc 07 "genuine per-runtime
	// differences", loud-refusal clause implemented by the 2026-07 GA
	// caveat sweep, doc 11): Kubernetes gates non-default sysctls behind
	// cluster-admin node policy (securityContext.sysctls allowlists), so
	// this adapter cannot honor ContainerSpec.Sysctls the way Docker's
	// HostConfig.Sysctls does. Every setter today (wireguard/ADR 023) is
	// Docker-only by its own ADR; if that changes, the K8s translation is
	// a deliberate design task — not a silent no-op discovered in prod.
	if len(spec.Sysctls) > 0 {
		return runtimeport.ContainerState{}, fmt.Errorf("container %q: ContainerSpec.Sysctls is not supported on the kubernetes runtime (docs/planning/07 per-runtime differences; K8s requires cluster-level sysctl allowlisting) — this workload is Docker-only today", spec.Name)
	}
	n := spec.ReplicaCount()
	if spec.StableIdentity {
		return r.ensureStatefulSet(ctx, spec, int32(n))
	}
	return r.ensureDeployment(ctx, spec, int32(n))
}

func (r *Runtime) ensureDeployment(ctx context.Context, spec runtimeport.ContainerSpec, replicas int32) (runtimeport.ContainerState, error) {
	ns, err := targetNamespace(spec.Networks)
	if err != nil {
		return runtimeport.ContainerState{}, err
	}
	desiredHash, err := specHash(spec)
	if err != nil {
		return runtimeport.ContainerState{}, err
	}

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

	// Shape-transition guard (docs/adr/004): a StatefulSet by this name
	// means the container was last applied with StableIdentity and Replicas >
	// 1. Taking the Deployment path anyway would leave the old StatefulSet
	// serving the same app=<name> selector — refuse instead, mirroring
	// ensureHeadlessService's refusal in the opposite direction.
	if _, serr := r.clientset.AppsV1().StatefulSets(ns).Get(ctx, spec.Name, metav1.GetOptions{}); serr == nil {
		return runtimeport.ContainerState{}, fmt.Errorf("statefulset %q exists for this container; refusing to convert it to a Deployment in place — remove it first (destroy and recreate) if switching this container off StableIdentity or below 2 replicas", spec.Name)
	} else if !apierrors.IsNotFound(serr) {
		return runtimeport.ContainerState{}, fmt.Errorf("get statefulset %q: %w", spec.Name, serr)
	}

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
	if err := r.ensureGraphScopedIngressPolicy(ctx, ns, spec); err != nil {
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
// (docs/adr/004-replicas-and-identity.md): a headless Service instead of
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
	desiredHash, err := specHash(spec)
	if err != nil {
		return runtimeport.ContainerState{}, err
	}

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

	// Shape-transition guard, the inverse of ensureDeployment's: a Deployment
	// by this name means the container was last applied without StableIdentity
	// (or with a single replica). Creating a StatefulSet alongside it would
	// leave both serving the same app=<name> selector — refuse instead.
	// ensureHeadlessService refuses the ClusterIP Service half of this
	// transition, but a portless Deployment has no Service, so the guard must
	// live here too.
	if _, derr := r.clientset.AppsV1().Deployments(ns).Get(ctx, spec.Name, metav1.GetOptions{}); derr == nil {
		return runtimeport.ContainerState{}, fmt.Errorf("deployment %q exists for this container; refusing to convert it to a StatefulSet in place — remove it first (destroy and recreate) if switching this container to StableIdentity", spec.Name)
	} else if !apierrors.IsNotFound(derr) {
		return runtimeport.ContainerState{}, fmt.Errorf("get deployment %q: %w", spec.Name, derr)
	}

	sts, err := buildStatefulSet(ns, spec, desiredHash, replicas)
	if err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureHeadlessService(ctx, ns, spec); err != nil {
		return runtimeport.ContainerState{}, err
	}
	if err := r.ensureOrdinalServices(ctx, ns, spec, replicas); err != nil {
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
	if err := r.ensureGraphScopedIngressPolicy(ctx, ns, spec); err != nil {
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

// ensureOrdinalServices gives every ordinal of a StableIdentity set an
// in-namespace short-name DNS identity ("<name>-<i>") matching Docker's
// per-ordinal container-name resolution — the cross-runtime claim
// docs/adr/004 stated, made real here (caught live by C2's multi-broker
// cluster, docs/adr/017 §a.3): a StatefulSet pod's DNS record is only
// "<pod>.<svc>.<ns>.svc...", which the namespace's search domain does NOT
// cover, so the bare ordinal name a peer's seed list or an in-network
// consumer's bootstrap list dials would never resolve without one Service
// per ordinal, selecting the StatefulSet's own per-pod label.
// PublishNotReadyAddresses is essential, not decorative: readiness is
// typically cluster-scoped for the workloads this shape exists for (C2's
// `rpk cluster health`), so no pod is Ready until the cluster forms —
// endpoints gated on readiness would deadlock the very bootstrap dial
// (ordinal 1 → seed ordinal 0) formation depends on. Ordinal Services from
// a previous, larger generation are pruned by their replica-ordinal label;
// teardown needs no special casing — they carry app=<name> like every other
// Service (removeCommonContainerObjects' label query).
func (r *Runtime) ensureOrdinalServices(ctx context.Context, ns string, spec runtimeport.ContainerSpec, replicas int32) error {
	ports := servicePorts(spec)
	if len(ports) == 0 {
		return nil
	}
	for i := 0; i < int(replicas); i++ {
		ord := runtimeport.OrdinalName(spec.Name, i)
		labels := withOwnership(spec.Labels)
		labels["app"] = spec.Name
		labels[runtimeport.LabelReplicaOrdinal] = strconv.Itoa(i)
		desired := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: ord, Namespace: ns, Labels: labels},
			Spec: corev1.ServiceSpec{
				Selector:                 map[string]string{"statefulset.kubernetes.io/pod-name": ord},
				Ports:                    ports,
				PublishNotReadyAddresses: true,
			},
		}
		existing, err := r.clientset.CoreV1().Services(ns).Get(ctx, ord, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			if _, err := r.clientset.CoreV1().Services(ns).Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create ordinal service %q: %w", ord, err)
			}
		case err != nil:
			return fmt.Errorf("get ordinal service %q: %w", ord, err)
		default:
			desired.ResourceVersion = existing.ResourceVersion
			desired.Spec.ClusterIP = existing.Spec.ClusterIP
			if _, err := r.clientset.CoreV1().Services(ns).Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("update ordinal service %q: %w", ord, err)
			}
		}
	}
	// Prune ordinal Services left over from a previous, larger generation
	// (the Service-side complement of the StatefulSet's own pod scale-down).
	svcs, err := r.clientset.CoreV1().Services(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + spec.Name + "," + runtimeport.LabelManagedBy + "=" + runtimeport.ManagedByValue + "," + runtimeport.LabelReplicaOrdinal,
	})
	if err != nil {
		return fmt.Errorf("list ordinal services for %q: %w", spec.Name, err)
	}
	for _, svc := range svcs.Items {
		ord, err := strconv.Atoi(svc.Labels[runtimeport.LabelReplicaOrdinal])
		if err != nil || ord < int(replicas) {
			continue
		}
		if err := r.clientset.CoreV1().Services(ns).Delete(ctx, svc.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete stale ordinal service %q: %w", svc.Name, err)
		}
	}
	return nil
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

// A marshal failure is propagated, never hashed over: a wrong-but-stable
// hash would silently break the zero-API-calls idempotency contract
// (doc 11 B4 finding 5).
func specHash(spec runtimeport.ContainerSpec) (string, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("fingerprint container spec %q: %w", spec.Name, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
