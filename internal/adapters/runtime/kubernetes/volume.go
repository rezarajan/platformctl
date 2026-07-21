// This file holds the PersistentVolumeClaim seam: EnsureVolume, RemoveVolume,
// and ListManagedVolumes (docs/planning/08 §7.6 G3).
package kubernetes

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

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
