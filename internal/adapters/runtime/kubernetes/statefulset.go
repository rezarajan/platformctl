package kubernetes

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// buildStatefulSet translates a StableIdentity ContainerSpec (Replicas > 1)
// into a StatefulSet (docs/adr/004-replicas-and-identity.md). The pod
// template is built the same way buildDeployment's is (health probes,
// resources, security context, files/pull secrets, soft anti-affinity), but
// this object is governed by a headless Service (buildHeadlessService)
// instead of a ClusterIP one, and carries one VolumeClaimTemplate per
// distinct VolumeMount.VolumeName the spec declares — Kubernetes creates and
// owns the per-ordinal PersistentVolumeClaims natively
// ("<claim>-<Name>-<i>"), so no adapter code manufactures them by hand.
// PodManagementPolicy is Parallel: the shared-nothing clustering protocols
// this primitive targets (Redpanda/MinIO — C2/C4) join via their own
// retrying handshake, not ordered Kubernetes pod startup, and Parallel scales
// and recovers faster.
func buildStatefulSet(namespace string, spec runtimeport.ContainerSpec, hash string, replicas int32) (*appsv1.StatefulSet, error) {
	labels := withOwnership(spec.Labels)
	labels["app"] = spec.Name

	pullPolicy, err := imagePullPolicy(spec.PullPolicy)
	if err != nil {
		return nil, fmt.Errorf("container %q: %w", spec.Name, err)
	}
	container := corev1.Container{
		Name:            spec.Name,
		Image:           spec.Image,
		ImagePullPolicy: pullPolicy,
		// See buildDeployment's identical comment: Cmd is Docker's
		// CMD-appends-to-ENTRYPOINT convention, so it maps to Args, never
		// Command.
		Args:  spec.Cmd,
		Env:   envVars(spec.Env),
		Ports: containerPorts(spec.Ports),
	}
	probe := healthProbe(spec.HealthCheck)
	container.ReadinessProbe = probe
	container.LivenessProbe = probe

	if spec.Resources != nil {
		container.Resources = resourceRequirements(spec.Resources)
	}
	if spec.Security != nil {
		sc, err := securityContext(spec.Security)
		if err != nil {
			return nil, fmt.Errorf("container %q: %w", spec.Name, err)
		}
		container.SecurityContext = sc
	}

	var extraVolumes []corev1.Volume
	var claimTemplates []corev1.PersistentVolumeClaim
	seenClaims := make(map[string]bool, len(spec.Volumes))
	for _, m := range spec.Volumes {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      m.VolumeName,
			MountPath: m.MountPath,
		})
		if seenClaims[m.VolumeName] {
			continue
		}
		seenClaims[m.VolumeName] = true
		claimTemplates = append(claimTemplates, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: m.VolumeName, Labels: withOwnership(spec.Labels)},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					// VolumeMount carries no per-volume size/StorageClass
					// override yet (docs/adr/004 "Known limitations") —
					// every ordinal volume uses the adapter's existing
					// EnsureVolume default (defaultVolumeSizeBytes, cluster
					// default StorageClass).
					Requests: corev1.ResourceList{corev1.ResourceStorage: *resource.NewQuantity(defaultVolumeSizeBytes, resource.BinarySI)},
				},
			},
		})
	}

	if len(spec.Files) > 0 {
		for i, f := range spec.Files {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      "datascape-files",
				MountPath: f.Path,
				SubPath:   fileKey(i),
				ReadOnly:  true,
			})
		}
		mode := int32(0o444)
		extraVolumes = append(extraVolumes, corev1.Volume{
			Name: "datascape-files",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  filesSecretName(spec.Name),
					DefaultMode: &mode,
				},
			},
		})
	}

	var pullSecrets []corev1.LocalObjectReference
	if spec.ImagePullAuth != nil {
		pullSecrets = []corev1.LocalObjectReference{{Name: pullSecretName(spec.Name)}}
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        spec.Name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: map[string]string{specHashAnnotation: hash, accessModeAnnotation: spec.AccessMode},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			// The headless Service ordinal DNS depends on
			// ("<Name>-<i>.<ServiceName>...") — always the container's own
			// name, matching buildHeadlessService.
			ServiceName:         spec.Name,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: map[string]string{"app": spec.Name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                 corev1.RestartPolicyAlways,
					TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
					Containers:                    []corev1.Container{container},
					Volumes:                       extraVolumes,
					ImagePullSecrets:              pullSecrets,
					Affinity:                      podAntiAffinity(spec, replicas),
				},
			},
			VolumeClaimTemplates: claimTemplates,
		},
	}, nil
}

// stateFromStatefulSet mirrors stateFromDeployment: no host binding exists
// for a headless-Service-backed StatefulSet, so Ports report ContainerPort
// only (HostIP/HostPort zero-valued).
func stateFromStatefulSet(sts *appsv1.StatefulSet) runtimeport.ContainerState {
	st := runtimeport.ContainerState{
		Name:          sts.Name,
		ID:            string(sts.UID),
		Labels:        sts.Labels,
		Running:       sts.Status.ReadyReplicas > 0,
		Healthy:       sts.Status.ReadyReplicas > 0,
		ReadyReplicas: int(sts.Status.ReadyReplicas),
	}
	if len(sts.Spec.Template.Spec.Containers) > 0 {
		c := sts.Spec.Template.Spec.Containers[0]
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
			st.Ports = append(st.Ports, runtimeport.PortBinding{
				ContainerPort: int(p.ContainerPort),
				Protocol:      proto,
			})
		}
	}
	return st
}
