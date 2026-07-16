package kubernetes

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

const one int32 = 1

var terminationGracePeriodSeconds int64 = 15

// buildDeployment translates a ContainerSpec into a single-replica
// Deployment. See the package doc comment for the mapping rationale.
func buildDeployment(namespace string, spec runtimeport.ContainerSpec, hash string) (*appsv1.Deployment, error) {
	labels := withOwnership(spec.Labels)
	labels["app"] = spec.Name

	container := corev1.Container{
		Name:  spec.Name,
		Image: spec.Image,
		// ContainerSpec.Cmd is Docker's Config.Cmd — appended after the
		// image's own ENTRYPOINT, never replacing it (the Docker adapter
		// only ever sets Cmd, never Entrypoint). Kubernetes' "command"
		// field maps to Docker's ENTRYPOINT and "args" maps to Docker's
		// CMD, so this must be Args, not Command — using Command here
		// would silently bypass any entrypoint script the image relies on
		// (found by running the redpanda provider, unmodified, against a
		// real cluster: its image's /entrypoint.sh got skipped entirely).
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

	var volumes []corev1.Volume
	for _, m := range spec.Volumes {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      m.VolumeName,
			MountPath: m.MountPath,
		})
		volumes = append(volumes, corev1.Volume{
			Name: m.VolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: m.VolumeName},
			},
		})
	}

	replicas := one
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        spec.Name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: map[string]string{specHashAnnotation: hash},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": spec.Name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					// Deployments require Always; Docker's on-failure/
					// unless-stopped/MaxRetries have no Pod-level
					// equivalent under a Deployment (see package doc).
					RestartPolicy: corev1.RestartPolicyAlways,
					// Kubernetes' 30s default grace period makes routine
					// teardown (destroy, conformance, replacement on drift)
					// needlessly slow for containers that don't trap
					// SIGTERM specially. 15s is generous for a clean
					// shutdown while keeping Remove reasonably fast.
					TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
					Containers:                    []corev1.Container{container},
					Volumes:                       volumes,
				},
			},
		},
	}, nil
}

// buildService creates the ClusterIP Service that gives the Deployment the
// same "<name>:<port>" addressing Docker's embedded network DNS provides.
func buildService(namespace string, spec runtimeport.ContainerSpec) *corev1.Service {
	var ports []corev1.ServicePort
	for _, p := range spec.Ports {
		proto := corev1.ProtocolTCP
		if strings.EqualFold(p.Protocol, "udp") {
			proto = corev1.ProtocolUDP
		}
		ports = append(ports, corev1.ServicePort{
			Name:       fmt.Sprintf("port-%d", p.ContainerPort),
			Port:       int32(p.ContainerPort),
			TargetPort: intstr.FromInt32(int32(p.ContainerPort)),
			Protocol:   proto,
		})
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: namespace,
			Labels:    withOwnership(spec.Labels),
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": spec.Name},
			Ports:    ports,
		},
	}
}

func envVars(env map[string]string) []corev1.EnvVar {
	if len(env) == 0 {
		return nil
	}
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]corev1.EnvVar, 0, len(keys))
	for _, k := range keys {
		out = append(out, corev1.EnvVar{Name: k, Value: env[k]})
	}
	return out
}

func containerPorts(ports []runtimeport.PortBinding) []corev1.ContainerPort {
	if len(ports) == 0 {
		return nil
	}
	out := make([]corev1.ContainerPort, 0, len(ports))
	for _, p := range ports {
		proto := corev1.ProtocolTCP
		if strings.EqualFold(p.Protocol, "udp") {
			proto = corev1.ProtocolUDP
		}
		out = append(out, corev1.ContainerPort{ContainerPort: int32(p.ContainerPort), Protocol: proto})
	}
	return out
}

// healthProbe translates Docker's HealthCheck.Test convention
// ("CMD-SHELL", "<shell command>" or "CMD", "<argv...>") into an exec probe
// used for both readiness and liveness — Docker's single healthcheck gates
// both "accepting traffic" and "should be restarted", so this mirrors that.
func healthProbe(hc *runtimeport.HealthCheck) *corev1.Probe {
	if hc == nil || len(hc.Test) == 0 {
		return nil
	}
	var command []string
	switch strings.ToUpper(hc.Test[0]) {
	case "NONE":
		return nil
	case "CMD-SHELL":
		if len(hc.Test) < 2 {
			return nil
		}
		command = []string{"/bin/sh", "-c", hc.Test[1]}
	case "CMD":
		command = hc.Test[1:]
	default:
		command = hc.Test
	}
	if len(command) == 0 {
		return nil
	}
	probe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: command}},
	}
	if hc.Interval > 0 {
		probe.PeriodSeconds = int32(hc.Interval.Seconds())
		if probe.PeriodSeconds < 1 {
			probe.PeriodSeconds = 1
		}
	}
	if hc.Timeout > 0 {
		probe.TimeoutSeconds = int32(hc.Timeout.Seconds())
		if probe.TimeoutSeconds < 1 {
			probe.TimeoutSeconds = 1
		}
	}
	if hc.Retries > 0 {
		probe.FailureThreshold = int32(hc.Retries)
	}
	return probe
}

func resourceRequirements(r *runtimeport.Resources) corev1.ResourceRequirements {
	limits := corev1.ResourceList{}
	requests := corev1.ResourceList{}
	if r.CPULimit > 0 {
		limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(r.CPULimit*1000), resource.DecimalSI)
	}
	if r.CPUReservation > 0 {
		requests[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(r.CPUReservation*1000), resource.DecimalSI)
	}
	if r.MemoryLimitBytes > 0 {
		limits[corev1.ResourceMemory] = *resource.NewQuantity(r.MemoryLimitBytes, resource.BinarySI)
	}
	if r.MemoryReservationBytes > 0 {
		requests[corev1.ResourceMemory] = *resource.NewQuantity(r.MemoryReservationBytes, resource.BinarySI)
	}
	out := corev1.ResourceRequirements{}
	if len(limits) > 0 {
		out.Limits = limits
	}
	if len(requests) > 0 {
		out.Requests = requests
	}
	return out
}

// securityContext translates SecurityContext. Only a numeric "uid[:gid]"
// User is supported — Docker also accepts usernames, which cannot be
// resolved to a numeric id without inspecting the image, so a non-numeric
// User is a clear error rather than a silently-dropped security setting.
func securityContext(s *runtimeport.SecurityContext) (*corev1.SecurityContext, error) {
	out := &corev1.SecurityContext{}
	if s.User != "" {
		uidStr, gidStr, hasGid := strings.Cut(s.User, ":")
		uid, err := strconv.ParseInt(uidStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("security.user %q: the kubernetes runtime only supports a numeric uid[:gid] (got non-numeric uid); Docker-style usernames are not resolvable without image inspection", s.User)
		}
		out.RunAsUser = &uid
		if hasGid {
			gid, err := strconv.ParseInt(gidStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("security.user %q: non-numeric gid", s.User)
			}
			out.RunAsGroup = &gid
		}
	}
	if s.ReadOnlyRootFS {
		v := true
		out.ReadOnlyRootFilesystem = &v
	}
	if len(s.CapAdd) > 0 || len(s.CapDrop) > 0 {
		caps := &corev1.Capabilities{}
		for _, c := range s.CapAdd {
			caps.Add = append(caps.Add, corev1.Capability(c))
		}
		for _, c := range s.CapDrop {
			caps.Drop = append(caps.Drop, corev1.Capability(c))
		}
		out.Capabilities = caps
	}
	// SecurityOpt is a Docker-specific escape hatch (e.g. "no-new-privileges")
	// with no generic Kubernetes translation; deliberately ignored.
	return out, nil
}

func stateFromDeployment(d *appsv1.Deployment) runtimeport.ContainerState {
	st := runtimeport.ContainerState{
		Name:    d.Name,
		ID:      string(d.UID),
		Labels:  d.Labels,
		Running: d.Status.ReadyReplicas > 0,
		Healthy: d.Status.ReadyReplicas > 0,
	}
	if len(d.Spec.Template.Spec.Containers) > 0 {
		c := d.Spec.Template.Spec.Containers[0]
		st.Image = c.Image
		env := make(map[string]string, len(c.Env))
		for _, e := range c.Env {
			env[e.Name] = e.Value
		}
		st.Env = env
		// No host binding exists for a ClusterIP-backed Deployment;
		// report container ports only (HostIP/HostPort zero-valued), per
		// the ContainerState.Ports contract.
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
