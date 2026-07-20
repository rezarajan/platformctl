package kubernetes

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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

	pullPolicy, err := imagePullPolicy(spec.PullPolicy)
	if err != nil {
		return nil, fmt.Errorf("container %q: %w", spec.Name, err)
	}
	container := corev1.Container{
		Name:            spec.Name,
		Image:           spec.Image,
		ImagePullPolicy: pullPolicy,
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
	if len(spec.Files) > 0 {
		// Each FileMount is one key in the container's files Secret,
		// surfaced at its absolute path via a subPath mount.
		for i, f := range spec.Files {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      "datascape-files",
				MountPath: f.Path,
				SubPath:   fileKey(i),
				ReadOnly:  true,
			})
		}
		mode := int32(0o444)
		volumes = append(volumes, corev1.Volume{
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

	replicas := one
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        spec.Name,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: map[string]string{specHashAnnotation: hash, accessModeAnnotation: spec.AccessMode},
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
					ImagePullSecrets:              pullSecrets,
				},
			},
		},
	}, nil
}

// buildService creates a ClusterIP Service that gives the Deployment the
// same "<serviceName>:<port>" addressing Docker's embedded network DNS
// provides. serviceName is the container name or one of its aliases; either
// way it selects the same pod and carries an "app" label pointing back at
// the owning container so Remove can find alias Services by selector.
func buildService(namespace, serviceName string, spec runtimeport.ContainerSpec) *corev1.Service {
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
	labels := withOwnership(spec.Labels)
	labels["app"] = spec.Name
	svcType := corev1.ServiceTypeClusterIP
	switch spec.AccessMode {
	case runtimeport.AccessNodePort:
		svcType = corev1.ServiceTypeNodePort
	case runtimeport.AccessLoadBalancer:
		svcType = corev1.ServiceTypeLoadBalancer
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: map[string]string{"app": spec.Name},
			Ports:    ports,
		},
	}
}

// Isolation-boundary NetworkPolicy names (docs/planning/08 B7).
const (
	denyAllIngressPolicyName     = "datascape-default-deny-ingress"
	allowSameNamespacePolicyName = "datascape-allow-same-namespace"
)

// buildNetworkPolicies returns the default-deny + allow-same-namespace pair
// that gives a Namespace the isolation boundary a Docker network always
// had. Every pod in the namespace is selected by an empty PodSelector; the
// allow rule's peer names no namespaceSelector, which Kubernetes' own
// NetworkPolicy semantics scope to the policy's own namespace — exactly
// "allow from anything in this namespace, deny everything else."
func buildNetworkPolicies(namespace string, labels map[string]string) []*networkingv1.NetworkPolicy {
	owned := withOwnership(labels)
	ingress := []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
	return []*networkingv1.NetworkPolicy{
		{
			ObjectMeta: metav1.ObjectMeta{Name: denyAllIngressPolicyName, Namespace: namespace, Labels: owned},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{},
				PolicyTypes: ingress,
				// No Ingress rules at all: nothing matches, so nothing is
				// allowed — the default-deny half of the pair.
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: allowSameNamespacePolicyName, Namespace: namespace, Labels: owned},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{},
				PolicyTypes: ingress,
				Ingress: []networkingv1.NetworkPolicyIngressRule{
					{From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}}},
				},
			},
		},
	}
}

// externalIngressPolicyName is the per-container NetworkPolicy that opens the
// namespace's default-deny boundary for a container deliberately exposed
// outside it (node-port/load-balancer access modes).
func externalIngressPolicyName(containerName string) string {
	return "datascape-allow-external-" + containerName
}

// buildExternalIngressPolicy returns a NetworkPolicy that admits external
// ingress to the ports of a container exposed outside the namespace via the
// node-port/load-balancer access modes, or nil when no such hole is needed.
//
// Without it, the namespace's default-deny-ingress boundary
// (buildNetworkPolicies) silently drops the very NodePort/LoadBalancer
// traffic those modes exist to admit: such traffic reaches the pod SNAT'd to
// a node/LB address (or, worst case, the client's own external IP) — never a
// pod in this namespace — so the allow-same-namespace rule never matches it,
// and the connection times out. This was observed live: kindnet enforces
// NetworkPolicy, and a NodePort Service that is dialable with no policy times
// out the instant the default-deny pair is applied.
//
// nil for modes that need no hole: port-forward reaches pods through the
// kubelet stream, which bypasses NetworkPolicy; in-cluster and the default
// ClusterIP are namespace-internal and already served by allow-same-namespace.
// An ingress rule carrying ports but no `from` admits any source, but only to
// these deliberately-exposed ports — every other port stays default-denied.
func buildExternalIngressPolicy(namespace string, spec runtimeport.ContainerSpec) *networkingv1.NetworkPolicy {
	if spec.AccessMode != runtimeport.AccessNodePort && spec.AccessMode != runtimeport.AccessLoadBalancer {
		return nil
	}
	var ports []networkingv1.NetworkPolicyPort
	for _, p := range spec.Ports {
		proto := corev1.ProtocolTCP
		if strings.EqualFold(p.Protocol, "udp") {
			proto = corev1.ProtocolUDP
		}
		port := intstr.FromInt32(int32(p.ContainerPort))
		ports = append(ports, networkingv1.NetworkPolicyPort{Protocol: &proto, Port: &port})
	}
	if len(ports) == 0 {
		return nil
	}
	labels := withOwnership(spec.Labels)
	labels["app"] = spec.Name
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      externalIngressPolicyName(spec.Name),
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": spec.Name}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress:     []networkingv1.NetworkPolicyIngressRule{{Ports: ports}},
		},
	}
}

func filesSecretName(containerName string) string { return containerName + "-files" }

func fileKey(i int) string { return fmt.Sprintf("f%d", i) }

// filePathsAnnotation records the FileMount path each Secret key holds so
// ReadFile can map a path back to its key.
const filePathsAnnotation = "io.datascape.file-paths"

// buildFilesSecret renders spec.Files into the Secret the Deployment mounts.
func buildFilesSecret(namespace string, spec runtimeport.ContainerSpec) *corev1.Secret {
	data := make(map[string][]byte, len(spec.Files))
	paths := make(map[string]string, len(spec.Files))
	for i, f := range spec.Files {
		data[fileKey(i)] = f.Content
		paths[f.Path] = fileKey(i)
	}
	pathsJSON, _ := json.Marshal(paths)
	labels := withOwnership(spec.Labels)
	labels["app"] = spec.Name
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        filesSecretName(spec.Name),
			Namespace:   namespace,
			Labels:      labels,
			Annotations: map[string]string{filePathsAnnotation: string(pathsJSON)},
		},
		Data: data,
	}
}

func pullSecretName(containerName string) string { return containerName + "-pull-secret" }

// dockerHubServer is the auths key Docker's own tooling uses for the
// default registry when an image reference names no host — the same
// convention `docker login` writes to ~/.docker/config.json.
const dockerHubServer = "https://index.docker.io/v1/"

// dockerConfigJSONAuth mirrors the ".dockerconfigjson" Secret shape
// (`kubectl create secret docker-registry`) with only the fields
// kubelet's image-pull code path reads.
type dockerConfigJSONAuth struct {
	Auths map[string]dockerConfigJSONEntry `json:"auths"`
}

type dockerConfigJSONEntry struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Auth     string `json:"auth"`
}

// buildImagePullSecret renders spec.ImagePullAuth into a
// kubernetes.io/dockerconfigjson Secret the Deployment's
// spec.imagePullSecrets references by name.
func buildImagePullSecret(namespace string, spec runtimeport.ContainerSpec) (*corev1.Secret, error) {
	auth := spec.ImagePullAuth
	server := auth.Registry
	if server == "" {
		server = dockerHubServer
	}
	cfg := dockerConfigJSONAuth{Auths: map[string]dockerConfigJSONEntry{
		server: {
			Username: auth.Username,
			Password: auth.Password,
			Auth:     base64.StdEncoding.EncodeToString([]byte(auth.Username + ":" + auth.Password)),
		},
	}}
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("encode dockerconfigjson: %w", err)
	}
	labels := withOwnership(spec.Labels)
	labels["app"] = spec.Name
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pullSecretName(spec.Name),
			Namespace: namespace,
			Labels:    labels,
		},
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{corev1.DockerConfigJsonKey: data},
	}, nil
}

// imagePullPolicy maps the port's Pull* constants onto Kubernetes'. The
// default maps to IfNotPresent explicitly rather than leaving it unset:
// Kubernetes' own default flips to Always for :latest tags, which would
// diverge from the Docker adapter's behavior for the same spec.
func imagePullPolicy(policy string) (corev1.PullPolicy, error) {
	switch policy {
	case runtimeport.PullIfNotPresent:
		return corev1.PullIfNotPresent, nil
	case runtimeport.PullAlways:
		return corev1.PullAlways, nil
	case runtimeport.PullNever:
		return corev1.PullNever, nil
	default:
		return "", fmt.Errorf("unknown image pull policy %q (allowed: %q, %q, %q)", policy, runtimeport.PullIfNotPresent, runtimeport.PullAlways, runtimeport.PullNever)
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
