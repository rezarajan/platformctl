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
//
// This file (kubernetes.go) holds only what the rest of the package shares:
// client construction, label/ownership helpers, and the cross-namespace
// lookup helpers used by every other file here. The seams themselves live
// in network.go, volume.go, container.go, container_remove.go,
// reachability.go, and exec.go (docs/planning/08 §7.6 G3).
package kubernetes

import (
	"context"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

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

// targetNamespace picks the namespace a volume/container belongs to: the
// first named network, since every provider always names exactly one.
func targetNamespace(networks []string) (string, error) {
	if len(networks) == 0 {
		return "", fmt.Errorf("no network specified; the kubernetes runtime requires exactly one (PersistentVolumeClaims and Deployments are namespace-scoped)")
	}
	return networks[0], nil
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
