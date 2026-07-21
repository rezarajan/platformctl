package kubernetes

import (
	"context"
	"fmt"
	"strings"

	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// This file implements docs/planning/08 B6: a fast, named connectivity and
// permission check at validate/plan time, before any mutating call, instead
// of a raw client-go error surfacing mid-apply. The verb/resource list here
// is the authoritative source deploy/kubernetes/rbac/role.yaml (B5) must
// stay in sync with — both enumerate exactly what this adapter's Ensure*/
// Remove*/List*/Logs methods call.
var preflightChecks = []authorizationv1.ResourceAttributes{
	{Verb: "get", Resource: "namespaces"},
	{Verb: "create", Resource: "namespaces"},
	{Verb: "delete", Resource: "namespaces"},
	{Verb: "list", Resource: "namespaces"},
	{Verb: "list", Resource: "nodes"}, // node-port access mode (docs/planning/08 B1): resolves a node address
	{Verb: "get", Group: "apps", Resource: "deployments"},
	{Verb: "create", Group: "apps", Resource: "deployments"},
	{Verb: "update", Group: "apps", Resource: "deployments"},
	{Verb: "delete", Group: "apps", Resource: "deployments"},
	{Verb: "list", Group: "apps", Resource: "deployments"},
	// StatefulSets and PodDisruptionBudgets: the StableIdentity replica-set
	// shape and the shape-transition guard (docs/planning/08 C1,
	// docs/adr/004) — EnsureContainer/Inspect/ListManaged/Remove consult
	// both workload shapes on every call.
	{Verb: "get", Group: "apps", Resource: "statefulsets"},
	{Verb: "create", Group: "apps", Resource: "statefulsets"},
	{Verb: "update", Group: "apps", Resource: "statefulsets"},
	{Verb: "delete", Group: "apps", Resource: "statefulsets"},
	{Verb: "list", Group: "apps", Resource: "statefulsets"},
	{Verb: "get", Group: "policy", Resource: "poddisruptionbudgets"},
	{Verb: "create", Group: "policy", Resource: "poddisruptionbudgets"},
	{Verb: "update", Group: "policy", Resource: "poddisruptionbudgets"},
	{Verb: "delete", Group: "policy", Resource: "poddisruptionbudgets"},
	{Verb: "get", Resource: "services"},
	{Verb: "create", Resource: "services"},
	{Verb: "update", Resource: "services"},
	{Verb: "delete", Resource: "services"},
	{Verb: "list", Resource: "services"},
	{Verb: "get", Resource: "persistentvolumeclaims"},
	{Verb: "create", Resource: "persistentvolumeclaims"},
	{Verb: "update", Resource: "persistentvolumeclaims"},
	{Verb: "delete", Resource: "persistentvolumeclaims"},
	{Verb: "list", Resource: "persistentvolumeclaims"},
	{Verb: "get", Resource: "secrets"},
	{Verb: "create", Resource: "secrets"},
	{Verb: "update", Resource: "secrets"},
	{Verb: "delete", Resource: "secrets"},
	{Verb: "get", Resource: "pods"},
	{Verb: "list", Resource: "pods"},
	{Verb: "create", Resource: "pods"}, // ProbeReachable's ephemeral probe pod fallback (docs/planning/08 C10)
	{Verb: "delete", Resource: "pods"}, // ProbeReachable's ephemeral probe pod cleanup (docs/planning/08 C10)
	{Verb: "get", Resource: "pods/log"},
	{Verb: "create", Resource: "pods/exec"}, // ReadFile's live-path fallback (readFileViaExec)
	{Verb: "create", Resource: "pods/portforward"},
	{Verb: "get", Group: "networking.k8s.io", Resource: "networkpolicies"},
	{Verb: "create", Group: "networking.k8s.io", Resource: "networkpolicies"},
	{Verb: "delete", Group: "networking.k8s.io", Resource: "networkpolicies"},
}

// Preflight checks that the cluster config.New would build is reachable and
// that the ambient credentials can perform every verb this adapter uses,
// failing fast with an error naming the kubeconfig, context, and exactly
// what's missing — the docs/planning/08 B6 contract.
func Preflight(ctx context.Context, config map[string]any) error {
	kubeconfigPath, _ := config["kubeconfig"].(string)
	contextName, _ := config["context"].(string)
	label := describeConfig(kubeconfigPath, contextName)

	restCfg, err := loadConfig(config)
	if err != nil {
		return fmt.Errorf("kubernetes (%s): load kubeconfig: %w", label, err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("kubernetes (%s): build client: %w", label, err)
	}
	return preflightWithClient(ctx, clientset, label)
}

// preflightWithClient is the testable seam: unit tests inject a fake
// clientset here without needing a real cluster or kubeconfig.
func preflightWithClient(ctx context.Context, clientset kubernetes.Interface, label string) error {
	if _, err := clientset.Discovery().ServerVersion(); err != nil {
		return fmt.Errorf("kubernetes (%s): cluster unreachable: %w", label, err)
	}

	var missing []string
	for _, attr := range preflightChecks {
		attr := attr
		review := &authorizationv1.SelfSubjectAccessReview{
			Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &attr},
		}
		result, err := clientset.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
		if err != nil {
			// Some clusters run without the authorization API meaningfully
			// enabled (e.g. AlwaysAllow); treat "can't verify" as pass
			// rather than a false-positive permission failure.
			continue
		}
		if !result.Status.Allowed {
			missing = append(missing, verbLabel(attr))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("kubernetes (%s): missing permission(s): %s — see deploy/kubernetes/rbac/role.yaml for the minimal Role this adapter needs",
			label, strings.Join(missing, ", "))
	}
	return nil
}

func verbLabel(attr authorizationv1.ResourceAttributes) string {
	resource := attr.Resource
	if attr.Group != "" {
		resource = attr.Resource + "." + attr.Group
	}
	return attr.Verb + " " + resource
}

func describeConfig(kubeconfigPath, contextName string) string {
	if kubeconfigPath == "" {
		kubeconfigPath = "default kubeconfig"
	}
	if contextName == "" {
		contextName = "current context"
	}
	return fmt.Sprintf("kubeconfig=%s, context=%s", kubeconfigPath, contextName)
}
