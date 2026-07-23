//go:build integration

package main

import (
	"context"
	"path/filepath"
	"testing"

	k8sruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes"
	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/testkit"
)

// TestIngressKubernetesEndToEnd covers docs/planning/08 C7's Kubernetes leg:
// one networking.k8s.io/v1 Ingress object per Connection, created via
// IngressCapableRuntime against a real cluster, correctly matching the
// Connection's declared Host and backend Service/port; idempotent re-apply;
// clean destroy. Complements the fake-clientset unit tests in
// internal/adapters/runtime/kubernetes/ingress_test.go, which cover the same
// contract but cannot catch a real-apiserver surprise the fake client
// wouldn't reproduce.
func TestIngressKubernetesEndToEnd(t *testing.T) {
	requireK8s(t)
	rt, err := k8sruntime.New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	ctx := context.Background()
	const ns = "datascape-ingk8s-test-ns"
	stateFile := filepath.Join(t.TempDir(), "state.json")
	manifests := "testdata/ingress-k8s-scenario"
	gate := "IngressProvider=true"

	// docs/adr/029 (J2 sweep): a namespace-only cleanup strands under the
	// holds-workloads refusal whenever the test dies before its inline
	// destroy — CLI destroy in cleanup (LIFO: runs before the janitor)
	// empties the namespace, the janitor removes it loud.
	jan := testkit.Janitor{RT: rt, Networks: []string{ns}}
	jan.CleanSilent(ctx)
	jan.Register(ctx, t)
	t.Cleanup(func() {
		_, _, _ = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	})

	out, err, code := run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("apply failed (code %d): %v\n%s", code, err, out)
	}

	ingState, found, err := rt.GetIngress(ctx, ns, "route-nessie")
	if err != nil {
		t.Fatalf("GetIngress: %v", err)
	}
	if !found {
		t.Fatal("Ingress route-nessie not found after apply")
	}
	if ingState.Host != "nessie.localhost" {
		t.Errorf("Ingress host = %q, want nessie.localhost", ingState.Host)
	}
	if ingState.TargetName != "ingk8s-nessie" || ingState.TargetPort != 19120 {
		t.Errorf("Ingress backend = %s:%d, want ingk8s-nessie:19120", ingState.TargetName, ingState.TargetPort)
	}

	// Idempotent re-apply.
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("re-apply failed (code %d): %v\n%s", code, err, out)
	}

	// Drift heal: mangle the live Ingress object directly (bypassing
	// platformctl), then re-apply and confirm it converges back.
	mangled, err := rt.EnsureIngress(ctx, runtimeport.IngressSpec{
		Name: "route-nessie", Namespace: ns, Host: "mangled.localhost",
		TargetName: "nowhere", TargetPort: 1,
	})
	if err != nil {
		t.Fatalf("mangle ingress: %v", err)
	}
	if mangled.Host != "mangled.localhost" {
		t.Fatalf("mangle did not take effect: %+v", mangled)
	}
	out, err, code = run(t, "apply", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("heal apply failed (code %d): %v\n%s", code, err, out)
	}
	healed, found, err := rt.GetIngress(ctx, ns, "route-nessie")
	if err != nil || !found {
		t.Fatalf("GetIngress after heal: found=%v err=%v", found, err)
	}
	if healed.Host != "nessie.localhost" {
		t.Errorf("Ingress host after heal = %q, want nessie.localhost (mangled route was not healed)", healed.Host)
	}

	// Clean destroy.
	out, err, code = run(t, "destroy", manifests, "--state-file", stateFile, "--auto-approve", "--feature-gates", gate)
	if err != nil || code != 0 {
		t.Fatalf("destroy failed (code %d): %v\n%s", code, err, out)
	}
	if _, found, _ := rt.GetIngress(ctx, ns, "route-nessie"); found {
		t.Error("Ingress route-nessie still present after destroy")
	}
}
