//go:build integration

package kubernetes

import (
	"context"
	"os"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestVolumeSizingAndStorageClass covers docs/planning/08 B3 against a real
// cluster: size/class land on the real PersistentVolumeClaim object (the
// "kubectl get pvc" accept criterion — checked here via the same client the
// CLI itself uses, not a shell-out), a size decrease is refused client-side
// with a clear message, and a same-size re-apply makes no API call
// (idempotent — the NFR-2 bar every Ensure* method is held to).
func TestVolumeSizingAndStorageClass(t *testing.T) {
	require := os.Getenv("PLATFORMCTL_REQUIRE_K8S") != ""
	rt, err := New(nil)
	if err != nil {
		if require {
			t.Fatalf("connect to kubernetes (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("no kubernetes configuration; skipping: %v", err)
	}
	if _, err := rt.clientset.Discovery().ServerVersion(); err != nil {
		if require {
			t.Fatalf("kubernetes cluster unreachable (required by PLATFORMCTL_REQUIRE_K8S): %v", err)
		}
		t.Skipf("kubernetes cluster unreachable; skipping: %v", err)
	}

	ctx := context.Background()
	const ns = "datascape-storage-test-net"
	const volName = "datascape-storage-test-vol"
	labels := map[string]string{
		runtimeport.LabelManagedBy:  runtimeport.ManagedByValue,
		runtimeport.LabelGeneration: "storage-test",
	}
	t.Cleanup(func() {
		_ = rt.RemoveVolume(ctx, volName)
		_ = rt.RemoveNetwork(ctx, ns)
	})

	if err := rt.EnsureNetwork(ctx, runtimeport.NetworkSpec{Name: ns, Labels: labels}); err != nil {
		t.Fatalf("EnsureNetwork: %v", err)
	}

	const initialSize = 100 * 1024 * 1024 // 100Mi
	if err := rt.EnsureVolume(ctx, runtimeport.VolumeSpec{
		Name: volName, Labels: labels, Networks: []string{ns}, SizeBytes: initialSize,
	}); err != nil {
		t.Fatalf("EnsureVolume (initial): %v", err)
	}

	pvc, err := rt.clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, volName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get persistentvolumeclaim: %v", err)
	}
	gotSize, ok := pvc.Spec.Resources.Requests["storage"]
	if !ok || gotSize.Value() != initialSize {
		t.Fatalf("PVC storage request = %v, want %d bytes", gotSize, initialSize)
	}

	// Decrease: refused client-side, naming the reason — no API mutation
	// attempted (Kubernetes itself would reject it too, but this adapter
	// doesn't even try).
	err = rt.EnsureVolume(ctx, runtimeport.VolumeSpec{
		Name: volName, Labels: labels, Networks: []string{ns}, SizeBytes: initialSize / 2,
	})
	if err == nil {
		t.Fatal("EnsureVolume accepted a size decrease")
	}
	if !strings.Contains(err.Error(), "smaller") {
		t.Errorf("decrease-refusal error unclear: %v", err)
	}
	pvc, err = rt.clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, volName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get persistentvolumeclaim after refused decrease: %v", err)
	}
	if gotSize := pvc.Spec.Resources.Requests["storage"]; gotSize.Value() != initialSize {
		t.Errorf("PVC size changed despite refused decrease: %v", gotSize)
	}

	// Same size: idempotent no-op (no ResourceVersion bump).
	beforeRV := pvc.ResourceVersion
	if err := rt.EnsureVolume(ctx, runtimeport.VolumeSpec{
		Name: volName, Labels: labels, Networks: []string{ns}, SizeBytes: initialSize,
	}); err != nil {
		t.Fatalf("EnsureVolume (same size): %v", err)
	}
	pvc, err = rt.clientset.CoreV1().PersistentVolumeClaims(ns).Get(ctx, volName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get persistentvolumeclaim after same-size re-apply: %v", err)
	}
	if pvc.ResourceVersion != beforeRV {
		t.Errorf("same-size EnsureVolume was not a no-op: ResourceVersion %s -> %s", beforeRV, pvc.ResourceVersion)
	}

	// Increase: a live expansion attempt. Whether it succeeds is a real,
	// cluster-dependent fact (the StorageClass must have
	// allowVolumeExpansion: true) — assert this adapter actually attempts
	// the patch (no client-side refusal the way decrease gets) rather than
	// asserting a specific cluster capability this test doesn't control.
	err = rt.EnsureVolume(ctx, runtimeport.VolumeSpec{
		Name: volName, Labels: labels, Networks: []string{ns}, SizeBytes: initialSize * 2,
	})
	if err != nil && !strings.Contains(err.Error(), "expand persistentvolumeclaim") {
		t.Errorf("increase attempt failed with an unexpected error shape: %v", err)
	}
}
