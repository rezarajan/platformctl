package kubernetes

import (
	"context"
	"strings"
	"testing"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestEnsureContainerRefusesSysctls pins the doc 07 loud-refusal clause
// (doc 11 GA caveat sweep): Sysctls was silently dropped on Kubernetes;
// now it is a named refusal, so a Docker-only workload (wireguard) fails
// honestly instead of running subtly broken.
func TestEnsureContainerRefusesSysctls(t *testing.T) {
	t.Parallel()
	r := &Runtime{}
	_, err := r.EnsureContainer(context.Background(), runtimeport.ContainerSpec{
		Name:    "wg",
		Image:   "img",
		Sysctls: map[string]string{"net.ipv4.ip_forward": "1"},
	})
	if err == nil || !strings.Contains(err.Error(), "Sysctls is not supported on the kubernetes runtime") {
		t.Fatalf("want named Sysctls refusal, got: %v", err)
	}
}
