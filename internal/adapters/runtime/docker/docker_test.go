package docker

import (
	"testing"

	"github.com/docker/go-connections/nat"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

func TestPortMapsDefaultHostIPLocalhost(t *testing.T) {
	t.Parallel()
	_, bindings, err := portMaps([]runtime.PortBinding{{HostPort: 19092, ContainerPort: 9092}})
	if err != nil {
		t.Fatal(err)
	}
	port := nat.Port("9092/tcp")
	got := bindings[port]
	if len(got) != 1 {
		t.Fatalf("bindings[%s] = %+v, want one binding", port, got)
	}
	if got[0].HostIP != "127.0.0.1" {
		t.Fatalf("HostIP = %q, want 127.0.0.1", got[0].HostIP)
	}
}

func TestPortMapsHonorsExplicitHostIP(t *testing.T) {
	t.Parallel()
	_, bindings, err := portMaps([]runtime.PortBinding{{HostIP: "0.0.0.0", HostPort: 19092, ContainerPort: 9092}})
	if err != nil {
		t.Fatal(err)
	}
	if got := bindings[nat.Port("9092/tcp")][0].HostIP; got != "0.0.0.0" {
		t.Fatalf("HostIP = %q, want explicit 0.0.0.0", got)
	}
}
