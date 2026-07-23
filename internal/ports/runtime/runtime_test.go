package runtime

import "testing"

func TestContainerSpec_ReplicaCount(t *testing.T) {
	t.Parallel()
	cases := []struct {
		replicas int
		want     int
	}{
		{0, 1},
		{1, 1},
		{-1, 1},
		{3, 3},
	}
	for _, c := range cases {
		spec := ContainerSpec{Replicas: c.replicas}
		if got := spec.ReplicaCount(); got != c.want {
			t.Errorf("ContainerSpec{Replicas: %d}.ReplicaCount() = %d, want %d", c.replicas, got, c.want)
		}
	}
}

func TestOrdinalName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		i    int
		want string
	}{
		{"redpanda", 0, "redpanda-0"},
		{"redpanda", 2, "redpanda-2"},
		{"trino-worker", 10, "trino-worker-10"},
	}
	for _, c := range cases {
		if got := OrdinalName(c.name, c.i); got != c.want {
			t.Errorf("OrdinalName(%q, %d) = %q, want %q", c.name, c.i, got, c.want)
		}
	}
}
