package kubernetes

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestBuildDeployment_ReplicasAndAntiAffinity pins the docs/design/004
// backward-compatibility guarantee (replicas == 1 reproduces today's exact
// behavior, no Affinity at all) and the new Replicas > 1 behavior (soft
// anti-affinity applied automatically, independent of StableIdentity).
func TestBuildDeployment_ReplicasAndAntiAffinity(t *testing.T) {
	spec := runtimeport.ContainerSpec{
		Name:  "trino-worker",
		Image: "trinodb/trino:435",
	}

	t.Run("replicas 1: no affinity, matches pre-existing behavior", func(t *testing.T) {
		d, err := buildDeployment("ns", spec, "hash", 1)
		if err != nil {
			t.Fatalf("buildDeployment: %v", err)
		}
		if got := *d.Spec.Replicas; got != 1 {
			t.Errorf("Replicas = %d, want 1", got)
		}
		if d.Spec.Template.Spec.Affinity != nil {
			t.Errorf("Affinity = %+v, want nil for a single replica", d.Spec.Template.Spec.Affinity)
		}
	})

	t.Run("replicas 3: soft anti-affinity on app label", func(t *testing.T) {
		d, err := buildDeployment("ns", spec, "hash", 3)
		if err != nil {
			t.Fatalf("buildDeployment: %v", err)
		}
		if got := *d.Spec.Replicas; got != 3 {
			t.Errorf("Replicas = %d, want 3", got)
		}
		aff := d.Spec.Template.Spec.Affinity
		if aff == nil || aff.PodAntiAffinity == nil {
			t.Fatalf("expected PodAntiAffinity for Replicas: 3, got %+v", aff)
		}
		terms := aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
		if len(terms) != 1 {
			t.Fatalf("preferred anti-affinity terms = %d, want 1", len(terms))
		}
		if terms[0].PodAffinityTerm.TopologyKey != "kubernetes.io/hostname" {
			t.Errorf("topologyKey = %q, want kubernetes.io/hostname", terms[0].PodAffinityTerm.TopologyKey)
		}
		if got := terms[0].PodAffinityTerm.LabelSelector.MatchLabels["app"]; got != "trino-worker" {
			t.Errorf("anti-affinity selector app = %q, want %q", got, "trino-worker")
		}
		// Requires a *hard* requirement, which would refuse to schedule on a
		// single-node cluster (minikube/kind/CI) — must stay empty.
		if len(aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0 {
			t.Errorf("expected no hard anti-affinity requirement, got %+v", aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
		}
	})
}

// TestBuildStatefulSet pins the StableIdentity translation: ServiceName,
// Parallel pod management, one VolumeClaimTemplate per distinct declared
// volume (deduplicated), and RWO access mode sized from the adapter's
// existing default.
func TestBuildStatefulSet(t *testing.T) {
	spec := runtimeport.ContainerSpec{
		Name:           "redpanda",
		Image:          "redpandadata/redpanda:v24.2.1",
		StableIdentity: true,
		Volumes: []runtimeport.VolumeMount{
			{VolumeName: "data", MountPath: "/var/lib/redpanda/data"},
			{VolumeName: "data", MountPath: "/var/lib/redpanda/data-again"}, // same claim, two mounts
		},
	}
	sts, err := buildStatefulSet("ns", spec, "hash", 3)
	if err != nil {
		t.Fatalf("buildStatefulSet: %v", err)
	}
	if sts.Spec.ServiceName != "redpanda" {
		t.Errorf("ServiceName = %q, want %q", sts.Spec.ServiceName, "redpanda")
	}
	if got := *sts.Spec.Replicas; got != 3 {
		t.Errorf("Replicas = %d, want 3", got)
	}
	if sts.Spec.PodManagementPolicy != "Parallel" {
		t.Errorf("PodManagementPolicy = %q, want Parallel", sts.Spec.PodManagementPolicy)
	}
	if got := sts.Spec.Selector.MatchLabels["app"]; got != "redpanda" {
		t.Errorf("selector app = %q, want %q", got, "redpanda")
	}
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("VolumeClaimTemplates = %d, want 1 (deduplicated by VolumeName)", len(sts.Spec.VolumeClaimTemplates))
	}
	vct := sts.Spec.VolumeClaimTemplates[0]
	if vct.Name != "data" {
		t.Errorf("VolumeClaimTemplate name = %q, want %q", vct.Name, "data")
	}
	if len(vct.Spec.AccessModes) != 1 || vct.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Errorf("AccessModes = %v, want [ReadWriteOnce]", vct.Spec.AccessModes)
	}
	got := vct.Spec.Resources.Requests[corev1.ResourceStorage]
	if got.Value() != defaultVolumeSizeBytes {
		t.Errorf("requested storage = %d, want %d (the adapter's default)", got.Value(), defaultVolumeSizeBytes)
	}
	if len(sts.Spec.Template.Spec.Containers) != 1 || len(sts.Spec.Template.Spec.Containers[0].VolumeMounts) != 2 {
		t.Fatalf("expected both VolumeMounts on the single container, got %+v", sts.Spec.Template.Spec.Containers)
	}
	if sts.Spec.Template.Spec.Affinity == nil {
		t.Errorf("expected soft anti-affinity for Replicas: 3")
	}
}

func TestBuildHeadlessService(t *testing.T) {
	spec := runtimeport.ContainerSpec{
		Name: "redpanda",
		Ports: []runtimeport.PortBinding{
			{ContainerPort: 9092, Audience: runtimeport.AudienceInternal},
		},
		// AccessMode is deliberately ignored for the headless governing
		// Service — see buildHeadlessService's doc comment.
		AccessMode: runtimeport.AccessNodePort,
	}
	svc := buildHeadlessService("ns", spec)
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("ClusterIP = %q, want %q", svc.Spec.ClusterIP, corev1.ClusterIPNone)
	}
	if svc.Spec.Type != "" {
		t.Errorf("Type = %q, want unset (headless Services do not set Type)", svc.Spec.Type)
	}
	if got := svc.Spec.Selector["app"]; got != "redpanda" {
		t.Errorf("selector app = %q, want %q", got, "redpanda")
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 9092 {
		t.Errorf("ports = %+v, want one port 9092", svc.Spec.Ports)
	}
}

func TestBuildPodDisruptionBudget(t *testing.T) {
	spec := runtimeport.ContainerSpec{Name: "redpanda"}
	pdb := buildPodDisruptionBudget("ns", spec)
	if pdb.Name != pdbName("redpanda") {
		t.Errorf("Name = %q, want %q", pdb.Name, pdbName("redpanda"))
	}
	if got := pdb.Spec.MaxUnavailable.IntValue(); got != 1 {
		t.Errorf("MaxUnavailable = %d, want 1", got)
	}
	if got := pdb.Spec.Selector.MatchLabels["app"]; got != "redpanda" {
		t.Errorf("selector app = %q, want %q", got, "redpanda")
	}
}
