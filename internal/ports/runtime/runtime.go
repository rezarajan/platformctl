// Package runtime defines the ContainerRuntime port and its value types.
// Every method is Ensure*, not Create* — idempotent-by-contract at the
// interface boundary. See docs/planning/02-architecture.md §4.1.
package runtime

import (
	"context"
	"time"
)

type NetworkSpec struct {
	Name   string
	Labels map[string]string
}

type VolumeSpec struct {
	Name   string
	Labels map[string]string
}

type HealthCheck struct {
	Test     []string
	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

type VolumeMount struct {
	VolumeName string
	MountPath  string
}

type PortBinding struct {
	HostIP        string
	HostPort      int
	ContainerPort int
	Protocol      string // "tcp" (default) | "udp"
}

type ContainerSpec struct {
	Name        string
	Image       string
	Cmd         []string // implementation-revealed addition: real providers need command/args; pending doc amendment to 02-architecture.md §4.1
	Networks    []string
	Volumes     []VolumeMount
	Env         map[string]string
	Ports       []PortBinding
	HealthCheck *HealthCheck
	Labels      map[string]string // Datascape ownership + generation labels for drift/GC
}

type ContainerState struct {
	Name    string
	ID      string
	Image   string
	Running bool
	Healthy bool
	Labels  map[string]string
	Env     map[string]string
}

type ContainerRuntime interface {
	EnsureNetwork(ctx context.Context, spec NetworkSpec) error
	EnsureVolume(ctx context.Context, spec VolumeSpec) error
	EnsureContainer(ctx context.Context, spec ContainerSpec) (ContainerState, error)
	WaitHealthy(ctx context.Context, name string, timeout time.Duration) error
	Inspect(ctx context.Context, name string) (ContainerState, bool, error)
	Remove(ctx context.Context, name string) error
	RemoveNetwork(ctx context.Context, name string) error
	RemoveVolume(ctx context.Context, name string) error
	ListManaged(ctx context.Context) ([]ContainerState, error) // everything labeled as Datascape-owned
}

// Datascape ownership labels — applied to every created object so
// ListManaged/destroy never touch unlabeled resources.
const (
	LabelManagedBy  = "io.datascape.managed-by"
	LabelGeneration = "io.datascape.generation"
	LabelNamespace  = "io.datascape.namespace"
	LabelKind       = "io.datascape.kind"
	LabelName       = "io.datascape.name"
	LabelProject    = "io.datascape.project"
	ManagedByValue  = "platformctl"
)

func ManagedLabels(namespace, kind, name, generation string) map[string]string {
	if namespace == "" {
		namespace = "default"
	}
	if generation == "" {
		generation = name
	}
	return map[string]string{
		LabelManagedBy:  ManagedByValue,
		LabelGeneration: generation,
		LabelNamespace:  namespace,
		LabelKind:       kind,
		LabelName:       name,
		LabelProject:    namespace,
	}
}
