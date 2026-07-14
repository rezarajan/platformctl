// Package resource defines the common envelope every manifest shares.
// See docs/planning/02-architecture.md §3.1.
package resource

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/status"
)

type GroupVersionKind struct {
	APIVersion string `json:"apiVersion"` // e.g. "datascape.io/v1alpha1"
	Kind       string `json:"kind"`       // e.g. "EventStream"
}

type ObserverRef struct {
	Name string `json:"name"` // must resolve to a Provider
}

type Metadata struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace,omitempty"` // reserved for v1; single implicit namespace "default"
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Observers   []ObserverRef     `json:"observers,omitempty"`
}

// Envelope is the parsed, validated form of any manifest before it's cast to
// a concrete typed resource.
type Envelope struct {
	GroupVersionKind `json:",inline"`
	Metadata         Metadata       `json:"metadata"`
	Spec             map[string]any `json:"spec"`
	Status           status.Status  `json:"status,omitempty"`
}

// Key uniquely identifies a resource: Kind/Name.
type Key struct {
	Kind string
	Name string
}

func (k Key) String() string { return k.Kind + "/" + k.Name }

func (e Envelope) Key() Key {
	return Key{Kind: e.Kind, Name: e.Metadata.Name}
}

// Lifecycle taxonomy — see docs/planning/02-architecture.md §3.2.
type Lifecycle int

const (
	Managed  Lifecycle = iota // Datascape creates and operates it
	External                  // configures something that already exists; never deletes it
	Imported                  // discovered and adopted; behaves like Managed, creation never re-attempted
)

func (l Lifecycle) String() string {
	switch l {
	case Managed:
		return "Managed"
	case External:
		return "External"
	case Imported:
		return "Imported"
	}
	return fmt.Sprintf("Lifecycle(%d)", int(l))
}

// LifecycleOf computes lifecycle from spec markers and imported state.
// spec.external: true → External; imported flag (set by a prior `import` run,
// carried in state, passed by the caller) → Imported; otherwise Managed.
func LifecycleOf(e Envelope, imported bool) Lifecycle {
	if ext, ok := e.Spec["external"].(bool); ok && ext {
		return External
	}
	if imported {
		return Imported
	}
	return Managed
}

// Validate checks the envelope-level requirements shared by all kinds.
func (e Envelope) Validate() error {
	if e.APIVersion == "" {
		return fmt.Errorf("resource %q: apiVersion is required", e.Metadata.Name)
	}
	if e.Kind == "" {
		return fmt.Errorf("resource %q: kind is required", e.Metadata.Name)
	}
	if e.Metadata.Name == "" {
		return fmt.Errorf("%s: metadata.name is required", e.Kind)
	}
	return nil
}
