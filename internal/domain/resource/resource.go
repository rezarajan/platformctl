// Package resource defines the common envelope every manifest shares.
// See docs/planning/02-architecture.md §3.1.
package resource

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/status"
)

const DefaultNamespace = "default"

var dnsLabelPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type GroupVersionKind struct {
	APIVersion string `json:"apiVersion"` // e.g. "datascape.io/v1alpha1"
	Kind       string `json:"kind"`       // e.g. "EventStream"
}

type ObserverRef struct {
	Name      string `json:"name"` // must resolve to a Provider
	Namespace string `json:"namespace,omitempty"`
}

type Metadata struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace,omitempty"`
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

// Key uniquely identifies a resource: Namespace/Kind/Name.
type Key struct {
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
}

func (k Key) String() string {
	return NormalizeNamespace(k.Namespace) + "/" + k.Kind + "/" + k.Name
}

func (k Key) InNamespace(namespace string) Key {
	k.Namespace = NormalizeNamespace(namespace)
	return k
}

func (e Envelope) Key() Key {
	return Key{Namespace: NormalizeNamespace(e.Metadata.Namespace), Kind: e.Kind, Name: e.Metadata.Name}
}

type NameRef struct {
	Name      string
	Namespace string
}

func (r NameRef) NamespaceOr(defaultNamespace string) string {
	if r.Namespace != "" {
		return r.Namespace
	}
	return NormalizeNamespace(defaultNamespace)
}

func (r NameRef) Key(defaultNamespace, kind string) Key {
	return Key{Namespace: r.NamespaceOr(defaultNamespace), Kind: kind, Name: r.Name}
}

func NormalizeNamespace(namespace string) string {
	if namespace == "" {
		return DefaultNamespace
	}
	return namespace
}

func ValidateDNSLabel(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > 63 {
		return fmt.Errorf("%s %q is invalid: must be at most 63 characters", field, value)
	}
	if !dnsLabelPattern.MatchString(value) {
		return fmt.Errorf("%s %q is invalid: must match DNS label syntax (lowercase alphanumeric or '-', start/end alphanumeric)", field, value)
	}
	return nil
}

func RefFromSpec(spec map[string]any, field string) NameRef {
	ref, ok := spec[field].(map[string]any)
	if !ok {
		return NameRef{}
	}
	name, _ := ref["name"].(string)
	namespace, _ := ref["namespace"].(string)
	return NameRef{Name: name, Namespace: namespace}
}

func RefName(spec map[string]any, field string) string {
	return RefFromSpec(spec, field).Name
}

func RefNamespace(spec map[string]any, field string, defaultNamespace string) string {
	return RefFromSpec(spec, field).NamespaceOr(defaultNamespace)
}

func ParseSelector(selector, namespace string) (Key, error) {
	kind, name, ok := strings.Cut(selector, "/")
	if !ok || kind == "" || name == "" || strings.Contains(name, "/") {
		return Key{}, fmt.Errorf("selector must be <Kind>/<name>, got %q", selector)
	}
	if err := ValidateDNSLabel("nameRef.name", name); err != nil {
		return Key{}, err
	}
	ns := NormalizeNamespace(namespace)
	if err := ValidateDNSLabel("metadata.namespace", ns); err != nil {
		return Key{}, err
	}
	return Key{Namespace: ns, Kind: kind, Name: name}, nil
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
	if err := ValidateDNSLabel("metadata.name", e.Metadata.Name); err != nil {
		return fmt.Errorf("%s %q: %w", e.Kind, e.Metadata.Name, err)
	}
	if err := ValidateDNSLabel("metadata.namespace", NormalizeNamespace(e.Metadata.Namespace)); err != nil {
		return fmt.Errorf("%s %q: %w", e.Kind, e.Metadata.Name, err)
	}
	for _, obs := range e.Metadata.Observers {
		if err := ValidateDNSLabel("metadata.observers.name", obs.Name); err != nil {
			return fmt.Errorf("%s: %w", e.Key(), err)
		}
		if obs.Namespace != "" {
			if err := ValidateDNSLabel("metadata.observers.namespace", obs.Namespace); err != nil {
				return fmt.Errorf("%s: %w", e.Key(), err)
			}
		}
	}
	return nil
}
