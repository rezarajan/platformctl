// Package resource defines the common envelope every manifest shares.
// See docs/planning/02-architecture.md §3.1.
package resource

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/status"
)

const DefaultNamespace = "default"

// DefaultDomain is metadata.domain's default value (docs/adr/022 Ring 0/1):
// a resource that never declares a domain lives in this one, implicit
// domain — the exact state every manifest set was already in before
// domains existed, which is what keeps an undeclared-domain manifest set a
// byte-identical no-op (docs/planning/08 H5).
const DefaultDomain = "default"

var dnsLabelPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// labelSegmentPattern is one Kubernetes label-key/value segment: alphanumeric,
// '-', '_', '.', starting and ending alphanumeric (K8s label-value/label-key
// name-segment grammar; see labelPrefixPattern for the key's optional
// DNS-subdomain prefix half).
var labelSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$`)

// labelPrefixPattern is a label key's optional prefix: a DNS subdomain
// (lowercase alphanumeric segments separated by '.', each segment optionally
// hyphenated internally) — the same grammar as a Kubernetes annotation/label
// key prefix (e.g. "example.com/tier").
var labelPrefixPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

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
	// Protect refuses any plan/apply/destroy action that would delete this
	// resource. See docs/planning/03-resource-model-reference.md §2.
	Protect bool `json:"protect,omitempty"`
	// Domain is the resource's governance/segmentation domain (docs/adr/022,
	// docs/planning/08 H5) — additive, defaults to DefaultDomain when unset.
	// See docs/planning/03-resource-model-reference.md §2.
	Domain string `json:"domain,omitempty"`
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

// NormalizeDomain mirrors NormalizeNamespace for metadata.domain (docs/adr/022).
func NormalizeDomain(domain string) string {
	if domain == "" {
		return DefaultDomain
	}
	return domain
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

// ValidateLabelKey validates one metadata.labels key against the Kubernetes
// label-key grammar (docs/planning/08 K1, docs/adr/033 decision 2): an
// optional DNS-subdomain prefix (<=253 chars) followed by '/', then a name
// segment (<=63 chars; alphanumeric, '-', '_', '.'; must start and end
// alphanumeric). This is the same grammar Kubernetes itself enforces on
// object labels — labels already flow through to runtime labels on every
// runtime (docker, kubernetes), so a value only one runtime happens to
// reject is exactly the docs/adr/030 failure class this closes off at
// validate time instead.
func ValidateLabelKey(key string) error {
	prefix, name, hasSlash := strings.Cut(key, "/")
	if !hasSlash {
		name, prefix = prefix, ""
	}
	if prefix != "" {
		if len(prefix) > 253 {
			return fmt.Errorf("label key prefix %q is invalid: must be at most 253 characters", prefix)
		}
		if !labelPrefixPattern.MatchString(prefix) {
			return fmt.Errorf("label key prefix %q is invalid: must be a lowercase DNS subdomain", prefix)
		}
	}
	if name == "" {
		return fmt.Errorf("label key %q is invalid: name segment is required", key)
	}
	if len(name) > 63 {
		return fmt.Errorf("label key %q is invalid: name segment must be at most 63 characters", key)
	}
	if !labelSegmentPattern.MatchString(name) {
		return fmt.Errorf("label key %q is invalid: name segment must be alphanumeric, '-', '_', or '.', and start/end alphanumeric", key)
	}
	return nil
}

// ValidateLabelValue validates one metadata.labels value against the
// Kubernetes label-value grammar (docs/planning/08 K1): empty is valid;
// otherwise <=63 chars, alphanumeric/'-'/'_'/'.', starting and ending
// alphanumeric.
func ValidateLabelValue(value string) error {
	if value == "" {
		return nil
	}
	if len(value) > 63 {
		return fmt.Errorf("label value %q is invalid: must be at most 63 characters", value)
	}
	if !labelSegmentPattern.MatchString(value) {
		return fmt.Errorf("label value %q is invalid: must be alphanumeric, '-', '_', or '.', and start/end alphanumeric", value)
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
	if err := ValidateDNSLabel("metadata.domain", NormalizeDomain(e.Metadata.Domain)); err != nil {
		return fmt.Errorf("%s %q: %w", e.Kind, e.Metadata.Name, err)
	}
	if len(e.Metadata.Labels) > 0 {
		keys := make([]string, 0, len(e.Metadata.Labels))
		for k := range e.Metadata.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := ValidateLabelKey(k); err != nil {
				return fmt.Errorf("%s %q: metadata.labels: %w", e.Kind, e.Metadata.Name, err)
			}
			if err := ValidateLabelValue(e.Metadata.Labels[k]); err != nil {
				return fmt.Errorf("%s %q: metadata.labels[%q]: %w", e.Kind, e.Metadata.Name, k, err)
			}
		}
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
