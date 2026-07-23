package resource

import (
	"strings"
	"testing"
)

func TestValidateDNSLabelRejectsInvalidNames(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"slash", "a/b"},
		{"dot", "a.b"},
		{"colon", "a:b"},
		{"uppercase", "Abc"},
		{"underscore", "a_b"},
		{"leading-hyphen", "-abc"},
		{"trailing-hyphen", "abc-"},
		{"space", "a b"},
		{"unicode", "café"},
		{"too-long", strings.Repeat("a", 64)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateDNSLabel("metadata.name", tc.value); err == nil {
				t.Errorf("ValidateDNSLabel(%q) = nil, want error", tc.value)
			}
		})
	}
}

func TestValidateDNSLabelAcceptsValidNames(t *testing.T) {
	t.Parallel()
	cases := []string{
		"a",
		"abc",
		"abc-def",
		"a1-b2-c3",
		strings.Repeat("a", 63),
	}
	for _, v := range cases {
		if err := ValidateDNSLabel("metadata.name", v); err != nil {
			t.Errorf("ValidateDNSLabel(%q) = %v, want nil", v, err)
		}
	}
}

func TestEnvelopeValidateRejectsSlashDotColonNames(t *testing.T) {
	t.Parallel()
	cases := []string{"a/b", "a.b", "a:b", "a b", "café", strings.Repeat("a", 64)}
	for _, name := range cases {
		e := Envelope{
			GroupVersionKind: GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
			Metadata:         Metadata{Name: name},
		}
		if err := e.Validate(); err == nil {
			t.Errorf("Envelope{Name: %q}.Validate() = nil, want error", name)
		}
	}
}

func TestEnvelopeValidateRejectsInvalidNamespace(t *testing.T) {
	t.Parallel()
	e := Envelope{
		GroupVersionKind: GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
		Metadata:         Metadata{Name: "ok", Namespace: "Bad_NS"},
	}
	if err := e.Validate(); err == nil {
		t.Error("Validate() = nil, want error for invalid namespace")
	}
}

func TestEnvelopeValidateRejectsInvalidObserverNames(t *testing.T) {
	t.Parallel()
	e := Envelope{
		GroupVersionKind: GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
		Metadata: Metadata{
			Name:      "ok",
			Observers: []ObserverRef{{Name: "bad/name"}},
		},
	}
	if err := e.Validate(); err == nil {
		t.Error("Validate() = nil, want error for invalid observer name")
	}
}

func TestEnvelopeValidateRejectsInvalidObserverNamespace(t *testing.T) {
	t.Parallel()
	e := Envelope{
		GroupVersionKind: GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
		Metadata: Metadata{
			Name:      "ok",
			Observers: []ObserverRef{{Name: "obs", Namespace: "Bad_NS"}},
		},
	}
	if err := e.Validate(); err == nil {
		t.Error("Validate() = nil, want error for invalid observer namespace")
	}
}

// TestKeySameNameDifferentNamespaceDoNotCollide guards the namespaced
// identity policy (docs/planning/07 §0.1): two resources with the same
// kind+name in different namespaces must produce distinct keys and distinct
// serialized forms.
// TestValidateLabelKeyRejectsInvalid and its value/Envelope counterparts are
// docs/planning/08 K1's fixtures: metadata.labels keys/values validated to
// the Kubernetes label grammar at validate time (docs/adr/033 decision 2).
func TestValidateLabelKeyRejectsInvalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		key  string
	}{
		{"empty", ""},
		{"space", "a b"},
		{"leading-hyphen", "-abc"},
		{"trailing-dot", "abc."},
		{"too-long-name", strings.Repeat("a", 64)},
		{"empty-name-with-prefix", "example.com/"},
		{"invalid-prefix-uppercase", "Example.com/tier"},
		{"invalid-prefix-too-long", strings.Repeat("a", 254) + ".com/tier"},
		{"two-slashes", "example.com/tier/extra"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateLabelKey(tc.key); err == nil {
				t.Errorf("ValidateLabelKey(%q) = nil, want error", tc.key)
			}
		})
	}
}

func TestValidateLabelKeyAcceptsValid(t *testing.T) {
	t.Parallel()
	cases := []string{
		"tier",
		"clearance",
		"tier-gold",
		"tier.gold",
		"tier_gold",
		"a1",
		"example.com/tier",
		"kubernetes.io/managed-by",
		strings.Repeat("a", 63),
	}
	for _, key := range cases {
		if err := ValidateLabelKey(key); err != nil {
			t.Errorf("ValidateLabelKey(%q) = %v, want nil", key, err)
		}
	}
}

func TestValidateLabelValueRejectsInvalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		value string
	}{
		{"leading-hyphen", "-gold"},
		{"trailing-underscore", "gold_"},
		{"space", "a b"},
		{"too-long", strings.Repeat("a", 64)},
		{"slash", "a/b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateLabelValue(tc.value); err == nil {
				t.Errorf("ValidateLabelValue(%q) = nil, want error", tc.value)
			}
		})
	}
}

func TestValidateLabelValueAcceptsValid(t *testing.T) {
	t.Parallel()
	cases := []string{"", "gold", "tier-1", "a.b_c", strings.Repeat("a", 63)}
	for _, v := range cases {
		if err := ValidateLabelValue(v); err != nil {
			t.Errorf("ValidateLabelValue(%q) = %v, want nil", v, err)
		}
	}
}

// TestEnvelopeValidateRejectsInvalidLabels is the negative fixture: an
// invalid label key/value at the Envelope.Validate() level (not just the
// standalone helpers), naming the offending key and the resource Kind/name.
func TestEnvelopeValidateRejectsInvalidLabels(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		labels map[string]string
	}{
		{"bad-key", map[string]string{"Bad Key": "gold"}},
		{"bad-value", map[string]string{"tier": "-gold"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := Envelope{
				GroupVersionKind: GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
				Metadata:         Metadata{Name: "ok", Labels: tc.labels},
			}
			err := e.Validate()
			if err == nil {
				t.Fatal("Validate() = nil, want error for invalid label")
			}
			if !strings.Contains(err.Error(), "Provider") || !strings.Contains(err.Error(), "ok") {
				t.Errorf("Validate() error %q must name the resource Kind and name", err)
			}
		})
	}
}

// TestEnvelopeValidateAcceptsValidLabels is the positive fixture: a
// well-formed metadata.labels map (including a prefixed key) never fails
// validation.
func TestEnvelopeValidateAcceptsValidLabels(t *testing.T) {
	t.Parallel()
	e := Envelope{
		GroupVersionKind: GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: "Provider"},
		Metadata: Metadata{
			Name:   "ok",
			Labels: map[string]string{"tier": "gold", "example.com/clearance": "gold-1"},
		},
	}
	if err := e.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil for well-formed labels", err)
	}
}

func TestKeySameNameDifferentNamespaceDoNotCollide(t *testing.T) {
	t.Parallel()
	a := Key{Namespace: "team-a", Kind: "Source", Name: "orders"}
	b := Key{Namespace: "team-b", Kind: "Source", Name: "orders"}
	if a == b {
		t.Fatal("keys with different namespaces compared equal")
	}
	if a.String() == b.String() {
		t.Fatalf("keys with different namespaces serialized identically: %q", a.String())
	}
}

// TestKeySameNamespaceSameNameDifferentKindDoNotCollide guards against
// cross-kind ambiguity (docs/planning/07 §0.2): Key includes Kind, so
// Provider/foo and Source/foo in the same namespace are distinct.
func TestKeySameNamespaceSameNameDifferentKindDoNotCollide(t *testing.T) {
	t.Parallel()
	a := Key{Namespace: DefaultNamespace, Kind: "Provider", Name: "foo"}
	b := Key{Namespace: DefaultNamespace, Kind: "Source", Name: "foo"}
	if a == b {
		t.Fatal("keys with different kinds compared equal")
	}
}

func TestKeyInNamespaceNormalizesEmpty(t *testing.T) {
	t.Parallel()
	k := Key{Kind: "Provider", Name: "foo"}.InNamespace("")
	if k.Namespace != DefaultNamespace {
		t.Errorf("InNamespace(\"\") = %q, want %q", k.Namespace, DefaultNamespace)
	}
}

func TestEnvelopeKeyNormalizesEmptyNamespace(t *testing.T) {
	t.Parallel()
	e := Envelope{
		GroupVersionKind: GroupVersionKind{Kind: "Provider"},
		Metadata:         Metadata{Name: "foo"},
	}
	if got := e.Key().Namespace; got != DefaultNamespace {
		t.Errorf("Envelope{}.Key().Namespace = %q, want %q", got, DefaultNamespace)
	}
}

func TestParseSelectorRejectsMalformedSelectors(t *testing.T) {
	t.Parallel()
	cases := []string{"", "NoSlash", "Kind/", "/name", "Kind/a/b"}
	for _, sel := range cases {
		if _, err := ParseSelector(sel, ""); err == nil {
			t.Errorf("ParseSelector(%q) = nil, want error", sel)
		}
	}
}

func TestParseSelectorRejectsInvalidNameOrNamespace(t *testing.T) {
	t.Parallel()
	if _, err := ParseSelector("Kind/Bad_Name", ""); err == nil {
		t.Error("ParseSelector with invalid name = nil, want error")
	}
	if _, err := ParseSelector("Kind/ok", "Bad_NS"); err == nil {
		t.Error("ParseSelector with invalid namespace = nil, want error")
	}
}

func TestParseSelectorAcceptsValidSelector(t *testing.T) {
	t.Parallel()
	k, err := ParseSelector("Source/orders", "team-a")
	if err != nil {
		t.Fatalf("ParseSelector: %v", err)
	}
	want := Key{Namespace: "team-a", Kind: "Source", Name: "orders"}
	if k != want {
		t.Errorf("ParseSelector() = %+v, want %+v", k, want)
	}
}

func TestNameRefKeyDefaultsNamespace(t *testing.T) {
	t.Parallel()
	ref := NameRef{Name: "orders"}
	k := ref.Key("default", "Source")
	if k.Namespace != DefaultNamespace {
		t.Errorf("NameRef.Key() namespace = %q, want %q", k.Namespace, DefaultNamespace)
	}
	ref2 := NameRef{Name: "orders", Namespace: "team-a"}
	k2 := ref2.Key("default", "Source")
	if k2.Namespace != "team-a" {
		t.Errorf("NameRef.Key() namespace = %q, want %q", k2.Namespace, "team-a")
	}
}
