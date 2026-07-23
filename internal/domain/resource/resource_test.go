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
