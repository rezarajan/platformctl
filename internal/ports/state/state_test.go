package state

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// TestKeyStringRoundTripsSpecialCharacters guards the structured, escaped
// state-key encoding (docs/planning/07 §0.1): names/namespaces/kinds
// containing characters that would otherwise corrupt a delimiter-based key
// must still round-trip exactly through KeyString/ParseKey.
func TestKeyStringRoundTripsSpecialCharacters(t *testing.T) {
	t.Parallel()
	cases := []resource.Key{
		{Namespace: "default", Kind: "Provider", Name: "with/slash"},
		{Namespace: "default", Kind: "Provider", Name: "with.dot"},
		{Namespace: "default", Kind: "Provider", Name: "with:colon"},
		{Namespace: "team/a", Kind: "Provider", Name: "foo"},
		{Namespace: "default", Kind: "Provider", Name: "café"},
		{Namespace: "default", Kind: "Provider", Name: strings.Repeat("a", 100)},
	}
	for _, k := range cases {
		encoded := KeyString(k)
		got := ParseKey(encoded)
		if got != k {
			t.Errorf("KeyString/ParseKey round-trip: got %+v, want %+v (encoded: %q)", got, k, encoded)
		}
	}
}

// TestKeyStringDistinctForDifferentNamespaces guards against the collision
// class two same-name resources in different namespaces would otherwise hit.
func TestKeyStringDistinctForDifferentNamespaces(t *testing.T) {
	t.Parallel()
	a := resource.Key{Namespace: "team-a", Kind: "Source", Name: "orders"}
	b := resource.Key{Namespace: "team-b", Kind: "Source", Name: "orders"}
	if KeyString(a) == KeyString(b) {
		t.Fatalf("KeyString collided for distinct namespaces: %q", KeyString(a))
	}
}

func TestParseKeyFallsBackToV1FormatForTwoPartKeys(t *testing.T) {
	t.Parallel()
	got := ParseKey("Provider/legacy")
	want := resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "legacy"}
	if got != want {
		t.Errorf("ParseKey(v1 format) = %+v, want %+v", got, want)
	}
}

func TestParseKeyEmptyNamespaceNormalizesToDefault(t *testing.T) {
	t.Parallel()
	// A key encoded with an empty namespace segment should still normalize,
	// matching Envelope/Key namespace-defaulting behavior elsewhere.
	got := ParseKey("/Provider/legacy")
	if got.Namespace != resource.DefaultNamespace {
		t.Errorf("ParseKey empty namespace segment = %q, want %q", got.Namespace, resource.DefaultNamespace)
	}
}
