package archview

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// TestGraphIDIsSafeUnquotedIdentifier guards docs/planning/07 §0.6: renderer
// ids must be usable *unquoted* in both DOT (alphanumerics/underscore only —
// a bare '-' is reserved for numeral signs) and Mermaid. base64 URL encoding
// legally emits '-' and would silently corrupt DOT output; hex does not.
var safeUnquotedID = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func TestGraphIDIsSafeUnquotedIdentifier(t *testing.T) {
	t.Parallel()
	keys := []resource.Key{
		{Namespace: "default", Kind: "Source", Name: "orders-db"},
		{Namespace: "team-a", Kind: "Provider", Name: "pg"},
		{Namespace: "default", Kind: "External", Name: "db.corp:5432/path?x=1"},
	}
	for _, k := range keys {
		id := graphID(k)
		if !safeUnquotedID.MatchString(id) {
			t.Errorf("graphID(%s) = %q, contains characters unsafe as an unquoted DOT/Mermaid id", k, id)
		}
	}
}

// TestGraphIDStableAndCollisionResistant: same key -> same id; different
// keys -> different ids (the encoding must stay injective after the
// base64->hex change).
func TestGraphIDStableAndCollisionResistant(t *testing.T) {
	t.Parallel()
	a := resource.Key{Namespace: "default", Kind: "Source", Name: "orders"}
	b := resource.Key{Namespace: "default", Kind: "Source", Name: "orders2"}
	if graphID(a) != graphID(a) { //nolint:staticcheck // SA4000: deliberate same-input-twice determinism check, not a copy-paste bug
		t.Fatal("graphID not stable for the same key")
	}
	if graphID(a) == graphID(b) {
		t.Fatalf("graphID collided for distinct keys: %q", graphID(a))
	}
}

// adversarialSet builds a manifest set whose Connection target (a raw,
// schema-unconstrained string) carries characters known to break naively
// escaped Mermaid/DOT output: quotes, backticks, braces, pipes, and an
// embedded newline via a Detail-bearing node.
func adversarialSet() []resource.Envelope {
	return []resource.Envelope{
		env("Provider", "edge", map[string]any{"type": "proxy"}),
		env("Connection", "weird", map[string]any{
			"providerRef": map[string]any{"name": "edge"},
			"port":        15999,
			"target":      `db"corp{a|b}<c>` + "`x`" + `:5432`,
		}),
	}
}

func TestRenderFormatsSurviveAdversarialCharacters(t *testing.T) {
	t.Parallel()
	v := Build(adversarialSet())
	for _, f := range []string{"tree", "dot", "mermaid", "json"} {
		var buf bytes.Buffer
		if err := v.Render(&buf, f); err != nil {
			t.Fatalf("render %s: %v", f, err)
		}
	}
}

func TestRenderJSONValidWithAdversarialCharacters(t *testing.T) {
	t.Parallel()
	v := Build(adversarialSet())
	var buf bytes.Buffer
	if err := v.Render(&buf, "json"); err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("json output with adversarial characters is not valid JSON: %v\n%s", err, buf.String())
	}
}

// TestRenderDOTQuotesAndEscapesAdversarialLabels: DOT labels must be
// double-quoted with embedded quotes escaped, and ids must never contain a
// bare '-' outside quotes.
func TestRenderDOTQuotesAndEscapesAdversarialLabels(t *testing.T) {
	t.Parallel()
	v := Build(adversarialSet())
	var buf bytes.Buffer
	if err := v.Render(&buf, "dot"); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "digraph") || strings.HasPrefix(line, "rankdir") ||
			strings.HasPrefix(line, "node [") || line == "}" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		id := fields[0]
		if !safeUnquotedID.MatchString(id) {
			t.Errorf("dot line has unsafe unquoted id %q: %s", id, line)
		}
	}
	// A raw, unescaped '"corp' (unescaped embedded quote) must not appear —
	// every embedded quote must be preceded by a backslash.
	if idx := strings.Index(out, `"corp`); idx > 0 && out[idx-1] != '\\' {
		t.Errorf("dot output has an unescaped embedded quote:\n%s", out)
	}
}

// TestRenderMermaidEscapesAdversarialLabels: pipe/quote/backslash/newline
// must be neutralized so they cannot break Mermaid edge/node syntax.
func TestRenderMermaidEscapesAdversarialLabels(t *testing.T) {
	t.Parallel()
	v := Build(adversarialSet())
	var buf bytes.Buffer
	if err := v.Render(&buf, "mermaid"); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "|b}") {
		t.Errorf("mermaid output contains an unescaped pipe that can break edge-label syntax:\n%s", out)
	}
}

func TestMermaidEscapeNeutralizesControlCharacters(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"a\"b":   `a#quot;b`,
		"a|b":    `a#124;b`,
		"a\nb":   `a<br/>b`,
		"a\r\nb": `a<br/>b`,
		`a\b`:    `a\\b`,
	}
	for in, want := range cases {
		if got := mermaidEscape(in); got != want {
			t.Errorf("mermaidEscape(%q) = %q, want %q", in, got, want)
		}
	}
}
