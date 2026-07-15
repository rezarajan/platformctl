package docsgen

import (
	"strings"
	"testing"
)

func TestSiteRendersAllKindsAsHTML(t *testing.T) {
	site, err := Site()
	if err != nil {
		t.Fatal(err)
	}
	// A real HTML document with the search UI.
	for _, want := range []string{"<!doctype html>", `id="search"`, "doc-section"} {
		if !strings.Contains(site, want) {
			t.Errorf("site missing %q", want)
		}
	}
	// Every Kind has a section.
	for _, kind := range []string{"provider", "source", "eventstream", "binding", "dataset", "catalog", "connection", "secretreference"} {
		if !strings.Contains(site, `id="`+kind+`"`) {
			t.Errorf("site missing section for %s", kind)
		}
	}
	// Markdown tables became HTML tables (goldmark GFM), not left as pipes.
	if !strings.Contains(site, "<table>") || !strings.Contains(site, "<th>") {
		t.Error("markdown tables were not rendered to HTML")
	}
	// No raw markdown heading markers leaked into the body.
	if strings.Contains(site, "\n# ") {
		t.Error("raw markdown headings leaked into the HTML")
	}
}
