package archview

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func env(kind, name string, spec map[string]any, observers ...string) resource.Envelope {
	e := resource.Envelope{}
	e.APIVersion = "datascape.io/v1alpha1"
	e.Kind = kind
	e.Metadata.Name = name
	e.Spec = spec
	for _, o := range observers {
		e.Metadata.Observers = append(e.Metadata.Observers, resource.ObserverRef{Name: o})
	}
	return e
}

func cdcSet() []resource.Envelope {
	return []resource.Envelope{
		env("Provider", "pg", map[string]any{"type": "postgres"}),
		env("Provider", "rp", map[string]any{"type": "redpanda"}),
		env("Provider", "dbz", map[string]any{"type": "debezium"}),
		env("Provider", "marquez", map[string]any{"type": "openlineage"}),
		env("Source", "students", map[string]any{"engine": "postgres", "providerRef": map[string]any{"name": "pg"}}),
		env("EventStream", "events", map[string]any{"providerRef": map[string]any{"name": "rp"}}),
		env("Binding", "s-to-e", map[string]any{
			"mode":        "cdc",
			"sourceRef":   map[string]any{"name": "students"},
			"targetRef":   map[string]any{"name": "events"},
			"providerRef": map[string]any{"name": "dbz"},
		}, "marquez"),
	}
}

// TestPipelineEdgesFollowDataFlow: a Binding renders as a source→target
// pipeline edge (the actual data movement), not a reverse-dependency hub.
func TestPipelineEdgesFollowDataFlow(t *testing.T) {
	v := Build(cdcSet())

	var pipeline *Edge
	for i := range v.Edges {
		if v.Edges[i].Kind == Pipeline {
			pipeline = &v.Edges[i]
		}
	}
	if pipeline == nil {
		t.Fatal("no pipeline edge produced for a cdc Binding")
	}
	if pipeline.From.Name != "students" || pipeline.To.Name != "events" {
		t.Errorf("pipeline direction = %s→%s, want students→events", pipeline.From, pipeline.To)
	}
	if !strings.Contains(pipeline.Label, "cdc") || !strings.Contains(pipeline.Label, "dbz") {
		t.Errorf("pipeline label = %q, want mode+provider", pipeline.Label)
	}
	if len(pipeline.Observers) != 1 || pipeline.Observers[0] != "marquez" {
		t.Errorf("pipeline observers = %v, want [marquez]", pipeline.Observers)
	}

	// Realization: provider→asset edges exist for the assets.
	realized := map[string]bool{}
	for _, e := range v.Edges {
		if e.Kind == Realizes {
			realized[e.To.String()] = true
		}
	}
	for _, want := range []string{"Source/students", "EventStream/events"} {
		if !realized[want] {
			t.Errorf("no realization edge for %s", want)
		}
	}
}

func TestRenderFormats(t *testing.T) {
	v := Build(cdcSet())
	for _, f := range []string{"tree", "dot", "mermaid", "json"} {
		var buf bytes.Buffer
		if err := v.Render(&buf, f); err != nil {
			t.Errorf("render %s: %v", f, err)
		}
		if buf.Len() == 0 {
			t.Errorf("render %s produced no output", f)
		}
	}
	// json must be valid.
	var buf bytes.Buffer
	if err := v.Render(&buf, "json"); err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Errorf("json output is not valid JSON: %v", err)
	}
	// dot must be balanced.
	buf.Reset()
	_ = v.Render(&buf, "dot")
	if !strings.HasPrefix(buf.String(), "digraph") || !strings.Contains(buf.String(), "}") {
		t.Errorf("dot output malformed:\n%s", buf.String())
	}
	// unknown format errors.
	if err := v.Render(&bytes.Buffer{}, "xml"); err == nil {
		t.Error("unknown format did not error")
	}
}

// TestConnectionTargetVisible: a managed Connection's external target shows
// as a synthetic node so the real system is visible in the picture.
func TestConnectionTargetVisible(t *testing.T) {
	set := []resource.Envelope{
		env("Provider", "edge", map[string]any{"type": "proxy"}),
		env("Connection", "orders-db", map[string]any{
			"providerRef": map[string]any{"name": "edge"},
			"port":        15999,
			"target":      "db.corp:5432",
		}),
	}
	v := Build(set)
	var found bool
	for _, n := range v.Nodes {
		if n.Kind == "External" && n.Key.Name == "db.corp:5432" {
			found = true
		}
	}
	if !found {
		t.Error("Connection target not rendered as an external node")
	}
}
