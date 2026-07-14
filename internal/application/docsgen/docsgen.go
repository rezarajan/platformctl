// Package docsgen renders the resource reference from the embedded JSON
// Schemas (schemas/) — every GA Kind and provider type documented with no
// manual doc-writing step (v1.0.0 DoD).
package docsgen

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/schemas"
)

// Build renders one markdown document per Kind plus an index, keyed by
// file name (e.g. "provider.md", "index.md").
func Build() (map[string]string, error) {
	out := make(map[string]string)
	var indexRows []string

	for apiVersion, files := range schemas.KindFiles {
		kinds := make([]string, 0, len(files))
		for kind := range files {
			kinds = append(kinds, kind)
		}
		sort.Strings(kinds)

		for _, kind := range kinds {
			raw, err := schemas.FS.ReadFile(files[kind])
			if err != nil {
				return nil, err
			}
			var sch map[string]any
			if err := json.Unmarshal(raw, &sch); err != nil {
				return nil, fmt.Errorf("parse %s: %w", files[kind], err)
			}
			name := strings.ToLower(kind) + ".md"
			out[name] = renderKind(apiVersion, kind, sch)
			indexRows = append(indexRows, fmt.Sprintf("| [%s](%s) | `%s` | %s |", kind, name, apiVersion, str(sch["description"])))
		}
	}

	var b strings.Builder
	b.WriteString("# Datascape resource reference\n\n")
	b.WriteString("Generated from `schemas/` by `platformctl docs build` — do not edit by hand.\n\n")
	b.WriteString("| Kind | apiVersion | Description |\n|---|---|---|\n")
	sort.Strings(indexRows)
	b.WriteString(strings.Join(indexRows, "\n"))
	b.WriteString("\n\n## Provider types\n\n")
	b.WriteString(providerTypes())
	out["index.md"] = b.String()
	return out, nil
}

func providerTypes() string {
	raw, err := schemas.FS.ReadFile(schemas.KindFiles["datascape.io/v1alpha1"]["Provider"])
	if err != nil {
		return ""
	}
	var sch map[string]any
	if err := json.Unmarshal(raw, &sch); err != nil {
		return ""
	}
	typeProp := dig(sch, "properties", "spec", "properties", "type")
	known, _ := typeProp["x-known-values"].([]any)
	var b strings.Builder
	b.WriteString(str(typeProp["description"]) + "\n\n")
	for _, v := range known {
		b.WriteString(fmt.Sprintf("- `%v`\n", v))
	}
	return b.String()
}

func renderKind(apiVersion, kind string, sch map[string]any) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n`%s`\n\n%s\n\n", kind, apiVersion, str(sch["description"]))
	b.WriteString("| Field | Type | Required | Description |\n|---|---|---|---|\n")
	fmt.Fprintf(&b, "| `metadata.name` | string | yes | Unique per Kind within a manifest set. |\n")
	fmt.Fprintf(&b, "| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |\n")
	spec := dig(sch, "properties", "spec")
	renderFields(&b, "spec", spec)
	return b.String()
}

func renderFields(b *strings.Builder, prefix string, node map[string]any) {
	if node == nil {
		return
	}
	required := map[string]bool{}
	if reqs, ok := node["required"].([]any); ok {
		for _, r := range reqs {
			if s, ok := r.(string); ok {
				required[s] = true
			}
		}
	}
	props, _ := node["properties"].(map[string]any)
	names := make([]string, 0, len(props))
	for name := range props {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		prop, _ := props[name].(map[string]any)
		path := prefix + "." + name
		typ := fieldType(prop)
		req := "no"
		if required[name] {
			req = "yes"
		}
		fmt.Fprintf(b, "| `%s` | %s | %s | %s |\n", path, typ, req, str(prop["description"]))
		if typ == "object" {
			renderFields(b, path, prop)
		}
	}
	if ap, ok := node["additionalProperties"].(map[string]any); ok {
		fmt.Fprintf(b, "| `%s.<other>` | %s | no | %s |\n", prefix, fieldType(ap), str(ap["description"]))
	}
}

func fieldType(prop map[string]any) string {
	if ref, ok := prop["$ref"].(string); ok {
		if strings.Contains(ref, "nameRef") {
			return "object `{name}`"
		}
		return "object"
	}
	if enum, ok := prop["enum"].([]any); ok {
		parts := make([]string, len(enum))
		for i, e := range enum {
			parts[i] = fmt.Sprintf("`%v`", e)
		}
		return strings.Join(parts, " \\| ")
	}
	if t, ok := prop["type"].(string); ok {
		if t == "array" {
			if items, ok := prop["items"].(map[string]any); ok {
				return "array of " + fieldType(items)
			}
			return "array"
		}
		return t
	}
	return "any"
}

func dig(m map[string]any, path ...string) map[string]any {
	cur := m
	for _, p := range path {
		next, ok := cur[p].(map[string]any)
		if !ok {
			return nil
		}
		cur = next
	}
	return cur
}

func str(v any) string {
	s, _ := v.(string)
	return strings.ReplaceAll(s, "\n", " ")
}
