// Package docsgen renders the resource reference from the embedded JSON
// Schemas (schemas/) — every GA Kind and provider type documented with no
// manual doc-writing step (v1.0.0 DoD).
package docsgen

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/status"
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
			out[name] = renderKind(apiVersion, kind, sch) + renderFragmentsFor(kind)
			indexRows = append(indexRows, fmt.Sprintf("| [%s](%s) | `%s` | %s |", kind, name, apiVersion, str(firstParagraph(sch["description"]))))
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
	b.WriteString("\n\nSee also: [Condition & reason catalog](explain.md) — every Condition Type\n")
	b.WriteString("and Reason a resource's `status`/`drift` output can show, with meaning,\n")
	b.WriteString("likely causes, and remedies (`platformctl explain <token>` for the same\n")
	b.WriteString("content interactively).\n")
	out["index.md"] = b.String()
	out["explain.md"] = renderExplainCatalog()
	return out, nil
}

// renderExplainCatalog renders status.Catalog (docs/planning/08 E4) as a
// standalone reference page: `platformctl explain <token>` looks the same
// entries up interactively; this is the same content, browsable and
// searchable via `platformctl docs build --html`/`docs serve`. Grouped by
// CatalogEntry.Area in the order areas first appear in the catalog slice
// (ConditionTypes first, then each provider/area section — mirroring
// reasons.go's own section order, since the catalog was built from it).
func renderExplainCatalog() string {
	var areaOrder []string
	seen := map[string]bool{}
	byArea := map[string][]status.CatalogEntry{}
	for _, e := range status.Catalog {
		if !seen[e.Area] {
			seen[e.Area] = true
			areaOrder = append(areaOrder, e.Area)
		}
		byArea[e.Area] = append(byArea[e.Area], e)
	}

	var b strings.Builder
	b.WriteString("# Condition & reason catalog\n\n")
	b.WriteString("Generated from `internal/domain/status.Catalog` by `platformctl docs build` — do not\n")
	b.WriteString("edit by hand. The same content is available interactively via\n")
	b.WriteString("`platformctl explain <ConditionType|reason|error-token>`, which resolves an\n")
	b.WriteString("exact match first, then a case-insensitive prefix/substring fallback.\n\n")

	for _, area := range areaOrder {
		entries := byArea[area]
		fmt.Fprintf(&b, "## %s\n\n", area)
		for _, e := range entries {
			token := e.Token
			if e.Prefix {
				token += "*"
			}
			fmt.Fprintf(&b, "### `%s` (%s)\n\n%s\n\n", token, e.Kind, description(e.Meaning))
			if len(e.Causes) > 0 {
				b.WriteString("Likely causes:\n\n")
				for _, c := range e.Causes {
					fmt.Fprintf(&b, "- %s\n", c)
				}
				b.WriteString("\n")
			}
			if len(e.Remedies) > 0 {
				b.WriteString("Remedies:\n\n")
				for _, r := range e.Remedies {
					fmt.Fprintf(&b, "- %s\n", r)
				}
				b.WriteString("\n")
			}
		}
	}
	return b.String()
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
		fmt.Fprintf(&b, "- `%v`\n", v)
	}
	return b.String()
}

// renderFragmentsFor appends the provider-owned schema fragment reference
// (docs/planning/08 E5) for the Kinds that have one: Provider
// (spec.configuration, keyed by spec.type), Source/Catalog (spec.<engine>,
// keyed by spec.engine), Binding (spec.options, keyed by
// "<mode>-<providerType>"). Every other Kind gets no extra section.
func renderFragmentsFor(kind string) string {
	switch kind {
	case "Provider":
		return renderFragmentGroup(
			"Provider configuration reference (by `spec.type`)",
			"This table is generated from each provider's own JSON-Schema fragment (`schemas/v1alpha1/fragments/provider/`) — the shape-only rules enforced on `spec.configuration` at `validate` time, in addition to the cross-field rules a provider's `SpecValidator` still checks (docs/planning/08 E5).",
			schemas.ProviderConfigFragments, "spec.configuration")
	case "Source":
		return renderFragmentGroup(
			"Source engine reference (by `spec.engine`)",
			"This table is generated from each engine's own JSON-Schema fragment (`schemas/v1alpha1/fragments/source/`) — the shape of the `spec.<engine>` block (docs/planning/08 E5).",
			schemas.SourceEngineFragments, "spec.<engine>")
	case "Catalog":
		return renderFragmentGroup(
			"Catalog engine reference (by `spec.engine`)",
			"This table is generated from each engine's own JSON-Schema fragment (`schemas/v1alpha1/fragments/catalog/`) — the shape of the `spec.<engine>` block (docs/planning/08 E5).",
			schemas.CatalogEngineFragments, "spec.<engine>")
	case "Binding":
		return renderFragmentGroup(
			"Binding options reference (by `spec.mode` + provider type)",
			"This table is generated from each mode/provider pairing's own JSON-Schema fragment (`schemas/v1alpha1/fragments/binding/`) — the shape of the `spec.options` block once the realizing provider's type is known (docs/planning/08 E5). Only pairings with a registered fragment appear; every other provider's options are checked solely by its `BindingOptionsValidator` Go code, if it implements one.",
			schemas.BindingOptionsFragments, "spec.options")
	default:
		return ""
	}
}

// renderFragmentGroup renders one "## title" section with one "### <names>"
// subsection per distinct fragment file in fragments (paths shared by more
// than one discriminator — mysql/mariadb, s3/minio — render once, headed by
// every discriminator that names them, comma-joined).
func renderFragmentGroup(title, intro string, fragments map[string]string, blockPath string) string {
	if len(fragments) == 0 {
		return ""
	}
	byPath := map[string][]string{}
	for disc, path := range fragments {
		byPath[path] = append(byPath[path], disc)
	}
	paths := make([]string, 0, len(byPath))
	for path, discs := range byPath {
		sort.Strings(discs)
		byPath[path] = discs
		paths = append(paths, path)
	}
	sort.Slice(paths, func(i, j int) bool { return byPath[paths[i]][0] < byPath[paths[j]][0] })

	var b strings.Builder
	fmt.Fprintf(&b, "\n## %s\n\n%s\n\n", title, intro)
	for _, path := range paths {
		raw, err := schemas.FragmentFS.ReadFile(path)
		if err != nil {
			continue
		}
		var sch map[string]any
		if err := json.Unmarshal(raw, &sch); err != nil {
			continue
		}
		fmt.Fprintf(&b, "### %s\n\n", strings.Join(byPath[path], ", "))
		if desc := description(sch["description"]); desc != "" {
			fmt.Fprintf(&b, "%s\n\n", desc)
		}
		b.WriteString("| Field | Type | Required | Description |\n|---|---|---|---|\n")
		renderFields(&b, blockPath, sch)
		b.WriteString("\n")
	}
	return b.String()
}

func renderKind(apiVersion, kind string, sch map[string]any) string {
	var b strings.Builder
	// The Kind header is prose, not a table cell, so a multi-paragraph
	// schema description (\n\n-separated) renders as real markdown
	// paragraphs here — unlike str(), used everywhere a description lands
	// in a table cell, where an embedded newline would break the table.
	fmt.Fprintf(&b, "# %s\n\n`%s`\n\n%s\n\n", kind, apiVersion, description(sch["description"]))
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

// description renders a top-level Kind description as prose: unlike str()
// (used for table cells, where a raw newline would break the table), a
// schema description may use "\n\n" for real paragraph breaks.
func description(v any) string {
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

// firstParagraph returns just the first "\n\n"-delimited paragraph of a
// description, for the index table's one-line summary column — a
// multi-paragraph description (e.g. SecretReference's rotation-behavior
// notes) would otherwise blow out that row.
func firstParagraph(v any) string {
	s, _ := v.(string)
	first, _, _ := strings.Cut(strings.TrimSpace(s), "\n\n")
	return first
}
