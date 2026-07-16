package archview

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// Render writes the view in the named format: tree (default, human
// readable), dot (Graphviz), mermaid, or json.
func (v *View) Render(w io.Writer, format string) error {
	switch format {
	case "", "table", "tree":
		return v.renderTree(w)
	case "dot":
		return v.renderDOT(w)
	case "mermaid":
		return v.renderMermaid(w)
	case "json":
		return v.renderJSON(w)
	default:
		return fmt.Errorf("unknown graph format %q (allowed: tree, dot, mermaid, json)", format)
	}
}

func (v *View) renderJSON(w io.Writer) error {
	type edge struct {
		From, To, Kind, Label string
	}
	out := struct {
		Nodes []Node `json:"nodes"`
		Edges []edge `json:"edges"`
	}{Nodes: v.Nodes}
	for _, e := range v.Edges {
		out.Edges = append(out.Edges, edge{e.From.String(), e.To.String(), string(e.Kind), e.Label})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func (v *View) renderDOT(w io.Writer) error {
	fmt.Fprintln(w, "digraph platform {")
	fmt.Fprintln(w, "  rankdir=LR;")
	fmt.Fprintln(w, "  node [shape=box, style=rounded];")
	// Group Providers on one rank so the technology layer reads as a band.
	for _, n := range v.Nodes {
		attrs := fmt.Sprintf("label=%q", nodeLabel(n))
		switch n.Kind {
		case "Provider":
			attrs += ", style=\"rounded,filled\", fillcolor=\"#e8eef7\""
		case "External":
			attrs += ", style=\"rounded,dashed\", shape=note"
		}
		if n.Lifecycle == "External" {
			attrs += ", style=\"rounded,dashed\""
		}
		fmt.Fprintf(w, "  %s [%s];\n", graphID(n.Key), attrs)
	}
	for _, e := range v.Edges {
		attrs := ""
		switch e.Kind {
		case Pipeline:
			attrs = fmt.Sprintf("label=%q, penwidth=2, color=\"#1f4e79\"", e.Label)
		case Realizes:
			attrs = "style=dashed, color=\"#888888\", arrowhead=empty"
		case Observes:
			attrs = "style=dotted, color=\"#77aa66\", label=\"observes\""
		case Reaches:
			attrs = "style=dashed, color=\"#b57\""
			if e.Label != "" {
				attrs += fmt.Sprintf(", label=%q", e.Label)
			}
		}
		fmt.Fprintf(w, "  %s -> %s [%s];\n", graphID(e.From), graphID(e.To), attrs)
	}
	fmt.Fprintln(w, "}")
	return nil
}

func (v *View) renderMermaid(w io.Writer) error {
	fmt.Fprintln(w, "flowchart LR")
	for _, n := range v.Nodes {
		id := graphID(n.Key)
		label := mermaidEscape(nodeLabel(n))
		switch n.Kind {
		case "Provider":
			fmt.Fprintf(w, "  %s([\"%s\"])\n", id, label)
		case "External":
			fmt.Fprintf(w, "  %s[/%s/]\n", id, label)
		default:
			fmt.Fprintf(w, "  %s[\"%s\"]\n", id, label)
		}
	}
	for _, e := range v.Edges {
		from, to := graphID(e.From), graphID(e.To)
		switch e.Kind {
		case Pipeline:
			fmt.Fprintf(w, "  %s ==>|%s| %s\n", from, mermaidEscape(e.Label), to)
		case Realizes:
			fmt.Fprintf(w, "  %s -.realizes.-> %s\n", from, to)
		case Observes:
			fmt.Fprintf(w, "  %s -.observes.-> %s\n", from, to)
		case Reaches:
			if e.Label != "" {
				fmt.Fprintf(w, "  %s -.%s.-> %s\n", from, mermaidEscape(e.Label), to)
			} else {
				fmt.Fprintf(w, "  %s -.-> %s\n", from, to)
			}
		}
	}
	return nil
}

// renderTree prints a human-readable architecture summary: the data-flow
// pipelines first (the point of the platform), then the technology layer.
func (v *View) renderTree(w io.Writer) error {
	pipelines := v.edgesOf(Pipeline)
	realizes := map[resource.Key][]resource.Key{}
	for _, e := range v.edgesOf(Realizes) {
		realizes[e.From] = append(realizes[e.From], e.To)
	}
	// Which Connection each asset reaches through (external integration).
	reachesConn := map[resource.Key]resource.Key{}
	for _, e := range v.edgesOf(Reaches) {
		if e.To.Kind == "Connection" {
			reachesConn[e.From] = e.To
		}
	}

	fmt.Fprintln(w, "DATA FLOW")
	if len(pipelines) == 0 {
		fmt.Fprintln(w, "  (no bindings — no data movement declared)")
	}
	for _, e := range pipelines {
		line := fmt.Sprintf("  %s ──[%s]──▶ %s", e.From.String(), e.Label, e.To.String())
		if len(e.Observers) > 0 {
			line += "   (lineage → " + strings.Join(e.Observers, ", ") + ")"
		}
		fmt.Fprintln(w, line)
	}

	fmt.Fprintln(w, "\nTECHNOLOGY LAYER  (provider ─realizes→ asset)")
	provs := make([]resource.Key, 0, len(realizes))
	for p := range realizes {
		provs = append(provs, p)
	}
	sort.Slice(provs, func(i, j int) bool { return provs[i].String() < provs[j].String() })
	for _, p := range provs {
		fmt.Fprintf(w, "  %s%s\n", p.String(), detailSuffix(v, p))
		assets := realizes[p]
		sort.Slice(assets, func(i, j int) bool { return assets[i].String() < assets[j].String() })
		for _, a := range assets {
			fmt.Fprintf(w, "    └─ %s%s\n", a.String(), detailSuffix(v, a))
		}
	}

	// External access: assets reached through a managed Connection — the
	// stable entrypoints you point external tools and CDC connectors at.
	if len(reachesConn) > 0 {
		fmt.Fprintln(w, "\nEXTERNAL ACCESS  (asset ─reached through→ Connection → real system)")
		froms := make([]resource.Key, 0, len(reachesConn))
		for f := range reachesConn {
			froms = append(froms, f)
		}
		sort.Slice(froms, func(i, j int) bool { return froms[i].String() < froms[j].String() })
		for _, f := range froms {
			conn := reachesConn[f]
			fmt.Fprintf(w, "  %s ─▶ %s%s\n", f.String(), conn.String(), detailSuffix(v, conn))
		}
	}

	// Providers that realize nothing (e.g. lineage backends) still matter.
	realizedProv := map[resource.Key]bool{}
	for p := range realizes {
		realizedProv[p] = true
	}
	var standalone []Node
	for _, n := range v.Nodes {
		if n.Kind == "Provider" && !realizedProv[n.Key] {
			standalone = append(standalone, n)
		}
	}
	if len(standalone) > 0 {
		fmt.Fprintln(w, "\nSTANDALONE PROVIDERS")
		for _, n := range standalone {
			fmt.Fprintf(w, "  %s (%s)\n", n.Key.String(), n.Detail)
		}
	}
	return nil
}

func (v *View) edgesOf(kind EdgeKind) []Edge {
	var out []Edge
	for _, e := range v.Edges {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func detailSuffix(v *View, k resource.Key) string {
	for _, n := range v.Nodes {
		if n.Key == k && n.Detail != "" {
			return "  (" + n.Detail + ")"
		}
	}
	return ""
}

func nodeLabel(n Node) string {
	label := n.Key.String()
	if n.Detail != "" {
		label += "\n" + n.Detail
	}
	return label
}

// graphID derives a renderer id from the full resource key. It must stay
// safe as an *unquoted* identifier in every renderer: DOT ids only allow
// alphanumerics/underscore (a bare '-' is reserved for numeral signs) and
// Mermaid ids are similarly restrictive, so this uses hex rather than
// base64 — collision-resistant like base64, but restricted to [0-9a-f].
func graphID(k resource.Key) string {
	return "n_" + hex.EncodeToString([]byte(k.String()))
}

func mermaidEscape(s string) string {
	repl := strings.NewReplacer(
		"\\", "\\\\",
		"\"", "#quot;",
		"\r\n", "<br/>",
		"\n", "<br/>",
		"|", "#124;",
	)
	return repl.Replace(s)
}
