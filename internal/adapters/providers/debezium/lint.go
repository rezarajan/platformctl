package debezium

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

var _ reconciler.DesignLinter = (*Provider)(nil)

// Design-lint codes this provider contributes (docs/adr/020-design-lints.md
// §5), namespaced DL-debezium-NNN.
const (
	// LintCodeReplicationSlotPressure: N cdc Bindings, each realized by a
	// debezium-typed Provider, capture from Source resources all backed by
	// the same physical Postgres Provider — each Binding is a separate
	// Debezium connector, and each connector opens its own logical
	// replication slot against that one server, independent of whether
	// DL001's table-overlap condition holds (different Source resources,
	// so DL001's "share a sourceRef" never fires here).
	LintCodeReplicationSlotPressure = "DL-debezium-001"
	// LintCodeOverlappingPatternCapture: two cdc Bindings on the same
	// sourceRef whose options.tables entries overlap only once Debezium's
	// own table.include.list regex semantics are applied — DL001's
	// generic, technology-agnostic form only compares literal strings (or
	// treats an entirely-unset list as "all"), so a pattern like "ord.*"
	// overlapping a literal "orders" is invisible to it.
	LintCodeOverlappingPatternCapture = "DL-debezium-002"
)

// LintCodes lists every code this file's LintDesign can produce — consulted
// by the E4 explain-catalog completeness guard
// (cmd/platformctl/lint_catalog_test.go).
var LintCodes = []string{LintCodeReplicationSlotPressure, LintCodeOverlappingPatternCapture}

// captureSet is one cdc Binding's parsed options.tables, reused by both
// checks below.
type captureSet struct {
	env    resource.Envelope
	tables map[string]bool
	all    bool
}

// LintDesign implements reconciler.DesignLinter. Pure, validate-time, no
// Request — operates over the full manifest set exactly like
// internal/application/lint's own built-in checks (it is called once per
// distinct provider Type(), not once per Provider envelope, so a manifest
// with two debezium Providers still gets each finding exactly once —
// docs/planning TASK_PROGRESS.md's recorded design decision).
func (p *Provider) LintDesign(envelopes []resource.Envelope, g *graph.Graph) []lint.Finding {
	byServer := map[resource.Key][]resource.Envelope{}
	bySource := map[resource.Key][]captureSet{}

	for _, e := range envelopes {
		if e.Kind != "Binding" {
			continue
		}
		b, err := binding.FromEnvelope(e)
		if err != nil || b.Mode != binding.ModeCDC {
			continue
		}
		provEnv, ok := resolveRef(g, e, "providerRef", "Provider")
		if !ok {
			continue
		}
		pr, err := provider.FromEnvelope(provEnv)
		if err != nil || pr.Type != "debezium" {
			continue
		}
		srcEnv, ok := resolveRef(g, e, "sourceRef", "Source")
		if !ok {
			continue
		}
		bySource[srcEnv.Key()] = append(bySource[srcEnv.Key()], captureSet{env: e, tables: parseTables(b), all: isAllTables(b)})

		src, err := source.FromEnvelope(srcEnv)
		if err != nil || src.External || src.ProviderRef == nil {
			// Only a managed Source has a physical-server Provider to
			// group by; an external Source's "server" isn't something
			// this manifest set realizes or can compare replication-slot
			// pressure against.
			continue
		}
		serverEnv, ok := resolveRef(g, srcEnv, "providerRef", "Provider")
		if !ok {
			continue
		}
		byServer[serverEnv.Key()] = append(byServer[serverEnv.Key()], e)
	}

	var findings []lint.Finding
	findings = append(findings, lintReplicationSlotPressure(byServer)...)
	findings = append(findings, lintOverlappingPatternCapture(bySource)...)
	return findings
}

func lintReplicationSlotPressure(byServer map[resource.Key][]resource.Envelope) []lint.Finding {
	var findings []lint.Finding
	for _, serverKey := range sortedKeys(byServer) {
		group := byServer[serverKey]
		if len(group) < 2 {
			continue
		}
		sort.Slice(group, func(i, j int) bool { return group[i].Key().String() < group[j].Key().String() })
		for i, e := range group {
			var others []string
			for j, o := range group {
				if j != i {
					others = append(others, o.Metadata.Name)
				}
			}
			findings = append(findings, lint.Finding{
				Code:     LintCodeReplicationSlotPressure,
				Severity: lint.Warning,
				Resource: e.Key(),
				Message: fmt.Sprintf(
					"cdc Binding %q is one of %d separate Debezium connectors capturing from Postgres/MySQL Provider %q — each opens its own replication slot; Binding(s) %s do the same. Consider one connector with a wider table list if these are independent views of the same data",
					e.Metadata.Name, len(group), serverKey.Name, strings.Join(others, ", ")),
			})
		}
	}
	return findings
}

func lintOverlappingPatternCapture(bySource map[resource.Key][]captureSet) []lint.Finding {
	var findings []lint.Finding
	for _, srcKey := range sortedKeys(bySource) {
		group := bySource[srcKey]
		if len(group) < 2 {
			continue
		}
		sort.Slice(group, func(i, j int) bool { return group[i].env.Key().String() < group[j].env.Key().String() })
		offending := map[int]bool{}
		for i := 0; i < len(group); i++ {
			for j := i + 1; j < len(group); j++ {
				if patternOverlap(group[i], group[j]) {
					offending[i] = true
					offending[j] = true
				}
			}
		}
		for i, c := range group {
			if !offending[i] {
				continue
			}
			var others []string
			for j, o := range group {
				if j != i && offending[j] {
					others = append(others, o.env.Metadata.Name)
				}
			}
			findings = append(findings, lint.Finding{
				Code:     LintCodeOverlappingPatternCapture,
				Severity: lint.Warning,
				Resource: c.env.Key(),
				Message: fmt.Sprintf(
					"cdc Binding %q's options.tables pattern overlaps Binding(s) %s once Debezium's table.include.list regex semantics are applied (DL001 only compares literal table names)",
					c.env.Metadata.Name, strings.Join(others, ", ")),
			})
		}
	}
	return findings
}

// parseTables/isAllTables mirror internal/application/lint's own DL001
// parsing of options.tables — duplicated rather than imported, since this
// adapter package cannot import internal/application/lint (CLAUDE.md: only
// cmd/platformctl and internal/application/registry import concrete
// adapters, and the inverse — an adapter importing application — is the
// same layering violation in the other direction).
func parseTables(b binding.Binding) map[string]bool {
	tables := map[string]bool{}
	if raw, ok := b.Options["tables"].([]any); ok {
		for _, t := range raw {
			if s, ok := t.(string); ok && s != "" {
				tables[s] = true
			}
		}
	}
	return tables
}

func isAllTables(b binding.Binding) bool {
	raw, ok := b.Options["tables"].([]any)
	return !ok || len(raw) == 0
}

// patternTriggerChars are the regex metacharacters treated as an
// intentional Debezium table.include.list pattern (as opposed to a literal
// dotted schema.table name, which contains "." but is not meant as a
// regex "any character" wildcard here — conservative on purpose, to avoid
// false positives on ordinary schema-qualified literal names).
const patternTriggerChars = "*?[]()|+"

func patternOverlap(a, b captureSet) bool {
	if a.all || b.all {
		// Unset options.tables ("all") is entirely DL001's domain
		// already — DL001 flags every pairing where either side is
		// unset, so this finer-grained check adds nothing there.
		return false
	}
	for ta := range a.tables {
		for tb := range b.tables {
			if ta == tb {
				continue // literal equality is DL001's domain
			}
			if strings.ContainsAny(ta, patternTriggerChars) && regexMatches(ta, tb) {
				return true
			}
			if strings.ContainsAny(tb, patternTriggerChars) && regexMatches(tb, ta) {
				return true
			}
		}
	}
	return false
}

func regexMatches(pattern, literal string) bool {
	re, err := regexp.Compile("^(?:" + pattern + ")$")
	if err != nil {
		return false
	}
	return re.MatchString(literal)
}

// resolveRef resolves spec.<field> on from to an envelope of exactly kind,
// namespace-defaulted to from's own namespace — the same resolution
// internal/application/compatibility's manifestIndex.resolveKind performs,
// reimplemented here in terms of graph.Graph.Nodes (which Build populates
// identically) since this adapter package cannot import
// internal/application/compatibility.
func resolveRef(g *graph.Graph, from resource.Envelope, field, kind string) (resource.Envelope, bool) {
	ref := resource.RefFromSpec(from.Spec, field)
	if ref.Name == "" {
		return resource.Envelope{}, false
	}
	env, ok := g.Nodes[ref.Key(from.Metadata.Namespace, kind)]
	return env, ok
}

func sortedKeys[V any](m map[resource.Key]V) []resource.Key {
	keys := make([]resource.Key, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	return keys
}
