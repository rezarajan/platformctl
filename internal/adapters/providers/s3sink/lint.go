package s3sink

import (
	"fmt"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

var _ reconciler.DesignLinter = (*Provider)(nil)

// LintCodePrefixHierarchyCollision refines internal/application/lint's
// generic DL002 (which only catches an *exact* bucket+prefix match):
// docs/adr/020-design-lints.md §5's "prefix-collision refinement". S3
// prefixes are hierarchical — an object key under "events/raw/" also lives
// under "events/" — so two sink Bindings whose Dataset prefixes are
// different strings but one contains the other still collide in the
// object-key space, invisible to DL002's plain equality check.
const LintCodePrefixHierarchyCollision = "DL-s3sink-001"

// LintCodes lists every code this file's LintDesign can produce.
var LintCodes = []string{LintCodePrefixHierarchyCollision}

type sinkTarget struct {
	env            resource.Envelope // the sink Binding
	bucket, prefix string
	datasetName    string
}

// LintDesign implements reconciler.DesignLinter.
func (p *Provider) LintDesign(envelopes []resource.Envelope, g *graph.Graph) []lint.Finding {
	var targets []sinkTarget
	for _, e := range envelopes {
		if e.Kind != "Binding" {
			continue
		}
		b, err := binding.FromEnvelope(e)
		if err != nil || b.Mode != binding.ModeSink {
			continue
		}
		provEnv, ok := resolveRef(g, e, "providerRef", "Provider")
		if !ok {
			continue
		}
		pr, err := provider.FromEnvelope(provEnv)
		if err != nil || pr.Type != "s3sink" {
			continue
		}
		tgtEnv, ok := resolveRef(g, e, "targetRef", "Dataset")
		if !ok {
			continue // sink-into-Source isn't this provider's own pairing
		}
		ds, err := dataset.FromEnvelope(tgtEnv)
		if err != nil {
			continue
		}
		targets = append(targets, sinkTarget{env: e, bucket: ds.Bucket, prefix: ds.Prefix, datasetName: tgtEnv.Metadata.Name})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].env.Key().String() < targets[j].env.Key().String() })

	offending := map[int]bool{}
	for i := 0; i < len(targets); i++ {
		for j := i + 1; j < len(targets); j++ {
			if targets[i].bucket != targets[j].bucket {
				continue
			}
			if targets[i].prefix == targets[j].prefix {
				continue // exact match is DL002's domain
			}
			if isPathPrefixOf(targets[i].prefix, targets[j].prefix) || isPathPrefixOf(targets[j].prefix, targets[i].prefix) {
				offending[i] = true
				offending[j] = true
			}
		}
	}

	var findings []lint.Finding
	for i, t := range targets {
		if !offending[i] {
			continue
		}
		var others []string
		for j, o := range targets {
			if j != i && offending[j] {
				others = append(others, o.env.Metadata.Name)
			}
		}
		findings = append(findings, lint.Finding{
			Code:     LintCodePrefixHierarchyCollision,
			Severity: lint.Warning,
			Resource: t.env.Key(),
			Message: fmt.Sprintf(
				"sink Binding %q writes bucket %q prefix %q, which sits inside/contains another sink Binding's prefix tree (%s) — S3 object keys under one prefix overlap the other's",
				t.env.Metadata.Name, t.bucket, t.prefix, strings.Join(others, ", ")),
		})
	}
	return findings
}

// isPathPrefixOf reports whether a is a path-hierarchy prefix of b — "/"
// terminated before comparing so "events" does not wrongly match
// "eventsomething".
func isPathPrefixOf(a, b string) bool {
	if a == "" {
		return false
	}
	an := a
	if !strings.HasSuffix(an, "/") {
		an += "/"
	}
	return strings.HasPrefix(b, an)
}

// resolveRef mirrors debezium's identically-named, identically-reasoned
// helper (internal/adapters/providers/debezium/lint.go) — this adapter
// package cannot import internal/application/compatibility either.
func resolveRef(g *graph.Graph, from resource.Envelope, field, kind string) (resource.Envelope, bool) {
	ref := resource.RefFromSpec(from.Spec, field)
	if ref.Name == "" {
		return resource.Envelope{}, false
	}
	env, ok := g.Nodes[ref.Key(from.Metadata.Namespace, kind)]
	return env, ok
}
