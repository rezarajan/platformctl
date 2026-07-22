package redpanda

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/eventstream"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

var _ reconciler.DesignLinter = (*Provider)(nil)

// LintCodeReplicationBelowBrokers is this provider's one design-lint code
// (docs/adr/020-design-lints.md §5): a multi-broker cluster (spec.
// configuration.brokers > 1) hosting a Topic whose spec.replication is
// lower than the broker count — a shape hint, not a hazard on the level of
// DL001-004: an intentionally low replication factor (a scratch/throwaway
// topic) is entirely reasonable, hence info like DL014's identically-shaped
// single-replica hint.
const LintCodeReplicationBelowBrokers = "DL-redpanda-001"

// LintCodes lists every code this file's LintDesign can produce.
var LintCodes = []string{LintCodeReplicationBelowBrokers}

// LintDesign implements reconciler.DesignLinter.
func (p *Provider) LintDesign(envelopes []resource.Envelope, g *graph.Graph) []lint.Finding {
	var findings []lint.Finding
	for _, e := range envelopes {
		if e.Kind != "EventStream" {
			continue
		}
		es, err := eventstream.FromEnvelope(e)
		if err != nil || es.External {
			continue
		}
		provEnv, ok := resolveRef(g, e, "providerRef", "Provider")
		if !ok {
			continue
		}
		pr, err := provider.FromEnvelope(provEnv)
		if err != nil || pr.Type != "redpanda" {
			continue
		}
		brokers, declared := brokersDeclared(pr)
		if !declared || brokers <= 1 {
			continue
		}
		if es.ReplicationFactor() >= brokers {
			continue
		}
		findings = append(findings, lint.Finding{
			Code:     LintCodeReplicationBelowBrokers,
			Severity: lint.Info,
			Resource: e.Key(),
			Message: fmt.Sprintf(
				"EventStream %q has spec.replication %d on a %d-broker Provider %q — raising replication improves durability against a single broker's loss",
				e.Metadata.Name, es.ReplicationFactor(), brokers, provEnv.Metadata.Name),
		})
	}
	return findings
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
