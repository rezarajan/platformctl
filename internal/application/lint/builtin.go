package lint

import (
	"fmt"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/application/compatibility"
	"github.com/rezarajan/platformctl/internal/application/graphaccess"
	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// capture is one cdc Binding's effective table-capture set, grouped by the
// Source it captures — DL001.
type capture struct {
	env    resource.Envelope
	tables map[string]bool
	all    bool // unset options.tables — "all", overlaps everything (ADR 020 §4)
}

func capturesOverlap(a, b capture) bool {
	if a.all || b.all {
		return true
	}
	for t := range a.tables {
		if b.tables[t] {
			return true
		}
	}
	return false
}

// lintDuplicateCapture is DL001: ≥2 cdc Bindings share a sourceRef with
// overlapping effective table sets.
func lintDuplicateCapture(envelopes []resource.Envelope, idx compatibility.Index) []Finding {
	groups := map[resource.Key][]capture{}
	for _, e := range envelopes {
		if e.Kind != "Binding" {
			continue
		}
		b, err := binding.FromEnvelope(e)
		if err != nil || b.Mode != binding.ModeCDC {
			continue
		}
		srcEnv, ok, ambiguous := idx.Resolve(e, resource.RefFromSpec(e.Spec, "sourceRef"), "Source")
		if !ok || ambiguous {
			// Already refused at validate time (compatibility.Check runs
			// before lint ever does) — nothing more for lint to say.
			continue
		}
		c := capture{env: e, tables: map[string]bool{}}
		if tables, ok := b.Options["tables"].([]any); ok && len(tables) > 0 {
			for _, t := range tables {
				if s, ok := t.(string); ok && s != "" {
					c.tables[s] = true
				}
			}
		} else {
			c.all = true
		}
		key := srcEnv.Key()
		groups[key] = append(groups[key], c)
	}

	var findings []Finding
	for _, srcKey := range sortedResourceKeys(groups) {
		group := groups[srcKey]
		if len(group) < 2 {
			continue
		}
		sort.Slice(group, func(i, j int) bool { return group[i].env.Key().String() < group[j].env.Key().String() })
		offending := map[int]bool{}
		for i := 0; i < len(group); i++ {
			for j := i + 1; j < len(group); j++ {
				if capturesOverlap(group[i], group[j]) {
					offending[i] = true
					offending[j] = true
				}
			}
		}
		for i := 0; i < len(group); i++ {
			if !offending[i] {
				continue
			}
			var others []string
			for j := 0; j < len(group); j++ {
				if j != i && offending[j] {
					others = append(others, group[j].env.Metadata.Name)
				}
			}
			findings = append(findings, Finding{
				Code:     CodeDuplicateCapture,
				Severity: lint.Warning,
				Resource: group[i].env.Key(),
				Message: fmt.Sprintf("cdc Binding %q captures Source %q with a table set overlapping Binding(s) %s (unset options.tables means \"all\", which overlaps everything)",
					group[i].env.Metadata.Name, srcKey.Name, strings.Join(others, ", ")),
			})
		}
	}
	return findings
}

// sinkTarget is one sink Binding's write target, grouped by physical
// location — DL002.
type sinkTarget struct {
	env   resource.Envelope
	key   string
	label string
}

// lintSinkCollision is DL002: ≥2 sink Bindings target the same Dataset
// bucket+prefix, or the same Source+table for a sink→Source pairing.
func lintSinkCollision(envelopes []resource.Envelope, idx compatibility.Index) []Finding {
	var toDataset, toSource []sinkTarget
	for _, e := range envelopes {
		if e.Kind != "Binding" {
			continue
		}
		b, err := binding.FromEnvelope(e)
		if err != nil || b.Mode != binding.ModeSink {
			continue
		}
		tgtEnv, ok, ambiguous := idx.Resolve(e, resource.RefFromSpec(e.Spec, "targetRef"), "Dataset", "Source")
		if !ok || ambiguous {
			continue
		}
		switch tgtEnv.Kind {
		case "Dataset":
			ds, err := dataset.FromEnvelope(tgtEnv)
			if err != nil {
				continue
			}
			toDataset = append(toDataset, sinkTarget{
				env:   e,
				key:   ds.Bucket + "\x00" + ds.Prefix,
				label: fmt.Sprintf("Dataset bucket %q prefix %q", ds.Bucket, ds.Prefix),
			})
		case "Source":
			// options.table (singular) is the sink-into-database table name
			// (jdbcsink's own convention); unset falls back to the
			// connector's own per-topic default, which this generic layer
			// cannot derive without engine-block introspection — grouped
			// under its own "<default>" bucket rather than assumed to
			// collide (docs/planning/08 D3, jdbcsink.go).
			table, _ := b.Options["table"].(string)
			if table == "" {
				table = "<default>"
			}
			toSource = append(toSource, sinkTarget{
				env:   e,
				key:   tgtEnv.Key().String() + "\x00" + table,
				label: fmt.Sprintf("Source %q table %q", tgtEnv.Metadata.Name, table),
			})
		}
	}
	var findings []Finding
	findings = append(findings, collideOnKey(toDataset)...)
	findings = append(findings, collideOnKey(toSource)...)
	return findings
}

func collideOnKey(targets []sinkTarget) []Finding {
	groups := map[string][]sinkTarget{}
	for _, t := range targets {
		groups[t.key] = append(groups[t.key], t)
	}
	var keys []string
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var findings []Finding
	for _, k := range keys {
		group := groups[k]
		if len(group) < 2 {
			continue
		}
		sort.Slice(group, func(i, j int) bool { return group[i].env.Key().String() < group[j].env.Key().String() })
		for i, t := range group {
			var others []string
			for j, o := range group {
				if j != i {
					others = append(others, o.env.Metadata.Name)
				}
			}
			findings = append(findings, Finding{
				Code:     CodeSinkCollision,
				Severity: lint.Warning,
				Resource: t.env.Key(),
				Message:  fmt.Sprintf("sink Binding %q writes %s, colliding with Binding(s) %s", t.env.Metadata.Name, t.label, strings.Join(others, ", ")),
			})
		}
	}
	return findings
}

// lintObserverNotConsumed is DL003: metadata.observers names a Provider but
// the declaring resource's own realizing Provider implements no
// LineageAware — the forwarded lineage event is a runtime no-op (see
// status.ReasonLineageNotConsumed), predicted here at validate time.
func lintObserverNotConsumed(envelopes []resource.Envelope, idx compatibility.Index, resolve compatibility.ProviderResolver) []Finding {
	var findings []Finding
	for _, e := range envelopes {
		if len(e.Metadata.Observers) == 0 {
			continue
		}
		provEnv, ok := idx.ResolveKind(e, resource.RefFromSpec(e.Spec, "providerRef"), "Provider")
		if !ok {
			continue
		}
		p, err := provider.FromEnvelope(provEnv)
		if err != nil {
			continue
		}
		impl, err := resolve(p.Type)
		if err != nil {
			continue
		}
		if _, ok := impl.(reconciler.LineageAware); ok {
			continue
		}
		findings = append(findings, Finding{
			Code:     CodeObserverNotConsumed,
			Severity: lint.Warning,
			Resource: e.Key(),
			Message: fmt.Sprintf("%s %q declares metadata.observers but its Provider %q (type: %s) implements no LineageAware capability — the forwarded lineage event is a no-op",
				e.Kind, e.Metadata.Name, provEnv.Metadata.Name, p.Type),
		})
	}
	return findings
}

// lintPlaintextBoundary is DL004: a managed Connection using a plaintext
// scheme while its realizing Provider's SupportedConnectionSchemes() also
// advertises "https" — a TLS-capable realization exists but wasn't chosen.
// No shipped provider advertises "https" yet (C8's seam, docs/adr/020 §4 —
// see this package's ADR 020 note); this activates automatically the day
// one does, with zero code change here.
func lintPlaintextBoundary(envelopes []resource.Envelope, idx compatibility.Index, resolve compatibility.ProviderResolver) []Finding {
	var findings []Finding
	for _, e := range envelopes {
		if e.Kind != "Connection" {
			continue
		}
		c, err := connection.FromEnvelope(e)
		if err != nil || c.External || c.Scheme == "https" {
			continue
		}
		provEnv, ok := idx.ResolveKind(e, resource.RefFromSpec(e.Spec, "providerRef"), "Provider")
		if !ok {
			continue
		}
		p, err := provider.FromEnvelope(provEnv)
		if err != nil {
			continue
		}
		impl, err := resolve(p.Type)
		if err != nil {
			continue
		}
		capable, ok := impl.(reconciler.ConnectionCapableProvider)
		if !ok || !containsStr(capable.SupportedConnectionSchemes(), "https") {
			continue
		}
		findings = append(findings, Finding{
			Code:     CodePlaintextBoundary,
			Severity: lint.Warning,
			Resource: e.Key(),
			Message: fmt.Sprintf("Connection %q uses plaintext scheme %q but its Provider %q (type: %s) can also serve \"https\" — consider a TLS-terminated scheme for non-loopback traffic",
				e.Metadata.Name, c.Scheme, provEnv.Metadata.Name, p.Type),
		})
	}
	return findings
}

// lintOrphanedEventStream is DL010: no Binding reads or writes this
// EventStream — graph.Dependents(k) is empty exactly when nothing's
// sourceRef/targetRef resolved to k (Build already resolved every
// reference unambiguously).
func lintOrphanedEventStream(envelopes []resource.Envelope, g *graph.Graph) []Finding {
	var findings []Finding
	for _, e := range envelopes {
		if e.Kind != "EventStream" {
			continue
		}
		if len(g.Dependents(e.Key())) > 0 {
			continue
		}
		findings = append(findings, Finding{
			Code:     CodeOrphanedEventStream,
			Severity: lint.Info,
			Resource: e.Key(),
			Message:  fmt.Sprintf("EventStream %q has no Binding reading or writing it", e.Metadata.Name),
		})
	}
	return findings
}

// lintUnreferencedResources is DL011 (Catalog) and DL012
// (SecretReference/Connection/Provider): nothing in the manifest set
// resolves it — graph.Dependents(k) empty means no other resource's ref
// field ever pointed at k.
func lintUnreferencedResources(envelopes []resource.Envelope, g *graph.Graph) []Finding {
	var findings []Finding
	for _, e := range envelopes {
		switch e.Kind {
		case "Catalog":
			if len(g.Dependents(e.Key())) > 0 {
				continue
			}
			findings = append(findings, Finding{
				Code:     CodeUnreferencedCatalog,
				Severity: lint.Info,
				Resource: e.Key(),
				Message:  fmt.Sprintf("Catalog %q has no catalogRef/warehouse consumer and no Connection routes to it", e.Metadata.Name),
			})
		case "SecretReference", "Connection", "Provider":
			if len(g.Dependents(e.Key())) > 0 {
				continue
			}
			findings = append(findings, Finding{
				Code:     CodeUnusedResource,
				Severity: lint.Info,
				Resource: e.Key(),
				Message:  fmt.Sprintf("%s %q is declared but nothing in this manifest set resolves it", e.Kind, e.Metadata.Name),
			})
		}
	}
	return findings
}

// lintDeadEndPipeline is DL013: a cdc Binding whose EventStream has no
// downstream sink/ingest Binding. g.Edges[bindingKey] always includes the
// cdc Binding's own edge to its EventStream (via targetRef); Dependents of
// that EventStream therefore always includes at least the Binding itself —
// size 1 means nothing else references it.
func lintDeadEndPipeline(envelopes []resource.Envelope, g *graph.Graph) []Finding {
	var findings []Finding
	for _, e := range envelopes {
		if e.Kind != "Binding" {
			continue
		}
		b, err := binding.FromEnvelope(e)
		if err != nil || b.Mode != binding.ModeCDC {
			continue
		}
		for _, dep := range g.Edges[e.Key()] {
			node, ok := g.Nodes[dep]
			if !ok || node.Kind != "EventStream" {
				continue
			}
			if len(g.Dependents(dep)) > 1 {
				continue
			}
			findings = append(findings, Finding{
				Code:     CodeDeadEndPipeline,
				Severity: lint.Info,
				Resource: e.Key(),
				Message:  fmt.Sprintf("cdc Binding %q's EventStream %q has no downstream sink/ingest Binding", e.Metadata.Name, node.Metadata.Name),
			})
		}
	}
	return findings
}

// replicaFieldsGuardedByHighAvailability mirrors
// cmd/platformctl/root.go's identically-named list (docs/adr/004 §a.8): the
// spec.configuration.<field> names every shipped provider's multi-replica
// knob. DL014 is the informational ==1 case; root.go's own gate check is
// the hard >1-without-the-gate refusal.
var replicaFieldsGuardedByHighAvailability = []string{"brokers", "workers", "nodes"}

// lintSingleReplicaWithHAGate is DL014: brokers/workers/nodes explicitly
// declared 1 while the HighAvailability gate is enabled.
func lintSingleReplicaWithHAGate(envelopes []resource.Envelope, opts Options) []Finding {
	if !opts.HighAvailabilityEnabled {
		return nil
	}
	var findings []Finding
	for _, e := range envelopes {
		if e.Kind != "Provider" {
			continue
		}
		cfg, _ := e.Spec["configuration"].(map[string]any)
		for _, field := range replicaFieldsGuardedByHighAvailability {
			raw, present := cfg[field]
			if !present {
				continue
			}
			n := 0
			switch v := raw.(type) {
			case int:
				n = v
			case float64:
				n = int(v)
			}
			if n != 1 {
				continue
			}
			findings = append(findings, Finding{
				Code:     CodeSingleReplicaWithHAGate,
				Severity: lint.Info,
				Resource: e.Key(),
				Message:  fmt.Sprintf("Provider %q declares spec.configuration.%s: 1 with the HighAvailability gate enabled — a single replica has no failover", e.Metadata.Name, field),
			})
		}
	}
	return findings
}

// lintDeletionPolicyUnset is DL020: spec.deletionPolicy is absent on a
// data-bearing kind (Dataset/Source). The domain FromEnvelope decoders
// already default it to "retain" when unset, which is why this checks the
// raw envelope spec map directly rather than the decoded value.
func lintDeletionPolicyUnset(envelopes []resource.Envelope) []Finding {
	var findings []Finding
	for _, e := range envelopes {
		if e.Kind != "Dataset" && e.Kind != "Source" {
			continue
		}
		if ext, _ := e.Spec["external"].(bool); ext {
			continue // ignored for external resources (source.go/dataset.go doc comments)
		}
		if _, has := e.Spec["deletionPolicy"]; has {
			continue
		}
		findings = append(findings, Finding{
			Code:     CodeDeletionPolicyUnset,
			Severity: lint.Warning,
			Resource: e.Key(),
			Message:  fmt.Sprintf("%s %q does not set spec.deletionPolicy (defaults to \"retain\") — set it explicitly", e.Kind, e.Metadata.Name),
		})
	}
	return findings
}

// lintProtectUnset is DL021, plan-aware: metadata.protect is unset on a
// data-bearing kind in a set whose plan would perform at least one
// authoritative delete (a resource recorded in state no longer appears in
// the current manifest set).
func lintProtectUnset(envelopes []resource.Envelope, opts Options) []Finding {
	if opts.State == nil || len(opts.State.Resources) == 0 {
		return nil
	}
	current := make(map[resource.Key]bool, len(envelopes))
	for _, e := range envelopes {
		current[e.Key()] = true
	}
	authoritativeDelete := false
	for k := range opts.State.Resources {
		if !current[k] {
			authoritativeDelete = true
			break
		}
	}
	if !authoritativeDelete {
		return nil
	}
	var findings []Finding
	for _, e := range envelopes {
		if e.Kind != "Dataset" && e.Kind != "Source" {
			continue
		}
		if ext, _ := e.Spec["external"].(bool); ext {
			continue
		}
		if e.Metadata.Protect {
			continue
		}
		findings = append(findings, Finding{
			Code:     CodeProtectUnset,
			Severity: lint.Warning,
			Resource: e.Key(),
			Message:  fmt.Sprintf("%s %q does not set metadata.protect while this set's plan includes an authoritative delete elsewhere", e.Kind, e.Metadata.Name),
		})
	}
	return findings
}

// lintNamespaceWideGrant is DL022 (docs/adr/033 decision 3, docs/planning/08
// K3): a spec.access entry with no selector — the bare namespace-wide form
// H7 shipped, now deprecated in favor of "namespace AND selector". Reuses
// graphaccess.AccessGrants (the SAME reader the H7 compiler/matchGrant
// evaluator already use) rather than re-parsing env.Spec["access"] here.
func lintNamespaceWideGrant(envelopes []resource.Envelope) []Finding {
	var findings []Finding
	for _, e := range envelopes {
		for _, grant := range graphaccess.AccessGrants(e) {
			if grant.Selector != nil {
				continue
			}
			findings = append(findings, Finding{
				Code:     CodeNamespaceWideGrant,
				Severity: lint.Warning,
				Resource: e.Key(),
				Message: fmt.Sprintf("%s %q declares a namespace-wide spec.access grant to %q with no selector — scope it with a selector (docs/adr/033 decision 3)",
					e.Kind, e.Metadata.Name, grant.Namespace),
			})
		}
	}
	return findings
}

func containsStr(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func sortedResourceKeys(m map[resource.Key][]capture) []resource.Key {
	keys := make([]resource.Key, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	return keys
}
