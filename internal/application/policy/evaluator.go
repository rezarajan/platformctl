package policy

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/application/graphaccess"
	planpkg "github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/policy"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// Decision re-exports domain/policy.Decision so callers of this package
// need only one import for the common case (mirrors
// internal/application/lint.Finding's identical re-export).
type Decision = policy.Decision

// Run evaluates every match+assert, matchFinding, and matchEdge rule across
// policies against envelopes/findings/the graph — the validate-time half of
// ADR 021's evaluator: "a pure function (policies, envelopes, graph, plan,
// findings) -> decisions". matchEdge.crossDomain (docs/adr/022 Ring 0,
// docs/planning/08 H5) is the first selector to actually need g: it fires
// against the graph's cross-domain data-flow edges (crossDomainEdges below),
// which is why g was accepted (unused) from H3 on — this is that future
// selector. matchPlan rules never fire here: RunPlan is their evaluation
// point, called only once a plan actually exists (plan/apply/destroy,
// never validate — see docs/planning/08 §7.7 H3's "loadAndValidate after
// compatibility + lint" wiring instruction).
func Run(policies []policy.Policy, envelopes []resource.Envelope, g *graph.Graph, findings []lint.Finding) ([]Decision, error) {
	var decisions []Decision
	for _, pol := range policies {
		for _, rule := range pol.Rules() {
			switch rule.Kind() {
			case policy.RuleKindFieldAssert:
				ds, err := evaluateFieldAssert(rule, envelopes)
				if err != nil {
					return nil, fmt.Errorf("policy rule %q: %w", rule.ID, err)
				}
				decisions = append(decisions, ds...)
			case policy.RuleKindFinding:
				decisions = append(decisions, evaluateFinding(rule, findings)...)
			case policy.RuleKindEdge:
				decisions = append(decisions, evaluateCrossDomain(rule, envelopes, g)...)
			case policy.RuleKindGrant:
				decisions = append(decisions, evaluateGrant(rule, envelopes)...)
			}
		}
	}
	decisions = applyExemptions(envelopes, decisions, policies)
	policy.SortDecisions(decisions)
	return decisions, nil
}

// RunPlan evaluates every matchPlan rule across policies against entries —
// the plan-scoped half of ADR 021's evaluator (docs/planning/08 §7.7 H3:
// "into plan/apply/destroy for matchPlan rules"), called once a plan has
// actually been computed.
func RunPlan(policies []policy.Policy, envelopes []resource.Envelope, entries []planpkg.Entry) ([]Decision, error) {
	var decisions []Decision
	for _, pol := range policies {
		for _, rule := range pol.Rules() {
			if rule.Kind() != policy.RuleKindPlan {
				continue
			}
			decisions = append(decisions, evaluatePlan(rule, entries)...)
		}
	}
	decisions = applyExemptions(envelopes, decisions, policies)
	policy.SortDecisions(decisions)
	return decisions, nil
}

func evaluateFieldAssert(rule policy.Rule, envelopes []resource.Envelope) ([]Decision, error) {
	var out []Decision
	for _, e := range envelopes {
		if !rule.Match.Selects(e) {
			continue
		}
		val, present, err := policy.FieldValue(e, rule.Assert.Field)
		if err != nil {
			return nil, err
		}
		satisfied, err := rule.Assert.Satisfied(val, present)
		if err != nil {
			return nil, err
		}
		if satisfied {
			continue
		}
		out = append(out, Decision{
			RuleID:   rule.ID,
			Effect:   rule.Effect,
			Resource: e.Key(),
			Message:  message(rule, fmt.Sprintf("%s %q fails assert on %s", e.Kind, e.Metadata.Name, rule.Assert.Field)),
		})
	}
	return out, nil
}

func evaluateFinding(rule policy.Rule, findings []lint.Finding) []Decision {
	var out []Decision
	for _, f := range findings {
		if f.Code != rule.MatchFinding.Code {
			continue
		}
		// Escalation intentionally ignores f.Waived: a lint waiver silences
		// the lint layer's own report, not a policy that has promoted the
		// same signal to enforcement (ADR 021 §3: exemptions are a distinct
		// mechanism from lint waivers, and only apply when the rule itself
		// declares exemptible: true).
		out = append(out, Decision{
			RuleID:   rule.ID,
			Effect:   rule.Effect,
			Resource: f.Resource,
			Message:  message(rule, fmt.Sprintf("escalated from lint %s on %s: %s", f.Code, f.Resource, f.Message)),
		})
	}
	return out
}

func evaluatePlan(rule policy.Rule, entries []planpkg.Entry) []Decision {
	var out []Decision
	for _, entry := range entries {
		if string(entry.Action) != rule.MatchPlan.Action {
			continue
		}
		if rule.MatchPlan.Kind != "" && entry.Key.Kind != rule.MatchPlan.Kind {
			continue
		}
		out = append(out, Decision{
			RuleID:   rule.ID,
			Effect:   rule.Effect,
			Resource: entry.Key,
			Message:  message(rule, fmt.Sprintf("plan action %q on %s is denied by policy", entry.Action, entry.Key)),
		})
	}
	return out
}

func message(rule policy.Rule, fallback string) string {
	if rule.Message != "" {
		return rule.Message
	}
	return fallback
}

// crossDomainEdge is one data-flow edge crossDomainEdges derives from the
// graph (docs/adr/022 Ring 0): a Binding's sourceRef domain -> targetRef
// domain, or a connectionRef consumer's own domain -> the Connection it
// references ("a Connection consumption is an edge"). Owner is the resource
// a firing rule's Decision attaches to — the Binding itself, or the
// connectionRef-declaring consumer — always the resource whose author can
// actually act on the denial.
type crossDomainEdge struct {
	Owner                resource.Envelope
	From, To             resource.Envelope
	FromDomain, ToDomain string
}

// connectionRefKinds mirrors graph.allowedKinds("connectionRef") (unexported
// there): connectionRef may resolve to a Connection or a SecretReference,
// but only a Connection is a "Connection consumption" edge for domain
// purposes — a SecretReference has no independent segmentation meaning.
var connectionRefKinds = map[string]bool{"Connection": true, "SecretReference": true}

// crossDomainEdges derives every cross-domain-relevant edge from envelopes/g.
// Re-resolving sourceRef/targetRef/connectionRef here (rather than reading
// g.Edges directly) is deliberate: g.Edges is an unordered, unlabeled
// dependency list (docs/domain/graph.Graph doc comment), so which edge came
// from which spec field is not recoverable from it alone. Re-resolution is
// safe to do without re-deriving graph.Build's own ambiguity checks: g was
// already built successfully by the time policy.Run is called (every ref
// field is guaranteed to resolve to exactly one node in g.Nodes), so a
// direct namespace+name(+kind) scan over g.Nodes here can only ever find
// the same single match graph.Build already validated.
func crossDomainEdges(envelopes []resource.Envelope, g *graph.Graph) []crossDomainEdge {
	if g == nil {
		return nil
	}
	var edges []crossDomainEdge
	for _, e := range envelopes {
		if e.Kind == "Binding" {
			from, okFrom := resolveRef(g, e, "sourceRef", nil)
			to, okTo := resolveRef(g, e, "targetRef", nil)
			if okFrom && okTo {
				edges = append(edges, crossDomainEdge{
					Owner: e, From: from, To: to,
					FromDomain: resource.NormalizeDomain(from.Metadata.Domain),
					ToDomain:   resource.NormalizeDomain(to.Metadata.Domain),
				})
			}
			continue
		}
		if ref := resource.RefFromSpec(e.Spec, "connectionRef"); ref.Name != "" {
			if to, ok := resolveRef(g, e, "connectionRef", connectionRefKinds); ok && to.Kind == "Connection" {
				edges = append(edges, crossDomainEdge{
					Owner: e, From: e, To: to,
					FromDomain: resource.NormalizeDomain(e.Metadata.Domain),
					ToDomain:   resource.NormalizeDomain(to.Metadata.Domain),
				})
			}
		}
	}
	return edges
}

// resolveRef resolves spec.<field> on from against g.Nodes by namespace+name,
// optionally filtered to allowed Kinds (nil = unfiltered, matching
// graph.allowedKinds' default case for sourceRef/targetRef). Returns
// (zero, false) when the field is unset — never an error, since every
// present ref is already guaranteed resolvable by graph.Build having
// succeeded.
func resolveRef(g *graph.Graph, from resource.Envelope, field string, allowed map[string]bool) (resource.Envelope, bool) {
	ref := resource.RefFromSpec(from.Spec, field)
	if ref.Name == "" {
		return resource.Envelope{}, false
	}
	ns := ref.NamespaceOr(from.Metadata.Namespace)
	for _, e := range g.Nodes {
		if e.Metadata.Name != ref.Name || resource.NormalizeNamespace(e.Metadata.Namespace) != ns {
			continue
		}
		if allowed != nil && !allowed[e.Kind] {
			continue
		}
		return e, true
	}
	return resource.Envelope{}, false
}

// evaluateCrossDomain evaluates one matchEdge.crossDomain rule against every
// cross-domain edge in the graph, denying (or warning) each edge whose
// (fromDomain, toDomain) matches the rule's selector exactly. The message
// names both domains and the edge — docs/planning/08 H5's accept bar.
func evaluateCrossDomain(rule policy.Rule, envelopes []resource.Envelope, g *graph.Graph) []Decision {
	sel := rule.MatchEdge.CrossDomain
	var out []Decision
	for _, edge := range crossDomainEdges(envelopes, g) {
		if edge.FromDomain != sel.From || edge.ToDomain != sel.To {
			continue
		}
		out = append(out, Decision{
			RuleID:   rule.ID,
			Effect:   rule.Effect,
			Resource: edge.Owner.Key(),
			Message: message(rule, fmt.Sprintf(
				"cross-domain edge %s (domain %q) -> %s (domain %q), via %s, is denied by policy",
				edge.From.Key(), edge.FromDomain, edge.To.Key(), edge.ToDomain, edge.Owner.Key(),
			)),
		})
	}
	return out
}

// evaluateGrant evaluates one matchGrant rule against every resource's
// declared docs/adr/026 §2 spec.access wide grants, denying (or warning)
// each resource that names the rule's selected namespace — ADR 026
// decision 2's "a matchGrant selector lets organizations deny or constrain
// wide grants" realized exactly like evaluateCrossDomain realizes
// matchEdge.crossDomain: a validate-time-only check, never re-evaluated by
// the H7 compiler itself (domainruntime.go's own holes comment documents
// the identical precedent this mirrors).
func evaluateGrant(rule policy.Rule, envelopes []resource.Envelope) []Decision {
	sel := rule.MatchGrant
	var out []Decision
	for _, e := range envelopes {
		for _, grant := range graphaccess.AccessGrants(e) {
			if grant.Namespace != sel.Namespace {
				continue
			}
			out = append(out, Decision{
				RuleID:   rule.ID,
				Effect:   rule.Effect,
				Resource: e.Key(),
				Message: message(rule, fmt.Sprintf(
					"%s declares a wide access grant to namespace %q, denied by policy",
					e.Key(), grant.Namespace,
				)),
			})
		}
	}
	return out
}
