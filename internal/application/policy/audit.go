package policy

import (
	"fmt"
	"sort"

	"github.com/rezarajan/platformctl/internal/application/graphaccess"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/policy"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// EdgeKind classifies the mechanism that declares an audited edge — the
// docs/planning/08 K5 / ADR 033 decision 5 "for EVERY declared edge, why
// it is permitted" table has to cover both shapes crossDomainEdges already
// derives (a Binding's sourceRef->targetRef, or a connectionRef
// consumption) AND the mechanism crossDomainEdges deliberately does NOT
// cover: a docs/adr/026 §2 spec.access wide/selector grant, which admits a
// whole namespace's audience without any per-resource Binding at all.
// ADR 033's own framing names this explicitly: "a permitted edge's
// justification may be a grant, not only a policy rule."
type EdgeKind string

const (
	EdgeKindBinding    EdgeKind = "binding"
	EdgeKindConnection EdgeKind = "connection"
	EdgeKindGrant      EdgeKind = "grant"
)

// Verdict is one audited edge's admission outcome.
type Verdict string

const (
	Permitted Verdict = "permitted"
	Denied    Verdict = "denied"
)

// Justification categorizes WHY an edge carries its Verdict — the three
// shapes docs/planning/08 K5 names: "which rule admitted" (JustifyGrant,
// for a grant-declared edge no deny rule touches — the grant itself is
// the positive authorization), "no matching deny" (JustifyNoDeny, for a
// Binding/connectionRef edge no matchEdge rule denies), and "which
// exemption" (JustifyExemption). JustifyDenyRule is the DENIED case's own
// justification: the specific deny rule that fired and was not exempted.
type Justification string

const (
	JustifyNoDeny    Justification = "no-matching-deny"
	JustifyGrant     Justification = "grant"
	JustifyExemption Justification = "exemption"
	JustifyDenyRule  Justification = "deny-rule"
)

// EdgeAudit is one declared edge's admission justification —
// `platformctl policy audit`'s per-row unit. Every value Audit returns has
// a non-empty Justification and (for Denied, or a Permitted-by-exemption
// row) a non-empty RuleID: "an edge with no nameable justification is a
// test failure" (docs/planning/08 K5 accept bar) is enforced by
// TestAuditEveryEdgeHasNameableJustification, not by this type alone, but
// every Audit code path below is written to satisfy it structurally.
type EdgeAudit struct {
	// Owner is the resource that declared the edge: the Binding, the
	// connectionRef-declaring consumer, or the spec.access-declaring
	// resource — always the resource an operator would edit to change
	// this row's outcome (mirrors crossDomainEdge.Owner's own doc
	// comment).
	Owner resource.Key
	// From/To name the edge's endpoints as rendered strings. For a
	// Binding/connectionRef edge these are the two resolved resource
	// keys; for a grant edge, From is Owner and To is a synthetic
	// "namespace/<ns>[ selector...]" description, since a wide grant's
	// audience is a namespace (optionally narrowed by selector), not one
	// resource.
	From, To string
	Kind     EdgeKind
	Verdict  Verdict
	Justification
	// RuleID names the specific rule this row's Justification cites:
	// the firing deny rule (Denied, or Permitted via JustifyExemption),
	// empty for JustifyNoDeny/JustifyGrant (there is no rule to name —
	// the absence of one, or the grant itself, IS the justification).
	RuleID string
	// Detail is a human-readable rendering of the match — the same
	// selector/crossDomain/grant description the K2/K3 Decision messages
	// already build, reused here rather than re-invented.
	Detail       string
	ExemptReason string
}

// matchingEdgeRules returns every RuleKindEdge rule (across policies) that
// selects edge — crossDomain rules matched by exact domain-pair equality,
// selector rules by the SAME from/to label-selector check
// evaluateEdgeSelector uses, gated by labelScopedAccessEnabled exactly as
// Run gates it (a selector rule contributes nothing when the gate is
// off — the edge falls through to whatever the remaining, gate-independent
// rules decide, same as Run's own dispatch).
func matchingEdgeRules(policies []policy.Policy, edge crossDomainEdge, labelScopedAccessEnabled bool) []policy.Rule {
	var out []policy.Rule
	for _, pol := range policies {
		for _, rule := range pol.Rules() {
			if rule.Kind() != policy.RuleKindEdge {
				continue
			}
			if rule.MatchEdge.CrossDomain != nil {
				sel := rule.MatchEdge.CrossDomain
				if edge.FromDomain == sel.From && edge.ToDomain == sel.To {
					out = append(out, rule)
				}
				continue
			}
			if !labelScopedAccessEnabled {
				continue
			}
			sel := rule.MatchEdge.Selector
			if sel.From.Selects(edge.From.Metadata.Labels) && sel.To.Selects(edge.To.Metadata.Labels) {
				out = append(out, rule)
			}
		}
	}
	return out
}

// matchingGrantRules returns every RuleKindGrant rule that selects grant
// (namespace equality — the exact evaluateGrant check, K3's Done-note:
// "unchanged by this task ... it still matches by namespace only").
func matchingGrantRules(policies []policy.Policy, ns string) []policy.Rule {
	var out []policy.Rule
	for _, pol := range policies {
		for _, rule := range pol.Rules() {
			if rule.Kind() == policy.RuleKindGrant && rule.MatchGrant.Namespace == ns {
				out = append(out, rule)
			}
		}
	}
	return out
}

// exemptionFor reports whether owner carries a live exemption for ruleID —
// live meaning both an annotation naming it AND the rule itself declaring
// exemptible: true (ADR 021 §3 — the same bar applyExemptions enforces).
func exemptionFor(owner resource.Envelope, ruleID string, exemptibleByID map[string]bool) (reason string, ok bool) {
	if !exemptibleByID[ruleID] {
		return "", false
	}
	for _, ex := range policy.ParseExemptions(owner.Metadata.Annotations) {
		if ex.RuleID == ruleID {
			return ex.Reason, true
		}
	}
	return "", false
}

// resolveVerdict applies deny-wins over matching (every rule an edge/grant
// matched, deny before warn per policy.Less/effectRank ordering already
// established), against ownerâ€™s own exemptions: the first unexempted deny
// wins (Denied, JustifyDenyRule); absent one, the first exempted deny wins
// (Permitted, JustifyExemption); absent any deny at all, base is returned
// unchanged (the caller's own default: JustifyNoDeny for an edge,
// JustifyGrant for a grant).
func resolveVerdict(matching []policy.Rule, owner resource.Envelope, exemptibleByID map[string]bool, base EdgeAudit) EdgeAudit {
	sort.SliceStable(matching, func(i, j int) bool { return matching[i].ID < matching[j].ID })
	var exemptedDeny *policy.Rule
	var exemptedReason string
	for i := range matching {
		rule := matching[i]
		if rule.Effect != policy.Deny {
			continue
		}
		if reason, ok := exemptionFor(owner, rule.ID, exemptibleByID); ok {
			if exemptedDeny == nil {
				exemptedDeny = &matching[i]
				exemptedReason = reason
			}
			continue
		}
		base.Verdict = Denied
		base.Justification = JustifyDenyRule
		base.RuleID = rule.ID
		base.Detail = message(rule, base.Detail)
		return base
	}
	if exemptedDeny != nil {
		base.Verdict = Permitted
		base.Justification = JustifyExemption
		base.RuleID = exemptedDeny.ID
		base.ExemptReason = exemptedReason
		base.Detail = message(*exemptedDeny, base.Detail)
		return base
	}
	return base
}

// Audit computes docs/planning/08 K5 / ADR 033 decision 5's per-edge
// admission justification for EVERY edge declared in the CURRENT
// manifests — the same set validate/plan already evaluate, so `policy
// audit`'s "denied" rows are exactly the ADR 021 severing amendment's
// "reportable ... at validate/plan" state: a Binding/connectionRef/grant
// whose authorization has been withdrawn shows up here as Denied even
// though standing infrastructure from a PRIOR apply may still be running
// (this function never reads runtime/state — manifests + policies only).
func Audit(policies []policy.Policy, envelopes []resource.Envelope, g *graph.Graph, labelScopedAccessEnabled bool) []EdgeAudit {
	exemptibleByID := map[string]bool{}
	for _, p := range policies {
		for _, r := range p.Rules() {
			exemptibleByID[r.ID] = r.Exemptible
		}
	}

	var out []EdgeAudit
	for _, edge := range crossDomainEdges(envelopes, g) {
		kind := EdgeKindConnection
		if edge.Owner.Kind == "Binding" {
			kind = EdgeKindBinding
		}
		base := EdgeAudit{
			Owner: edge.Owner.Key(), From: edge.From.Key().String(), To: edge.To.Key().String(),
			Kind: kind, Verdict: Permitted, Justification: JustifyNoDeny,
			Detail: fmt.Sprintf("no policy rule denies %s -> %s", edge.From.Key(), edge.To.Key()),
		}
		matching := matchingEdgeRules(policies, edge, labelScopedAccessEnabled)
		out = append(out, resolveVerdict(matching, edge.Owner, exemptibleByID, base))
	}

	for _, e := range envelopes {
		for _, grant := range graphaccess.AccessGrants(e) {
			detail := fmt.Sprintf("spec.access grant on %s to namespace %q", e.Key(), grant.Namespace)
			if grant.Selector != nil {
				detail += fmt.Sprintf(" (selector %s)", grant.Selector.String())
			}
			base := EdgeAudit{
				Owner: e.Key(), From: e.Key().String(), To: "namespace/" + grant.Namespace,
				Kind: EdgeKindGrant, Verdict: Permitted, Justification: JustifyGrant,
				Detail: detail,
			}
			matching := matchingGrantRules(policies, grant.Namespace)
			out = append(out, resolveVerdict(matching, e, exemptibleByID, base))
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Owner.String() != out[j].Owner.String() {
			return out[i].Owner.String() < out[j].Owner.String()
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].To < out[j].To
	})
	return out
}
