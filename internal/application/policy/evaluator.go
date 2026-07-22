package policy

import (
	"fmt"

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

// Run evaluates every match+assert and matchFinding rule across policies
// against envelopes/findings — the validate-time half of ADR 021's
// evaluator: "a pure function (policies, envelopes, graph, plan, findings)
// -> decisions". g is accepted for parity with that stated signature; no
// shipped selector needs it yet (kept so a future selector — e.g. an
// edge-aware one — doesn't need another signature change across every call
// site). matchPlan rules never fire here: RunPlan is their evaluation
// point, called only once a plan actually exists (plan/apply/destroy,
// never validate — see docs/planning/08 §7.7 H3's "loadAndValidate after
// compatibility + lint" wiring instruction).
func Run(policies []policy.Policy, envelopes []resource.Envelope, g *graph.Graph, findings []lint.Finding) ([]Decision, error) {
	_ = g // see doc comment: accepted for signature parity, unused today.
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
