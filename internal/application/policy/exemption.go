package policy

import (
	"github.com/rezarajan/platformctl/internal/domain/policy"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// applyExemptions matches every decision's (RuleID, Resource) against the
// resource's own metadata.annotations[policy.ExemptAnnotation] entries,
// setting Exempted/ExemptReason on a match — but only when the rule that
// produced the decision declares exemptible: true (ADR 021 §3: "unlike
// lint waivers — only honored if the policy itself declares exemptible:
// true"). A non-exemptible rule's decisions are therefore never touched
// here, regardless of what annotation the resource carries. Exempted
// decisions are kept in the returned slice (not dropped) — mirroring
// internal/application/lint's Waived findings — so callers can still report
// them, just not block on them.
func applyExemptions(envelopes []resource.Envelope, decisions []Decision, policies []policy.Policy) []Decision {
	exemptibleByID := map[string]bool{}
	for _, p := range policies {
		for _, r := range p.Rules() {
			exemptibleByID[r.ID] = r.Exemptible
		}
	}
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, e := range envelopes {
		byKey[e.Key()] = e
	}

	for i := range decisions {
		if !exemptibleByID[decisions[i].RuleID] {
			continue
		}
		env, ok := byKey[decisions[i].Resource]
		if !ok {
			continue
		}
		for _, ex := range policy.ParseExemptions(env.Metadata.Annotations) {
			if ex.RuleID == decisions[i].RuleID {
				decisions[i].Exempted = true
				decisions[i].ExemptReason = ex.Reason
				break
			}
		}
	}
	return decisions
}
