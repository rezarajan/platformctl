// Package policy defines the Policy kind (policy.datascape.io/v1alpha1) and
// its closed rule vocabulary: docs/adr/021-policy-engine-zero-trust.md is
// the design this package follows to the letter. A Policy is a distinct
// governance input, never a datascape.io/v1alpha1 resource kind (ADR 021
// §1) — it lives in its own domain package (a sibling of domain/resource,
// domain/lint, ...), its schema lives in a parallel schemas/policy/
// directory kept out of the resource-kind schema set (schemas.KindFiles),
// and it loads from a separate channel (--policies / .datascape/policies/),
// never from the governed manifest set.
//
// This package holds types, decoding, and single-resource/single-value pure
// predicates only (Match.Selects, Assert.Satisfied, FieldValue) — mirroring
// domain/lint's split from internal/application/lint: iterating a policy
// set against a manifest set, a finding set, or a plan is the deterministic
// evaluator's job (internal/application/policy), not this package's.
package policy

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// APIVersion is the one apiVersion a Policy document declares — deliberately
// not under "datascape.io/" (ADR 021 §1).
const APIVersion = "policy.datascape.io/v1alpha1"

// KindName is the one kind a Policy document declares. (Named KindName, not
// Kind, to avoid colliding with the Kind field of resource.GroupVersionKind
// callers commonly import alongside this package.)
const KindName = "Policy"

// Effect is a rule's enforcement strength (ADR 021 §2 — exactly two values).
type Effect string

const (
	Deny Effect = "deny"
	Warn Effect = "warn"
)

// Valid reports whether e is one of the two closed Effect values.
func (e Effect) Valid() bool { return e == Deny || e == Warn }

// effectRank orders Deny before Warn — a deny is more actionable, mirroring
// domain/lint.Severity's Warning-before-Info ordering for determinism.
func effectRank(e Effect) int {
	if e == Deny {
		return 0
	}
	return 1
}

// StringList decodes either a single YAML/JSON string or an array of
// strings into a []string — ADR 021 §2's own worked example uses both
// shapes for match.kind ("match: {kind: Connection}" vs. "match: {kind:
// [Dataset, Source]}").
type StringList []string

func (s *StringList) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		if single != "" {
			*s = StringList{single}
		}
		return nil
	}
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return fmt.Errorf("match.kind: must be a string or an array of strings: %w", err)
	}
	*s = StringList(list)
	return nil
}

// Match selects which resources a Match+Assert rule applies to — kind/
// label/name selectors, plus the docs/adr/033 (Stage K2) full label
// Selector below (ADR 021 §2: "+ optional label/name selectors"). There is
// deliberately no spec-field selector here: Match answers "which
// resources"; Assert answers "what must hold" — conflating the two would
// reopen the closed vocabulary mid-task.
type Match struct {
	Kind  StringList        `json:"kind,omitempty"`
	Label map[string]string `json:"label,omitempty"`
	Name  string            `json:"name,omitempty"`
	// Selector is the docs/adr/033 (Stage K2) Kubernetes-style label
	// selector — matchLabels/matchExpressions over metadata.labels — the
	// "matchResource gains the same selector form" decision: label
	// integrity is itself policy-governed (e.g. "deny any resource
	// carrying clearance: * outside namespace trusted"). A distinct,
	// richer mechanism from the plain equality-only Label map above (kept
	// for backward compatibility); both may be set, and both must be
	// satisfied. Evaluation gated behind the LabelScopedAccess feature
	// gate (internal/application/policy) — gate-off never evaluates it.
	Selector *Selector `json:"selector,omitempty"`
}

// Selects reports whether e is governed by this Match (nil Match selects
// everything — never constructed by the decoder, since Assert-bearing
// rules always require match, but kept total for callers/tests). Selector
// matching is included unconditionally here — internal/application/policy
// is responsible for never invoking Selects on a Selector-bearing Match
// when the LabelScopedAccess gate is disabled (this package has no
// knowledge of feature gates; that's an application-layer concern).
func (m *Match) Selects(e resource.Envelope) bool {
	if m == nil {
		return true
	}
	if len(m.Kind) > 0 && !containsStr(m.Kind, e.Kind) {
		return false
	}
	if m.Name != "" && m.Name != e.Metadata.Name {
		return false
	}
	for k, v := range m.Label {
		if e.Metadata.Labels[k] != v {
			return false
		}
	}
	if m.Selector != nil && !m.Selector.Selects(e.Metadata.Labels) {
		return false
	}
	return true
}

// SelectorOperator is one of the four closed docs/adr/033 (Stage K2)
// matchExpressions operators — mirroring the Kubernetes LabelSelector
// vocabulary exactly (metav1.LabelSelectorOperator), since that's the
// grammar operators are already familiar with and ADR 033 decision 1
// names explicitly ("Kubernetes-style label selectors").
type SelectorOperator string

const (
	SelectorIn           SelectorOperator = "In"
	SelectorNotIn        SelectorOperator = "NotIn"
	SelectorExists       SelectorOperator = "Exists"
	SelectorDoesNotExist SelectorOperator = "DoesNotExist"
)

// Valid reports whether o is one of the four closed SelectorOperator values.
func (o SelectorOperator) Valid() bool {
	switch o {
	case SelectorIn, SelectorNotIn, SelectorExists, SelectorDoesNotExist:
		return true
	}
	return false
}

// SelectorRequirement is one matchExpressions entry — a key plus an
// operator plus (for In/NotIn) a value set, the same shape as Kubernetes'
// own LabelSelectorRequirement.
type SelectorRequirement struct {
	Key      string           `json:"key"`
	Operator SelectorOperator `json:"operator"`
	Values   []string         `json:"values,omitempty"`
}

// Satisfied reports whether labels satisfies this requirement, following
// Kubernetes' own labels.Requirement.Matches semantics exactly: In requires
// the key present with a value in Values; NotIn matches when the key is
// absent OR present with a value not in Values (the key is not required to
// exist — this is what lets a matchExpressions entry express a negative
// audience condition, e.g. "consumer lacks clearance: gold"); Exists/
// DoesNotExist only inspect key presence.
func (r SelectorRequirement) Satisfied(labels map[string]string) bool {
	value, present := labels[r.Key]
	switch r.Operator {
	case SelectorIn:
		return present && containsStr(r.Values, value)
	case SelectorNotIn:
		return !present || !containsStr(r.Values, value)
	case SelectorExists:
		return present
	case SelectorDoesNotExist:
		return !present
	default:
		return false
	}
}

// validate checks r's own shape: key required, operator one of the four
// closed values, In/NotIn require at least one value, Exists/DoesNotExist
// require none (mirroring Kubernetes' own LabelSelectorRequirement
// validation).
func (r SelectorRequirement) validate() error {
	if r.Key == "" {
		return fmt.Errorf("matchExpressions entry: key is required")
	}
	if !r.Operator.Valid() {
		return fmt.Errorf("matchExpressions entry %q: operator must be one of In/NotIn/Exists/DoesNotExist, got %q", r.Key, r.Operator)
	}
	switch r.Operator {
	case SelectorIn, SelectorNotIn:
		if len(r.Values) == 0 {
			return fmt.Errorf("matchExpressions entry %q: operator %q requires at least one value", r.Key, r.Operator)
		}
	case SelectorExists, SelectorDoesNotExist:
		if len(r.Values) > 0 {
			return fmt.Errorf("matchExpressions entry %q: operator %q must not set values", r.Key, r.Operator)
		}
	}
	return nil
}

// Selector is a docs/adr/033 (Stage K2) Kubernetes-style label selector:
// matchLabels (equality, ANDed) plus matchExpressions (In/NotIn/Exists/
// DoesNotExist, ANDed with each other and with matchLabels) — the exact
// vocabulary ADR 033 decision 1 names, reused identically for both
// matchEdge.selector's From/To and match.selector (matchResource).
type Selector struct {
	MatchLabels      map[string]string     `json:"matchLabels,omitempty"`
	MatchExpressions []SelectorRequirement `json:"matchExpressions,omitempty"`
}

// Selects reports whether labels satisfies every matchLabels equality and
// every matchExpressions requirement (AND across both, and across every
// entry within each) — a nil Selector never occurs mid-evaluation (callers
// only invoke Selects on a non-nil Selector) but is kept total, matching
// every other pure predicate in this file.
func (s *Selector) Selects(labels map[string]string) bool {
	if s == nil {
		return true
	}
	for k, v := range s.MatchLabels {
		if labels[k] != v {
			return false
		}
	}
	for _, req := range s.MatchExpressions {
		if !req.Satisfied(labels) {
			return false
		}
	}
	return true
}

// String renders s deterministically (matchLabels keys sorted; matchExpressions
// in declared order) — the evaluator's Decision.Message must name a firing
// selector (docs/planning/08 K2 accept: "deny output names ... both
// selectors"), and a nondeterministic map-iteration rendering would make
// that message flaky.
func (s *Selector) String() string {
	if s == nil {
		return "{}"
	}
	var parts []string
	if len(s.MatchLabels) > 0 {
		keys := make([]string, 0, len(s.MatchLabels))
		for k := range s.MatchLabels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		pairs := make([]string, 0, len(keys))
		for _, k := range keys {
			pairs = append(pairs, fmt.Sprintf("%s=%s", k, s.MatchLabels[k]))
		}
		parts = append(parts, fmt.Sprintf("matchLabels{%s}", strings.Join(pairs, ",")))
	}
	for _, req := range s.MatchExpressions {
		if len(req.Values) > 0 {
			parts = append(parts, fmt.Sprintf("%s %s [%s]", req.Key, req.Operator, strings.Join(req.Values, ",")))
		} else {
			parts = append(parts, fmt.Sprintf("%s %s", req.Key, req.Operator))
		}
	}
	return strings.Join(parts, " AND ")
}

// validate checks s's own shape: every matchExpressions entry validates,
// and at least one of matchLabels/matchExpressions is set (an empty
// Selector selects everything, which is never a useful rule and is almost
// certainly an authoring mistake — Validate below rejects it, mirroring
// Assert.operatorCount's "must set exactly one" spirit of catching
// vacuous rules at load time rather than silently matching the world).
func (s *Selector) validate() error {
	if s == nil {
		return fmt.Errorf("selector is required")
	}
	if len(s.MatchLabels) == 0 && len(s.MatchExpressions) == 0 {
		return fmt.Errorf("selector must set matchLabels and/or matchExpressions")
	}
	for _, req := range s.MatchExpressions {
		if err := req.validate(); err != nil {
			return err
		}
	}
	return nil
}

// Assert is the condition Match's selected resources must satisfy — exactly
// one of Equals/NotEquals/In/Absent/Matches is set (Validate enforces this).
// A field the resource doesn't declare is never special-cased away: each
// operator decides the absent outcome from its own ordinary comparison
// semantics, treating the missing value as Go nil —
//
//   - equals: nil == X is false for any non-nil X, so an unset field always
//     fails an equals assertion (e.g. "protect-data": metadata.protect must
//     equal true — an unset protect is exactly as non-compliant as protect:
//     false).
//   - notEquals: nil != X is true for any non-nil X, so an unset field
//     always satisfies a notEquals assertion (e.g. "no-isolation-optout":
//     spec.runtime.networkPolicy must not equal "none" — a Provider that
//     never mentions networkPolicy hasn't opted out of anything).
//   - in: nil is a member of a list only if the list explicitly contains
//     null, so an unset field almost always fails (e.g.
//     "no-plaintext-connections": spec.scheme must be in [https] — a
//     Connection that omits scheme entirely (defaulting to plaintext tcp
//     downstream) must still be denied).
//   - matches: the field is stringified (nil -> "") and matched against the
//     regexp — a pattern author who wants an absent field to pass can write
//     that into the pattern itself (e.g. "^(allowed\\.host)?$" — the built-in
//     external-allowlist rule uses exactly this to let managed Connections,
//     which never set spec.host, pass trivially while still denying
//     unlisted external targets).
//   - absent: the only operator that inspects presence directly, unaffected
//     by the value-comparison rules above.
type Assert struct {
	Field     string `json:"field"`
	Equals    any    `json:"equals,omitempty"`
	NotEquals any    `json:"notEquals,omitempty"`
	In        []any  `json:"in,omitempty"`
	Absent    bool   `json:"absent,omitempty"`
	Matches   string `json:"matches,omitempty"`
}

// operatorCount reports how many of the five operators this Assert sets —
// Validate requires exactly one.
func (a Assert) operatorCount() int {
	n := 0
	if a.Absent {
		n++
	}
	if a.Matches != "" {
		n++
	}
	if len(a.In) > 0 {
		n++
	}
	if a.Equals != nil {
		n++
	}
	if a.NotEquals != nil {
		n++
	}
	return n
}

// Satisfied evaluates this Assert against a field's value (present reports
// whether the field existed at all; value is nil when it did not) — see the
// Assert doc comment for each operator's absent-field outcome.
func (a Assert) Satisfied(value any, present bool) (bool, error) {
	switch {
	case a.Absent:
		return !present, nil
	case a.Matches != "":
		re, err := regexp.Compile(a.Matches)
		if err != nil {
			return false, fmt.Errorf("assert.matches %q: %w", a.Matches, err)
		}
		return re.MatchString(stringify(value)), nil
	case len(a.In) > 0:
		for _, want := range a.In {
			if valuesEqual(value, want) {
				return true, nil
			}
		}
		return false, nil
	case a.Equals != nil:
		return valuesEqual(value, a.Equals), nil
	case a.NotEquals != nil:
		return !valuesEqual(value, a.NotEquals), nil
	default:
		return false, fmt.Errorf("assert on field %q sets none of equals/notEquals/in/absent/matches", a.Field)
	}
}

func stringify(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// valuesEqual compares two values decoded from the same JSON/YAML universe
// (both sides always pass through encoding/json, which normalizes numbers
// to float64) via reflect.DeepEqual.
func valuesEqual(a, b any) bool {
	return reflect.DeepEqual(a, b)
}

// FieldValue resolves a dotted path (e.g. "spec.configuration.image",
// "metadata.protect") against e's own JSON representation — the raw,
// undecoded shape (no domain-defaulting), matching the precedent
// internal/application/lint's DL020/DL021 checks set: "the raw envelope
// spec map directly rather than the decoded value" (defaults are filled in
// by each Kind's FromEnvelope, which a policy field selector must not
// silently rely on to know whether an author actually wrote a value).
func FieldValue(e resource.Envelope, path string) (value any, present bool, err error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, false, fmt.Errorf("encode %s for field lookup: %w", e.Key(), err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, false, fmt.Errorf("decode %s for field lookup: %w", e.Key(), err)
	}
	segments := strings.Split(path, ".")
	var cur any = root
	for i, seg := range segments {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false, nil
		}
		v, ok := m[seg]
		if !ok {
			return nil, false, nil
		}
		if i == len(segments)-1 {
			return v, true, nil
		}
		cur = v
	}
	return nil, false, nil
}

// FindingMatch escalates every design-lint finding (docs/adr/020) with the
// named Code to this rule's Effect — "escalate a lint into enforcement"
// (ADR 021 §2's "escalate-duplicate-capture" example).
type FindingMatch struct {
	Code string `json:"code"`
}

// PlanMatch selects plan.Entry values by action×kind — ADR 021 §2's
// "no-dataset-deletes-in-ci" example. Kind empty matches every kind.
type PlanMatch struct {
	Action string `json:"action"`
	Kind   string `json:"kind,omitempty"`
}

// CrossDomainSelector matches a domain-to-domain data-flow edge (docs/adr/022
// Ring 0): From/To name the source/target metadata.domain values exactly —
// a Binding's sourceRef domain -> targetRef domain (the owner's exact
// scenario: a cdc Binding whose source lives in one domain and whose sink
// chain lives in another), or a connectionRef consumer's own domain -> the
// Connection it references ("a Connection consumption is an edge"). Both are
// required — there is no wildcard in this closed vocabulary's first cut.
type CrossDomainSelector struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// EdgeMatch selects graph edges rather than resources or findings — a
// distinct match shape from Match (resource selectors) because an edge's
// identity is the pair of resources it connects, not one resource's own
// fields (docs/adr/022 Ring 0). crossDomain is the compartment-granularity
// shape (domain-to-domain); selector is the docs/adr/033 (Stage K2)
// label-granularity generalization — exactly one of the two is set.
type EdgeMatch struct {
	CrossDomain *CrossDomainSelector `json:"crossDomain,omitempty"`
	// Selector matches an edge by label selectors on each endpoint
	// (docs/adr/033 decision 1: "crossDomain becomes one special case of a
	// general edge-matching vocabulary") — evaluated over the SAME
	// graph-derived edges crossDomain already uses (internal/application/
	// policy's crossDomainEdges). Gated behind the LabelScopedAccess
	// feature gate.
	Selector *EdgeSelector `json:"selector,omitempty"`
}

// EdgeSelector is matchEdge.selector's {from, to} pair (docs/adr/033
// decision 1) — both required, mirroring CrossDomainSelector's own
// From/To-both-required shape: an edge matches when the FROM endpoint's
// labels satisfy From AND the TO endpoint's labels satisfy To.
type EdgeSelector struct {
	From *Selector `json:"from"`
	To   *Selector `json:"to"`
}

// GrantMatch selects docs/adr/026 §2 spec.access wide-grant declarations
// rather than resources or graph edges — a distinct match shape from both
// (a grant's identity is the declaring resource plus the namespace it
// names, not one resource's own fields the way Match works, and not a pair
// of graph nodes the way EdgeMatch works). Namespace is the only selector
// this closed vocabulary defines: "deny any resource that declares a wide
// grant to this namespace."
type GrantMatch struct {
	Namespace string `json:"namespace"`
}

// Rule is one typed policy rule — exactly one of (Match+Assert),
// MatchFinding, MatchPlan, MatchEdge, or MatchGrant is set (Validate
// enforces this).
type Rule struct {
	ID           string        `json:"id"`
	Match        *Match        `json:"match,omitempty"`
	Assert       *Assert       `json:"assert,omitempty"`
	MatchFinding *FindingMatch `json:"matchFinding,omitempty"`
	MatchPlan    *PlanMatch    `json:"matchPlan,omitempty"`
	MatchEdge    *EdgeMatch    `json:"matchEdge,omitempty"`
	// MatchGrant selects docs/adr/026 §2 wide-grant declarations
	// (docs/planning/08 H7) — the mechanism ADR 026 decision 2 promises:
	// "a matchGrant selector lets organizations deny or constrain wide
	// grants."
	MatchGrant *GrantMatch `json:"matchGrant,omitempty"`
	Effect     Effect      `json:"effect"`
	Exemptible bool        `json:"exemptible,omitempty"`
	Message    string      `json:"message,omitempty"`
}

// RuleKind categorizes which of the five closed selector shapes a Rule
// uses, for the evaluator's dispatch.
type RuleKind int

const (
	RuleKindInvalid RuleKind = iota
	RuleKindFieldAssert
	RuleKindFinding
	RuleKindPlan
	RuleKindEdge
	RuleKindGrant
)

func (r Rule) Kind() RuleKind {
	switch {
	case r.Match != nil && r.Assert != nil && r.MatchFinding == nil && r.MatchPlan == nil && r.MatchEdge == nil && r.MatchGrant == nil:
		return RuleKindFieldAssert
	case r.MatchFinding != nil && r.Match == nil && r.Assert == nil && r.MatchPlan == nil && r.MatchEdge == nil && r.MatchGrant == nil:
		return RuleKindFinding
	case r.MatchPlan != nil && r.Match == nil && r.Assert == nil && r.MatchFinding == nil && r.MatchEdge == nil && r.MatchGrant == nil:
		return RuleKindPlan
	case r.MatchEdge != nil && (r.MatchEdge.CrossDomain != nil) != (r.MatchEdge.Selector != nil) && r.Match == nil && r.Assert == nil && r.MatchFinding == nil && r.MatchPlan == nil && r.MatchGrant == nil:
		return RuleKindEdge
	case r.MatchGrant != nil && r.Match == nil && r.Assert == nil && r.MatchFinding == nil && r.MatchPlan == nil && r.MatchEdge == nil:
		return RuleKindGrant
	default:
		return RuleKindInvalid
	}
}

// Metadata is a Policy document's metadata — name only; Policy is not a
// governed resource, so it carries none of resource.Metadata's
// lifecycle/observer machinery.
type Metadata struct {
	Name string `json:"name"`
}

// policySpec is Policy's spec block.
type policySpec struct {
	Rules []Rule `json:"rules"`
}

// Policy is one decoded policy.datascape.io/v1alpha1 document.
type Policy struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	Metadata   Metadata   `json:"metadata"`
	Spec       policySpec `json:"spec"`
}

// Rules is a convenience accessor for p.Spec.Rules.
func (p Policy) Rules() []Rule { return p.Spec.Rules }

// Decode parses one raw document (already schema-validated by the caller,
// mirroring manifest.envelopeFrom's division of labor) into a Policy.
func Decode(raw map[string]any) (Policy, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return Policy{}, fmt.Errorf("encode policy document: %w", err)
	}
	var p Policy
	if err := json.Unmarshal(data, &p); err != nil {
		return Policy{}, fmt.Errorf("decode policy document: %w", err)
	}
	if p.APIVersion != APIVersion {
		return Policy{}, fmt.Errorf("policy %q: unsupported apiVersion %q (expected %s)", p.Metadata.Name, p.APIVersion, APIVersion)
	}
	if p.Kind != KindName {
		return Policy{}, fmt.Errorf("policy %q: unsupported kind %q (expected %s)", p.Metadata.Name, p.Kind, KindName)
	}
	if p.Metadata.Name == "" {
		return Policy{}, fmt.Errorf("policy: metadata.name is required")
	}
	return p, nil
}

// Validate checks the cross-cutting, schema-independent invariants over an
// entire loaded policy set: every rule id is non-empty and globally unique
// (rule ids are the explain-catalog key and the exemption-annotation key,
// so ambiguity there is a load-time error, not a runtime surprise), each
// rule uses exactly one of the four closed selector shapes, effect is one
// of deny/warn, and any assert.matches regex compiles.
func Validate(policies []Policy) error {
	seen := map[string]string{} // rule id -> owning policy name
	for _, p := range policies {
		if len(p.Spec.Rules) == 0 {
			return fmt.Errorf("policy %q: spec.rules is empty", p.Metadata.Name)
		}
		for _, r := range p.Spec.Rules {
			if r.ID == "" {
				return fmt.Errorf("policy %q: a rule has no id", p.Metadata.Name)
			}
			if owner, dup := seen[r.ID]; dup {
				return fmt.Errorf("duplicate policy rule id %q (in policy %q and %q) — rule ids must be globally unique", r.ID, owner, p.Metadata.Name)
			}
			seen[r.ID] = p.Metadata.Name
			if !r.Effect.Valid() {
				return fmt.Errorf("rule %q: effect must be \"deny\" or \"warn\", got %q", r.ID, r.Effect)
			}
			switch r.Kind() {
			case RuleKindFieldAssert:
				if r.Assert.Field == "" {
					return fmt.Errorf("rule %q: assert.field is required", r.ID)
				}
				if n := r.Assert.operatorCount(); n != 1 {
					return fmt.Errorf("rule %q: assert must set exactly one of equals/notEquals/in/absent/matches, got %d", r.ID, n)
				}
				if r.Assert.Matches != "" {
					if _, err := regexp.Compile(r.Assert.Matches); err != nil {
						return fmt.Errorf("rule %q: assert.matches %q: %w", r.ID, r.Assert.Matches, err)
					}
				}
				if r.Match.Selector != nil {
					if err := r.Match.Selector.validate(); err != nil {
						return fmt.Errorf("rule %q: match.selector: %w", r.ID, err)
					}
				}
			case RuleKindFinding:
				if r.MatchFinding.Code == "" {
					return fmt.Errorf("rule %q: matchFinding.code is required", r.ID)
				}
			case RuleKindPlan:
				if r.MatchPlan.Action == "" {
					return fmt.Errorf("rule %q: matchPlan.action is required", r.ID)
				}
			case RuleKindEdge:
				if r.MatchEdge.CrossDomain != nil {
					if r.MatchEdge.CrossDomain.From == "" || r.MatchEdge.CrossDomain.To == "" {
						return fmt.Errorf("rule %q: matchEdge.crossDomain.from and matchEdge.crossDomain.to are required", r.ID)
					}
				} else {
					if r.MatchEdge.Selector.From == nil || r.MatchEdge.Selector.To == nil {
						return fmt.Errorf("rule %q: matchEdge.selector.from and matchEdge.selector.to are required", r.ID)
					}
					if err := r.MatchEdge.Selector.From.validate(); err != nil {
						return fmt.Errorf("rule %q: matchEdge.selector.from: %w", r.ID, err)
					}
					if err := r.MatchEdge.Selector.To.validate(); err != nil {
						return fmt.Errorf("rule %q: matchEdge.selector.to: %w", r.ID, err)
					}
				}
			case RuleKindGrant:
				if r.MatchGrant.Namespace == "" {
					return fmt.Errorf("rule %q: matchGrant.namespace is required", r.ID)
				}
			default:
				return fmt.Errorf("rule %q: must set exactly one of (match+assert), matchFinding, matchPlan, matchEdge, or matchGrant", r.ID)
			}
		}
	}
	return nil
}

// Decision is one rule's outcome against one resource/finding/plan entry —
// domain/lint.Finding's counterpart for the policy vocabulary.
type Decision struct {
	RuleID       string
	Effect       Effect
	Resource     resource.Key
	Message      string
	Exempted     bool
	ExemptReason string
}

// Less orders decisions by (effect, rule id, resource key) — the same
// determinism bar domain/lint.Less holds lint findings to.
func Less(a, b Decision) bool {
	if ra, rb := effectRank(a.Effect), effectRank(b.Effect); ra != rb {
		return ra < rb
	}
	if a.RuleID != b.RuleID {
		return a.RuleID < b.RuleID
	}
	return a.Resource.String() < b.Resource.String()
}

// SortDecisions sorts in place per Less — exported so the evaluator and its
// tests share one determinism-ordering entry point.
func SortDecisions(decisions []Decision) {
	sort.SliceStable(decisions, func(i, j int) bool { return Less(decisions[i], decisions[j]) })
}

// ExemptAnnotation is the metadata.annotations key a resource sets to claim
// an exemption from one or more policy rules (ADR 021 §3):
//
//	metadata:
//	  annotations:
//	    policy.datascape.io/exempt: "no-plaintext-connections: local dev only"
//
// Mirrors domain/lint.WaiveAnnotation's "CODE: reason" shape and
// newline-separated multi-entry convention, but is a distinct annotation
// key and mechanism: an exemption is only ever honored when the rule itself
// declares exemptible: true (ADR 021 §3) — unlike a lint waiver, which any
// resource can always claim.
const ExemptAnnotation = "policy.datascape.io/exempt"

// Exemption is one parsed "rule-id: reason" entry from ExemptAnnotation.
type Exemption struct {
	RuleID string
	Reason string
}

// ParseExemptions parses metadata.annotations[ExemptAnnotation] into
// individual Exemptions, one per newline-separated entry. An entry with no
// reason (or no colon at all) is dropped — an exemption without a reason is
// not a claim the evaluator can honor, and unlike lint's malformed-waiver
// case (ADR 020 §2's explicit DL000), ADR 021 does not call for a
// housekeeping finding of its own here.
func ParseExemptions(annotations map[string]string) []Exemption {
	raw, ok := annotations[ExemptAnnotation]
	if !ok || raw == "" {
		return nil
	}
	var out []Exemption
	for _, entry := range strings.Split(raw, "\n") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, reason, found := strings.Cut(entry, ":")
		if !found {
			continue
		}
		id = strings.TrimSpace(id)
		reason = strings.TrimSpace(reason)
		if id == "" || reason == "" {
			continue
		}
		out = append(out, Exemption{RuleID: id, Reason: reason})
	}
	return out
}

func containsStr(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}
