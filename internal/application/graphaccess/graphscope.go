// This file is docs/planning/08 H7's own consumer of DeriveEdges (docs/adr/026,
// as amended by docs/adr/027) — compiling the FULL graph edge set into the
// docs/adr/026 §1 pairwise access bar: "a workload may reach exactly the
// endpoints its resource's declared references imply... and nothing else."
package graphaccess

import (
	"encoding/json"
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/policy"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// ContainerOf resolves any resource.Key to the resource.Key of the
// Provider/Connection whose runtime container actually realizes it
// (docs/domain/graph.go: only Provider and Connection Kinds ever call
// EnsureContainer — every other Kind, Source/EventStream/Binding/Dataset,
// configures state ON its own providerRef's already-existing container).
// A Provider or Connection resolves to itself; a Kind with no providerRef
// at all (malformed, or a Kind graph.Build never required one for) also
// resolves to itself — the caller then simply finds no membership edges
// for it, which is the safe default (no container, no exposure).
func ContainerOf(k resource.Key, resources map[resource.Key]resource.Envelope) resource.Key {
	env, ok := resources[k]
	if !ok || env.Kind == "Provider" || env.Kind == "Connection" {
		return k
	}
	ref := resource.RefFromSpec(env.Spec, "providerRef")
	if ref.Name == "" {
		return k
	}
	return ref.Key(env.Metadata.Namespace, "Provider")
}

// ContainerDomain resolves the metadata.domain of the resource that
// actually governs a container's runtime placement (docs/adr/022
// addendum: "containers live in their provider's domain") — for a
// Provider, its own domain; for a Connection, its REALIZING provider's
// domain (a Connection's own declared metadata.domain governs graph/policy
// edges only, exactly like resolveRequest's provEnv resolution). k should
// already be a container key (ContainerOf's output); called on anything
// else it falls back to k's own declared domain.
func ContainerDomain(k resource.Key, resources map[resource.Key]resource.Envelope) string {
	env, ok := resources[k]
	if !ok {
		return resource.DefaultDomain
	}
	if env.Kind == "Connection" {
		ref := resource.RefFromSpec(env.Spec, "providerRef")
		if ref.Name != "" {
			if p, ok := resources[ref.Key(env.Metadata.Namespace, "Provider")]; ok {
				return resource.NormalizeDomain(p.Metadata.Domain)
			}
		}
	}
	return resource.NormalizeDomain(env.Metadata.Domain)
}

// AccessGrant is one docs/adr/026 §2 spec.access wide-grant entry: "all of
// namespace Namespace" reachability, declared explicitly on the requesting
// resource (visible in review, deniable/constrainable by a policy
// matchGrant rule — see internal/domain/policy). Selector (docs/adr/033
// decision 3, docs/planning/08 K3) optionally narrows the audience: nil
// means the original bare namespace-wide form (deprecated but still fully
// working — DL022 flags it); non-nil means "namespace AND selector" —
// reuses internal/domain/policy.Selector's exact matchLabels/
// matchExpressions vocabulary (the SAME type K2 already gave the policy
// engine) rather than duplicating selector matching here.
type AccessGrant struct {
	Namespace string
	Selector  *policy.Selector
}

// AccessGrants reads env.Spec["access"] into typed grants. Malformed or
// incomplete entries are silently skipped — JSON Schema validation, run
// earlier in the pipeline (schemas/v1alpha1/meta.json#/$defs/accessGrant),
// already refuses a manifest that doesn't match the declared shape; this
// reader is defensive, not authoritative, exactly like graphaccess'
// sibling helpers (resolveProviderRef) that also assume a pre-validated
// envelope. A malformed selector block (shouldn't happen post-schema-
// validation) decodes to nil rather than erroring, for the same reason.
func AccessGrants(env resource.Envelope) []AccessGrant {
	raw, ok := env.Spec["access"].([]any)
	if !ok {
		return nil
	}
	var out []AccessGrant
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ns, _ := m["namespace"].(string)
		if ns == "" {
			continue
		}
		grant := AccessGrant{Namespace: resource.NormalizeNamespace(ns)}
		if sel, ok := m["selector"].(map[string]any); ok {
			grant.Selector = decodeSelector(sel)
		}
		out = append(out, grant)
	}
	return out
}

// decodeSelector round-trips raw (already schema-validated JSON) through
// encoding/json into a policy.Selector — the same round-trip
// policy.Decode/manifest.validateAgainstSchema use elsewhere in this
// codebase to bridge a raw map[string]any onto a typed Go struct.
func decodeSelector(raw map[string]any) *policy.Selector {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var sel policy.Selector
	if err := json.Unmarshal(data, &sel); err != nil {
		return nil
	}
	return &sel
}

// containerMembers returns every resource.Key that ContainerOf-collapses
// onto self, PLUS self itself (a Provider/Connection's own edges count
// too — e.g. a Connection's own connectionRef/target/via fields).
func containerMembers(self resource.Key, resources map[resource.Key]resource.Envelope) map[resource.Key]bool {
	out := map[resource.Key]bool{self: true}
	for k := range resources {
		if ContainerOf(k, resources) == self {
			out[k] = true
		}
	}
	return out
}

// EgressPeers computes the containers self may DIAL under docs/adr/026's
// graph-scoped access: every edge (from DeriveEdges' full graph edge set)
// where a resource collapsing onto self is the FROM side, resolved to the
// TO side's own realizing container, plus every OTHER container found in a
// namespace one of self's own (or self-collapsed) resources' spec.access
// grants names (narrowed by the grant's own selector, if any — see
// grantAdmits). self is never included (ADR 026 decision 1's "brokers
// reach brokers" — a container's own internal topology needs no grant).
// labelScopedAccessEnabled is the docs/adr/033 (K3) LabelScopedAccess gate
// state — see grantAdmits for what it controls; a bare namespace-wide
// grant (Selector == nil) is entirely unaffected by it.
func EgressPeers(edges []Edge, self resource.Key, resources map[resource.Key]resource.Envelope, labelScopedAccessEnabled bool) []resource.Key {
	members := containerMembers(self, resources)
	out := map[resource.Key]bool{}
	for _, e := range edges {
		if !members[e.From] {
			continue
		}
		if other := ContainerOf(e.To, resources); other != self {
			out[other] = true
		}
	}
	for k := range members {
		env, ok := resources[k]
		if !ok {
			continue
		}
		for _, grant := range AccessGrants(env) {
			addGrantedContainers(out, grant, self, resources, labelScopedAccessEnabled)
		}
	}
	return sortedKeys(out)
}

// IngressPeers computes the containers that may DIAL self — the mirror
// image of EgressPeers, and the ONLY direction Kubernetes NetworkPolicy
// needs to compile (this codebase's default-deny wall governs ingress
// only; egress has never been restricted, docs/planning/08 B7): every edge
// where a resource collapsing onto self is the TO side, resolved to the
// FROM side's own realizing container, plus every OTHER container whose
// OWN spec.access grants a namespace self's container belongs to — a
// selector-bearing grant additionally requires self's OWN container
// envelope to satisfy the selector (grantAdmits), since from self's
// vantage self IS "the resource in the namespace" the selector narrows.
func IngressPeers(edges []Edge, self resource.Key, resources map[resource.Key]resource.Envelope, labelScopedAccessEnabled bool) []resource.Key {
	members := containerMembers(self, resources)
	out := map[resource.Key]bool{}
	for _, e := range edges {
		if !members[e.To] {
			continue
		}
		if other := ContainerOf(e.From, resources); other != self {
			out[other] = true
		}
	}
	selfEnv := resources[self]
	for k, env := range resources {
		c := ContainerOf(k, resources)
		if c == self {
			continue
		}
		for _, grant := range AccessGrants(env) {
			if grant.Namespace != self.Namespace {
				continue
			}
			if !grantAdmits(grant, selfEnv, labelScopedAccessEnabled) {
				continue
			}
			out[c] = true
		}
	}
	return sortedKeys(out)
}

// MembershipEdges is EgressPeers ∪ IngressPeers — the shape Docker needs
// (a per-edge network is joined by both endpoints regardless of which one
// declared the reference; Docker's network membership grants reachability
// symmetrically, so the direction that matters for Kubernetes NetworkPolicy
// is moot there). See EgressPeers/IngressPeers for the two directions this
// unions.
func MembershipEdges(edges []Edge, self resource.Key, resources map[resource.Key]resource.Envelope, labelScopedAccessEnabled bool) []resource.Key {
	out := map[resource.Key]bool{}
	for _, p := range EgressPeers(edges, self, resources, labelScopedAccessEnabled) {
		out[p] = true
	}
	for _, p := range IngressPeers(edges, self, resources, labelScopedAccessEnabled) {
		out[p] = true
	}
	return sortedKeys(out)
}

// grantAdmits reports whether grant's audience covers candidateEnv — true
// unconditionally for a bare namespace-wide grant (Selector == nil, the
// pre-K3 form, callers already filtered candidateEnv's namespace before
// calling this). For a selector-bearing grant (docs/adr/033 decision 3,
// docs/planning/08 K3): INERT (never admits anyone) when
// labelScopedAccessEnabled is false — the zero-trust answer ADR 033's K3
// note settles on ("never wider than declared intent when the gate is
// off" — a selector grant must not silently fall back to namespace-wide);
// otherwise candidateEnv's own metadata.labels must satisfy the selector.
func grantAdmits(grant AccessGrant, candidateEnv resource.Envelope, labelScopedAccessEnabled bool) bool {
	if grant.Selector == nil {
		return true
	}
	if !labelScopedAccessEnabled {
		return false
	}
	return grant.Selector.Selects(candidateEnv.Metadata.Labels)
}

// addGrantedContainers adds every container in grant.Namespace whose own
// envelope grantAdmits admits (self excluded) to out — the EgressPeers
// side of grant compilation. A selector-bearing grant is evaluated against
// the CANDIDATE CONTAINER's own labels (the resource that actually becomes
// the peer/network member), not any collapsed resource's labels, mirroring
// how peers are container-granular everywhere else in this file.
func addGrantedContainers(out map[resource.Key]bool, grant AccessGrant, self resource.Key, resources map[resource.Key]resource.Envelope, labelScopedAccessEnabled bool) {
	for candidate := range resources {
		c := ContainerOf(candidate, resources)
		if c == self || c.Namespace != grant.Namespace {
			continue
		}
		containerEnv, ok := resources[c]
		if !ok {
			continue
		}
		if !grantAdmits(grant, containerEnv, labelScopedAccessEnabled) {
			continue
		}
		out[c] = true
	}
}

func sortedKeys(m map[resource.Key]bool) []resource.Key {
	out := make([]resource.Key, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
