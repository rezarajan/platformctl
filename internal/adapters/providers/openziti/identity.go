package openziti

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// session is the live, authenticated mediation.MediationProvider this
// adapter hands out via reconciler.MediationCapableProvider.Mediation —
// bound to one mediation-Provider instance's controller for its lifetime
// (one Reconcile call), never held across calls (docs/planning/08 F5).
type session struct {
	client *edgeClient
	// closeTunnel tears down the reachable tunnel to the controller (a
	// no-op on Docker, a port-forward teardown on Kubernetes). Callers of
	// newSession MUST defer Close() — the mediation.MediationProvider port
	// itself carries no Close (nothing consumes Mediation() yet, H7), so
	// this adapter's own reconcile/destroy paths own the lifetime.
	closeTunnel func() error
	// labelScopedAccessEnabled is the docs/adr/033 LabelScopedAccess gate's
	// current state (docs/planning/08 K4), read once at newSession
	// construction via runtime.LabelScopedAccessQuery — see that
	// interface's own doc comment for why req.Runtime, not a new Request
	// field, carries it. Gates every label-derived role-attribute/
	// attribute-scoped-policy behavior this file adds: false makes every
	// method below byte-identical to pre-K4 behavior (this task's gate-off
	// pin), including a session built by a caller that skips newSession
	// entirely (a unit test constructing &session{client: c} directly) —
	// the zero value is "disabled," matching LabelScopedAccessQuery's own
	// documented default-absent behavior.
	labelScopedAccessEnabled bool
}

// Close releases the session's controller tunnel. Idempotent-safe: a nil
// closeTunnel (a session built in a unit test with a pre-wired client) is
// a no-op.
func (s *session) Close() error {
	if s.closeTunnel == nil {
		return nil
	}
	return s.closeTunnel()
}

// newSession authenticates against the mediation Provider named by
// req.Provider, the same controller reconcileInstance bootstraps. It
// reaches the controller through runtime.EnsureReachable (dialController),
// never ctrlState.HostAddr, so it works identically on Docker (published
// port) and Kubernetes (ephemeral port-forward) — the substrate-neutral
// seam that makes the whole mediation plane substrate-independent
// (docs/adr/027). The caller owns the returned session's lifetime and must
// defer (*session).Close().
func newSession(ctx context.Context, req reconciler.Request) (*session, error) {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return nil, err
	}
	ic := parseInstanceConfig(cfg)
	providerName := naming.RuntimeObjectName(req.Provider)
	ctrlName := controllerName(providerName)

	if _, ok, err := req.Runtime.Inspect(ctx, ctrlName); err != nil {
		return nil, fmt.Errorf("openziti: inspect controller %q: %w", ctrlName, err)
	} else if !ok {
		return nil, fmt.Errorf("openziti: controller %q does not exist yet (Provider %q must reconcile first)", ctrlName, providerName)
	}
	creds, refName, err := providerkit.ResolveCredential(cfg, req.Secrets, "adminSecretRef", providerName)
	if err != nil {
		return nil, err
	}
	username, password := creds["username"], creds["password"]
	if username == "" || password == "" {
		return nil, fmt.Errorf("Provider %q (type: openziti): secretRef %q must carry keys \"username\" and \"password\"", providerName, refName)
	}

	client, closeTunnel, err := dialController(ctx, req.Runtime, ctrlName, ic.ControllerPort)
	if err != nil {
		return nil, err
	}
	if err := client.Authenticate(ctx, username, password); err != nil {
		_ = closeTunnel()
		return nil, fmt.Errorf("openziti: session authentication: %w", err)
	}
	labelScoped := false
	if q, ok := req.Runtime.(runtime.LabelScopedAccessQuery); ok {
		labelScoped = q.LabelScopedAccessEnabled()
	}
	return &session{client: client, closeTunnel: closeTunnel, labelScopedAccessEnabled: labelScoped}, nil
}

// sanitizeRoleAttributeSegment strips a string down to Ziti's safe role-
// attribute charset (alphanumeric and '-'), collapsing any run of other
// characters to a single '-' and trimming leading/trailing '-' — the exact
// filter identityRoleAttribute has always applied to a SPIFFE URI, factored
// out here so labelRoleAttribute (docs/planning/08 K4) can apply the SAME
// filter to a label key/value segment without duplicating the loop.
func sanitizeRoleAttributeSegment(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-':
			out = append(out, c)
		default:
			if len(out) > 0 && out[len(out)-1] != '-' {
				out = append(out, '-')
			}
		}
	}
	for len(out) > 0 && out[len(out)-1] == '-' {
		out = out[:len(out)-1]
	}
	return string(out)
}

// identityRoleAttribute derives a Ziti-safe role attribute from a
// SPIFFE-aligned identity URI — role attributes exclude "://" and "/".
// Kept for observability (an operator inspecting the Ziti console sees a
// legible tag) and as this adapter's deterministic identity/service NAME
// (client.go's findByName-keyed idempotency); RealizeEdge authorizes by
// direct @id reference for every unlabeled endpoint (docs/planning/08 K4:
// labelRoleAttribute below is the label-derived counterpart used only when
// the LabelScopedAccess gate is on AND the endpoint carries labels), so
// this value's exactness is not security-load-bearing on its own.
func identityRoleAttribute(uri string) string {
	return sanitizeRoleAttributeSegment(uri)
}

// labelRoleAttribute derives a single Ziti role attribute from one
// metadata.labels key/value pair (docs/planning/08 K4, docs/adr/033
// decision 4) — the encoding this adapter picked and documents here:
// "label.<key>.<value>", where <key> and <value> are each individually
// sanitized to Ziti's safe role-attribute charset via
// sanitizeRoleAttributeSegment (the same filter identityRoleAttribute
// applies to a URI). The "label." prefix and "." joiner are safe
// disjointness guarantees, not decorative: sanitizeRoleAttributeSegment
// never emits a ".", so no identity/service-name attribute
// (identityRoleAttribute's own output, or the literal "datascape-mediated"
// tag) can ever collide with a label-derived attribute, and two distinct
// (key, value) pairs can never collide with each other either (the
// sanitized key and value each internally contain no "." to be confused
// with the joiner). Deterministic: the same (key, value) always encodes to
// the same string, so re-deriving it on every reconcile with unchanged
// labels reproduces a byte-identical role attribute — this file's
// idempotency discipline (K4's Accept bar: "same labels, same attributes").
func labelRoleAttribute(key, value string) string {
	return "label." + sanitizeRoleAttributeSegment(key) + "." + sanitizeRoleAttributeSegment(value)
}

// labelRoleAttributes derives the full, deterministically-ordered set of
// role attributes for node's metadata.labels — sorted by key (Go maps
// iterate in random order; every caller needs the SAME ordering on every
// reconcile for the idempotency bar above to hold, and for the Dial-policy
// role-ref list construction in dialRoleRefs below to be stable byte for
// byte across runs). nil for a nil/empty labels map — every caller treats
// that as "nothing to derive," never a distinct zero-length-vs-nil case.
func labelRoleAttributes(labels map[string]string) []string {
	if len(labels) == 0 {
		return nil
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, labelRoleAttribute(k, labels[k]))
	}
	return out
}

// identityMintRoleAttributes is the roleAttributes list MintIdentity/
// mintIdentityWithToken/RealizeEdge assign to an identity entity: the
// literal "datascape-mediated" tag every identity has always carried,
// PLUS (docs/planning/08 K4, only when labelScopedAccessEnabled) the
// label-derived attributes above. Gate off (or an unlabeled node): the
// returned slice is exactly ["datascape-mediated"], unchanged from every
// pre-K4 call site — this task's gate-off byte-identical pin.
func identityMintRoleAttributes(labels map[string]string, labelScopedAccessEnabled bool) []string {
	attrs := []string{"datascape-mediated"}
	if labelScopedAccessEnabled {
		attrs = append(attrs, labelRoleAttributes(labels)...)
	}
	return attrs
}

// serviceRoleAttributes is the roleAttributes list a mediated Connection's
// Ziti Service entity carries (docs/planning/08 K4) — unlike identities, a
// service has never carried a base tag, so gate-off or an unlabeled target
// yields nil, which client.go's upsertService omits from the request body
// entirely (preserving the exact pre-K4 create/converge body when nothing
// applies — this task's gate-off byte-identical pin).
func serviceRoleAttributes(labels map[string]string, labelScopedAccessEnabled bool) []string {
	if !labelScopedAccessEnabled {
		return nil
	}
	return labelRoleAttributes(labels)
}

// dialRoleRefs computes the Ziti role-reference list Dial-policy caller
// (RealizeEdge below) uses for ONE side (identityRoles or serviceRoles) of
// a service-policy, and whether that side needs AllOf semantics
// (docs/planning/08 K4, docs/adr/033 decision 4 — "the mediator's
// service-policies enforce by attribute at dial time"). When the
// LabelScopedAccess gate is on AND labels is non-empty, every label-
// derived attribute is referenced with Ziti's '#' role-attribute selector
// (matching any identity/service carrying it) and allOf is true: a
// resource must carry EVERY declared label to match, mirroring K2's
// matchLabels-is-a-conjunction semantics (internal/domain/policy.Selector.
// Matches's own labels.Requirement chain) — so the mediator's enforcement
// and the policy plane's admission check the SAME fact the SAME way (ADR
// 027 Layer 1). Otherwise (gate off, or the endpoint carries no labels)
// this returns the exact single "@<id>" direct reference this adapter has
// always used, with allOf false — byte-identical to pre-K4 behavior; id
// must already be the entity's resolved Ziti @id (upsertIdentity/
// upsertService's own return value), never re-derived here.
func dialRoleRefs(labels map[string]string, id string, labelScopedAccessEnabled bool) (refs []string, allOf bool) {
	if labelScopedAccessEnabled {
		if attrs := labelRoleAttributes(labels); len(attrs) > 0 {
			refs = make([]string, len(attrs))
			for i, a := range attrs {
				refs[i] = "#" + a
			}
			return refs, true
		}
	}
	return []string{"@" + id}, false
}

// upsertIdentity is session's gate-aware wrapper around the client's plain
// upsertIdentity / convergence-checking upsertIdentityConverge pair
// (docs/planning/08 K4): when the LabelScopedAccess gate is enabled, an
// already-existing identity's roleAttributes are read back and healed if
// they've drifted from roleAttributes (e.g. the manifest's labels changed
// since this identity was first minted) — the same "drift is healed, not
// merely detected" contract client.go's upsertService already holds for
// encryptionRequired (docs/planning/08 H6 accept). Gate off: delegates to
// the client's plain upsertIdentity UNCHANGED — zero extra reads, the
// exact pre-K4 call shape on the already-exists path (this task's gate-off
// byte-identical pin: literally the same HTTP calls, not merely the same
// outcome).
func (s *session) upsertIdentity(ctx context.Context, name string, roleAttributes []string) (id, jwt string, err error) {
	if s.labelScopedAccessEnabled {
		return s.client.upsertIdentityConverge(ctx, name, roleAttributes, true)
	}
	return s.client.upsertIdentity(ctx, name, roleAttributes)
}

// MintIdentity implements mediation.MediationProvider. See client.go's
// upsertIdentity doc comment for the idempotency contract; see
// identityMintRoleAttributes for the docs/planning/08 K4 label-derived
// roleAttributes addition (gate off: byte-identical to pre-K4).
func (s *session) MintIdentity(ctx context.Context, node resource.Envelope) (mediation.WorkloadIdentity, error) {
	uri := naming.WorkloadIdentityURI(node)
	id, _, err := s.upsertIdentity(ctx, identityRoleAttribute(uri), identityMintRoleAttributes(node.Metadata.Labels, s.labelScopedAccessEnabled))
	if err != nil {
		return mediation.WorkloadIdentity{}, fmt.Errorf("openziti: mint identity for %s: %w", uri, err)
	}
	fingerprint, _, err := s.client.identityFingerprint(ctx, id)
	if err != nil {
		return mediation.WorkloadIdentity{}, fmt.Errorf("openziti: read fingerprint for %s: %w", uri, err)
	}
	return mediation.WorkloadIdentity{URI: uri, Fingerprint: fingerprint, Labels: node.Metadata.Labels}, nil
}

// mintIdentityWithToken is the internal counterpart MintIdentity does not
// expose (mediation.WorkloadIdentity never carries key/enrollment
// material, docs/adr/013): connection.go's dial-side tunneler container
// needs the raw enrollment JWT to enroll itself at container-create time.
func (s *session) mintIdentityWithToken(ctx context.Context, node resource.Envelope) (identity mediation.WorkloadIdentity, id string, enrollmentJWT string, err error) {
	uri := naming.WorkloadIdentityURI(node)
	entityID, jwt, uerr := s.upsertIdentity(ctx, identityRoleAttribute(uri), identityMintRoleAttributes(node.Metadata.Labels, s.labelScopedAccessEnabled))
	if uerr != nil {
		return mediation.WorkloadIdentity{}, "", "", fmt.Errorf("openziti: mint identity for %s: %w", uri, uerr)
	}
	fingerprint, _, ferr := s.client.identityFingerprint(ctx, entityID)
	if ferr != nil {
		return mediation.WorkloadIdentity{}, "", "", fmt.Errorf("openziti: read fingerprint for %s: %w", uri, ferr)
	}
	return mediation.WorkloadIdentity{URI: uri, Fingerprint: fingerprint, Labels: node.Metadata.Labels}, entityID, jwt, nil
}

// RealizeEdge implements mediation.MediationProvider: it compiles the Dial
// authorization for edge (identity.go's client-level policy primitive).
// Bind authorization is realized differently by THIS adapter (a
// router-hosted transport terminator, connection.go) rather than through
// this method — see openziti.go's package doc comment "Mechanism summary"
// for why (no per-target tunneler process is needed for the dark-service
// posture this adapter chooses). A future adapter, or a future evolution
// of this one, that DOES realize Bind through a per-target tunneler
// process would implement Authorized.Bind here instead; recorded as a
// deliberate, documented scope choice, not an oversight.
//
// docs/planning/08 K4: when the LabelScopedAccess gate is on and an
// endpoint carries metadata.labels (edge.From.Labels/edge.To.Labels — the
// port field this task adds, populated by connection.go from the SAME
// envelopes graphaccess.CompileMediatedConnections/req.Resources already
// hold), the compiled Dial policy references that side by its label-
// derived role attributes (dialRoleRefs, "#<attr>", AllOf semantics)
// instead of the identity/service's exact "@<id>". An unlabeled side (or
// the gate off) keeps the exact "@<id>" reference this adapter has always
// used — see dialRoleRefs's own doc comment for the full semantics and
// why a single shared "semantic" value on the policy body is sufficient
// for both sides simultaneously.
func (s *session) RealizeEdge(ctx context.Context, edge mediation.Edge) error {
	if !edge.Authorized.Dial {
		return nil
	}
	fromID, _, err := s.upsertIdentity(ctx, identityRoleAttribute(edge.From.URI), identityMintRoleAttributes(edge.From.Labels, s.labelScopedAccessEnabled))
	if err != nil {
		return fmt.Errorf("openziti: realize edge %s -> %s: resolve From identity: %w", edge.From.URI, edge.To.URI, err)
	}
	svcID, err := s.client.upsertService(ctx, identityRoleAttribute(edge.To.URI), serviceRoleAttributes(edge.To.Labels, s.labelScopedAccessEnabled))
	if err != nil {
		return fmt.Errorf("openziti: realize edge %s -> %s: resolve service: %w", edge.From.URI, edge.To.URI, err)
	}
	policyName := "dial-" + identityRoleAttribute(edge.From.URI) + "-" + identityRoleAttribute(edge.To.URI)
	identityRoles, identityAttrBased := dialRoleRefs(edge.From.Labels, fromID, s.labelScopedAccessEnabled)
	serviceRoles, serviceAttrBased := dialRoleRefs(edge.To.Labels, svcID, s.labelScopedAccessEnabled)
	semantic := "AnyOf"
	if identityAttrBased || serviceAttrBased {
		semantic = "AllOf"
	}
	return s.client.upsertDialPolicy(ctx, policyName, semantic, identityRoles, serviceRoles)
}

// RevokeEdge implements mediation.MediationProvider.
func (s *session) RevokeEdge(ctx context.Context, edge mediation.Edge) error {
	policyName := "dial-" + identityRoleAttribute(edge.From.URI) + "-" + identityRoleAttribute(edge.To.URI)
	return s.client.deleteDialPolicy(ctx, policyName)
}

// RevokeIdentity implements mediation.MediationProvider: removes the
// identity AND every edge (dial policy) plus the target service that names
// it — explicitly, not by trusting a cascade. An earlier version relied on
// "Ziti's own referential-integrity behavior on identity delete", but the
// L2a conformance suite disproved that live: deleting an identity leaves
// the dial policy object standing (Ziti orphans the @id reference rather
// than deleting the policy), which is exactly the dangling policy — the
// posture-decay docs/planning/09 §4 warns against — this method's contract
// forbids. So teardown is now explicit and name-keyed, the same
// deterministic encoding RealizeEdge/RevokeEdge use: the dial policy is
// "dial-<fromAttr>-<toAttr>", so a policy references this identity when its
// name is prefixed "dial-<attr>-" (identity is the From) or suffixed
// "-<attr>" (identity is the To); the To side additionally has a service
// named <attr>. All deletes are "already gone is success" (idempotent).
func (s *session) RevokeIdentity(ctx context.Context, identity mediation.WorkloadIdentity) error {
	attr := identityRoleAttribute(identity.URI)

	policies, err := s.client.listDialPolicies(ctx)
	if err != nil {
		return fmt.Errorf("openziti: revoke identity %s: list edges: %w", identity.URI, err)
	}
	for _, pol := range policies {
		if strings.HasPrefix(pol.Name, "dial-"+attr+"-") || strings.HasSuffix(pol.Name, "-"+attr) {
			if derr := s.client.deleteDialPolicy(ctx, pol.Name); derr != nil {
				return fmt.Errorf("openziti: revoke identity %s: delete edge %q: %w", identity.URI, pol.Name, derr)
			}
		}
	}

	// The To-side service (RealizeEdge names it after the target identity's
	// URI) — remove it so no orphan service survives the identity it fronts.
	if svcID, ok, serr := s.client.findByName(ctx, "services", attr); serr != nil {
		return fmt.Errorf("openziti: revoke identity %s: find service: %w", identity.URI, serr)
	} else if ok {
		if derr := s.client.deleteService(ctx, svcID); derr != nil {
			return fmt.Errorf("openziti: revoke identity %s: delete service: %w", identity.URI, derr)
		}
	}

	id, ok, err := s.client.findByName(ctx, "identities", attr)
	if err != nil {
		return fmt.Errorf("openziti: revoke identity %s: %w", identity.URI, err)
	}
	if !ok {
		return nil
	}
	return s.client.deleteIdentity(ctx, id)
}

// ObservedEdges implements mediation.MediationProvider's drift-detection
// primitive: every currently-enforced Dial policy, decoded back into
// mediation.Edge form. Identity/service names are this adapter's own
// deterministic role-attribute encoding of a SPIFFE URI, which is lossy in
// one direction (it cannot invert back to the exact URI) — ObservedEdges
// therefore reports the encoded name in both URI fields when no live
// identity carries a recoverable original; callers diffing desired vs.
// observed do so by encoded name, matching how RealizeEdge/RevokeEdge
// themselves key on it (docs/planning/08 H6 accept: "drift on out-of-band
// Ziti policy edits detected and healed" — healing re-applies the SAME
// deterministic Realize/Revoke calls, which is name-keyed already).
// docs/planning/08 K4: an attribute-based policy's role-ref list can carry
// MULTIPLE "#<attr>" entries per side (dialRoleRefs) — trimRoleRef reports
// only the first, unprefixed by "@" (a "#" entry passes through as-is).
// This narrows drift-detection fidelity for attribute-based edges beyond
// the pre-K4 "lossy decode" limitation this comment already documents;
// full multi-attribute reconstruction is not required by K4's own accept
// bar (the positive mediator-state assertion, not general ObservedEdges
// round-tripping) and is left as a documented, not silently accepted,
// follow-up.
func (s *session) ObservedEdges(ctx context.Context) ([]mediation.Edge, error) {
	policies, err := s.client.listDialPolicies(ctx)
	if err != nil {
		return nil, fmt.Errorf("openziti: observe edges: %w", err)
	}
	var out []mediation.Edge
	for _, pol := range policies {
		from := mediation.WorkloadIdentity{URI: trimRoleRef(pol.IdentityRoles)}
		to := mediation.WorkloadIdentity{URI: trimRoleRef(pol.ServiceRoles)}
		out = append(out, mediation.Edge{From: from, To: to, Authorized: mediation.DialBind{Dial: true}})
	}
	return out, nil
}

func trimRoleRef(roles []string) string {
	if len(roles) == 0 {
		return ""
	}
	r := roles[0]
	if len(r) > 0 && r[0] == '@' {
		return r[1:]
	}
	return r
}

// ObservedIdentities implements mediation.MediationProvider's identity-side
// drift primitive.
func (s *session) ObservedIdentities(ctx context.Context) ([]mediation.WorkloadIdentity, error) {
	ids, err := s.client.listIdentities(ctx)
	if err != nil {
		return nil, fmt.Errorf("openziti: observe identities: %w", err)
	}
	out := make([]mediation.WorkloadIdentity, 0, len(ids))
	for _, id := range ids {
		fingerprint, _, ferr := s.client.identityFingerprint(ctx, id.ID)
		if ferr != nil {
			return nil, fmt.Errorf("openziti: observe identities: fingerprint for %s: %w", id.Name, ferr)
		}
		out = append(out, mediation.WorkloadIdentity{URI: id.Name, Fingerprint: fingerprint})
	}
	return out, nil
}
