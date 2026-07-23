package openziti

import (
	"context"
	"fmt"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// session is the live, authenticated mediation.MediationProvider this
// adapter hands out via reconciler.MediationCapableProvider.Mediation —
// bound to one mediation-Provider instance's controller for its lifetime
// (one Reconcile call), never held across calls (docs/planning/08 F5).
type session struct {
	client *edgeClient
}

// newSession authenticates against the mediation Provider named by
// req.Provider, the same controller reconcileInstance bootstraps —
// Inspecting its container for the live host address rather than assuming
// a fixed name/port lets a session work regardless of which resource
// (the Provider itself, or a Connection it realizes) is being reconciled.
func newSession(ctx context.Context, req reconciler.Request) (*session, error) {
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return nil, err
	}
	ic := parseInstanceConfig(cfg)
	providerName := naming.RuntimeObjectName(req.Provider)
	ctrlName := controllerName(providerName)

	ctrlState, ok, err := req.Runtime.Inspect(ctx, ctrlName)
	if err != nil {
		return nil, fmt.Errorf("openziti: inspect controller %q: %w", ctrlName, err)
	}
	if !ok {
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

	client := newEdgeClient(fmt.Sprintf("https://%s", ctrlState.HostAddr(ic.ControllerPort)))
	if err := client.Authenticate(ctx, username, password); err != nil {
		return nil, fmt.Errorf("openziti: session authentication: %w", err)
	}
	return &session{client: client}, nil
}

// identityRoleAttribute derives a Ziti-safe role attribute from a
// SPIFFE-aligned identity URI — role attributes exclude "://" and "/".
// Kept for observability (an operator inspecting the Ziti console sees a
// legible tag); RealizeEdge itself authorizes by direct @id reference
// (client.go's upsertDialPolicy), so this value's exactness is not
// security-load-bearing.
func identityRoleAttribute(uri string) string {
	out := make([]byte, 0, len(uri))
	for i := 0; i < len(uri); i++ {
		c := uri[i]
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

// MintIdentity implements mediation.MediationProvider. See client.go's
// upsertIdentity doc comment for the idempotency contract.
func (s *session) MintIdentity(ctx context.Context, node resource.Envelope) (mediation.WorkloadIdentity, error) {
	uri := naming.WorkloadIdentityURI(node)
	id, _, err := s.client.upsertIdentity(ctx, identityRoleAttribute(uri), []string{"datascape-mediated"})
	if err != nil {
		return mediation.WorkloadIdentity{}, fmt.Errorf("openziti: mint identity for %s: %w", uri, err)
	}
	fingerprint, _, err := s.client.identityFingerprint(ctx, id)
	if err != nil {
		return mediation.WorkloadIdentity{}, fmt.Errorf("openziti: read fingerprint for %s: %w", uri, err)
	}
	return mediation.WorkloadIdentity{URI: uri, Fingerprint: fingerprint}, nil
}

// mintIdentityWithToken is the internal counterpart MintIdentity does not
// expose (mediation.WorkloadIdentity never carries key/enrollment
// material, docs/adr/013): connection.go's dial-side tunneler container
// needs the raw enrollment JWT to enroll itself at container-create time.
func (s *session) mintIdentityWithToken(ctx context.Context, node resource.Envelope) (identity mediation.WorkloadIdentity, id string, enrollmentJWT string, err error) {
	uri := naming.WorkloadIdentityURI(node)
	entityID, jwt, uerr := s.client.upsertIdentity(ctx, identityRoleAttribute(uri), []string{"datascape-mediated"})
	if uerr != nil {
		return mediation.WorkloadIdentity{}, "", "", fmt.Errorf("openziti: mint identity for %s: %w", uri, uerr)
	}
	fingerprint, _, ferr := s.client.identityFingerprint(ctx, entityID)
	if ferr != nil {
		return mediation.WorkloadIdentity{}, "", "", fmt.Errorf("openziti: read fingerprint for %s: %w", uri, ferr)
	}
	return mediation.WorkloadIdentity{URI: uri, Fingerprint: fingerprint}, entityID, jwt, nil
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
func (s *session) RealizeEdge(ctx context.Context, edge mediation.Edge) error {
	if !edge.Authorized.Dial {
		return nil
	}
	fromID, _, err := s.client.upsertIdentity(ctx, identityRoleAttribute(edge.From.URI), []string{"datascape-mediated"})
	if err != nil {
		return fmt.Errorf("openziti: realize edge %s -> %s: resolve From identity: %w", edge.From.URI, edge.To.URI, err)
	}
	svcID, err := s.client.upsertService(ctx, identityRoleAttribute(edge.To.URI))
	if err != nil {
		return fmt.Errorf("openziti: realize edge %s -> %s: resolve service: %w", edge.From.URI, edge.To.URI, err)
	}
	policyName := "dial-" + identityRoleAttribute(edge.From.URI) + "-" + identityRoleAttribute(edge.To.URI)
	return s.client.upsertDialPolicy(ctx, policyName, svcID, []string{fromID})
}

// RevokeEdge implements mediation.MediationProvider.
func (s *session) RevokeEdge(ctx context.Context, edge mediation.Edge) error {
	policyName := "dial-" + identityRoleAttribute(edge.From.URI) + "-" + identityRoleAttribute(edge.To.URI)
	return s.client.deleteDialPolicy(ctx, policyName)
}

// RevokeIdentity implements mediation.MediationProvider: removing the
// identity by name (its role-attribute-derived, deterministic name — the
// same value MintIdentity/RealizeEdge compute) removes every policy
// referencing it too (Ziti's own referential-integrity behavior on
// identity delete cascades to service-policy identityRoles entries that
// name it by @id).
func (s *session) RevokeIdentity(ctx context.Context, identity mediation.WorkloadIdentity) error {
	name := identityRoleAttribute(identity.URI)
	id, ok, err := s.client.findByName(ctx, "identities", name)
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
