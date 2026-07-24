// Package connection defines the Connection kind: a first-class, non-secret
// description of how to reach a system, separating address (this kind) from
// credentials (a SecretReference named by spec.secretRef).
//
// Two lifecycles compose from one shape:
//
//   - Managed: realized by a connection-capable Provider as a stable
//     platform-owned entrypoint (a forwarder listening on spec.port, on the
//     shared network and the host) whose target is where the real system
//     lives. Consumers address the Connection, never the target — when the
//     external endpoint moves, the manifest changes and nothing else does.
//   - External (spec.external: true): a plain address record (host/port);
//     nothing is created, the address is consumed as-is.
//
// External resources' connectionRef resolves to a Connection (preferred) or
// directly to a SecretReference (the v1.0.0 shorthand, still supported).
// See docs/planning/03-resource-model-reference.md §8.2.
package connection

import (
	"fmt"
	"net"
	"strconv"

	"github.com/rezarajan/platformctl/internal/domain/hostport"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type Connection struct {
	ProviderRef *string // required unless External: the Provider realizing the entrypoint
	External    bool
	Scheme      string // transport scheme; "tcp" (default) is what v1 forwarders support
	Host        string // External only: where the system answers
	// Port is the port consumers use (managed: listen port on network +
	// host). Managed connections auto-allocate it deterministically
	// (internal/domain/hostport, keyed on this Connection's own runtime
	// object name — docs/adr/035 decision 2) when spec.port is omitted; an
	// explicit value is kept byte-identically. External connections have no
	// entrypoint to auto-allocate for, so Port stays the required, literal
	// port the external system answers on.
	Port      int
	Target    string  // managed only: host:port the entrypoint forwards to
	SecretRef *string // optional SecretReference carrying credentials for whatever answers
	// Via is the optional nameRef to a tunnel-capable Provider (managed
	// only) this Connection's egress additionally routes through
	// (docs/adr/002's addendum, docs/adr/023). Schema-accepted and
	// validate-time capability-checked; no realizing provider consumes it
	// yet — see docs/adr/023's "Scope" section.
	Via *string
	// TLS declares that this managed Connection terminates TLS at its
	// entrypoint (docs/planning/08 C8, docs/adr/018 addendum) — nil means
	// plaintext, the pre-C8 behavior unchanged. Only meaningful together
	// with scheme: https.
	TLS *TLS
	// Transport is docs/planning/08 L1 / docs/adr/034's per-edge escape
	// hatch, the same shape and default as Binding.Transport: "" (unset)
	// means edges reaching this Connection are mediated when the
	// MediatedTransport gate is on; "direct" opts them out. Connection is
	// the second of the two Kinds L1 picked for this field (alongside
	// Binding) because it is already the existing H6/ADR-027
	// MediatedConnection abstraction's own subject — ADR 034 inverts that
	// boundary's default without moving which Kind carries the
	// declaration.
	Transport string
}

// TLS is Connection.spec.tls's decoded shape. On a managed connection,
// exactly one of SecretRef, SelfSigned, or SecretName is set — the
// entrypoint-termination shape (docs/planning/08 C8, validated below). On an
// external connection, Mode (required) plus an optional CASecretRef declare
// the outbound TLS posture used to reach a TLS-requiring database
// (docs/planning/08 I2, docs/adr/025) — SecretRef/SelfSigned/SecretName are
// meaningless there (nothing terminates TLS on Datascape's side of an
// external connection). The two shapes are mutually exclusive by
// construction: a Connection is either managed or external, never both.
type TLS struct {
	// SecretRef names a SecretReference carrying the cert+key PEM material
	// (keys: "cert", "key"). Resolves through Request.Secrets only when the
	// realizing Provider's own spec.secretRefs also lists this name — the
	// same plumbing Connection.SecretRef already uses (mirrors debezium's
	// Connection-secretRef-must-also-be-in-the-Provider's-secretRefs
	// pattern), never resolved by this package (domain has no secret
	// store access).
	SecretRef *string
	// SelfSigned requests a Provider-managed local CA + per-host leaf
	// certificate for dev use. The CA's public certificate is published in
	// providerState so tools can trust it; the private key never appears
	// in state/logs/inspect output (see docs/planning/03 §8.2.2 for where
	// it persists instead, and why).
	SelfSigned bool
	// SecretName is Kubernetes-only: references an existing
	// kubernetes.io/tls Secret by name (e.g. cert-manager-managed).
	// platformctl only ever reads this Secret, never creates, updates, or
	// deletes it.
	SecretName *string
	// Mode is the external-only outbound TLS posture: one of TLSModeRequire,
	// TLSModeVerifyCA, TLSModeVerifyFull. Required when spec.tls is declared
	// on an external Connection; spec.tls absent entirely preserves the
	// historical plaintext behavior (back-compat).
	Mode string
	// CASecretRef is the external-only optional SecretReference holding a CA
	// bundle PEM under key "ca" (e.g. an RDS/private CA), used to verify the
	// server certificate under TLSModeVerifyCA/TLSModeVerifyFull. Resolves
	// through Request.Secrets only when the realizing consumer Provider's
	// own spec.secretRefs also lists this name (same discipline as
	// SecretRef above). Never resolved by this package.
	CASecretRef *string
}

// External-connection TLS modes (docs/planning/03 §8.2.4, docs/adr/025).
const (
	TLSModeRequire    = "require"
	TLSModeVerifyCA   = "verify-ca"
	TLSModeVerifyFull = "verify-full"
)

// FromEnvelope decodes a Connection from a validated Envelope.
func FromEnvelope(e resource.Envelope) (Connection, error) {
	c := Connection{Scheme: "tcp"}
	if s, ok := e.Spec["scheme"].(string); ok && s != "" {
		c.Scheme = s
	}
	if ext, ok := e.Spec["external"].(bool); ok {
		c.External = ext
	}
	if ref := refName(e.Spec, "providerRef"); ref != "" {
		c.ProviderRef = &ref
	}
	if ref := refName(e.Spec, "secretRef"); ref != "" {
		c.SecretRef = &ref
	}
	if ref := refName(e.Spec, "via"); ref != "" {
		c.Via = &ref
	}
	c.Host, _ = e.Spec["host"].(string)
	c.Target, _ = e.Spec["target"].(string)
	switch n := e.Spec["port"].(type) {
	case int:
		c.Port = n
	case float64:
		c.Port = int(n)
	}
	// Managed connections auto-allocate an omitted port deterministically,
	// keyed on this Connection's own runtime object name — the identical
	// mechanism (and the identical shared collision table, docs/adr/035
	// decision 2) a Provider's own omitted host port already resolves
	// through (internal/adapters/providers/providerkit.HostPort). An
	// explicit pin (c.Port > 0) passes through Resolve unchanged. External
	// connections have no entrypoint to allocate for — their port is the
	// external system's own, and stays required (checked in validate).
	if !c.External {
		c.Port = hostport.Resolve(c.Port, naming.RuntimeObjectName(e))
	}
	if raw, ok := e.Spec["tls"].(map[string]any); ok {
		t := &TLS{}
		if ref := refName(raw, "secretRef"); ref != "" {
			t.SecretRef = &ref
		}
		if v, ok := raw["selfSigned"].(bool); ok {
			t.SelfSigned = v
		}
		if v, ok := raw["secretName"].(string); ok && v != "" {
			t.SecretName = &v
		}
		if v, ok := raw["mode"].(string); ok && v != "" {
			t.Mode = v
		}
		if ref := refName(raw, "caSecretRef"); ref != "" {
			t.CASecretRef = &ref
		}
		c.TLS = t
	}
	c.Transport, _ = e.Spec["transport"].(string)
	return c, c.validate(e.Metadata.Name)
}

func (c Connection) validate(name string) error {
	// A managed connection's port is auto-allocated above when omitted, so
	// it is never <= 0 by the time validate runs; an external connection
	// has no auto-allocation path (nothing Datascape controls to allocate a
	// port for) and so still requires an explicit spec.port.
	if c.External && c.Port <= 0 {
		return fmt.Errorf("Connection %q: spec.port is required when spec.external is true", name)
	}
	if c.Transport != "" && c.Transport != "direct" {
		return fmt.Errorf("Connection %q: spec.transport must be \"direct\" when set (docs/adr/034: mediated is the unset default)", name)
	}
	if c.External {
		if c.Host == "" {
			return fmt.Errorf("Connection %q: spec.host is required when spec.external is true", name)
		}
		if c.Target != "" {
			return fmt.Errorf("Connection %q: spec.target is only meaningful on managed connections (an external connection is consumed as-is)", name)
		}
		if c.Via != nil {
			return fmt.Errorf("Connection %q: spec.via is only meaningful on managed connections (an external connection is consumed as-is)", name)
		}
	} else {
		if c.ProviderRef == nil {
			return fmt.Errorf("Connection %q: spec.providerRef is required unless spec.external is true", name)
		}
		if c.Target == "" {
			return fmt.Errorf("Connection %q: spec.target (host:port the entrypoint forwards to) is required on managed connections", name)
		}
	}
	if err := c.validateTLS(name); err != nil {
		return err
	}
	return nil
}

// validateTLS enforces spec.tls's shape, which differs by lifecycle: a
// managed connection pairs spec.tls with scheme: https and the
// exactly-one-of entrypoint-termination shape (docs/planning/03 §8.2.2); an
// external connection's spec.tls declares an outbound TLS posture
// (docs/planning/03 §8.2.4, docs/adr/025) independent of scheme. Which of
// the managed modes is actually usable on a given runtime (secretName is
// Kubernetes-only) is checked by the realizing provider, not here — domain
// has no runtime-type access.
func (c Connection) validateTLS(name string) error {
	if c.External {
		return c.validateExternalTLS(name)
	}
	return c.validateManagedTLS(name)
}

// validateExternalTLS enforces the external-only mode-shape: spec.tls absent
// preserves plaintext (back-compat); declared, it requires spec.tls.mode and
// forbids the managed-only entrypoint-termination fields.
func (c Connection) validateExternalTLS(name string) error {
	if c.TLS == nil {
		return nil
	}
	switch c.TLS.Mode {
	case TLSModeRequire, TLSModeVerifyCA, TLSModeVerifyFull:
	case "":
		return fmt.Errorf("Connection %q: spec.tls.mode is required on an external connection (%s, %s, or %s)", name, TLSModeRequire, TLSModeVerifyCA, TLSModeVerifyFull)
	default:
		return fmt.Errorf("Connection %q: spec.tls.mode must be one of %s, %s, %s, got %q", name, TLSModeRequire, TLSModeVerifyCA, TLSModeVerifyFull, c.TLS.Mode)
	}
	if c.TLS.SecretRef != nil || c.TLS.SelfSigned || c.TLS.SecretName != nil {
		return fmt.Errorf("Connection %q: spec.tls.secretRef/selfSigned/secretName terminate TLS at a managed entrypoint and are not meaningful on an external connection; use spec.tls.mode and, optionally, spec.tls.caSecretRef", name)
	}
	return nil
}

// validateManagedTLS enforces the pre-I2 managed-connection shape: scheme
// https requires spec.tls, and spec.tls must set exactly one of secretRef,
// selfSigned, or secretName.
func (c Connection) validateManagedTLS(name string) error {
	if c.TLS == nil {
		if c.Scheme == "https" {
			return fmt.Errorf("Connection %q: scheme https requires spec.tls to be declared (secretRef, selfSigned, or secretName)", name)
		}
		return nil
	}
	if c.Scheme != "https" {
		return fmt.Errorf("Connection %q: spec.tls is declared but scheme is %q (must be https)", name, c.Scheme)
	}
	if c.TLS.Mode != "" || c.TLS.CASecretRef != nil {
		return fmt.Errorf("Connection %q: spec.tls.mode/caSecretRef reach a TLS-requiring database over an external connection and are not meaningful on a managed connection", name)
	}
	set := 0
	if c.TLS.SecretRef != nil {
		set++
	}
	if c.TLS.SelfSigned {
		set++
	}
	if c.TLS.SecretName != nil {
		set++
	}
	if set != 1 {
		return fmt.Errorf("Connection %q: spec.tls must set exactly one of secretRef, selfSigned, or secretName (got %d)", name, set)
	}
	return nil
}

// Endpoint returns the address consumers on the shared network use: a
// managed Connection answers at its own name (the forwarder container), an
// external one at its declared host.
func (c Connection) Endpoint(name string) (host string, port int) {
	if c.External {
		return c.Host, c.Port
	}
	return name, c.Port
}

// ExternalAddress returns the declared host:port for an External
// Connection's target, and true. It returns ("", false) for a managed
// Connection: that address is only known by resolving the forwarder
// through the runtime (runtime.WithReachable against the Connection's own
// name and port) — domain has no runtime access, so this method refuses to
// guess rather than return a loopback address that is only correct on
// Docker (docs/planning/09 Class 1 — this was exactly that guess, until it
// was found live against a real cluster and both of its call sites were
// migrated to EnsureReachable, leaving the guess itself unused and unsafe
// to resurrect).
func (c Connection) ExternalAddress() (string, bool) {
	if !c.External {
		return "", false
	}
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), true
}

// ViaFactName is the published-endpoint-fact name a tunnel Provider uses
// (in its own Provider-kind status) to publish the per-Connection dial
// address a via-consuming provider (docs/planning/08 I1) reads back via
// reconciler.Request.Facts.Endpoint (docs/planning/08 I9 — originally a
// bespoke reconciler.Request.TunnelFacts.Internal field, migrated to the
// generic query and deleted) — a single shared convention so the
// publishing side (the tunnel Provider) and the reading side (the
// via-consuming provider's own reconcile) never have to be told the
// other's key by hand (docs/adr/015).
func ViaFactName(namespace, name string) string {
	return "via:" + resource.NormalizeNamespace(namespace) + "/" + name
}

func refName(spec map[string]any, field string) string {
	ref, ok := spec[field].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := ref["name"].(string)
	return name
}
