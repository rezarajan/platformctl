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

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type Connection struct {
	ProviderRef *string // required unless External: the Provider realizing the entrypoint
	External    bool
	Scheme      string  // transport scheme; "tcp" (default) is what v1 forwarders support
	Host        string  // External only: where the system answers
	Port        int     // the port consumers use (managed: listen port on network + host)
	Target      string  // managed only: host:port the entrypoint forwards to
	SecretRef   *string // optional SecretReference carrying credentials for whatever answers
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
}

// TLS is Connection.spec.tls's decoded shape — exactly one of SecretRef,
// SelfSigned, or SecretName is set (validated below).
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
}

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
		c.TLS = t
	}
	return c, c.validate(e.Metadata.Name)
}

func (c Connection) validate(name string) error {
	if c.Port <= 0 {
		return fmt.Errorf("Connection %q: spec.port is required", name)
	}
	if c.External {
		if c.Host == "" {
			return fmt.Errorf("Connection %q: spec.host is required when spec.external is true", name)
		}
		if c.Target != "" {
			return fmt.Errorf("Connection %q: spec.target is only meaningful on managed connections (an external connection is consumed as-is)", name)
		}
		if c.TLS != nil {
			return fmt.Errorf("Connection %q: spec.tls is only meaningful on managed connections (TLS terminates at the entrypoint; an external connection has none)", name)
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

// validateTLS enforces the spec.tls/spec.scheme pairing and the
// exactly-one-of shape (docs/planning/03 §8.2.2). Which of the three modes
// is actually usable on a given runtime (secretName is Kubernetes-only) is
// checked by the realizing provider, not here — domain has no runtime-type
// access.
func (c Connection) validateTLS(name string) error {
	if c.TLS == nil {
		if c.Scheme == "https" {
			return fmt.Errorf("Connection %q: scheme https requires spec.tls to be declared (secretRef, selfSigned, or secretName)", name)
		}
		return nil
	}
	if c.Scheme != "https" {
		return fmt.Errorf("Connection %q: spec.tls is declared but scheme is %q (must be https)", name, c.Scheme)
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

func refName(spec map[string]any, field string) string {
	ref, ok := spec[field].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := ref["name"].(string)
	return name
}
