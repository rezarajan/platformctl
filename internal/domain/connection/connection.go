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
	c.Host, _ = e.Spec["host"].(string)
	c.Target, _ = e.Spec["target"].(string)
	switch n := e.Spec["port"].(type) {
	case int:
		c.Port = n
	case float64:
		c.Port = int(n)
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
	} else {
		if c.ProviderRef == nil {
			return fmt.Errorf("Connection %q: spec.providerRef is required unless spec.external is true", name)
		}
		if c.Target == "" {
			return fmt.Errorf("Connection %q: spec.target (host:port the entrypoint forwards to) is required on managed connections", name)
		}
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

// HostEndpoint returns the address host-side tools use, or "" when the
// Connection is external (nothing is published locally for it).
func (c Connection) HostEndpoint() string {
	if c.External {
		return ""
	}
	return "127.0.0.1:" + strconv.Itoa(c.Port)
}

func refName(spec map[string]any, field string) string {
	ref, ok := spec[field].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := ref["name"].(string)
	return name
}
