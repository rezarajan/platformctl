// Package endpoint is the provider-agnostic description of a network address
// a component exposes — the stable access identifiers `platformctl
// inventory` surfaces and external tools (orchestrators, BI, psql) connect
// to. Providers publish a List in status.providerState["endpoints"]; nothing
// else needs to know a technology's private port conventions.
package endpoint

// Endpoint is one reachable address of a component.
type Endpoint struct {
	// Name is the logical port name — "kafka", "s3", "admin",
	// "iceberg-rest", "postgres" — stable across image/port changes.
	Name string `json:"name"`
	// Scheme is how to speak to it: tcp | http | https | postgres | mysql |
	// kafka | s3. Advisory; helps a human/tool pick a client.
	Scheme string `json:"scheme"`
	// Host is the address reachable from the machine running platformctl
	// (a published container port), or "" when nothing is published to the
	// host (in-network only).
	Host string `json:"host,omitempty"`
	// Internal is the address reachable from other containers on the shared
	// runtime network (<container>:<port>).
	Internal string `json:"internal,omitempty"`
}

// List is an ordered set of a component's endpoints.
type List []Endpoint

// ToState renders the list into the JSON-map form stored under
// providerState["endpoints"] (providerState is persisted as JSON, so the
// value must survive a map[string]any round-trip).
func (l List) ToState() []map[string]any {
	out := make([]map[string]any, 0, len(l))
	for _, e := range l {
		m := map[string]any{"name": e.Name, "scheme": e.Scheme}
		if e.Host != "" {
			m["host"] = e.Host
		}
		if e.Internal != "" {
			m["internal"] = e.Internal
		}
		out = append(out, m)
	}
	return out
}

// Key is the well-known providerState key the list is stored under.
const Key = "endpoints"

// FromState parses providerState["endpoints"] (an []any of maps after a JSON
// round-trip) back into a List. Unknown shapes yield an empty list.
func FromState(v any) List {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make(List, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		e := Endpoint{}
		e.Name, _ = m["name"].(string)
		e.Scheme, _ = m["scheme"].(string)
		e.Host, _ = m["host"].(string)
		e.Internal, _ = m["internal"].(string)
		out = append(out, e)
	}
	return out
}
