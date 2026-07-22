package providerkit

import (
	"net"
	"strconv"

	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// EndpointResolution is a Source's resolved database dial address plus the
// preflight-reachability parameters (docs/planning/08 B8) and the
// Connection's own secretRef, if any — everything debezium (resolving the
// SOURCE side of a cdc Binding) and jdbcsink (resolving the TARGET side of
// a sink Binding) need from a Source/Connection lookup, byte-for-byte
// identical between the two (docs/planning/08 I5).
type EndpointResolution struct {
	// Host/Port is the address to dial the database at.
	Host string
	Port int
	// PreflightHost/PreflightPort dial an external Connection's declared
	// address directly — no runtime involved, since it's outside
	// platformctl's management entirely.
	PreflightHost string
	PreflightPort int
	// PreflightConnectionName names a managed Connection's own forwarder
	// container (+ PreflightPort), resolved through runtime.EnsureReachable
	// at reconcile time instead.
	PreflightConnectionName string
	// ConnectionSecretRef is the Connection's own SecretRef name, when the
	// Source declared an external Connection that in turn declared one —
	// takes precedence over the Provider-level fallback in
	// ResolveEndpointCredentials.
	ConnectionSecretRef string
}

// ResolveEndpoint resolves a Source's database address and preflight-dial
// parameters. Preference order: the Source's Connection (external — its
// declared host; in-cluster/managed — the Connection's own name on the
// shared network), the Source's Provider container name, then an explicit
// options.databaseHostname/databasePort override.
//
// srcEnv is the Source's own envelope (for its connectionRef lookup);
// defaultPort is the engine's default port, used unless a Connection or
// options.databasePort overrides it; options is the Binding's
// spec.options.
//
// ok is false (Host == "") when neither a Connection/Provider on the
// Source nor an explicit override yields an address — the two callers
// construct their own "cannot determine ... hostname" error so the message
// can name what's being resolved (source vs target) without this helper
// picking a caller-specific wording.
func ResolveEndpoint(req reconciler.Request, src source.Source, srcEnv resource.Envelope, defaultPort int, options map[string]any) (EndpointResolution, bool) {
	res := EndpointResolution{Port: defaultPort}
	if src.ProviderRef != nil {
		res.Host = *src.ProviderRef
	}
	if src.External && src.ConnectionRef != nil {
		connRef := resource.RefFromSpec(srcEnv.Spec, "connectionRef")
		if connEnv, ok := req.Resources[connRef.Key(srcEnv.Metadata.Namespace, "Connection")]; ok {
			conn, err := connection.FromEnvelope(connEnv)
			if err != nil {
				return EndpointResolution{}, false
			}
			res.Host, res.Port = conn.Endpoint(naming.RuntimeObjectName(connEnv))
			if conn.External {
				if addr, ok := conn.ExternalAddress(); ok {
					if host, port, ok := splitHostPort(addr); ok {
						res.PreflightHost, res.PreflightPort = host, port
					}
				}
			} else {
				res.PreflightConnectionName, res.PreflightPort = naming.RuntimeObjectName(connEnv), conn.Port
			}
			if conn.SecretRef != nil {
				res.ConnectionSecretRef = *conn.SecretRef
			}
		}
	}
	if h, ok := options["databaseHostname"].(string); ok && h != "" {
		res.Host = h // explicit override
	}
	if v, ok := options["databasePort"]; ok {
		switch n := v.(type) {
		case int:
			res.Port = n
		case float64:
			res.Port = int(n)
		}
	}
	if res.Host == "" {
		return EndpointResolution{}, false
	}
	return res, true
}

// ResolveEndpointCredentials resolves the credentials for a database
// endpoint resolved by ResolveEndpoint: the Connection's own secretRef
// (connSecretRef — EndpointResolution.ConnectionSecretRef, non-empty only
// when the Source's Connection declared one and the engine resolved it
// into req.Secrets) takes precedence over the Provider-level secretRefKey
// (debezium's "replicationSecretRef", jdbcsink's "credentialsSecretRef")
// named in cfg.Configuration.
//
// ok is false when neither resolves — the caller constructs its own "no
// resolved credentials" error naming its own provider type and config key.
func ResolveEndpointCredentials(req reconciler.Request, cfg provider.Provider, connSecretRef, secretRefKey string) (map[string]string, bool) {
	refName, _ := cfg.Configuration[secretRefKey].(string)
	creds, ok := req.Secrets[connSecretRef]
	if !ok {
		creds, ok = req.Secrets[refName]
	}
	return creds, ok
}

// splitHostPort is net.SplitHostPort plus the strconv.Atoi port parse,
// swallowing both errors into a single ok bool — the tiny address-parsing
// step ResolveEndpoint needs when turning a Connection's external address
// into preflight dial parameters.
func splitHostPort(address string) (string, int, bool) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, false
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, false
	}
	return host, port, true
}
