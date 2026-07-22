package compose

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// enginePort is the in-network container port each composable database
// engine's Provider listens on (internal/adapters/providers/{postgres,
// mysql}: both bind their engine's standard port).
var enginePort = map[string]int{"postgres": 5432, "mysql": 3306, "mariadb": 3306}

// providerInternalPort is the in-network container port a Provider type
// answers on for the (currently narrow) set of Provider kinds `expose`
// knows how to target directly (expose Provider/<name>) — redpanda's
// *internal* listener (internal/adapters/providers/redpanda:
// internalKafkaPort) and minio/s3's API port
// (internal/adapters/providers/s3: apiPort).
var providerInternalPort = map[string]int{"redpanda": 29092, "minio": 9000, "s3": 9000}

// ExposeOptions is `platformctl expose <Kind>/<name>`'s flag-mode input.
type ExposeOptions struct {
	TargetKind string // "Source" | "Provider" (the only kinds this version can compute a target address for)
	TargetName string

	Scheme string // tcp | http | https
	Port   int    // required: the Connection's listen port

	// ConnectionName also drives the routed Host header for http(s):
	// internal/adapters/providers/ingress derives it as
	// "<ConnectionName>.<domain suffix>" — there is no separate Connection
	// field for it.
	ConnectionName string // default "<TargetName>-conn"

	Provider     RefChoice // the realizing Provider (proxy for tcp, ingress for http/https); required
	ProviderName string    // used when Provider.New; default "expose-<realizing-type>"
}

// PlanExpose computes the manifest patch for `expose <Kind>/<name>`.
// resolve is consulted only for --scheme https, to check whether the
// registered ingress Provider actually supports it yet (ADR 018 §C8).
func PlanExpose(snap Snapshot, dir string, opts ExposeOptions, resolve Resolver) (Patch, error) {
	command := fmt.Sprintf("expose %s/%s", opts.TargetKind, opts.TargetName)

	if opts.TargetKind != "Source" && opts.TargetKind != "Provider" {
		return Patch{}, fmt.Errorf("expose only supports Source/<name> or Provider/<name> today, got %s/%s", opts.TargetKind, opts.TargetName)
	}
	if !snap.NameExists(opts.TargetKind, opts.TargetName) {
		return Patch{}, fmt.Errorf("%s %q does not exist in the manifest set", opts.TargetKind, opts.TargetName)
	}
	if opts.Port <= 0 {
		return Patch{}, fmt.Errorf("--port is required")
	}

	target, err := resolveExposeTarget(snap, opts.TargetKind, opts.TargetName)
	if err != nil {
		return Patch{}, err
	}

	realizingType, err := RealizingProviderType(opts.Scheme)
	if err != nil {
		return Patch{}, err
	}

	if opts.Scheme == "https" {
		if err := checkHTTPSSupported(resolve); err != nil {
			return Patch{}, err
		}
	}

	patch := Patch{Command: command, Dir: dir}
	var pieces []piece
	var providerName string
	if opts.Provider.New {
		providerName = opts.ProviderName
		if providerName == "" {
			providerName = "expose-" + realizingType
		}
		pieces = append(pieces, piece{"Provider", providerName, renderExposeProvider(command, realizingType, providerName)})
		patch.Notes = append(patch.Notes, fmt.Sprintf("creating new %s Provider %q", realizingType, providerName))
	} else {
		if opts.Provider.Name == "" {
			return Patch{}, fmt.Errorf("--provider requires \"new\" or \"existing:<name>\"")
		}
		if !hasCandidate(snap.ProviderCandidates(realizingType), opts.Provider.Name) {
			return Patch{}, fmt.Errorf("--provider existing:%s: no such %s Provider (candidates: %s)", opts.Provider.Name, realizingType, candidateNames(snap.ProviderCandidates(realizingType)))
		}
		providerName = opts.Provider.Name
		patch.Notes = append(patch.Notes, fmt.Sprintf("reusing %s Provider %q", realizingType, providerName))
	}

	connName := opts.ConnectionName
	if connName == "" {
		connName = opts.TargetName + "-conn"
	}
	pieces = append(pieces, piece{"Connection", connName, renderConnection(command, connName, providerName, opts.Scheme, opts.Port, target)})

	for _, p := range pieces {
		op, err := resolveFile(dir, snap, p.kind, p.name, p.content)
		if err != nil {
			return Patch{}, err
		}
		patch.Files = append(patch.Files, op)
	}
	return patch, nil
}

// resolveExposeTarget computes the in-network host:port `spec.target` the
// generated Connection forwards to: for a Source, its realizing Provider's
// own name + that engine's standard port; for a Provider, its own name +
// that Provider type's internal port.
func resolveExposeTarget(snap Snapshot, kind, name string) (string, error) {
	switch kind {
	case "Source":
		src, _ := snap.byName("Source", name)
		engine, _ := src.Spec["engine"].(string)
		port, ok := enginePort[engine]
		if !ok {
			return "", fmt.Errorf("Source %q: engine %q has no known default port for expose (known: postgres, mysql, mariadb)", name, engine)
		}
		providerName := resource.RefName(src.Spec, "providerRef")
		if providerName == "" {
			return "", fmt.Errorf("Source %q: spec.providerRef is required to expose it (external sources are not supported)", name)
		}
		return fmt.Sprintf("%s:%d", providerName, port), nil
	case "Provider":
		prov, _ := snap.byName("Provider", name)
		provType, _ := prov.Spec["type"].(string)
		port, ok := providerInternalPort[provType]
		if !ok {
			return "", fmt.Errorf("Provider %q (type %s): no known default port for expose (known: redpanda, minio, s3)", name, provType)
		}
		return fmt.Sprintf("%s:%d", name, port), nil
	default:
		return "", fmt.Errorf("unsupported expose target kind %q", kind)
	}
}

// RealizingProviderType maps a Connection scheme to the Provider type that
// realizes it (docs/adr/018): tcp -> proxy (socat forwarder), http/https ->
// ingress (Caddy/native Ingress). Exported so cmd/platformctl's expose
// command can pick the right candidate list for an interactive --provider
// prompt before Plan time.
func RealizingProviderType(scheme string) (string, error) {
	switch scheme {
	case "tcp":
		return "proxy", nil
	case "http", "https":
		return "ingress", nil
	default:
		return "", fmt.Errorf("--scheme %q is invalid (must be tcp, http, or https)", scheme)
	}
}

// checkHTTPSSupported asks the registered ingress Provider whether it
// actually supports "https" yet — it doesn't, until ADR 018 §C8 (ingress
// TLS) merges — and degrades with a clear, specific message instead of
// emitting a Connection apply would reject or silently misconfigure.
func checkHTTPSSupported(resolve Resolver) error {
	if resolve == nil {
		return fmt.Errorf("--scheme https cannot be verified without a provider registry")
	}
	impl, err := resolve("ingress")
	if err != nil {
		return fmt.Errorf("--scheme https: %w", err)
	}
	cc, ok := impl.(reconciler.ConnectionCapableProvider)
	if !ok {
		return fmt.Errorf("--scheme https: the ingress provider implements no connection capability")
	}
	for _, s := range cc.SupportedConnectionSchemes() {
		if s == "https" {
			return nil
		}
	}
	return fmt.Errorf("--scheme https requires ingress TLS support (docs/adr/018-ingress-routing.md §C8), which has not merged into this tree yet; use --scheme http (no TLS) or --scheme tcp instead")
}

func renderExposeProvider(command, providerType, name string) string {
	var explain string
	switch providerType {
	case "proxy":
		explain = "Managed proxy — a socat forwarder realizing the Connection below."
	case "ingress":
		explain = "Managed ingress — the HTTP router realizing the Connection below."
	}
	lines := []string{"type: " + providerType, "runtime:", "  type: docker"}
	return renderDoc(command, explain, "Provider", name, lines)
}

func renderConnection(command, name, providerName, scheme string, port int, target string) string {
	var lines []string
	lines = append(lines, refBlock("providerRef", providerName)...)
	lines = append(lines, fmt.Sprintf("scheme: %s", scheme))
	lines = append(lines, fmt.Sprintf("port: %d", port))
	lines = append(lines, "target: "+target)
	return renderDoc(command, "", "Connection", name, lines)
}
