// This file holds docs/adr/022 Ring 1's ONE translation chokepoint: a
// decorating runtime.ContainerRuntime that maps the logical platform-network
// name every provider already passes ("datascape", or an explicit
// spec.runtime.network override) onto a domain-scoped concrete network/
// namespace name — entirely inside the engine, so no provider under
// internal/adapters/providers ever needs to know domains exist
// (docs/planning/08 H5, the owner's core-facility invariant: network
// routing/access-policy changes require zero provider changes).
package engine

import (
	"context"
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// networkToken resolves spec.runtime.network exactly the way every provider
// adapter's own providerkit.Network(cfg)/local network(cfg) helper already
// does — duplicated here (as those were already duplicated across
// providers, and once more in this package's own former inNetworkConsumers)
// rather than imported, since only application/registry may import concrete
// provider packages (CLAUDE.md's layering invariant); pinned is true when an
// explicit override was configured, so callers can honor "an explicit
// override passes through verbatim in every domain" (the same
// configured-value-always-wins precedent hostport.Resolve already
// establishes for ports).
func networkToken(runtimeConfig map[string]any) (token string, pinned bool) {
	if n, ok := runtimeConfig["network"].(string); ok && n != "" {
		return n, true
	}
	return "datascape", false
}

// domainRuntime decorates a ContainerRuntime with the logical-token ->
// concrete-name translation. Constructed fresh per reconciler.Request
// (Engine.resolveRequest), since the translation depends on which resource
// is actually being reconciled — env, not necessarily its realizing
// Provider: a managed Connection's home network is its OWN metadata.domain
// (ADR 022's "every kind" field), which this achieves with zero proxy-
// provider code, simply because env IS the Connection here, not the proxy
// Provider — and on the full resource set, to compute which other domains'
// networks a Connection's forwarder must also open a path to/from
// ("exactly the holes the mediated entrypoint needs").
type domainRuntime struct {
	runtime.ContainerRuntime
	token  string
	pinned bool
	domain string
	// holes are the additional domains (already normalized) some other
	// resource in the manifest set reaches this Connection from via
	// connectionRef — non-empty only when env.Kind == "Connection". Ring 0
	// (matchEdge.crossDomain deny at validate) already refused any such
	// edge policy denies before apply ever runs, so this never re-evaluates
	// policy — it only wires what validate allowed through.
	holes []string
}

// newDomainRuntime builds the decorator for one reconciler.Request. env is
// the resource actually being reconciled (Request.Resource); runtimeConfig
// is its realizing Provider's spec.runtime block (Request.Provider's, via
// provider.Provider.RuntimeConfig — the same value Registry.Runtime(...)
// itself was already constructed from, so the decorator and the underlying
// runtime always agree on which config governs); byKey is the full
// validated resource set (Request.Resources).
// provEnv is the realizing Provider's envelope — the DOMAIN-OF-RECORD
// for every runtime object this request touches (docs/adr/022 addendum:
// containers live in their provider's domain; the dependent's own
// declared domain governs graph/policy edges only, and validate refuses
// an explicit mismatch). env is the resource under reconcile — used only
// for resource-shaped concerns like a Connection's cross-domain holes.
func newDomainRuntime(rt runtime.ContainerRuntime, runtimeConfig map[string]any, provEnv, env resource.Envelope, byKey map[resource.Key]resource.Envelope) runtime.ContainerRuntime {
	token, pinned := networkToken(runtimeConfig)
	d := &domainRuntime{
		ContainerRuntime: rt,
		token:            token,
		pinned:           pinned,
		domain:           resource.NormalizeDomain(provEnv.Metadata.Domain),
	}
	if env.Kind == "Connection" {
		d.holes = consumerDomainHoles(env, byKey)
	}
	return d
}

// translate maps name to its concrete form: only a call naming EXACTLY the
// resolved token, on a non-pinned config, is domain-scoped
// (naming.NetworkName is a no-op for the default domain, which is the
// undeclared-domain byte-identical pin); anything else — an explicit
// override (pinned), or a name a provider computed for its own unrelated
// purpose (docs/planning/08 I1's transit network, an ordinal volume name,
// ...) — passes through completely verbatim, by construction, with no
// signal from the provider needed.
func (d *domainRuntime) translate(name string) string {
	if d.pinned || name != d.token {
		return name
	}
	return naming.NetworkName(name, d.domain)
}

func (d *domainRuntime) isHomeToken(name string) bool {
	return !d.pinned && name == d.token
}

func (d *domainRuntime) holeNetworks() []string {
	nets := make([]string, len(d.holes))
	for i, h := range d.holes {
		nets[i] = naming.NetworkName(d.token, h)
	}
	return nets
}

// EnsureNetwork translates spec.Name and, when it is the home token for a
// Connection with consumer holes, additionally: (1) requests
// AllowFromNetworks for each hole (Kubernetes: opens the home namespace's
// B7 default-deny wall to exactly the consumer namespaces); (2) ensures
// each hole network/namespace itself exists (Docker: EnsureContainer below
// then attaches directly — a network a container is about to join must
// already exist).
func (d *domainRuntime) EnsureNetwork(ctx context.Context, spec runtime.NetworkSpec) error {
	home := d.isHomeToken(spec.Name)
	spec.Name = d.translate(spec.Name)
	if home && len(d.holes) > 0 {
		spec.AllowFromNetworks = append(append([]string{}, spec.AllowFromNetworks...), d.holeNetworks()...)
	}
	if err := d.ContainerRuntime.EnsureNetwork(ctx, spec); err != nil {
		return err
	}
	if home {
		for _, holeNet := range d.holeNetworks() {
			if err := d.ContainerRuntime.EnsureNetwork(ctx, runtime.NetworkSpec{Name: holeNet, Labels: spec.Labels}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *domainRuntime) RemoveNetwork(ctx context.Context, name string) error {
	return d.ContainerRuntime.RemoveNetwork(ctx, d.translate(name))
}

func (d *domainRuntime) EnsureVolume(ctx context.Context, spec runtime.VolumeSpec) error {
	spec.Networks = d.translateAll(spec.Networks)
	return d.ContainerRuntime.EnsureVolume(ctx, spec)
}

// EnsureContainer translates spec.Networks and, for a Connection with
// consumer holes, additionally attaches each hole network directly (Docker:
// a container joins multiple named networks; Kubernetes: only the first
// entry actually places the Pod — internal/adapters/runtime/kubernetes's
// targetNamespace — so the extra entries are harmless there and the real
// K8s mechanism is EnsureNetwork's AllowFromNetworks above).
func (d *domainRuntime) EnsureContainer(ctx context.Context, spec runtime.ContainerSpec) (runtime.ContainerState, error) {
	nets := d.translateAll(spec.Networks)
	for _, holeNet := range d.holeNetworks() {
		nets = appendUnique(nets, holeNet)
	}
	spec.Networks = nets
	return d.ContainerRuntime.EnsureContainer(ctx, spec)
}

func (d *domainRuntime) ProbeReachable(ctx context.Context, network, target string) error {
	return d.ContainerRuntime.ProbeReachable(ctx, d.translate(network), target)
}

func (d *domainRuntime) translateAll(names []string) []string {
	if len(names) == 0 {
		return names
	}
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = d.translate(n)
	}
	return out
}

func appendUnique(list []string, v string) []string {
	for _, s := range list {
		if s == v {
			return list
		}
	}
	return append(list, v)
}

// consumerDomainHoles returns the distinct, normalized domains (docs/adr/022
// Ring 1) among every OTHER resource in byKey that consumes conn via
// spec.connectionRef in conn's own namespace ("a connectionRef consumer's
// own domain -> the Connection it references"), sorted for determinism —
// conn's own domain is never included (it is not a hole, it is the home
// network).
func consumerDomainHoles(conn resource.Envelope, byKey map[resource.Key]resource.Envelope) []string {
	connNS := resource.NormalizeNamespace(conn.Metadata.Namespace)
	seen := map[string]bool{resource.NormalizeDomain(conn.Metadata.Domain): true}
	var holes []string
	for _, e := range byKey {
		if e.Kind == "Connection" {
			continue
		}
		ref := resource.RefFromSpec(e.Spec, "connectionRef")
		if ref.Name != conn.Metadata.Name {
			continue
		}
		if ref.NamespaceOr(e.Metadata.Namespace) != connNS {
			continue
		}
		d := resource.NormalizeDomain(e.Metadata.Domain)
		if seen[d] {
			continue
		}
		seen[d] = true
		holes = append(holes, d)
	}
	sort.Strings(holes)
	return holes
}
