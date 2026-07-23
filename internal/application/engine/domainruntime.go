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
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/rezarajan/platformctl/internal/application/graphaccess"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
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
	// warn is the engine's diagnostics channel (docs/adr/031, Engine.warnf)
	// — nil in tests wrapped via WrapDomainRuntimeForTest.
	warn func(format string, args ...any)
	// containerResources is spec.runtime.resources parsed once at
	// construction — injected into every EnsureContainer spec that
	// carries none (J5). Distinct from resources below (the byKey map).
	containerResources *runtime.Resources
	token              string
	pinned             bool
	domain             string
	// holes are the additional domains (already normalized) some other
	// resource in the manifest set reaches this Connection from via
	// connectionRef — non-empty only when env.Kind == "Connection" AND
	// graphScoped is false. Ring 0 (matchEdge.crossDomain deny at
	// validate) already refused any such edge policy denies before apply
	// ever runs, so this never re-evaluates policy — it only wires what
	// validate allowed through. Under docs/adr/026 H7's GraphScopedAccess
	// gate, this whole-domain hole mechanism is superseded (never merely
	// layered on top of) by the resource-granular peers/ingressPeers
	// below — a Connection is a container-of-record exactly like any other
	// Provider (graphaccess.ContainerOf resolves it to itself), so its
	// cross-resource reachability is already covered generically there;
	// leaving both mechanisms active would let H5's coarse whole-namespace
	// hole silently re-widen what H7 exists to narrow.
	holes []string

	// graphScoped and the fields below realize docs/adr/026 H7 — all
	// zero-valued (graphScoped false) when the GraphScopedAccess gate is
	// disabled, which is what makes every method below byte-identical to
	// pre-H7 behavior (the gate-off pin).
	graphScoped bool
	// labelScopedAccessEnabled is the docs/adr/033 (K3) LabelScopedAccess
	// gate state, read once at construction and passed straight through to
	// graphaccess.MembershipEdges/IngressPeers below — it decides ONLY
	// whether a selector-bearing spec.access grant's audience is honored
	// (gate on) or INERT (gate off, ADR 033's K3 note: never wider than
	// declared intent). It does not affect a bare namespace-wide grant, and
	// has no effect at all unless graphScoped is also true — selector
	// grants ride the SAME H7 realization, no independent mechanism.
	labelScopedAccessEnabled bool
	// labelScopedGate is the RAW docs/adr/033 LabelScopedAccess gate value
	// this decorator was constructed with, stored UNCONDITIONALLY (unlike
	// labelScopedAccessEnabled above, which is deliberately zero-valued
	// whenever graphScoped is false — it feeds ONLY the H7 grant-
	// realization path, which itself is only reachable under
	// GraphScopedAccess). docs/planning/08 K4's mediation attribute
	// derivation is a SEPARATE, independent consumer of the SAME gate
	// (ADR 033's own "rides the SAME gate ... independently of
	// GraphScopedAccess"), reached via LabelScopedAccessEnabled()
	// (runtime.LabelScopedAccessQuery) — it must see the gate's true state
	// even when GraphScopedAccess is off.
	labelScopedGate bool
	// namespaced is true for runtimes where a network IS a pre-existing
	// namespace boundary a workload cannot leave (set from p.RuntimeType
	// == "kubernetes" — see newDomainRuntime's doc comment for why a
	// plain type string, not a capability type-assert, decides this).
	// Docker/fake treat a network as pure ACL-by-membership, so false
	// there. See graphscoped.go's package doc for why the two
	// realizations differ this much.
	namespaced bool
	// self is provEnv.Key() — the container this decorator's request
	// realizes (see newDomainRuntime's provEnv doc: the domain-of-record
	// container, not necessarily env itself).
	self resource.Key
	// peers is graphaccess.MembershipEdges(edges, self, byKey) — the
	// Docker realization's per-edge-network join list (egress ∪ ingress;
	// Docker network membership is symmetric, so direction is moot there).
	peers []resource.Key
	// ingressPeers is graphaccess.IngressPeers(edges, self, byKey) — the
	// Kubernetes realization's ContainerSpec.AllowFromPeers list (only
	// "who may dial ME" needs a NetworkPolicy ingress rule; egress is
	// unrestricted by construction in this codebase).
	ingressPeers []resource.Key
	// resources is byKey, kept for graphscoped.go's k8sPeers (resolving a
	// peer container's own runtime object name/home namespace).
	resources map[resource.Key]resource.Envelope
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
// graphScoped/edges realize docs/adr/026 H7 (graphscoped.go): graphScoped
// is the GraphScopedAccess gate's current state, and edges is the full
// docs/adr/026 access-request graph (graphaccess.DeriveEdges) — both
// resolved once per resolveRequest call by the caller (engine.go), never
// by this function, so newDomainRuntime itself stays a pure constructor.
// edges is ignored (and may be nil) when graphScoped is false. runtimeType
// is p.RuntimeType ("docker"/"kubernetes"/"fake", the same string
// Registry.Runtime(typeName, ...) was already constructed from) — passed
// explicitly rather than type-asserting rt against an optional capability
// (the way domainRuntime picks between IngressCapableRuntime-gated
// behaviors elsewhere would seem to suggest): found live that
// runtime.IngressCapableRuntime cannot be reused for this signal, because
// registry.haGuardRuntime unconditionally implements it (explicit
// delegation trio, for a DIFFERENT reason — docs/adr/018's provider-facing
// promotion gotcha) regardless of what the WRAPPED runtime actually is, so
// asserting against it through the registry-obtained rt this function
// always receives would see "Kubernetes" for every runtime, Docker
// included. The plain type string sidesteps that gotcha entirely.
// labelScopedAccessEnabled is the docs/adr/033 (K3) LabelScopedAccess gate's
// current state, resolved once per resolveRequest by the caller exactly
// like graphScoped — see the labelScopedAccessEnabled field doc for what it
// controls (selector-bearing spec.access grants only).
func newDomainRuntime(rt runtime.ContainerRuntime, runtimeConfig map[string]any, provEnv, env resource.Envelope, byKey map[resource.Key]resource.Envelope, graphScoped bool, labelScopedAccessEnabled bool, edges []graphaccess.Edge, runtimeType string, warn func(format string, args ...any)) runtime.ContainerRuntime {
	token, pinned := networkToken(runtimeConfig)
	d := &domainRuntime{
		ContainerRuntime: rt,
		token:            token,
		pinned:           pinned,
		domain:           resource.NormalizeDomain(provEnv.Metadata.Domain),
		warn:             warn,
	}
	d.containerResources = parseRuntimeResources(runtimeConfig)
	d.namespaced = runtimeType == provider.RuntimeTypeKubernetes
	d.labelScopedGate = labelScopedAccessEnabled
	if graphScoped {
		d.graphScoped = true
		d.labelScopedAccessEnabled = labelScopedAccessEnabled
		d.self = provEnv.Key()
		d.resources = byKey
		d.peers = graphaccess.MembershipEdges(edges, d.self, byKey, labelScopedAccessEnabled)
		d.ingressPeers = graphaccess.IngressPeers(edges, d.self, byKey, labelScopedAccessEnabled)
	} else if env.Kind == "Connection" {
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
	if d.graphScoped && !d.namespaced {
		// docs/adr/026 H7's Docker realization: under the gate, the home
		// token maps to a network EXCLUSIVE to this one owner rather than
		// the domain-wide network every Provider in a domain otherwise
		// shares — see graphscoped.go's package doc for why Docker's
		// "networks are the only isolation primitive" makes this the only
		// way pairwise access is representable there at all.
		return naming.PrivateNetworkName(name, d.domain, d.self)
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
	// networkPolicy: none is declared configuration the ENGINE resolved, so
	// the engine warns about it (docs/adr/031) — moved here from the
	// Kubernetes adapter's EnsureNetwork, which had been writing to
	// os.Stderr directly. The isolation observer (H8) deliberately skips
	// opted-out namespaces, so this reconcile-time warning is the only
	// signal an operator gets that a namespace runs wall-less by request.
	if d.namespaced && spec.IsolationPolicy == runtime.IsolationNone && d.warn != nil {
		d.warn("namespace %q uses networkPolicy: none — no isolation boundary is provisioned; every pod in the cluster can reach it unless something else in the cluster restricts it", spec.Name)
	}
	if d.graphScoped && d.namespaced && home && spec.IsolationPolicy != runtime.IsolationNone {
		// docs/adr/026 H7's Kubernetes realization: drop the
		// allow-same-namespace rule for this namespace (default-deny
		// only) — namespace membership no longer implies reachability;
		// only the per-container graph-scoped policy
		// (ContainerSpec.AllowFromPeers, applied in EnsureContainer)
		// does. A provider that explicitly opted all the way out
		// (IsolationNone) is left alone — that is a stronger, distinct
		// declaration this gate must not silently override.
		spec.IsolationPolicy = runtime.IsolationGraphScoped
	}
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
	// spec.runtime.resources (docs/planning/08 J5): resource bounds are
	// declared config the ENGINE resolved, injected here at the one
	// chokepoint every provider's EnsureContainer passes through — zero
	// provider changes, the same rule as the domain-name translation
	// below. A provider that someday sets its own Resources wins; today
	// none do.
	if spec.Resources == nil && d.containerResources != nil {
		spec.Resources = d.containerResources
	}
	nets := d.translateAll(spec.Networks)
	for _, holeNet := range d.holeNetworks() {
		nets = appendUnique(nets, holeNet)
	}
	if d.graphScoped && !d.namespaced {
		edgeNets, err := d.edgeNetworks(ctx, spec.Labels)
		if err != nil {
			return runtime.ContainerState{}, err
		}
		for _, n := range edgeNets {
			nets = appendUnique(nets, n)
		}
	}
	if d.graphScoped && d.namespaced {
		spec.AllowFromPeers = append(append([]runtime.NetworkPeer{}, spec.AllowFromPeers...), d.k8sPeers()...)
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

// Optional-capability delegation (the ADR 018 promotion trap, third
// occurrence — this decorator shipped promoting NONE of them, so every
// capability assertion through Request.Runtime failed at the 2026-07-23
// single gate: IngressCapableRuntime on K8s, MemberSetRuntime for
// workers>1, all of it). Each method forwards to the wrapped runtime
// when IT implements the capability; the assertion helpers below make
// the decorator transparent. internal/archtest's wrapper-completeness
// guard now FAILS THE BUILD if an adapter grows an optional capability
// any wrapper does not forward — this class of bug is closed, not
// patched.

func (d *domainRuntime) AddressesMembersCollectively() bool {
	if m, ok := d.ContainerRuntime.(runtime.MemberSetRuntime); ok {
		return m.AddressesMembersCollectively()
	}
	return false
}

func (d *domainRuntime) EnsureIngress(ctx context.Context, spec runtime.IngressSpec) (runtime.IngressState, error) {
	ic, ok := d.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return runtime.IngressState{}, fmt.Errorf("runtime does not implement IngressCapableRuntime")
	}
	return ic.EnsureIngress(ctx, spec)
}

func (d *domainRuntime) GetIngress(ctx context.Context, namespace, name string) (runtime.IngressState, bool, error) {
	ic, ok := d.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return runtime.IngressState{}, false, fmt.Errorf("runtime does not implement IngressCapableRuntime")
	}
	return ic.GetIngress(ctx, namespace, name)
}

func (d *domainRuntime) RemoveIngress(ctx context.Context, namespace, name string) error {
	ic, ok := d.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return fmt.Errorf("runtime does not implement IngressCapableRuntime")
	}
	return ic.RemoveIngress(ctx, namespace, name)
}

func (d *domainRuntime) EnsureTLSSecret(ctx context.Context, namespace, name string, certPEM, keyPEM []byte, labels map[string]string) error {
	tc, ok := d.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return fmt.Errorf("runtime does not implement IngressCapableRuntime")
	}
	return tc.EnsureTLSSecret(ctx, namespace, name, certPEM, keyPEM, labels)
}

func (d *domainRuntime) GetTLSSecret(ctx context.Context, namespace, name string) ([]byte, []byte, bool, error) {
	tc, ok := d.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return nil, nil, false, fmt.Errorf("runtime does not implement IngressCapableRuntime")
	}
	return tc.GetTLSSecret(ctx, namespace, name)
}

func (d *domainRuntime) RemoveTLSSecret(ctx context.Context, namespace, name string) error {
	tc, ok := d.ContainerRuntime.(runtime.IngressCapableRuntime)
	if !ok {
		return fmt.Errorf("runtime does not implement IngressCapableRuntime")
	}
	return tc.RemoveTLSSecret(ctx, namespace, name)
}

// QualifyTargetAddress implements runtime.AddressQualifier (docs/planning/08
// H9): the mediated-Connection bind-side fix for the domain-of-record FQDN
// gap the H6 Kubernetes addendum recorded as designed-but-unexercised.
// Docker (d.namespaced false) is always a no-op — see the port doc comment
// for why name-qualification cannot substitute for network membership
// there. Kubernetes qualifies only when target and caller declare different
// domains; same-domain (including both-default, the byte-identical-no-op
// case every other Ring 1 mechanism in this file already pins) returns
// hostport unchanged. d.token (not target's own spec.runtime.network,
// which this decorator never reads off an arbitrary envelope) is reused
// exactly the way holeNetworks() above reuses it: the caller's own resolved
// token names the SAME base network/namespace family the target is assumed
// to share, matching every other cross-domain name in this file.
func (d *domainRuntime) QualifyTargetAddress(ctx context.Context, target, caller resource.Envelope, hostport string) (string, error) {
	if !d.namespaced {
		return hostport, nil
	}
	// A pinned (explicitly configured) network passes through verbatim in
	// EVERY domain (H5's configured-value-always-wins rule, translate()
	// above) — so on Kubernetes every domain shares the one pinned
	// namespace and a bare name already resolves in it; qualifying would
	// point at a "<pinned>-<domain>" namespace that never exists.
	if d.pinned {
		return hostport, nil
	}
	targetDomain := resource.NormalizeDomain(target.Metadata.Domain)
	callerDomain := resource.NormalizeDomain(caller.Metadata.Domain)
	if targetDomain == callerDomain {
		return hostport, nil
	}
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return hostport, nil
	}
	ns := naming.NetworkName(d.token, targetDomain)
	return host + "." + ns + ".svc.cluster.local:" + port, nil
}

// LabelScopedAccessEnabled implements runtime.LabelScopedAccessQuery
// (docs/planning/08 K4): reports the RAW LabelScopedAccess gate value this
// decorator was constructed with, regardless of GraphScopedAccess — see
// labelScopedGate's own field doc for why this is a distinct field from
// labelScopedAccessEnabled above rather than a reuse of it.
func (d *domainRuntime) LabelScopedAccessEnabled() bool {
	return d.labelScopedGate
}

func (d *domainRuntime) ObserveIsolationEnforcement(ctx context.Context) (runtime.IsolationStatus, error) {
	io, ok := d.ContainerRuntime.(runtime.IsolationObserver)
	if !ok {
		return runtime.IsolationStatus{State: runtime.IsolationUnknown, Reason: "runtime does not implement IsolationObserver"}, nil // archtest:allow-reason-literal: IsolationStatus.Reason is free-text diagnostics, not a condition Reason token
	}
	return io.ObserveIsolationEnforcement(ctx)
}

// WrapDomainRuntimeForTest wraps rt exactly as resolveRequest does —
// exported ONLY for the archtest wrapper-completeness guard.
func WrapDomainRuntimeForTest(rt runtime.ContainerRuntime) runtime.ContainerRuntime {
	return &domainRuntime{ContainerRuntime: rt, token: "datascape", domain: "default"}
}

// ExecInContainer delegates the optional ExecCapableRuntime capability
// (docs/planning/08 I14) — required by the wrapper-completeness archtest.
func (d *domainRuntime) ExecInContainer(ctx context.Context, name string, cmd []string) (string, string, int, error) {
	ec, ok := d.ContainerRuntime.(runtime.ExecCapableRuntime)
	if !ok {
		return "", "", 0, fmt.Errorf("runtime does not implement ExecCapableRuntime")
	}
	return ec.ExecInContainer(ctx, name, cmd)
}

// JobCapableRuntime delegation (domainRuntime — docs/planning/08 I13/I15):
// required by the wrapper-completeness archtest so dbjob's type assertion
// through this wrapper reaches the Kubernetes adapter's Job path.
func (d *domainRuntime) EnsureJob(ctx context.Context, spec runtime.JobSpec) (runtime.JobState, error) {
	jc, ok := d.ContainerRuntime.(runtime.JobCapableRuntime)
	if !ok {
		return runtime.JobState{}, fmt.Errorf("runtime does not implement JobCapableRuntime")
	}
	return jc.EnsureJob(ctx, spec)
}

func (d *domainRuntime) InspectJob(ctx context.Context, namespace, name string) (runtime.JobState, bool, error) {
	jc, ok := d.ContainerRuntime.(runtime.JobCapableRuntime)
	if !ok {
		return runtime.JobState{}, false, fmt.Errorf("runtime does not implement JobCapableRuntime")
	}
	return jc.InspectJob(ctx, namespace, name)
}

func (d *domainRuntime) ReadJobFile(ctx context.Context, namespace, name, path string) ([]byte, error) {
	jc, ok := d.ContainerRuntime.(runtime.JobCapableRuntime)
	if !ok {
		return nil, fmt.Errorf("runtime does not implement JobCapableRuntime")
	}
	return jc.ReadJobFile(ctx, namespace, name, path)
}

func (d *domainRuntime) JobLogs(ctx context.Context, namespace, name, containerName string, tail int) (string, error) {
	jc, ok := d.ContainerRuntime.(runtime.JobCapableRuntime)
	if !ok {
		return "", fmt.Errorf("runtime does not implement JobCapableRuntime")
	}
	return jc.JobLogs(ctx, namespace, name, containerName, tail)
}

func (d *domainRuntime) RemoveJob(ctx context.Context, namespace, name string) error {
	jc, ok := d.ContainerRuntime.(runtime.JobCapableRuntime)
	if !ok {
		return fmt.Errorf("runtime does not implement JobCapableRuntime")
	}
	return jc.RemoveJob(ctx, namespace, name)
}

func (d *domainRuntime) NodeNameOf(ctx context.Context, name string) (string, error) {
	jc, ok := d.ContainerRuntime.(runtime.JobCapableRuntime)
	if !ok {
		return "", fmt.Errorf("runtime does not implement JobCapableRuntime")
	}
	return jc.NodeNameOf(ctx, name)
}

// parseRuntimeResources reads spec.runtime.resources (docs/planning/08 J5)
// into the runtime port's Resources. Schema validation has already pinned
// the shape (numbers in cores; memory as "<int>(Ki|Mi|Gi)"), so parsing is
// permissive here: a missing or malformed block yields nil (no bounds),
// never an error — bounds are an operator instruction, not a correctness
// gate, and validate is where malformed specs are refused.
func parseRuntimeResources(cfg map[string]any) *runtime.Resources {
	raw, ok := cfg["resources"].(map[string]any)
	if !ok {
		return nil
	}
	res := &runtime.Resources{}
	if v, ok := raw["cpu"].(float64); ok {
		res.CPULimit = v
	}
	if v, ok := raw["cpuReservation"].(float64); ok {
		res.CPUReservation = v
	}
	if v, ok := raw["memory"].(string); ok {
		res.MemoryLimitBytes = parseQuantityBytes(v)
	}
	if v, ok := raw["memoryReservation"].(string); ok {
		res.MemoryReservationBytes = parseQuantityBytes(v)
	}
	if *res == (runtime.Resources{}) {
		return nil
	}
	return res
}

// parseQuantityBytes parses "<int>(Ki|Mi|Gi)" (the schema's memory
// quantity format) to bytes; 0 on any mismatch.
func parseQuantityBytes(s string) int64 {
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "Ki"):
		mult, s = 1024, strings.TrimSuffix(s, "Ki")
	case strings.HasSuffix(s, "Mi"):
		mult, s = 1024*1024, strings.TrimSuffix(s, "Mi")
	case strings.HasSuffix(s, "Gi"):
		mult, s = 1024*1024*1024, strings.TrimSuffix(s, "Gi")
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n * mult
}
