// Package engine is the topological executor: walks the Plan in dependency
// order, calls Provider.Reconcile per resource, persists state after each
// resource (NFR-9), and resolves/forwards LineageEndpoints.
// See docs/planning/02-architecture.md §5.5.
package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/secret"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/clock"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/ports/secretstore"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

type Engine struct {
	Registry    *registry.Registry
	StateStore  state.StateStore
	SecretStore secretstore.SecretStore // nil disables secretRefs resolution
	Clock       clock.Clock
	HaltOnError bool
	// HealDrift makes Apply probe plan-noop resources against live
	// infrastructure and re-reconcile the ones that drifted (gated by
	// DriftDetection). Without it, apply trusts recorded state.
	HealDrift bool
	// AllowDestructive permits Destroy to act on External-lifecycle
	// resources. It is the engine half of NFR-3's double lock: the CLI only
	// sets it when both --include-external and
	// --yes-i-understand-this-is-destructive were passed.
	AllowDestructive bool
	// Parallelism bounds concurrent reconciliation within a topological
	// level (resources in the same level share no dependency relationship).
	// Values <= 1 mean fully sequential. Gated by ParallelReconciliation.
	Parallelism int
	// Log receives one line per reconciliation action; nil disables.
	Log func(format string, args ...any)

	// stateMu serializes state-map access and persistence when levels
	// execute concurrently; reconciliation itself runs unlocked.
	stateMu sync.Mutex
}

type Result struct {
	Succeeded []resource.Key
	Failed    map[resource.Key]error
	Skipped   []resource.Key // dependents of failed resources
}

func (e *Engine) logf(format string, args ...any) {
	if e.Log != nil {
		e.Log(format, args...)
	}
}

// Apply executes a plan. State is persisted after each resource, not only at
// the end, so a crash partway through leaves state accurate.
func (e *Engine) Apply(ctx context.Context, p plan.Plan, envelopes []resource.Envelope, depGraph DependencyGraph) (Result, error) {
	res := Result{Failed: make(map[resource.Key]error)}

	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, env := range envelopes {
		byKey[env.Key()] = env
	}
	entryByKey := make(map[resource.Key]plan.Entry, len(p.Entries))
	for _, entry := range p.Entries {
		entryByKey[entry.Key] = entry
	}

	st, err := e.StateStore.Load(ctx)
	if err != nil {
		return res, err
	}

	blocked := make(map[resource.Key]bool)
	var mu sync.Mutex // guards res and blocked during a concurrent level

	// processEntry runs one plan entry; resources in the same topological
	// level share no dependency relationship, so entries within a level are
	// safe to run concurrently (bounded by Parallelism).
	processEntry := func(key resource.Key, entry plan.Entry) {
		mu.Lock()
		isBlocked := blocked[key]
		mu.Unlock()
		if isBlocked {
			mu.Lock()
			res.Skipped = append(res.Skipped, key)
			mu.Unlock()
			e.logf("skip %s: a dependency failed", key)
			return
		}
		env := byKey[key]
		if entry.Action == plan.ActionNoop {
			// The spec is unchanged, but the infrastructure may not be:
			// probe, and re-reconcile on drift. Managed resources only —
			// externals are never mutated uninvited.
			if !e.HealDrift {
				return
			}
			e.stateMu.Lock()
			rs, inState := st.Resources[key]
			e.stateMu.Unlock()
			if !inState || resource.LifecycleOf(env, rs.Imported) != resource.Managed {
				return
			}
			probed := e.probeOne(ctx, env, byKey)
			if !HasDrift(probed) {
				return
			}
			if c, ok := probed.Condition(status.DriftDetected); ok {
				e.logf("drift %s (%s); healing", key, c.Reason)
			}
		}

		start := time.Now()
		err := e.reconcileOne(ctx, entry, env, byKey, &st)
		if err != nil {
			mu.Lock()
			res.Failed[key] = err
			for dep := range depGraph.Dependents(key) {
				blocked[dep] = true
			}
			mu.Unlock()
			e.logf("fail %s (%s) after %s: %v", key, entry.Action, time.Since(start).Round(time.Millisecond), err)
			return
		}
		mu.Lock()
		res.Succeeded = append(res.Succeeded, key)
		mu.Unlock()
		e.logf("ok   %s (%s) in %s", key, entry.Action, time.Since(start).Round(time.Millisecond))
	}

	parallelism := e.Parallelism
	if parallelism < 1 {
		parallelism = 1
	}

	for _, level := range p.Levels {
		if parallelism == 1 {
			for _, key := range level {
				if entry, ok := entryByKey[key]; ok {
					processEntry(key, entry)
				}
				if e.HaltOnError && len(res.Failed) > 0 {
					return res, fmt.Errorf("%d resource(s) failed to reconcile (halting: --halt-on-error)", len(res.Failed))
				}
			}
			continue
		}
		sem := make(chan struct{}, parallelism)
		var wg sync.WaitGroup
		for _, key := range level {
			entry, ok := entryByKey[key]
			if !ok {
				continue
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(key resource.Key, entry plan.Entry) {
				defer wg.Done()
				defer func() { <-sem }()
				processEntry(key, entry)
			}(key, entry)
		}
		wg.Wait()
		if e.HaltOnError && len(res.Failed) > 0 {
			return res, fmt.Errorf("%d resource(s) failed to reconcile (halting: --halt-on-error)", len(res.Failed))
		}
	}

	if len(res.Failed) > 0 {
		return res, fmt.Errorf("%d resource(s) failed to reconcile", len(res.Failed))
	}
	return res, nil
}

// DependencyGraph is the subset of domain/graph the engine needs; avoids the
// engine depending on graph construction.
type DependencyGraph interface {
	Dependents(k resource.Key) map[resource.Key]bool
}

func secretRefFrom(env resource.Envelope) secret.SecretReference {
	ref := secret.SecretReference{Name: env.Metadata.Name}
	backend, _ := env.Spec["backend"].(string)
	ref.Backend = secret.Backend(backend)
	if keys, ok := env.Spec["keys"].([]any); ok {
		for _, k := range keys {
			if s, ok := k.(string); ok {
				ref.Keys = append(ref.Keys, s)
			}
		}
	}
	return ref
}

func (e *Engine) reconcileOne(ctx context.Context, entry plan.Entry, env resource.Envelope, byKey map[resource.Key]resource.Envelope, st *state.State) error {
	// SecretReference is a pure declaration with no provider or runtime
	// behind it: reconciling one means verifying it resolves.
	if env.Kind == "SecretReference" {
		return e.reconcileSecretReference(ctx, entry, env, st)
	}
	// An External resource without a providerRef lives entirely outside
	// Datascape: "reconciling" it means verifying its connection is
	// resolvable, never creating anything (plan.ActionConfigure path).
	if isExternalNoProvider(env) {
		return e.reconcileExternal(ctx, entry, env, byKey, st)
	}

	prov, rt, err := e.resolveProviderAndRuntime(ctx, env, byKey)
	if err != nil {
		return err
	}

	newStatus, err := prov.Reconcile(ctx, env, rt)
	if err != nil {
		return err
	}

	// Lineage forwarding: after a successful Reconcile, resolve observers and
	// hand the endpoint to a LineageAware provider — or record the
	// informational condition and move on. Never a failure, never a retry.
	if len(env.Metadata.Observers) > 0 {
		if la, ok := prov.(reconciler.LineageAware); ok {
			for _, obs := range env.Metadata.Observers {
				endpoint, err := e.resolveLineageEndpoint(ctx, obs.Name, byKey, st)
				if err != nil {
					return fmt.Errorf("resolve observer %q: %w", obs.Name, err)
				}
				if err := la.ConfigureLineage(ctx, endpoint); err != nil {
					return fmt.Errorf("configure lineage from observer %q: %w", obs.Name, err)
				}
			}
		} else {
			newStatus.SetCondition(status.Condition{
				Type:    status.Ready,
				Status:  status.True,
				Reason:  status.ReasonLineageNotConsumed,
				Message: fmt.Sprintf("Provider type %q does not implement LineageAware; observers were not forwarded.", prov.Type()),
			}, e.Clock.Now())
		}
	}

	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	lifecycle := resource.LifecycleOf(env, st.Resources[env.Key()].Imported)
	st.Resources[env.Key()] = state.ResourceState{
		SpecHash:  entry.SpecHash,
		Status:    newStatus,
		Lifecycle: lifecycle.String(),
		Imported:  st.Resources[env.Key()].Imported,
		Provider:  newStatus.ProviderState,
	}
	return e.StateStore.Save(ctx, *st)
}

// ProbeResult pairs a resource with its live-probed status.
type ProbeResult struct {
	Key    resource.Key
	Status status.Status
}

// HasDrift reports whether a status carries DriftDetected=True.
func HasDrift(s status.Status) bool {
	c, ok := s.Condition(status.DriftDetected)
	return ok && c.Status == status.True
}

// Probe checks every state-recorded resource against live infrastructure,
// merges the observed Ready/DriftDetected conditions into recorded state,
// and persists it — so `status` reflects the last observation. Probing never
// mutates infrastructure. Resources not yet applied are skipped: there is
// nothing recorded to compare against.
func (e *Engine) Probe(ctx context.Context, envelopes []resource.Envelope) ([]ProbeResult, error) {
	st, err := e.StateStore.Load(ctx)
	if err != nil {
		return nil, err
	}
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, env := range envelopes {
		byKey[env.Key()] = env
	}

	var results []ProbeResult
	for _, env := range envelopes {
		key := env.Key()
		rs, ok := st.Resources[key]
		if !ok {
			continue
		}
		probed := e.probeOne(ctx, env, byKey)
		merged := rs.Status
		for _, c := range probed.Conditions {
			merged.SetCondition(c, e.Clock.Now())
		}
		rs.Status = merged
		st.Resources[key] = rs
		results = append(results, ProbeResult{Key: key, Status: merged})
	}
	if err := e.StateStore.Save(ctx, st); err != nil {
		return results, err
	}
	return results, nil
}

// probeOne asks the provider for the resource's live status. It never
// returns an error: an unreachable or unresolvable resource *is* drift —
// things failing out-of-band is the expected case, not an exception.
func (e *Engine) probeOne(ctx context.Context, env resource.Envelope, byKey map[resource.Key]resource.Envelope) status.Status {
	now := e.Clock.Now()
	st := status.Status{}

	if env.Kind == "SecretReference" {
		ref := secretRefFrom(env)
		err := ref.Validate()
		if err == nil {
			if e.SecretStore == nil {
				err = fmt.Errorf("no secret store is configured")
			} else {
				_, err = e.SecretStore.Resolve(ctx, ref)
			}
		}
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "SecretUnresolvable", Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "SecretUnresolvable"}, now)
			return st
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "SecretResolvable"}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
		return st
	}

	if isExternalNoProvider(env) {
		return e.externalConnectionStatus(ctx, env, byKey)
	}

	prov, rt, err := e.resolveProviderAndRuntime(ctx, env, byKey)
	if err == nil {
		var probed status.Status
		probed, err = prov.Probe(ctx, env, rt)
		if err == nil {
			return probed
		}
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ProbeFailed", Message: err.Error()}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ProbeFailed"}, now)
	return st
}

// reconcileSecretReference verifies the reference resolves through the
// configured SecretStore (without storing any secret material) and records it
// Ready in state.
func (e *Engine) reconcileSecretReference(ctx context.Context, entry plan.Entry, env resource.Envelope, st *state.State) error {
	ref := secretRefFrom(env)
	if err := ref.Validate(); err != nil {
		return err
	}
	if e.SecretStore == nil {
		return fmt.Errorf("SecretReference %q: no secret store is configured", env.Metadata.Name)
	}
	if _, err := e.SecretStore.Resolve(ctx, ref); err != nil {
		return err
	}
	newStatus := status.Status{}
	now := e.Clock.Now()
	newStatus.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "SecretResolvable"}, now)
	newStatus.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: "ReconcileComplete"}, now)
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	st.Resources[env.Key()] = state.ResourceState{
		SpecHash:  entry.SpecHash,
		Status:    newStatus,
		Lifecycle: resource.LifecycleOf(env, st.Resources[env.Key()].Imported).String(),
		Imported:  st.Resources[env.Key()].Imported,
	}
	return e.StateStore.Save(ctx, *st)
}

// DependencyResolver is the subset of domain/graph Destroy needs to block
// teardown of resources whose dependents failed to destroy.
type DependencyResolver interface {
	Dependencies(k resource.Key) map[resource.Key]bool
}

// isExternalNoProvider reports whether the resource declares
// spec.external: true and names no providerRef — i.e. nothing in the
// platform realizes it; only its reachability can be verified.
func isExternalNoProvider(env resource.Envelope) bool {
	ext, _ := env.Spec["external"].(bool)
	if !ext {
		return false
	}
	if ref, ok := env.Spec["providerRef"].(map[string]any); ok {
		if name, _ := ref["name"].(string); name != "" {
			return false
		}
	}
	return true
}

// externalConnectionStatus verifies the resource's connectionRef resolves to
// a SecretReference whose keys resolve through the secret store.
func (e *Engine) externalConnectionStatus(ctx context.Context, env resource.Envelope, byKey map[resource.Key]resource.Envelope) status.Status {
	now := e.Clock.Now()
	st := status.Status{}
	connName := ""
	if ref, ok := env.Spec["connectionRef"].(map[string]any); ok {
		connName, _ = ref["name"].(string)
	}
	var err error
	if connName != "" {
		refEnv, ok := byKey[resource.Key{Kind: "SecretReference", Name: connName}]
		switch {
		case !ok:
			err = fmt.Errorf("connectionRef %q does not resolve to a SecretReference in the manifest set", connName)
		case e.SecretStore == nil:
			err = fmt.Errorf("no secret store is configured")
		default:
			_, err = e.SecretStore.Resolve(ctx, secretRefFrom(refEnv))
		}
	}
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: "ExternalConnectionUnresolvable", Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: "ExternalConnectionUnresolvable"}, now)
		return st
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: "ExternalConnectionResolvable"}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: "NoDrift"}, now)
	return st
}

func (e *Engine) reconcileExternal(ctx context.Context, entry plan.Entry, env resource.Envelope, byKey map[resource.Key]resource.Envelope, st *state.State) error {
	probed := e.externalConnectionStatus(ctx, env, byKey)
	if !probed.IsReady() {
		if c, ok := probed.Condition(status.Ready); ok {
			return fmt.Errorf("%s: %s", env.Key(), c.Message)
		}
	}
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	st.Resources[env.Key()] = state.ResourceState{
		SpecHash:  entry.SpecHash,
		Status:    probed,
		Lifecycle: resource.External.String(),
		Imported:  st.Resources[env.Key()].Imported,
	}
	return e.StateStore.Save(ctx, *st)
}

// Import adopts a pre-existing, out-of-band-created resource into state as
// Imported: it is probed (never created) and recorded with the manifest's
// current spec hash, so a subsequent apply plans a no-op rather than a
// create. v1 adopts by name: the backing object must carry the name the
// provider derives from metadata.name, which is what `from` must equal.
func (e *Engine) Import(ctx context.Context, envelopes []resource.Envelope, key resource.Key, from string) (status.Status, error) {
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, env := range envelopes {
		byKey[env.Key()] = env
	}
	env, ok := byKey[key]
	if !ok {
		return status.Status{}, fmt.Errorf("%s is not declared in the manifest set", key)
	}
	if from != env.Metadata.Name {
		return status.Status{}, fmt.Errorf("--from %q: v1 adopts by name — the backing object must be named %q (the name providers derive from metadata.name)", from, env.Metadata.Name)
	}

	probed := e.probeOne(ctx, env, byKey)
	if !probed.IsReady() {
		msg := "backing object not found or unhealthy"
		if c, ok := probed.Condition(status.Ready); ok && c.Message != "" {
			msg = c.Message
		}
		return probed, fmt.Errorf("cannot import %s: %s", key, msg)
	}

	hash, err := plan.SpecHash(env)
	if err != nil {
		return probed, err
	}
	st, err := e.StateStore.Load(ctx)
	if err != nil {
		return probed, err
	}
	st.Resources[key] = state.ResourceState{
		SpecHash:  hash,
		Status:    probed,
		Lifecycle: resource.Imported.String(),
		Imported:  true,
		Provider:  probed.ProviderState,
	}
	return probed, e.StateStore.Save(ctx, st)
}

// Destroy executes a destroy plan in reverse dependency order. The engine is
// the single enforcement point for NFR-3: External resources are never
// destroyed unless the plan explicitly marked them. When a resource fails to
// destroy, everything it depends on is skipped — deleting a connector's
// broker or a provider's secrets out from under it would strand the survivor
// in an unrecoverable state.
func (e *Engine) Destroy(ctx context.Context, p plan.Plan, envelopes []resource.Envelope, deps DependencyResolver) (Result, error) {
	res := Result{Failed: make(map[resource.Key]error)}
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, env := range envelopes {
		byKey[env.Key()] = env
	}

	st, err := e.StateStore.Load(ctx)
	if err != nil {
		return res, err
	}

	blocked := make(map[resource.Key]bool)
	block := func(k resource.Key) {
		if deps == nil {
			return
		}
		for dep := range deps.Dependencies(k) {
			blocked[dep] = true
		}
	}

	for _, entry := range p.Entries {
		if entry.Action != plan.ActionDelete {
			continue
		}
		env, ok := byKey[entry.Key]
		if !ok {
			continue
		}
		if blocked[entry.Key] {
			res.Skipped = append(res.Skipped, entry.Key)
			e.logf("skip destroy %s: a resource depending on it failed to destroy", entry.Key)
			continue
		}
		// NFR-3, engine-enforced (not per-provider convention): External
		// resources are never destroyed without the explicit double opt-in,
		// even if a plan claims otherwise.
		if resource.LifecycleOf(env, st.Resources[entry.Key].Imported) == resource.External {
			if !e.AllowDestructive {
				err := fmt.Errorf("%s is External: destroying it requires both --include-external and --yes-i-understand-this-is-destructive", entry.Key)
				res.Failed[entry.Key] = err
				e.logf("fail destroy %s: %v", entry.Key, err)
				block(entry.Key)
				continue
			}
			if isExternalNoProvider(env) {
				// Nothing in the platform realizes it; forgetting it is all
				// destroy can (and should) do.
				delete(st.Resources, entry.Key)
				if err := e.StateStore.Save(ctx, st); err != nil {
					return res, err
				}
				res.Succeeded = append(res.Succeeded, entry.Key)
				e.logf("ok   destroy %s (external: removed from state only)", entry.Key)
				continue
			}
		}
		if env.Kind == "SecretReference" {
			delete(st.Resources, entry.Key)
			if err := e.StateStore.Save(ctx, st); err != nil {
				return res, err
			}
			res.Succeeded = append(res.Succeeded, entry.Key)
			e.logf("ok   destroy %s", entry.Key)
			continue
		}
		prov, rt, err := e.resolveProviderAndRuntime(ctx, env, byKey)
		if err != nil {
			res.Failed[entry.Key] = err
			e.logf("fail destroy %s: %v", entry.Key, err)
			block(entry.Key)
			continue
		}
		if err := prov.Destroy(ctx, env, rt); err != nil {
			res.Failed[entry.Key] = err
			e.logf("fail destroy %s: %v", entry.Key, err)
			block(entry.Key)
			continue
		}
		delete(st.Resources, entry.Key)
		if err := e.StateStore.Save(ctx, st); err != nil {
			return res, err
		}
		res.Succeeded = append(res.Succeeded, entry.Key)
		e.logf("ok   destroy %s", entry.Key)
	}

	if len(res.Failed) > 0 {
		return res, fmt.Errorf("%d resource(s) failed to destroy", len(res.Failed))
	}
	return res, nil
}

// resolveProviderAndRuntime resolves the resource's Provider (via providerRef,
// or the resource itself if it is a Provider) and constructs its runtime.
func (e *Engine) resolveProviderAndRuntime(ctx context.Context, env resource.Envelope, byKey map[resource.Key]resource.Envelope) (reconciler.Provider, runtime.ContainerRuntime, error) {
	provEnv := env
	if env.Kind != "Provider" {
		refName := ""
		if ref, ok := env.Spec["providerRef"].(map[string]any); ok {
			refName, _ = ref["name"].(string)
		}
		if refName == "" {
			return nil, nil, fmt.Errorf("%s: no providerRef to resolve a provider from", env.Key())
		}
		pe, ok := byKey[resource.Key{Kind: "Provider", Name: refName}]
		if !ok {
			return nil, nil, fmt.Errorf("%s: providerRef %q does not resolve to a Provider", env.Key(), refName)
		}
		provEnv = pe
	}

	p, err := provider.FromEnvelope(provEnv)
	if err != nil {
		return nil, nil, err
	}
	prov, err := e.Registry.Provider(p.Type)
	if err != nil {
		return nil, nil, err
	}
	if aware, ok := prov.(reconciler.ProviderResourceAware); ok {
		aware.SetProviderResource(provEnv)
	}
	if aware, ok := prov.(reconciler.ResourceSetAware); ok {
		aware.SetResourceSet(byKey)
	}
	if aware, ok := prov.(reconciler.SecretsAware); ok && len(p.SecretRefs) > 0 {
		if e.SecretStore == nil {
			return nil, nil, fmt.Errorf("Provider %q declares secretRefs but no secret store is configured", provEnv.Metadata.Name)
		}
		secrets := make(map[string]map[string]string, len(p.SecretRefs))
		for _, refName := range p.SecretRefs {
			refEnv, ok := byKey[resource.Key{Kind: "SecretReference", Name: refName}]
			if !ok {
				return nil, nil, fmt.Errorf("Provider %q: secretRef %q does not resolve to a SecretReference", provEnv.Metadata.Name, refName)
			}
			ref := secretRefFrom(refEnv)
			resolved, err := e.SecretStore.Resolve(ctx, ref)
			if err != nil {
				return nil, nil, err
			}
			secrets[refName] = resolved
		}
		aware.SetSecrets(secrets)
	}
	rt, err := e.Registry.Runtime(p.RuntimeType, p.RuntimeConfig)
	if err != nil {
		return nil, nil, err
	}
	return prov, rt, nil
}

func (e *Engine) resolveLineageEndpoint(ctx context.Context, observerName string, byKey map[resource.Key]resource.Envelope, st *state.State) (lineage.LineageEndpoint, error) {
	provEnv, ok := byKey[resource.Key{Kind: "Provider", Name: observerName}]
	if !ok {
		return lineage.LineageEndpoint{}, fmt.Errorf("observer %q does not resolve to a Provider", observerName)
	}
	p, err := provider.FromEnvelope(provEnv)
	if err != nil {
		return lineage.LineageEndpoint{}, err
	}
	// The endpoint comes from the observed provider's state/configuration:
	// prefer an explicit configuration.url, fall back to providerState.
	if url, ok := p.Configuration["url"].(string); ok && url != "" {
		return lineage.LineageEndpoint{URL: url, Namespace: "datascape"}, nil
	}
	if rs, ok := st.Resources[provEnv.Key()]; ok {
		if url, ok := rs.Provider["url"].(string); ok && url != "" {
			return lineage.LineageEndpoint{URL: url, Namespace: "datascape"}, nil
		}
	}
	return lineage.LineageEndpoint{}, fmt.Errorf("observer %q: no resolvable endpoint (set spec.configuration.url or reconcile the provider first)", observerName)
}
