// Package engine is the topological executor: walks the Plan in dependency
// order, calls Provider.Reconcile per resource, persists state after each
// resource (NFR-9), and resolves/forwards LineageEndpoints.
// See docs/planning/02-architecture.md §5.5.
package engine

import (
	"context"
	"fmt"
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
	// Log receives one line per reconciliation action; nil disables.
	Log func(format string, args ...any)
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

	for _, level := range p.Levels {
		for _, key := range level {
			entry, ok := entryByKey[key]
			if !ok {
				continue
			}
			if blocked[key] {
				res.Skipped = append(res.Skipped, key)
				e.logf("skip %s: a dependency failed", key)
				continue
			}
			if entry.Action == plan.ActionNoop {
				continue
			}

			env := byKey[key]
			start := time.Now()
			err := e.reconcileOne(ctx, entry, env, byKey, &st)
			if err != nil {
				res.Failed[key] = err
				e.logf("fail %s (%s) after %s: %v", key, entry.Action, time.Since(start).Round(time.Millisecond), err)
				if e.HaltOnError {
					return res, fmt.Errorf("%s: %w (halting: --halt-on-error)", key, err)
				}
				for dep := range depGraph.Dependents(key) {
					blocked[dep] = true
				}
				continue
			}
			res.Succeeded = append(res.Succeeded, key)
			e.logf("ok   %s (%s) in %s", key, entry.Action, time.Since(start).Round(time.Millisecond))
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

// Destroy executes a destroy plan in reverse dependency order. The engine is
// the single enforcement point for NFR-3: External resources are never
// destroyed unless the plan explicitly marked them.
func (e *Engine) Destroy(ctx context.Context, p plan.Plan, envelopes []resource.Envelope) (Result, error) {
	res := Result{Failed: make(map[resource.Key]error)}
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, env := range envelopes {
		byKey[env.Key()] = env
	}

	st, err := e.StateStore.Load(ctx)
	if err != nil {
		return res, err
	}

	for _, entry := range p.Entries {
		if entry.Action != plan.ActionDelete {
			continue
		}
		env, ok := byKey[entry.Key]
		if !ok {
			continue
		}
		prov, rt, err := e.resolveProviderAndRuntime(ctx, env, byKey)
		if err != nil {
			res.Failed[entry.Key] = err
			continue
		}
		if err := prov.Destroy(ctx, env, rt); err != nil {
			res.Failed[entry.Key] = err
			e.logf("fail destroy %s: %v", entry.Key, err)
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
