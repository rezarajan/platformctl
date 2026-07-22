// Package engine is the topological executor: walks the Plan in dependency
// order, calls Provider.Reconcile per resource, persists state after each
// resource (NFR-9), and resolves/forwards LineageEndpoints.
// See docs/planning/02-architecture.md §5.5.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rezarajan/platformctl/internal/application/compatibility"
	"github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/lineage"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/secret"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/clock"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
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
	// AllowImportedDeletes permits authoritative apply to delete resources
	// recorded as Imported when they are absent from desired manifests.
	AllowImportedDeletes bool
	// AllowOverwrite permits Restore to replace a resource's existing data.
	// It is the engine half of NFR-3-style safety: the CLI only sets it when
	// --yes-i-understand-this-overwrites-existing-data was passed.
	AllowOverwrite bool
	// Parallelism bounds concurrent reconciliation within a topological
	// level (resources in the same level share no dependency relationship).
	// Values <= 1 mean fully sequential. Gated by ParallelReconciliation.
	Parallelism int
	// Log receives one line per reconciliation action; nil disables. Used by
	// Destroy and for one-off messages.
	Log func(format string, args ...any)
	// Reporter receives structured apply progress events (start/done/skip)
	// for a rich, ordered, countable CLI display. nil disables.
	Reporter Reporter

	// stateMu serializes state-map access and persistence when levels
	// execute concurrently; reconciliation itself runs unlocked.
	stateMu sync.Mutex
}

type Result struct {
	Succeeded []resource.Key
	Failed    map[resource.Key]error
	Skipped   []resource.Key // dependents of failed resources
}

// Reporter receives structured apply progress so the CLI can render an
// ordered, countable, Docker-style view. All methods must be safe to call
// concurrently: with parallelism, several steps run at once. seq is the
// 1-based order in which steps started; total is the planned step count
// (healing steps discovered at runtime arrive with seq > total).
type Reporter interface {
	Begin(total int)
	StepStarted(seq, total int, key resource.Key, action string)
	StepFinished(seq, total int, key resource.Key, action string, d time.Duration, err error)
	StepSkipped(key resource.Key, reason string)
	StepHealing(key resource.Key, reason string)
	End(succeeded, failed, skipped int)
}

func (e *Engine) report(fn func(Reporter)) {
	if e.Reporter != nil {
		fn(e.Reporter)
	}
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
	for _, entry := range p.Entries {
		if _, ok := byKey[entry.Key]; ok {
			continue
		}
		if rs, ok := st.Resources[entry.Key]; ok && rs.LastApplied != nil {
			byKey[entry.Key] = *rs.LastApplied
		}
	}

	total := 0
	for _, entry := range p.Entries {
		if entry.Action != plan.ActionNoop {
			total++
		}
	}
	e.report(func(r Reporter) { r.Begin(total) })

	blocked := make(map[resource.Key]bool)
	var mu sync.Mutex // guards res and blocked during a concurrent level
	var seq int64     // 1-based order in which steps started

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
			e.report(func(r Reporter) { r.StepSkipped(key, "a dependency failed") })
			return
		}
		env, hasEnv := byKey[key]
		if entry.Action == plan.ActionOrphanUnknown {
			err := fmt.Errorf("%s cannot be deleted by apply because its state has no last-applied manifest; re-apply the resource once with this platformctl version, or use destroy with an explicit manifest to remove it", key)
			mu.Lock()
			res.Failed[key] = err
			mu.Unlock()
			e.logf("fail %s (%s): %v", key, entry.Action, err)
			e.report(func(r Reporter) { r.StepSkipped(key, err.Error()) })
			return
		}
		if entry.Action == plan.ActionRefused {
			err := errors.New(entry.Reason)
			mu.Lock()
			res.Failed[key] = err
			mu.Unlock()
			e.logf("fail %s (%s): %v", key, entry.Action, err)
			e.report(func(r Reporter) { r.StepSkipped(key, err.Error()) })
			return
		}
		if !hasEnv {
			err := fmt.Errorf("%s: no manifest is available for planned action %s", key, entry.Action)
			mu.Lock()
			res.Failed[key] = err
			mu.Unlock()
			e.logf("fail %s (%s): %v", key, entry.Action, err)
			e.report(func(r Reporter) { r.StepSkipped(key, err.Error()) })
			return
		}
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
			probed := e.probeOne(ctx, env, byKey, &st)
			if !HasDrift(probed) {
				return
			}
			if c, ok := probed.Condition(status.DriftDetected); ok {
				e.logf("drift %s (%s); healing", key, c.Reason)
				reason := c.Reason
				e.report(func(r Reporter) { r.StepHealing(key, reason) })
			}
		}

		n := int(atomic.AddInt64(&seq, 1))
		e.report(func(r Reporter) { r.StepStarted(n, total, key, string(entry.Action)) })
		start := time.Now()
		var err error
		if entry.Action == plan.ActionDelete {
			err = e.applyDeleteOne(ctx, entry, env, byKey, &st)
		} else {
			err = e.reconcileOne(ctx, entry, env, byKey, depGraph, &st)
		}
		dur := time.Since(start).Round(time.Millisecond)
		if err != nil {
			mu.Lock()
			res.Failed[key] = err
			for dep := range depGraph.Dependents(key) {
				blocked[dep] = true
			}
			mu.Unlock()
			e.logf("fail %s (%s) after %s: %v", key, entry.Action, dur, err)
			rerr := err
			e.report(func(r Reporter) { r.StepFinished(n, total, key, string(entry.Action), dur, rerr) })
			return
		}
		mu.Lock()
		res.Succeeded = append(res.Succeeded, key)
		mu.Unlock()
		e.logf("ok   %s (%s) in %s", key, entry.Action, dur)
		e.report(func(r Reporter) { r.StepFinished(n, total, key, string(entry.Action), dur, nil) })
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

	e.report(func(r Reporter) { r.End(len(res.Succeeded), len(res.Failed), len(res.Skipped)) })
	if len(res.Failed) > 0 {
		return res, fmt.Errorf("%d resource(s) failed to reconcile", len(res.Failed))
	}
	return res, nil
}

// DependencyGraph is the subset of domain/graph the engine needs; avoids the
// engine depending on graph construction.
type DependencyGraph interface {
	Dependents(k resource.Key) map[resource.Key]bool
	Dependencies(k resource.Key) map[resource.Key]bool
}

// PreflightSecrets checks that every SecretReference declared in the set
// resolves through the configured store, aggregating all failures so the
// user fixes them in one pass rather than one apply at a time. It touches no
// infrastructure and materializes no secret values — the fail-fast guard
// that a manifest set can never half-apply for want of a credential.
func (e *Engine) PreflightSecrets(ctx context.Context, envelopes []resource.Envelope) error {
	if e.SecretStore == nil {
		return nil
	}
	var problems []string
	for _, env := range envelopes {
		if env.Kind != "SecretReference" {
			continue
		}
		ref := secretRefFrom(env)
		if err := ref.Validate(); err != nil {
			problems = append(problems, err.Error())
			continue
		}
		if err := e.SecretStore.Preflight(ctx, ref); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%d secret(s) cannot be resolved — apply would half-apply the platform, so nothing was changed:\n  - %s",
			len(problems), strings.Join(problems, "\n  - "))
	}
	return nil
}

// SecretHashes resolves every SecretReference and returns deterministic,
// one-way fingerprints of the resolved material. The resolved values are not
// persisted or logged; the fingerprints let apply detect that dependencies
// must be reconciled after an operator rotates a secret out-of-band.
func (e *Engine) SecretHashes(ctx context.Context, envelopes []resource.Envelope) (map[resource.Key]string, error) {
	out := make(map[resource.Key]string)
	for _, env := range envelopes {
		if env.Kind != "SecretReference" {
			continue
		}
		ref := secretRefFrom(env)
		if err := ref.Validate(); err != nil {
			return nil, err
		}
		if e.SecretStore == nil {
			return nil, fmt.Errorf("SecretReference %q: no secret store is configured", env.Metadata.Name)
		}
		values, err := e.SecretStore.Resolve(ctx, ref)
		if err != nil {
			return nil, err
		}
		out[env.Key()] = SecretFingerprint(ref, values)
	}
	return out, nil
}

// SecretFingerprint returns a deterministic, one-way hash over the reference
// identity and resolved values. It is exported for tests and intentionally
// does not reveal the secret material.
func SecretFingerprint(ref secret.SecretReference, values map[string]string) string {
	keys := append([]string(nil), ref.Keys...)
	sort.Strings(keys)
	payload := struct {
		Name    string            `json:"name"`
		Backend secret.Backend    `json:"backend"`
		Keys    []string          `json:"keys"`
		Values  map[string]string `json:"values"`
	}{
		Name:    ref.Name,
		Backend: ref.Backend,
		Keys:    keys,
		Values:  values,
	}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func secretRefFrom(env resource.Envelope) secret.SecretReference {
	ref := secret.SecretReference{Name: env.Metadata.Name, Namespace: resource.NormalizeNamespace(env.Metadata.Namespace)}
	backend, _ := env.Spec["backend"].(string)
	ref.Backend = secret.Backend(backend)
	if keys, ok := env.Spec["keys"].([]any); ok {
		for _, k := range keys {
			if s, ok := k.(string); ok {
				ref.Keys = append(ref.Keys, s)
			}
		}
	}
	if k8s, ok := env.Spec["kubernetes"].(map[string]any); ok {
		ref.Kubernetes.Name, _ = k8s["name"].(string)
		ref.Kubernetes.Namespace, _ = k8s["namespace"].(string)
	}
	return ref
}

func (e *Engine) reconcileOne(ctx context.Context, entry plan.Entry, env resource.Envelope, byKey map[resource.Key]resource.Envelope, deps DependencyGraph, st *state.State) error {
	// SecretReference, external-no-provider, and external-with-provider are
	// all kind/lifecycle special cases dispatched through the single table
	// in kind_handler.go — see its doc comment for why table order doesn't
	// matter here despite the four engine methods checking these cases in
	// different orders.
	if h := lookupKindHandler(env); h != nil && h.reconcile != nil {
		return h.reconcile(e, ctx, entry, env, byKey, deps, st)
	}

	prov, req, err := e.resolveRequest(ctx, env, byKey, st)
	if err != nil {
		return err
	}

	newStatus, err := prov.Reconcile(ctx, req)
	if err != nil {
		return err
	}

	// Lineage forwarding: after a successful Reconcile, resolve observers and
	// hand the endpoint to a LineageAware provider — or record the
	// informational condition and move on. Never a failure, never a retry.
	if len(env.Metadata.Observers) > 0 {
		if la, ok := prov.(reconciler.LineageAware); ok {
			for _, obs := range env.Metadata.Observers {
				endpoint, err := e.resolveLineageEndpoint(ctx, obs, env.Metadata.Namespace, byKey, st)
				if err != nil {
					return fmt.Errorf("resolve observer %q: %w", obs.Name, err)
				}
				if err := la.ConfigureLineage(ctx, req, endpoint); err != nil {
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
	imported := st.Resources[env.Key()].Imported
	st.Resources[env.Key()] = e.resourceState(env, entry.SpecHash, newStatus, lifecycle, imported, deps)
	e.recordDependencyHashes(st, env.Key(), deps)
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
		probed := e.probeOneAgainstState(ctx, env, byKey, rs, &st)
		merged := rs.Status
		for _, c := range probed.Conditions {
			merged.SetCondition(c, e.Clock.Now())
		}
		// Observed provider facts ride under providerState["observed"] so
		// `status -o json` answers "what did the probe actually see" without
		// clobbering the reconcile-written providerState
		// (docs/planning/07 §2.1).
		if len(probed.ProviderState) > 0 {
			if rs.Provider == nil {
				rs.Provider = map[string]any{}
			}
			rs.Provider["observed"] = probed.ProviderState
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
func (e *Engine) probeOne(ctx context.Context, env resource.Envelope, byKey map[resource.Key]resource.Envelope, fullState *state.State) status.Status {
	return e.probeOneAgainstState(ctx, env, byKey, state.ResourceState{}, fullState)
}

func (e *Engine) probeOneAgainstState(ctx context.Context, env resource.Envelope, byKey map[resource.Key]resource.Envelope, rs state.ResourceState, fullState *state.State) status.Status {
	now := e.Clock.Now()
	st := status.Status{}

	// See reconcileOne: same kind/lifecycle dispatch table, consulted the
	// same way.
	if h := lookupKindHandler(env); h != nil && h.probe != nil {
		return h.probe(e, ctx, env, byKey, rs, fullState)
	}

	prov, req, err := e.resolveRequest(ctx, env, byKey, fullState)
	if err == nil {
		var probed status.Status
		probed, err = prov.Probe(ctx, req)
		if err == nil {
			return probed
		}
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProbeFailed, Message: err.Error()}, now)
	// The drift condition carries the same message: drift output rows read
	// it from here, and an empty Message hid the root cause of a live CI
	// failure (2026-07-22) behind a bare ProbeFailed.
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProbeFailed, Message: err.Error()}, now)
	return st
}

func (e *Engine) secretReferenceStatus(ctx context.Context, env resource.Envelope, priorHash string) status.Status {
	now := e.Clock.Now()
	st := status.Status{}
	ref := secretRefFrom(env)
	err := ref.Validate()
	var currentHash string
	if err == nil {
		if e.SecretStore == nil {
			err = fmt.Errorf("no secret store is configured")
		} else {
			var values map[string]string
			values, err = e.SecretStore.Resolve(ctx, ref)
			if err == nil {
				currentHash = SecretFingerprint(ref, values)
			}
		}
	}
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonSecretUnresolvable, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonSecretUnresolvable}, now)
		return st
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonSecretResolvable}, now)
	if priorHash != "" && currentHash != "" && priorHash != currentHash {
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonSecretChanged, Message: "resolved secret material differs from the last applied fingerprint"}, now)
		return st
	}
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st
}

// reconcileSecretReference verifies the reference resolves through the
// configured SecretStore (without storing any secret material) and records it
// Ready in state.
func (e *Engine) reconcileSecretReference(ctx context.Context, entry plan.Entry, env resource.Envelope, deps DependencyGraph, st *state.State) error {
	ref := secretRefFrom(env)
	if err := ref.Validate(); err != nil {
		return err
	}
	if e.SecretStore == nil {
		return fmt.Errorf("SecretReference %q: no secret store is configured", env.Metadata.Name)
	}
	values, err := e.SecretStore.Resolve(ctx, ref)
	if err != nil {
		return err
	}
	secretHash := entry.SecretHash
	if secretHash == "" {
		secretHash = SecretFingerprint(ref, values)
	}
	newStatus := status.Status{}
	now := e.Clock.Now()
	newStatus.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonSecretResolvable}, now)
	newStatus.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	imported := st.Resources[env.Key()].Imported
	rs := e.resourceState(env, entry.SpecHash, newStatus, resource.LifecycleOf(env, imported), imported, deps)
	rs.SecretHash = secretHash
	st.Resources[env.Key()] = rs
	e.recordDependencyHashes(st, env.Key(), deps)
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
	if !isExternal(env) {
		return false
	}
	if resource.RefName(env.Spec, "providerRef") != "" {
		return false
	}
	return true
}

func isExternal(env resource.Envelope) bool {
	ext, _ := env.Spec["external"].(bool)
	return ext
}

// externalConnectionStatus verifies the resource's connectionRef resolves:
// preferably to a Connection (whose optional secretRef must itself resolve
// through the secret store), or directly to a SecretReference — the v1.0.0
// shorthand, still supported.
func (e *Engine) externalConnectionStatus(ctx context.Context, env resource.Envelope, byKey map[resource.Key]resource.Envelope) status.Status {
	now := e.Clock.Now()
	st := status.Status{}
	connRef := resource.RefFromSpec(env.Spec, "connectionRef")
	connName := connRef.Name

	// 1. The connection details (address + credentials) must resolve.
	if connName != "" {
		if err := e.resolveConnectionRef(ctx, connRef, env.Metadata.Namespace, byKey); err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonExternalConnectionUnresolvable, Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonExternalConnectionUnresolvable}, now)
			return st
		}
	}

	// 2. When the connectionRef names a Connection with an address, actually
	// verify the endpoint answers — "resolvable" is not "reachable", and an
	// external resource that isn't reachable must not report Ready
	// (docs/history/errors.md: an unreachable external source claiming health is a lie).
	if connEnv, ok := byKey[connRef.Key(env.Metadata.Namespace, "Connection")]; ok {
		conn, err := connection.FromEnvelope(connEnv)
		if err == nil {
			addr, closeAddr := e.connectionDialAddress(ctx, connEnv, conn, byKey)
			if addr != "" {
				if closeAddr != nil {
					defer closeAddr()
				}
				if derr := probeTCPReachable(ctx, addr); derr != nil {
					st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonExternalEndpointUnreachable, Message: derr.Error()}, now)
					st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonExternalEndpointUnreachable}, now)
					return st
				}
				// Host-reachable is not the whole truth (ADR 015, docs/planning/08
				// C10): only a genuinely External connection's addr is a real
				// address meaningful to dial from *inside* a network too (a
				// managed connection's addr here is the forwarder's
				// host-audience tunnel address, e.g. a Docker published port or
				// a Kubernetes port-forward, and is not a useful in-network dial
				// target). When a Binding will dial this External connection
				// in-network (the CDC/sink/ingest connector shape), additionally
				// prove it's reachable from there — a firewall or network policy
				// can make the host and in-network answers diverge in either
				// direction, so this is reported as a distinct condition reason,
				// never folded into the host-side ExternalEndpointUnreachable.
				if conn.External {
					if inerr := e.probeInNetworkUnreachable(ctx, env, addr, byKey); inerr != nil {
						st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonExternalEndpointUnreachableInNetwork, Message: inerr.Error()}, now)
						st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonExternalEndpointUnreachableInNetwork}, now)
						return st
					}
				}
				st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonExternalEndpointReachable}, now)
				st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
				return st
			}
		}
	}

	// Bare-SecretReference shorthand (no address to probe): the most we can
	// assert is that the connection details resolve.
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonExternalConnectionResolvable}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st
}

// connectionDialAddress returns an address this process can dial right now
// to reach conn, plus a close func (nil when none is needed) that must be
// called once done. An external Connection's ExternalAddress() is already a
// plain declared address, needing no runtime. A managed Connection has no
// domain-layer address at all (domain can't import ports/runtime to guess
// one — docs/planning/09 F1), so here, with runtime access, resolve the
// forwarder's real reachable address through the same runtime.EnsureReachable
// mechanism every provider's own admin calls use — by (runtime name,
// container port) fact published by the realizing provider itself
// (endpoint.Endpoint.RuntimeName/ContainerPort, docs/planning/08 F4), not by
// re-deriving which resource's name the forwarder runs under. Before facts
// existed, this re-derived the name directly from the Connection's own
// Metadata.Name — correct only because proxy happens to name the forwarder
// after the Connection, not its realizing Provider (found live against
// minikube after a first version guessed "name it after the Provider" and
// failed with "container \"edge\" not found", the exact same mistake fixed
// once already in debezium's equivalent preflight). The fallback below
// keeps working the same way for state persisted before facts existed.
func (e *Engine) connectionDialAddress(ctx context.Context, connEnv resource.Envelope, conn connection.Connection, byKey map[resource.Key]resource.Envelope) (string, func() error) {
	if conn.External {
		addr, _ := conn.ExternalAddress()
		return addr, nil
	}
	_, req, err := e.resolveRequest(ctx, connEnv, byKey, nil)
	if err != nil {
		return "", nil
	}
	runtimeName, containerPort := naming.RuntimeObjectName(connEnv), conn.Port
	for _, ep := range endpoint.FromState(connEnv.Status.ProviderState[endpoint.Key]) {
		if ep.RuntimeName != "" && ep.ContainerPort != 0 {
			runtimeName, containerPort = ep.RuntimeName, ep.ContainerPort
			break
		}
	}
	addr, closeAddr, err := req.Runtime.EnsureReachable(ctx, runtimeName, containerPort)
	if err != nil {
		return "", nil
	}
	return addr, closeAddr
}

// inNetworkConsumer names one network an External connection consumer
// (a Binding's realizing Provider) would dial addr from — enough to resolve
// a runtime.ContainerRuntime and call ProbeReachable against.
type inNetworkConsumer struct {
	runtimeType   string
	runtimeConfig map[string]any
	network       string
}

// inNetworkConsumers finds every network a Binding that names env as its
// sourceRef/targetRef would dial env's external endpoint from — env being
// whatever resource (a Source, most commonly) declares the connectionRef
// externalConnectionStatus is checking (docs/planning/08 C10). A Binding's
// realizing Provider is exactly what will make that connection (the
// CDC/sink/ingest connector shape — e.g. debezium's desiredConnector dials
// an external Source's Connection directly from inside its own
// spec.runtime.network), so its declared network is the vantage point to
// probe from. Deduplicated by (runtime type, network): several Bindings
// commonly share one Provider/network.
func (e *Engine) inNetworkConsumers(env resource.Envelope, byKey map[resource.Key]resource.Envelope) []inNetworkConsumer {
	var out []inNetworkConsumer
	seen := map[string]bool{}
	for _, cand := range byKey {
		if cand.Kind != "Binding" {
			continue
		}
		ns := cand.Metadata.Namespace
		matched := false
		for _, field := range []string{"sourceRef", "targetRef"} {
			ref := resource.RefFromSpec(cand.Spec, field)
			if ref.Name == "" {
				continue
			}
			if ref.Key(ns, env.Kind) == env.Key() {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		provRef := resource.RefFromSpec(cand.Spec, "providerRef")
		provEnv, ok := byKey[provRef.Key(ns, "Provider")]
		if !ok {
			continue
		}
		p, err := provider.FromEnvelope(provEnv)
		if err != nil {
			continue
		}
		// spec.runtime.network, default "datascape" — the same convention
		// every provider adapter's own network(cfg) helper applies (e.g.
		// internal/adapters/providers/debezium.network); duplicated here
		// rather than imported since only application/registry may import
		// concrete provider packages (CLAUDE.md's layering invariant).
		netName := "datascape"
		if n, ok := p.RuntimeConfig["network"].(string); ok && n != "" {
			netName = n
		}
		key := p.RuntimeType + "|" + netName
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, inNetworkConsumer{runtimeType: p.RuntimeType, runtimeConfig: p.RuntimeConfig, network: netName})
	}
	return out
}

// probeInNetworkUnreachable reports the first in-network consumer (if any)
// that cannot reach addr from inside its own network, or nil when every
// consumer can (including "no consumers at all" — nothing to check).
func (e *Engine) probeInNetworkUnreachable(ctx context.Context, env resource.Envelope, addr string, byKey map[resource.Key]resource.Envelope) error {
	for _, c := range e.inNetworkConsumers(env, byKey) {
		rt, err := e.Registry.Runtime(c.runtimeType, c.runtimeConfig)
		if err != nil {
			continue // no runtime to check this consumer's network with — nothing more to say
		}
		pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		perr := rt.ProbeReachable(pctx, c.network, addr)
		cancel()
		if perr != nil {
			return fmt.Errorf("unreachable from network %q: %w", c.network, perr)
		}
	}
	return nil
}

// probeTCPReachable verifies an endpoint answers a TCP connection. A managed
// forwarder (socat) accepts the connection and then dials its upstream; if
// the upstream is down it closes ours immediately, so an immediate EOF/reset
// means unreachable. A live server that waits for the client to speak
// (Postgres, MySQL) leaves the connection open, so a short read that times
// out — or a server banner — means reachable.
func probeTCPReachable(ctx context.Context, address string) error {
	dctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	d := net.Dialer{}
	conn, err := d.DialContext(dctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("dial %s: %w", address, err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(750 * time.Millisecond))
	buf := make([]byte, 1)
	_, rerr := conn.Read(buf)
	if rerr == nil {
		return nil // server spoke first — reachable
	}
	var ne net.Error
	if errors.As(rerr, &ne) && ne.Timeout() {
		return nil // connection stayed open waiting for us — reachable
	}
	return fmt.Errorf("endpoint %s closed the connection immediately (upstream unreachable): %w", address, rerr)
}

// resolveConnectionRef checks a connectionRef target: a Connection whose
// credentials (if declared) resolve, or a bare SecretReference.
func (e *Engine) resolveConnectionRef(ctx context.Context, connRef resource.NameRef, defaultNamespace string, byKey map[resource.Key]resource.Envelope) error {
	connName := connRef.Name
	if connEnv, ok := byKey[connRef.Key(defaultNamespace, "Connection")]; ok {
		conn, err := connection.FromEnvelope(connEnv)
		if err != nil {
			return err
		}
		if conn.SecretRef == nil {
			return nil
		}
		refEnv, ok := byKey[resource.Key{Namespace: connEnv.Key().Namespace, Kind: "SecretReference", Name: *conn.SecretRef}]
		if !ok {
			return fmt.Errorf("Connection %q: secretRef %q does not resolve to a SecretReference in namespace %q", connName, *conn.SecretRef, connEnv.Key().Namespace)
		}
		if e.SecretStore == nil {
			return fmt.Errorf("no secret store is configured")
		}
		_, err = e.SecretStore.Resolve(ctx, secretRefFrom(refEnv))
		return err
	}
	if refEnv, ok := byKey[connRef.Key(defaultNamespace, "SecretReference")]; ok {
		if e.SecretStore == nil {
			return fmt.Errorf("no secret store is configured")
		}
		_, err := e.SecretStore.Resolve(ctx, secretRefFrom(refEnv))
		return err
	}
	return fmt.Errorf("connectionRef %q does not resolve to a Connection or SecretReference in namespace %q", connName, connRef.NamespaceOr(defaultNamespace))
}

// externalDatabaseTLSGate decodes env (when it is a Connection) and, if it
// declares an external-side spec.tls.mode, checks the ExternalDatabaseTLS
// gate (docs/planning/08 I2) — mirrors resolveRequest's TLSTermination
// check above, but a bare external Connection carries no providerRef and so
// never reaches resolveRequest at all (kind_handler.go routes it to
// reconcileExternal/externalConnectionStatus instead); this is their
// equivalent chokepoint. ok is false only when the gate is declared-and-
// disabled; msg then names it, mirroring registry.RequireGate's own message.
func (e *Engine) externalDatabaseTLSGate(env resource.Envelope) (msg string, ok bool) {
	if env.Kind != "Connection" {
		return "", true
	}
	conn, err := connection.FromEnvelope(env)
	if err != nil || !conn.External || conn.TLS == nil {
		return "", true
	}
	if gerr := e.Registry.RequireGate("ExternalDatabaseTLS"); gerr != nil {
		return gerr.Error(), false
	}
	return "", true
}

func (e *Engine) reconcileExternal(ctx context.Context, entry plan.Entry, env resource.Envelope, byKey map[resource.Key]resource.Envelope, deps DependencyGraph, st *state.State) error {
	if msg, ok := e.externalDatabaseTLSGate(env); !ok {
		return fmt.Errorf("%s: %s", env.Key(), msg)
	}
	// Reconcile is the "make it so" path: give a just-started external system
	// (or its forwarder) a bounded window to come up before declaring it
	// unreachable, rather than failing on the first dial. Drift/status use
	// the single-shot check for a fast, honest snapshot.
	deadline := time.Now().Add(30 * time.Second)
	probed := e.externalConnectionStatus(ctx, env, byKey)
	for !probed.IsReady() && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
		probed = e.externalConnectionStatus(ctx, env, byKey)
	}
	if !probed.IsReady() {
		if c, ok := probed.Condition(status.Ready); ok {
			return fmt.Errorf("%s: %s", env.Key(), c.Message)
		}
	}
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	imported := st.Resources[env.Key()].Imported
	st.Resources[env.Key()] = e.resourceState(env, entry.SpecHash, probed, resource.External, imported, deps)
	e.recordDependencyHashes(st, env.Key(), deps)
	return e.StateStore.Save(ctx, *st)
}

// reconcileExternalWithProvider is the reconcile hook (kind_handler.go) for
// an External resource that names a providerRef: unlike a bare external
// declaration, something in the platform actively configures it through
// ExternalConfigurer rather than only verifying reachability.
func (e *Engine) reconcileExternalWithProvider(ctx context.Context, entry plan.Entry, env resource.Envelope, byKey map[resource.Key]resource.Envelope, deps DependencyGraph, st *state.State) error {
	prov, req, err := e.resolveRequest(ctx, env, byKey, st)
	if err != nil {
		return err
	}
	configurer, ok := prov.(reconciler.ExternalConfigurer)
	if !ok {
		return fmt.Errorf("%s is External with providerRef, but provider type %q does not implement ExternalConfigurer", env.Key(), prov.Type())
	}
	newStatus, err := configurer.ConfigureExternal(ctx, req)
	if err != nil {
		return err
	}
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	imported := st.Resources[env.Key()].Imported
	st.Resources[env.Key()] = e.resourceState(env, entry.SpecHash, newStatus, resource.External, imported, deps)
	e.recordDependencyHashes(st, env.Key(), deps)
	return e.StateStore.Save(ctx, *st)
}

func (e *Engine) resourceState(env resource.Envelope, specHash string, st status.Status, lifecycle resource.Lifecycle, imported bool, deps DependencyGraph) state.ResourceState {
	env.Metadata.Namespace = resource.NormalizeNamespace(env.Metadata.Namespace)
	return state.ResourceState{
		SpecHash:     specHash,
		Status:       st,
		Lifecycle:    lifecycle.String(),
		Imported:     imported,
		Provider:     st.ProviderState,
		LastApplied:  &env,
		Dependencies: dependencyKeys(deps, env.Key()),
	}
}

func dependencyKeys(deps DependencyGraph, key resource.Key) []resource.Key {
	if deps == nil {
		return nil
	}
	depSet := deps.Dependencies(key)
	if len(depSet) == 0 {
		return nil
	}
	out := make([]resource.Key, 0, len(depSet))
	for dep := range depSet {
		out = append(out, dep)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func (e *Engine) recordDependencyHashes(st *state.State, key resource.Key, deps DependencyGraph) {
	if deps == nil {
		return
	}
	rs := st.Resources[key]
	hashes := make(map[string]string)
	for _, dep := range dependencyKeys(deps, key) {
		depState := st.Resources[dep]
		if depState.SecretHash == "" {
			continue
		}
		hashes[state.KeyString(dep)] = depState.SecretHash
	}
	if len(hashes) > 0 {
		rs.DependencyHashes = hashes
	} else {
		rs.DependencyHashes = nil
	}
	st.Resources[key] = rs
}

func (e *Engine) applyDeleteOne(ctx context.Context, entry plan.Entry, env resource.Envelope, byKey map[resource.Key]resource.Envelope, st *state.State) error {
	e.stateMu.Lock()
	rs := st.Resources[entry.Key]
	e.stateMu.Unlock()

	// See reconcileOne: same kind/lifecycle dispatch table. This method
	// (like Destroy) checks the External-lifecycle case before consulting
	// the table generically, whereas reconcileOne/probeOneAgainstState check
	// SecretReference first — see kind_handler.go's doc comment for why
	// this doesn't change behavior (spec.external and Kind=="SecretReference"
	// are mutually exclusive by schema).
	lifecycle := resource.LifecycleOf(env, rs.Imported)
	if lifecycle == resource.External {
		if !e.AllowDestructive {
			return fmt.Errorf("%s is External: deleting it during apply requires both --include-external and --yes-i-understand-this-is-destructive", entry.Key)
		}
		if h := lookupKindHandler(env); h != nil && h.del != nil {
			e.stateMu.Lock()
			err := h.del(e, ctx, env, entry.Key, st)
			e.stateMu.Unlock()
			return err
		}
	}
	if lifecycle == resource.Imported && !e.AllowImportedDeletes {
		return fmt.Errorf("%s is Imported: deleting it during apply requires --include-imported-deletes", entry.Key)
	}
	if h := lookupKindHandler(env); h != nil && h.del != nil {
		e.stateMu.Lock()
		err := h.del(e, ctx, env, entry.Key, st)
		e.stateMu.Unlock()
		return err
	}
	prov, req, err := e.resolveRequest(ctx, env, byKey, st)
	if err != nil {
		return err
	}
	if err := prov.Destroy(ctx, req); err != nil {
		return err
	}
	e.stateMu.Lock()
	delete(st.Resources, entry.Key)
	err = e.StateStore.Save(ctx, *st)
	e.stateMu.Unlock()
	return err
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

	// Import loads no state (there is none yet for a not-applied-before
	// resource); a schema-carrying Binding's registry endpoint simply won't
	// be resolved this way here (nil st) — resolveSchemaRegistryURL treats
	// that the same as "not yet published".
	probed := e.probeOne(ctx, env, byKey, nil)
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
		SpecHash:    hash,
		Status:      probed,
		Lifecycle:   resource.Imported.String(),
		Imported:    true,
		Provider:    probed.ProviderState,
		LastApplied: &env,
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
		if entry.Action != plan.ActionDelete && entry.Action != plan.ActionRefused {
			continue
		}
		if entry.Action == plan.ActionRefused {
			err := errors.New(entry.Reason)
			res.Failed[entry.Key] = err
			e.logf("fail destroy %s: %v", entry.Key, err)
			block(entry.Key)
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
		// even if a plan claims otherwise. The kind/lifecycle special cases
		// below (external-no-provider, SecretReference) share the same
		// dispatch table reconcileOne/probeOneAgainstState/applyDeleteOne
		// use — see kind_handler.go's doc comment.
		if resource.LifecycleOf(env, st.Resources[entry.Key].Imported) == resource.External {
			if !e.AllowDestructive {
				err := fmt.Errorf("%s is External: destroying it requires both --include-external and --yes-i-understand-this-is-destructive", entry.Key)
				res.Failed[entry.Key] = err
				e.logf("fail destroy %s: %v", entry.Key, err)
				block(entry.Key)
				continue
			}
			if h := lookupKindHandler(env); h != nil && h.del != nil {
				// Nothing in the platform realizes it; forgetting it is all
				// destroy can (and should) do.
				if err := h.del(e, ctx, env, entry.Key, &st); err != nil {
					return res, err
				}
				res.Succeeded = append(res.Succeeded, entry.Key)
				e.logf("ok   destroy %s (external: removed from state only)", entry.Key)
				continue
			}
		}
		if h := lookupKindHandler(env); h != nil && h.del != nil {
			if err := h.del(e, ctx, env, entry.Key, &st); err != nil {
				return res, err
			}
			res.Succeeded = append(res.Succeeded, entry.Key)
			e.logf("ok   destroy %s", entry.Key)
			continue
		}
		prov, req, err := e.resolveRequest(ctx, env, byKey, &st)
		if err != nil {
			res.Failed[entry.Key] = err
			e.logf("fail destroy %s: %v", entry.Key, err)
			block(entry.Key)
			continue
		}
		if err := prov.Destroy(ctx, req); err != nil {
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

// resolveRequest resolves the resource's Provider (via providerRef, or the
// resource itself if it is a Provider), constructs its runtime, resolves
// any declared secrets, and assembles the reconciler.Request every
// Reconcile/Destroy/Probe/capability-method call uses as its single input
// (docs/planning/08 F5) — replacing the old resolveProviderAndRuntime's
// Set*-before-call dance (SetProviderResource/SetResourceSet/SetSecrets)
// with one immutable value built once per call, so a provider never holds
// state across calls.
func (e *Engine) resolveRequest(ctx context.Context, env resource.Envelope, byKey map[resource.Key]resource.Envelope, st *state.State) (reconciler.Provider, reconciler.Request, error) {
	provEnv := env
	if env.Kind != "Provider" {
		ref := resource.RefFromSpec(env.Spec, "providerRef")
		if ref.Name == "" {
			return nil, reconciler.Request{}, fmt.Errorf("%s: no providerRef to resolve a provider from", env.Key())
		}
		pe, ok := byKey[ref.Key(env.Metadata.Namespace, "Provider")]
		if !ok {
			return nil, reconciler.Request{}, fmt.Errorf("%s: providerRef %q does not resolve to a Provider in namespace %q", env.Key(), ref.Name, ref.NamespaceOr(env.Metadata.Namespace))
		}
		provEnv = pe
	}

	// TLSTermination gate (docs/planning/08 C8): Connection.spec.tls has no
	// natural provider-construction or runtime-call choke point of its own
	// (unlike IngressProvider itself, which gates at Registry.Provider
	// below) — this is the one place every Reconcile/Probe/Destroy call for
	// a TLS-declared Connection passes through, mirroring HighAvailability's
	// own backstop-at-point-of-use pattern (registry.haGuardRuntime).
	if env.Kind == "Connection" {
		if conn, cerr := connection.FromEnvelope(env); cerr == nil && conn.TLS != nil {
			if gerr := e.Registry.RequireGate("TLSTermination"); gerr != nil {
				return nil, reconciler.Request{}, fmt.Errorf("%s: %w", env.Key(), gerr)
			}
		}
	}

	p, err := provider.FromEnvelope(provEnv)
	if err != nil {
		return nil, reconciler.Request{}, err
	}
	prov, err := e.Registry.Provider(p.Type)
	if err != nil {
		return nil, reconciler.Request{}, err
	}
	var secrets map[string]map[string]string
	if len(p.SecretRefs) > 0 {
		if e.SecretStore == nil {
			return nil, reconciler.Request{}, fmt.Errorf("Provider %q declares secretRefs but no secret store is configured", provEnv.Metadata.Name)
		}
		secrets = make(map[string]map[string]string, len(p.SecretRefs))
		for _, refName := range p.SecretRefs {
			refEnv, ok := byKey[resource.Key{Namespace: provEnv.Key().Namespace, Kind: "SecretReference", Name: refName}]
			if !ok {
				return nil, reconciler.Request{}, fmt.Errorf("Provider %q: secretRef %q does not resolve to a SecretReference in namespace %q", provEnv.Metadata.Name, refName, provEnv.Key().Namespace)
			}
			ref := secretRefFrom(refEnv)
			resolved, err := e.SecretStore.Resolve(ctx, ref)
			if err != nil {
				return nil, reconciler.Request{}, err
			}
			secrets[refName] = resolved
		}
	}
	rt, err := e.Registry.Runtime(p.RuntimeType, p.RuntimeConfig)
	if err != nil {
		return nil, reconciler.Request{}, err
	}
	return prov, reconciler.Request{
		Resource:              env,
		Runtime:               rt,
		Provider:              provEnv,
		Secrets:               secrets,
		Resources:             byKey,
		SchemaRegistryURL:     e.resolveSchemaRegistryURL(env, byKey, st),
		KafkaBootstrapServers: e.resolveKafkaBootstrapServers(provEnv, p, byKey),
		MetricsTargets:        e.resolveMetricsTargets(env, st),
		CatalogFacts:          e.resolveCatalogFacts(env, p, byKey, st),
		PrometheusURL:         e.resolvePrometheusURL(env, p, byKey, st),
		WarehouseFacts:        e.resolveWarehouseFacts(env, byKey, st),
	}, nil
}

// resolvePrometheusURL resolves a grafana Provider's datasource-provisioning
// input (docs/planning/08 C9 completion) — see reconciler.Request.
// PrometheusURL's doc comment for the exact resolution rule and the
// published-facts-only discipline (ADR 015). Mirrors resolveCatalogFacts's
// warehouseProviderRef inference: an explicit configuration.prometheusRef
// wins; otherwise the sole prometheus-typed Provider in the namespace,
// left unresolved ("") when 0 or more than 1 candidate exists.
func (e *Engine) resolvePrometheusURL(env resource.Envelope, p provider.Provider, byKey map[resource.Key]resource.Envelope, st *state.State) string {
	if st == nil || env.Kind != "Provider" {
		return ""
	}
	var promEnv resource.Envelope
	found := false
	if ref := resource.RefFromSpec(p.Configuration, "prometheusRef"); ref.Name != "" {
		promEnv, found = byKey[ref.Key(env.Metadata.Namespace, "Provider")]
	} else {
		ns := resource.NormalizeNamespace(env.Metadata.Namespace)
		ambiguous := false
		for _, cand := range byKey {
			if cand.Kind != "Provider" || resource.NormalizeNamespace(cand.Metadata.Namespace) != ns {
				continue
			}
			if t, _ := cand.Spec["type"].(string); t != "prometheus" {
				continue
			}
			if found {
				ambiguous = true
				break
			}
			promEnv, found = cand, true
		}
		if ambiguous {
			found = false
		}
	}
	if !found {
		return ""
	}

	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	rs, ok := st.Resources[promEnv.Key()]
	if !ok {
		return ""
	}
	for _, ep := range endpoint.FromState(rs.Provider[endpoint.Key]) {
		if ep.Name == "prometheus" && ep.Internal != "" {
			return ep.Internal
		}
	}
	return ""
}

// resolveCatalogFacts resolves Provider(type: trino).spec.configuration.
// catalogRef (docs/planning/08 D10) into the published facts the trino
// provider needs to write etc/catalog/lakehouse.properties — see
// reconciler.CatalogFacts's doc comment for the exact shape and the
// published-facts-only discipline (ADR 015), same as
// resolveSchemaRegistryURL/resolveMetricsTargets above. Populated only for a
// Provider-kind env declaring configuration.catalogRef; nil whenever the
// referenced Catalog, or the resolved warehouse-backing S3/MinIO Provider,
// has not yet published its endpoint in st — a provider reading this field
// must treat nil as "not ready yet", never construct a substitute.
func (e *Engine) resolveCatalogFacts(env resource.Envelope, p provider.Provider, byKey map[resource.Key]resource.Envelope, st *state.State) *reconciler.CatalogFacts {
	if st == nil || env.Kind != "Provider" {
		return nil
	}
	catalogRef := resource.RefFromSpec(p.Configuration, "catalogRef")
	if catalogRef.Name == "" {
		return nil
	}
	catEnv, ok := byKey[catalogRef.Key(env.Metadata.Namespace, "Catalog")]
	if !ok {
		return nil
	}

	restInternal := e.publishedEndpointFact(catEnv.Key(), "iceberg-rest", st)
	if restInternal == "" {
		return nil
	}

	// Resolve the warehouse-backing S3/MinIO Provider, in this order
	// (docs/planning/08 D8's recorded resolution order):
	//  1. the referenced Catalog's own spec.warehouseRef chain (Catalog ->
	//     Dataset -> the Dataset's realizing Provider) — canonical once the
	//     Catalog declares it (D8).
	//  2. this Provider's own explicit configuration.warehouseProviderRef
	//     disambiguator (D10, unchanged) — becomes unnecessary once (1)
	//     applies, but not removed: additive coexistence.
	//  3. the sole S3/MinIO-typed Provider in the manifest's namespace,
	//     auto-inferred (D10, unchanged); left unresolved (nil) when 0 or >1
	//     candidates exist — this provider's own ValidateSpec/Reconcile
	//     surfaces that ambiguity explicitly rather than this engine seam
	//     guessing.
	if whRef := resource.RefFromSpec(catEnv.Spec, "warehouseRef"); whRef.Name != "" {
		if dsEnv, ok := byKey[whRef.Key(catEnv.Metadata.Namespace, "Dataset")]; ok {
			if ds, err := dataset.FromEnvelope(dsEnv); err == nil {
				if s3Internal, s3SecretRef, ok := e.resolveDatasetS3Facts(ds, dsEnv, byKey, st); ok {
					return &reconciler.CatalogFacts{RestInternal: restInternal, S3Internal: s3Internal, S3SecretRef: s3SecretRef}
				}
			}
		}
	}

	var s3Env resource.Envelope
	s3Found := false
	if whRef := resource.RefFromSpec(p.Configuration, "warehouseProviderRef"); whRef.Name != "" {
		s3Env, s3Found = byKey[whRef.Key(env.Metadata.Namespace, "Provider")]
	} else {
		ns := resource.NormalizeNamespace(env.Metadata.Namespace)
		ambiguous := false
		for _, cand := range byKey {
			if cand.Kind != "Provider" || resource.NormalizeNamespace(cand.Metadata.Namespace) != ns {
				continue
			}
			t, _ := cand.Spec["type"].(string)
			if t != "s3" && t != "minio" {
				continue
			}
			if s3Found {
				ambiguous = true
				break
			}
			s3Env, s3Found = cand, true
		}
		if ambiguous {
			s3Found = false
		}
	}
	if !s3Found {
		return nil
	}

	s3Internal, s3SecretRef, ok := e.resolveProviderS3Fact(s3Env, st)
	if !ok {
		return nil
	}

	return &reconciler.CatalogFacts{
		RestInternal: restInternal,
		S3Internal:   s3Internal,
		S3SecretRef:  s3SecretRef,
	}
}

// resolveWarehouseFacts resolves Catalog.spec.warehouseRef (docs/planning/08
// D8) into reconciler.WarehouseFacts — see that type's doc comment for the
// exact shape and the published-facts-only discipline (ADR 015); mirrors
// resolveCatalogFacts above, but for a Catalog-kind Resource's own
// warehouseRef rather than a trino-like Provider's configuration.catalogRef.
// Populated only for env.Kind == "Catalog" declaring spec.warehouseRef; nil
// otherwise, or when the referenced Dataset or its realizing Provider is
// missing or has not yet published its "s3" endpoint fact — a provider
// reading this field must treat nil as "not ready", never construct a
// substitute. graph.Build's warehouseRef -> Dataset edge (plus the Dataset's
// own pre-existing providerRef edge) means this is, in practice, always
// non-nil by the time a Catalog with a valid warehouseRef reconciles within
// the same apply.
func (e *Engine) resolveWarehouseFacts(env resource.Envelope, byKey map[resource.Key]resource.Envelope, st *state.State) *reconciler.WarehouseFacts {
	if st == nil || env.Kind != "Catalog" {
		return nil
	}
	whRef := resource.RefFromSpec(env.Spec, "warehouseRef")
	if whRef.Name == "" {
		return nil
	}
	dsEnv, ok := byKey[whRef.Key(env.Metadata.Namespace, "Dataset")]
	if !ok {
		return nil
	}
	ds, err := dataset.FromEnvelope(dsEnv)
	if err != nil {
		return nil
	}
	s3Internal, s3SecretRef, ok := e.resolveDatasetS3Facts(ds, dsEnv, byKey, st)
	if !ok {
		return nil
	}
	return &reconciler.WarehouseFacts{
		Bucket:      ds.Bucket,
		Prefix:      ds.Prefix,
		S3Internal:  s3Internal,
		S3SecretRef: s3SecretRef,
	}
}

// resolveDatasetS3Facts resolves a Dataset's own realizing Provider's
// published "s3" endpoint fact plus its credential SecretReference *name* —
// the shared tail end of resolveWarehouseFacts above and resolveCatalogFacts's
// warehouseRef-chain preference (docs/planning/08 D8).
func (e *Engine) resolveDatasetS3Facts(ds dataset.Dataset, dsEnv resource.Envelope, byKey map[resource.Key]resource.Envelope, st *state.State) (s3Internal, s3SecretRef string, ok bool) {
	if ds.ProviderRef == "" {
		return "", "", false
	}
	s3Env, found := byKey[resource.Key{Namespace: resource.NormalizeNamespace(dsEnv.Metadata.Namespace), Kind: "Provider", Name: ds.ProviderRef}]
	if !found {
		return "", "", false
	}
	return e.resolveProviderS3Fact(s3Env, st)
}

// resolveProviderS3Fact reads an S3/MinIO-typed Provider's published "s3"
// endpoint fact plus its credential SecretReference *name* (its own
// configuration.rootSecretRef, or the first entry of its spec.secretRefs) —
// the state-reading tail shared by resolveCatalogFacts's warehouseProviderRef/
// auto-infer path and resolveDatasetS3Facts above.
func (e *Engine) resolveProviderS3Fact(s3Env resource.Envelope, st *state.State) (s3Internal, s3SecretRef string, ok bool) {
	s3Internal = e.publishedEndpointFact(s3Env.Key(), "s3", st)
	if s3Internal == "" {
		return "", "", false
	}
	if s3p, err := provider.FromEnvelope(s3Env); err == nil {
		s3SecretRef, _ = s3p.Configuration["rootSecretRef"].(string)
		if s3SecretRef == "" && len(s3p.SecretRefs) > 0 {
			s3SecretRef = s3p.SecretRefs[0]
		}
	}
	return s3Internal, s3SecretRef, true
}

// publishedEndpointFact reads a single named endpoint fact's Internal
// address from key's published state — the shared, lock-guarded primitive
// resolveCatalogFacts/resolveWarehouseFacts's fact lookups build on (ADR
// 015: published facts, never constructed).
func (e *Engine) publishedEndpointFact(key resource.Key, name string, st *state.State) string {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	rs, ok := st.Resources[key]
	if !ok {
		return ""
	}
	for _, ep := range endpoint.FromState(rs.Provider[endpoint.Key]) {
		if ep.Name == name && ep.Internal != "" {
			return ep.Internal
		}
	}
	return ""
}

// resolveKafkaBootstrapServers mirrors resolveSchemaRegistryURL's seam
// (docs/planning/08 E2): computed once per request from provEnv (the
// realizing Provider — env itself when env.Kind == "Provider", or the
// Provider a dependent resource like Binding resolves through), so both
// reconcileWorker (Provider-kind request) and buildDesiredConnector
// (Binding-kind request, for the same worker) see the identical effective
// value the worker container was actually started with. p is provEnv's
// already-decoded configuration; an explicit spec.configuration.
// bootstrapServers always wins and skips the graph walk entirely.
func (e *Engine) resolveKafkaBootstrapServers(provEnv resource.Envelope, p provider.Provider, byKey map[resource.Key]resource.Envelope) string {
	if _, has := p.Configuration["bootstrapServers"]; has {
		return ""
	}
	envelopes := make([]resource.Envelope, 0, len(byKey))
	for _, v := range byKey {
		envelopes = append(envelopes, v)
	}
	return compatibility.ResolveKafkaBootstrapAddress(provEnv, envelopes, e.Registry.Provider)
}

// resolveMetricsTargets resolves the prometheus provider's scrape-target
// list (docs/planning/08 C9) by scanning state for every Provider
// resource's published "metrics" endpoint fact — never constructed by the
// provider itself (ADR 015), the same published-fact-only discipline
// resolveSchemaRegistryURL/resolveLineageEndpoint already follow. Populated
// only for a Provider-kind Resource (the only kind the prometheus provider
// reconciles); nil for anything else, including Import (nil st), which
// resolveSchemaRegistryURL treats the same way. st.Resources is shared,
// mutated-in-place engine state — the same lock discipline as every other
// access in this file.
func (e *Engine) resolveMetricsTargets(env resource.Envelope, st *state.State) []reconciler.MetricsTarget {
	if st == nil || env.Kind != "Provider" {
		return nil
	}
	ns := resource.NormalizeNamespace(env.Metadata.Namespace)
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	var out []reconciler.MetricsTarget
	for key, rs := range st.Resources {
		if key.Kind != "Provider" || key.Namespace != ns {
			continue
		}
		for _, ep := range endpoint.FromState(rs.Provider[endpoint.Key]) {
			if ep.Name == "metrics" && ep.Internal != "" {
				out = append(out, reconciler.MetricsTarget{JobName: key.Name, Endpoint: ep})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobName < out[j].JobName })
	return out
}

// resolveSchemaRegistryURL resolves the schema registry endpoint from the
// EventStream endpoint's own realizing Provider's already-published
// "schema-registry" endpoint fact — the same providerState lookup
// resolveLineageEndpoint uses for a lineage backend's url, never a guessed
// address (docs/planning/08 D1, docs/planning/09 F4) — needed by two
// callers: a Binding's schema-carrying spec.options.format (avro, protobuf,
// D1), or a sink Binding targeting a Dataset with spec.format: parquet (D2,
// s3sink wires Avro key/value converters for parquet's schema-carrying
// requirement even when the Binding itself declares no options.format).
// Returns "" when neither condition holds, it isn't a Binding, or the
// upstream Provider hasn't published the endpoint yet in st (nil st — e.g.
// Import, which loads no state — behaves the same as "not yet published").
func (e *Engine) resolveSchemaRegistryURL(env resource.Envelope, byKey map[resource.Key]resource.Envelope, st *state.State) string {
	if st == nil || env.Kind != "Binding" {
		return ""
	}
	b, err := binding.FromEnvelope(env)
	if err != nil {
		return ""
	}
	format, _ := b.Options["format"].(string)
	needsRegistry := format == "avro" || format == "protobuf"
	if !needsRegistry && b.Mode == binding.ModeSink {
		tgtRef := resource.RefFromSpec(env.Spec, "targetRef")
		if dsEnv, ok := byKey[tgtRef.Key(env.Metadata.Namespace, "Dataset")]; ok {
			if ds, err := dataset.FromEnvelope(dsEnv); err == nil && ds.Format == "parquet" {
				needsRegistry = true
			}
		}
	}
	if !needsRegistry {
		return ""
	}

	var esEnv resource.Envelope
	var found bool
	for _, field := range []string{"sourceRef", "targetRef"} {
		ref := resource.RefFromSpec(env.Spec, field)
		if candidate, ok := byKey[ref.Key(env.Metadata.Namespace, "EventStream")]; ok {
			esEnv, found = candidate, true
			break
		}
	}
	if !found {
		return ""
	}
	provRef := resource.RefFromSpec(esEnv.Spec, "providerRef")
	esProvEnv, ok := byKey[provRef.Key(esEnv.Metadata.Namespace, "Provider")]
	if !ok {
		return ""
	}
	// st.Resources is shared, mutated-in-place engine state (docs/planning/08
	// D1): under ParallelReconciliation, another goroutine may be writing a
	// sibling resource's entry concurrently, so this read takes the same
	// lock every other st.Resources access in this file does.
	e.stateMu.Lock()
	rs, ok := st.Resources[esProvEnv.Key()]
	e.stateMu.Unlock()
	if !ok {
		return ""
	}
	for _, ep := range endpoint.FromState(rs.Provider[endpoint.Key]) {
		if ep.Name == "schema-registry" && ep.Internal != "" {
			return ep.Internal
		}
	}
	return ""
}

func (e *Engine) resolveLineageEndpoint(ctx context.Context, observer resource.ObserverRef, defaultNamespace string, byKey map[resource.Key]resource.Envelope, st *state.State) (lineage.LineageEndpoint, error) {
	ref := resource.NameRef{Name: observer.Name, Namespace: observer.Namespace}
	provEnv, ok := byKey[ref.Key(defaultNamespace, "Provider")]
	if !ok {
		return lineage.LineageEndpoint{}, fmt.Errorf("observer %q does not resolve to a Provider in namespace %q", observer.Name, ref.NamespaceOr(defaultNamespace))
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
	return lineage.LineageEndpoint{}, fmt.Errorf("observer %q: no resolvable endpoint (set spec.configuration.url or reconcile the provider first)", observer.Name)
}
