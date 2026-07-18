package plan

import (
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

func testEnvelope(kind, name string, spec map[string]any) resource.Envelope {
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: kind},
		Metadata:         resource.Metadata{Name: name, Namespace: resource.DefaultNamespace},
		Spec:             spec,
	}
}

func TestComputePlansAuthoritativeDeletes(t *testing.T) {
	keep := testEnvelope("Provider", "keep", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	removed := testEnvelope("EventStream", "removed", map[string]any{"providerRef": map[string]any{"name": "keep"}})
	g, err := graph.Build([]resource.Envelope{keep})
	if err != nil {
		t.Fatal(err)
	}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		keep.Key(): {
			SpecHash:    "same",
			Lifecycle:   resource.Managed.String(),
			LastApplied: &keep,
		},
		removed.Key(): {
			SpecHash:     "old",
			Lifecycle:    resource.Managed.String(),
			LastApplied:  &removed,
			Dependencies: []resource.Key{keep.Key()},
		},
	}}
	hash, err := SpecHash(keep)
	if err != nil {
		t.Fatal(err)
	}
	rs := st.Resources[keep.Key()]
	rs.SpecHash = hash
	st.Resources[keep.Key()] = rs

	p, err := Compute([]resource.Envelope{keep}, st, g)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Entries[len(p.Entries)-1]; got.Key != removed.Key() || got.Action != ActionDelete {
		t.Fatalf("last entry = %+v, want delete for %s", got, removed.Key())
	}
	if got := p.Levels[len(p.Levels)-1][0]; got != removed.Key() {
		t.Fatalf("last level key = %s, want %s", got, removed.Key())
	}
}

// TestComputePlansProtectedDeleteAsRefused guards docs/planning/08 A5:
// removing a protected resource from manifests must never fall through to an
// authoritative delete — it must be reported as a refusal naming the remedy.
func TestComputePlansProtectedDeleteAsRefused(t *testing.T) {
	keep := testEnvelope("Provider", "keep", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	protected := testEnvelope("EventStream", "protected", map[string]any{"providerRef": map[string]any{"name": "keep"}})
	protected.Metadata.Protect = true
	g, err := graph.Build([]resource.Envelope{keep})
	if err != nil {
		t.Fatal(err)
	}
	hash, err := SpecHash(keep)
	if err != nil {
		t.Fatal(err)
	}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		keep.Key(): {SpecHash: hash, Lifecycle: resource.Managed.String(), LastApplied: &keep},
		protected.Key(): {
			SpecHash:     "old",
			Lifecycle:    resource.Managed.String(),
			LastApplied:  &protected,
			Dependencies: []resource.Key{keep.Key()},
		},
	}}

	p, err := Compute([]resource.Envelope{keep}, st, g)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range p.Entries {
		if entry.Key != protected.Key() {
			continue
		}
		found = true
		if entry.Action != ActionRefused {
			t.Fatalf("protected entry action = %s, want %s", entry.Action, ActionRefused)
		}
		if !strings.Contains(entry.Reason, "protect") {
			t.Fatalf("protected entry reason = %q, want it to name the remedy", entry.Reason)
		}
	}
	if !found {
		t.Fatalf("protected key %s not reported", protected.Key())
	}
}

func TestComputeDestroyRefusesProtectedResource(t *testing.T) {
	protected := testEnvelope("Provider", "protected", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	protected.Metadata.Protect = true
	g, err := graph.Build([]resource.Envelope{protected})
	if err != nil {
		t.Fatal(err)
	}
	hash, err := SpecHash(protected)
	if err != nil {
		t.Fatal(err)
	}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		protected.Key(): {SpecHash: hash, Lifecycle: resource.Managed.String(), LastApplied: &protected},
	}}

	p, err := ComputeDestroy([]resource.Envelope{protected}, st, g, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Entries[0]; got.Action != ActionRefused || !strings.Contains(got.Reason, "protect") {
		t.Fatalf("destroy entry = %+v, want refused naming the protect remedy", got)
	}
}

func TestComputeReportsLegacyOrphanUnknown(t *testing.T) {
	keep := testEnvelope("Provider", "keep", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	legacy := resource.Key{Namespace: resource.DefaultNamespace, Kind: "Provider", Name: "legacy"}
	g, err := graph.Build([]resource.Envelope{keep})
	if err != nil {
		t.Fatal(err)
	}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		legacy: {SpecHash: "old", Lifecycle: resource.Managed.String()},
	}}

	p, err := Compute([]resource.Envelope{keep}, st, g)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range p.Entries {
		if entry.Key == legacy {
			found = true
			if entry.Action != ActionOrphanUnknown {
				t.Fatalf("legacy action = %s, want %s", entry.Action, ActionOrphanUnknown)
			}
		}
	}
	if !found {
		t.Fatalf("legacy key %s not reported", legacy)
	}
}

// TestComputePlansRenameAsDeleteAndCreate guards docs/planning/07 §0.4: a
// rename (old manifest name removed, a new name added in the same apply)
// must be explicit — an authoritative delete of the old key plus a create
// of the new key — never a silent update-in-place of one for the other.
func TestComputePlansRenameAsDeleteAndCreate(t *testing.T) {
	prov := testEnvelope("Provider", "keep", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	oldName := testEnvelope("EventStream", "old-name", map[string]any{"providerRef": map[string]any{"name": "keep"}})
	newName := testEnvelope("EventStream", "new-name", map[string]any{"providerRef": map[string]any{"name": "keep"}})

	provHash, err := SpecHash(prov)
	if err != nil {
		t.Fatal(err)
	}
	oldHash, err := SpecHash(oldName)
	if err != nil {
		t.Fatal(err)
	}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		prov.Key():    {SpecHash: provHash, Lifecycle: resource.Managed.String(), LastApplied: &prov},
		oldName.Key(): {SpecHash: oldHash, Lifecycle: resource.Managed.String(), LastApplied: &oldName, Dependencies: []resource.Key{prov.Key()}},
	}}

	// The manifest set now has prov + newName (oldName was renamed away).
	g2, err := graph.Build([]resource.Envelope{prov, newName})
	if err != nil {
		t.Fatal(err)
	}
	p, err := Compute([]resource.Envelope{prov, newName}, st, g2)
	if err != nil {
		t.Fatal(err)
	}

	actions := make(map[resource.Key]Action, len(p.Entries))
	for _, e := range p.Entries {
		actions[e.Key] = e.Action
	}
	if got := actions[oldName.Key()]; got != ActionDelete {
		t.Errorf("old-name action = %s, want %s", got, ActionDelete)
	}
	if got := actions[newName.Key()]; got != ActionCreate {
		t.Errorf("new-name action = %s, want %s", got, ActionCreate)
	}
}

// TestComputePlansProviderTypeChangeAsUpdate guards docs/planning/07 §0.4:
// changing which provider a resource points at (or a provider's own type)
// without renaming the resource must show up as an in-place update, driven
// purely by the spec-hash diff — never silently treated as a no-op.
func TestComputePlansProviderTypeChangeAsUpdate(t *testing.T) {
	before := testEnvelope("EventStream", "events", map[string]any{"providerRef": map[string]any{"name": "old-provider"}})
	after := testEnvelope("EventStream", "events", map[string]any{"providerRef": map[string]any{"name": "new-provider"}})
	oldProv := testEnvelope("Provider", "old-provider", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})
	newProv := testEnvelope("Provider", "new-provider", map[string]any{"type": "noop", "runtime": map[string]any{"type": "fake"}})

	beforeHash, err := SpecHash(before)
	if err != nil {
		t.Fatal(err)
	}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		before.Key(): {SpecHash: beforeHash, Lifecycle: resource.Managed.String(), LastApplied: &before, Dependencies: []resource.Key{oldProv.Key()}},
	}}

	g, err := graph.Build([]resource.Envelope{newProv, oldProv, after})
	if err != nil {
		t.Fatal(err)
	}
	p, err := Compute([]resource.Envelope{newProv, oldProv, after}, st, g)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range p.Entries {
		if e.Key == after.Key() {
			if e.Action != ActionUpdate {
				t.Errorf("provider-type-change action = %s, want %s (reason: %s)", e.Action, ActionUpdate, e.Reason)
			}
			return
		}
	}
	t.Fatalf("no plan entry for %s", after.Key())
}

func TestComputePlansSecretHashChangeAndDependents(t *testing.T) {
	creds := testEnvelope("SecretReference", "db-creds", map[string]any{"backend": "env", "keys": []any{"password"}})
	prov := testEnvelope("Provider", "postgres", map[string]any{
		"type":       "noop",
		"runtime":    map[string]any{"type": "fake"},
		"secretRefs": []any{"db-creds"},
	})
	g, err := graph.Build([]resource.Envelope{creds, prov})
	if err != nil {
		t.Fatal(err)
	}
	credsHash, err := SpecHash(creds)
	if err != nil {
		t.Fatal(err)
	}
	provHash, err := SpecHash(prov)
	if err != nil {
		t.Fatal(err)
	}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		creds.Key(): {
			SpecHash:    credsHash,
			SecretHash:  "old",
			Lifecycle:   resource.Managed.String(),
			LastApplied: &creds,
		},
		prov.Key(): {
			SpecHash:    provHash,
			Lifecycle:   resource.Managed.String(),
			LastApplied: &prov,
		},
	}}

	p, err := ComputeWithSecretHashes([]resource.Envelope{creds, prov}, st, g, map[resource.Key]string{creds.Key(): "new"})
	if err != nil {
		t.Fatal(err)
	}
	entries := map[resource.Key]Entry{}
	for _, entry := range p.Entries {
		entries[entry.Key] = entry
	}
	if got := entries[creds.Key()]; got.Action != ActionUpdate || got.Reason != "resolved secret material changed since last apply" {
		t.Fatalf("secret entry = %+v, want update for secret material change", got)
	}
	if got := entries[prov.Key()]; got.Action != ActionUpdate || got.Reason != "dependency default/SecretReference/db-creds changed" {
		t.Fatalf("provider entry = %+v, want dependency update", got)
	}
}

func TestComputePlansSecretBaselineWhenHashMissing(t *testing.T) {
	creds := testEnvelope("SecretReference", "db-creds", map[string]any{"backend": "env", "keys": []any{"password"}})
	g, err := graph.Build([]resource.Envelope{creds})
	if err != nil {
		t.Fatal(err)
	}
	specHash, err := SpecHash(creds)
	if err != nil {
		t.Fatal(err)
	}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		creds.Key(): {
			SpecHash:    specHash,
			Lifecycle:   resource.Managed.String(),
			LastApplied: &creds,
		},
	}}

	p, err := ComputeWithSecretHashes([]resource.Envelope{creds}, st, g, map[resource.Key]string{creds.Key(): "new"})
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Entries[0]; got.Action != ActionUpdate || got.Reason != "resolved secret fingerprint not recorded in state" {
		t.Fatalf("secret entry = %+v, want baseline update", got)
	}
}

func TestComputePlansDependentWhenSecretAlreadyAdvancedAfterFailedApply(t *testing.T) {
	creds := testEnvelope("SecretReference", "db-creds", map[string]any{"backend": "env", "keys": []any{"password"}})
	prov := testEnvelope("Provider", "mysql", map[string]any{
		"type":       "noop",
		"runtime":    map[string]any{"type": "fake"},
		"secretRefs": []any{"db-creds"},
	})
	g, err := graph.Build([]resource.Envelope{creds, prov})
	if err != nil {
		t.Fatal(err)
	}
	credsHash, err := SpecHash(creds)
	if err != nil {
		t.Fatal(err)
	}
	provHash, err := SpecHash(prov)
	if err != nil {
		t.Fatal(err)
	}
	st := state.State{Version: state.CurrentVersion, Resources: map[resource.Key]state.ResourceState{
		creds.Key(): {
			SpecHash:    credsHash,
			SecretHash:  "new",
			Lifecycle:   resource.Managed.String(),
			LastApplied: &creds,
		},
		prov.Key(): {
			SpecHash:    provHash,
			Lifecycle:   resource.Managed.String(),
			LastApplied: &prov,
		},
	}}

	p, err := ComputeWithSecretHashes([]resource.Envelope{creds, prov}, st, g, map[resource.Key]string{creds.Key(): "new"})
	if err != nil {
		t.Fatal(err)
	}
	entries := map[resource.Key]Entry{}
	for _, entry := range p.Entries {
		entries[entry.Key] = entry
	}
	if got := entries[creds.Key()]; got.Action != ActionNoop {
		t.Fatalf("secret entry = %+v, want no-op because failed apply already recorded it", got)
	}
	if got := entries[prov.Key()]; got.Action != ActionUpdate || got.Reason != "dependency default/SecretReference/db-creds secret material changed since last apply" {
		t.Fatalf("provider entry = %+v, want dependency secret update", got)
	}
}
