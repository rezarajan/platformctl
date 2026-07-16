package plan

import (
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
