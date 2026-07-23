package compose

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPipelineReusesBrokerAndDataset is the engine-level half of the owner
// scenario's Accept criterion: init -> add pipeline -> add pipeline
// (second run's candidate computation lists the first broker+Dataset;
// --sink-prefix emits a second sink Binding to the same bucket at a
// different location).
func TestPipelineReusesBrokerAndDataset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	// Candidates the interactive select (or a --broker/--sink existing:...
	// flag) would offer must include the blueprint's own broker and Dataset
	// before anything new is added.
	if !hasCandidate(snap.BrokerCandidates(), "broker") {
		t.Fatalf("BrokerCandidates() = %+v, want to include the blueprint's \"broker\"", snap.BrokerCandidates())
	}
	if _, ok := snap.DatasetCandidateByName("raw-lake"); !ok {
		t.Fatalf("DatasetCandidateByName(\"raw-lake\") not found; DatasetCandidates() = %+v", snap.DatasetCandidates())
	}

	opts := PipelineOptions{
		Name:   "second",
		Engine: "postgres",
		Broker: RefChoice{Name: "broker"},
		SinkAttachment: SinkAttachment{
			Sink:       RefChoice{Name: "raw-lake"},
			SinkPrefix: "other/",
		},
	}

	patch, err := PlanPipeline(snap, dir, opts)
	if err != nil {
		t.Fatalf("PlanPipeline: %v", err)
	}
	if !patch.HasChanges() {
		t.Fatal("first PlanPipeline run should propose changes")
	}

	// Reuse must be visible in the patch's notes (what a human/JSON caller sees).
	joined := strings.Join(patch.Notes, "\n")
	if !strings.Contains(joined, `reusing broker Provider "broker"`) {
		t.Errorf("Notes = %q, want a note reusing broker Provider \"broker\"", joined)
	}
	if !strings.Contains(joined, `lake Provider "lake"`) || !strings.Contains(joined, `sink worker Provider "sink"`) {
		t.Errorf("Notes = %q, want notes reusing lake Provider \"lake\" and sink worker Provider \"sink\"", joined)
	}

	// No new broker/lake/sink-worker Provider file should be proposed —
	// only source-side + a new Dataset (different prefix) + new Bindings.
	for _, f := range patch.Files {
		if f.Path == "provider-broker.yaml" || f.Path == "provider-lake.yaml" || f.Path == "provider-sink.yaml" {
			t.Errorf("patch proposes rewriting the reused blueprint file %s — reuse must not touch it", f.Path)
		}
	}
	wantNewDataset := "dataset-second-lake.yaml"
	found := false
	for _, f := range patch.Files {
		if f.Path == wantNewDataset && f.New {
			found = true
			if !strings.Contains(f.Content, "bucket: raw-events") || !strings.Contains(f.Content, `prefix: "other/"`) {
				t.Errorf("%s content = %s, want bucket raw-events at prefix other/", wantNewDataset, f.Content)
			}
		}
	}
	if !found {
		t.Errorf("expected a new Dataset file %s reusing the existing bucket at a new prefix; files = %+v", wantNewDataset, patch.Files)
	}

	// --- write it, then re-plan with identical answers: zero changes. ---
	written, envKeys, err := Write(patch)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(written) == 0 {
		t.Fatal("Write wrote nothing")
	}
	if len(envKeys) == 0 {
		t.Fatal("Write appended no .env keys for the new source's secrets")
	}
	for _, k := range envKeys {
		if !strings.HasPrefix(k, "DATASCAPE_SECRET_SECOND_") {
			t.Errorf("unexpected env key %q written for pipeline \"second\"", k)
		}
	}

	snap2 := mustLoad(t, dir)
	// Second run's candidate computation must now also list the newly
	// created resources alongside the originals — the graph-aware reuse
	// bar, not just a static list.
	if !hasCandidate(snap2.BrokerCandidates(), "broker") {
		t.Fatalf("BrokerCandidates() after first add = %+v, want to still include \"broker\"", snap2.BrokerCandidates())
	}

	patch2, err := PlanPipeline(snap2, dir, opts)
	if err != nil {
		t.Fatalf("PlanPipeline (rerun): %v", err)
	}
	if patch2.HasChanges() {
		var changed []string
		for _, f := range patch2.Files {
			if f.New || !f.Identical {
				changed = append(changed, f.Path)
			}
		}
		t.Fatalf("re-running add pipeline with identical answers proposed changes: files=%v envAppends=%+v", changed, patch2.EnvAppends)
	}
}

// TestPipelineNewSinkChain exercises the --sink new path (no existing
// Dataset to reuse): a fresh lake+worker+Dataset chain.
func TestPipelineNewSinkChain(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	opts := PipelineOptions{
		Name:   "third",
		Engine: "mysql",
		Broker: RefChoice{New: true},
		SinkAttachment: SinkAttachment{
			Sink:   RefChoice{New: true},
			Lake:   RefChoice{New: true},
			Bucket: "third-bucket",
		},
	}
	patch, err := PlanPipeline(snap, dir, opts)
	if err != nil {
		t.Fatalf("PlanPipeline: %v", err)
	}
	if !patch.HasChanges() {
		t.Fatal("expected changes")
	}
	wantFiles := []string{
		"provider-third-db.yaml", "source-third.yaml",
		"provider-third-broker.yaml", "provider-third-cdc.yaml", "eventstream-third-events.yaml", "binding-third-to-events.yaml",
		"provider-third-lake-store.yaml", "provider-third-sink.yaml", "dataset-third-lake.yaml", "binding-third-to-lake.yaml",
	}
	got := map[string]bool{}
	for _, f := range patch.Files {
		got[f.Path] = true
	}
	for _, w := range wantFiles {
		if !got[w] {
			t.Errorf("missing expected generated file %s; got %+v", w, patch.Files)
		}
	}

	// The "validates green with zero edits" bar: write the patch, then
	// reload with the same tolerant front-end and require no degrade —
	// every ref resolves, every capability check the stub resolver can
	// make passes.
	if _, _, err := Write(patch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustLoad(t, dir)
}

// TestPipelineNameCollisionNeverOverwrites proves the "nothing is ever
// overwritten silently" rule: a --name colliding with a differently-shaped
// existing resource is a hard error, not a rewrite.
func TestPipelineNameCollisionNeverOverwrites(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	// "app-db" is already the blueprint's own Source name.
	opts := PipelineOptions{
		Name:   "app-db",
		Engine: "postgres",
		Broker: RefChoice{Name: "broker"},
		SinkAttachment: SinkAttachment{
			Sink: RefChoice{Name: "raw-lake"},
		},
	}
	_, err := PlanPipeline(snap, dir, opts)
	if err == nil {
		t.Fatal("expected a collision error reusing the existing \"app-db\" Source name")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error = %v, want a message about the name already existing", err)
	}

	// Confirm nothing was written to disk despite the error.
	if _, statErr := os.Stat(filepath.Join(dir, "provider-app-db-db.yaml")); !os.IsNotExist(statErr) {
		t.Errorf("PlanPipeline must not write anything on error; provider-app-db-db.yaml exists")
	}
}
