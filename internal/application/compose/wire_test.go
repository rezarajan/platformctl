package compose

import (
	"strings"
	"testing"
)

func TestWireCDCCreatesMissingEventStreamReuseFirst(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	opts := WireOptions{
		Mode:     "cdc",
		From:     "Source/app-db",
		To:       "EventStream/app-db-audit",
		Provider: RefChoice{New: true}, ProviderType: "debezium",
	}
	patch, err := PlanWire(snap, dir, opts)
	if err != nil {
		t.Fatalf("PlanWire: %v", err)
	}
	if !patch.HasChanges() {
		t.Fatal("expected changes")
	}
	joined := strings.Join(patch.Notes, "\n")
	if !strings.Contains(joined, `reusing broker Provider "broker"`) {
		t.Errorf("Notes = %q, want the missing EventStream's broker glue to reuse the sole existing broker", joined)
	}

	got := map[string]bool{}
	for _, f := range patch.Files {
		got[f.Path] = true
	}
	for _, want := range []string{"eventstream-app-db-audit.yaml", "provider-app-db-cdc.yaml", "binding-app-db-to-app-db-audit.yaml"} {
		if !got[want] {
			t.Errorf("missing expected file %s; got %+v", want, patch.Files)
		}
	}

	if _, _, err := Write(patch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustLoad(t, dir)
}

func TestWireSinkReusesExistingWorker(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	opts := WireOptions{
		Mode:     "sink",
		From:     "EventStream/app-events",
		To:       "Dataset/raw-lake",
		Provider: RefChoice{Name: "sink"},
		Name:     "app-events-to-raw-lake-again",
	}
	patch, err := PlanWire(snap, dir, opts)
	if err != nil {
		t.Fatalf("PlanWire: %v", err)
	}
	if len(patch.Files) != 1 || patch.Files[0].Path != "binding-app-events-to-raw-lake-again.yaml" {
		t.Fatalf("expected exactly one new Binding file, got %+v", patch.Files)
	}
	if !strings.Contains(patch.Files[0].Content, "providerRef") || !strings.Contains(patch.Files[0].Content, "name: sink") {
		t.Errorf("expected the reused \"sink\" worker Provider in the Binding, got:\n%s", patch.Files[0].Content)
	}
}

func TestWireRejectsUnknownFromResource(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	_, err := PlanWire(snap, dir, WireOptions{
		Mode: "cdc", From: "Source/does-not-exist", To: "EventStream/x",
		Provider: RefChoice{Name: "cdc"},
	})
	if err == nil {
		t.Fatal("expected an error for a --from resource that does not exist")
	}
}

func TestWireRejectsInvalidKindPairing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	_, err := PlanWire(snap, dir, WireOptions{
		Mode: "cdc", From: "Source/app-db", To: "Dataset/raw-lake",
		Provider: RefChoice{Name: "cdc"},
	})
	if err == nil {
		t.Fatal("expected an error: cdc mode does not pair Source -> Dataset")
	}
	if !strings.Contains(err.Error(), "allowed pairings") {
		t.Errorf("error = %v, want it to name the allowed pairings", err)
	}
}
