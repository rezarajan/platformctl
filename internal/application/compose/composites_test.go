package compose

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlanSourceStandalone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	patch, err := PlanSource(snap, dir, SourceOptions{Name: "legacy", Engine: "mysql"})
	if err != nil {
		t.Fatalf("PlanSource: %v", err)
	}
	if !patch.HasChanges() {
		t.Fatal("expected changes")
	}
	if _, _, err := Write(patch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustLoad(t, dir)

	// Idempotent regeneration.
	snap2 := mustLoad(t, dir)
	patch2, err := PlanSource(snap2, dir, SourceOptions{Name: "legacy", Engine: "mysql"})
	if err != nil {
		t.Fatalf("PlanSource (rerun): %v", err)
	}
	if patch2.HasChanges() {
		t.Fatalf("re-running add source with identical answers proposed changes: %+v", patch2.Files)
	}
}

func TestPlanCatalogNewAndReuse(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	patch, err := PlanCatalog(snap, dir, CatalogOptions{Name: "lakehouse-catalog", Provider: RefChoice{New: true}})
	if err != nil {
		t.Fatalf("PlanCatalog: %v", err)
	}
	if _, _, err := Write(patch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustLoad(t, dir)

	// A second catalog reusing the just-created nessie Provider.
	snap2 := mustLoad(t, dir)
	patch2, err := PlanCatalog(snap2, dir, CatalogOptions{Name: "second-catalog", Provider: RefChoice{Name: "lakehouse-catalog-provider"}})
	if err != nil {
		t.Fatalf("PlanCatalog (reuse): %v", err)
	}
	if len(patch2.Files) != 1 {
		t.Fatalf("reuse should only add the Catalog resource itself, got %+v", patch2.Files)
	}
}

func TestPlanMonitoringStandalone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	patch, err := PlanMonitoring(snap, dir, MonitoringOptions{Name: "monitoring"})
	if err != nil {
		t.Fatalf("PlanMonitoring: %v", err)
	}
	if len(patch.Files) != 1 || patch.Files[0].Path != "provider-monitoring.yaml" {
		t.Fatalf("unexpected files: %+v", patch.Files)
	}
	if _, _, err := Write(patch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustLoad(t, dir)
}

func TestPlanSinkStandalone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	patch, err := PlanSink(snap, dir, SinkOptions{
		Name:   "extra",
		Stream: "app-events",
		SinkAttachment: SinkAttachment{
			Sink: RefChoice{Name: "raw-lake"},
		},
	})
	if err != nil {
		t.Fatalf("PlanSink: %v", err)
	}
	if len(patch.Files) != 1 || patch.Files[0].Path != "binding-extra-to-lake.yaml" {
		t.Fatalf("unexpected files: %+v", patch.Files)
	}
}

// TestEnvAppendsAreIdempotent proves the .env half of the idempotent-
// regeneration bar: appending the same keys twice must not duplicate
// lines or clobber a value the user has since filled in.
func TestEnvAppendsAreIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	opts := SourceOptions{Name: "extra-src", Engine: "postgres"}
	patch, err := PlanSource(snap, dir, opts)
	if err != nil {
		t.Fatalf("PlanSource: %v", err)
	}
	_, keys, err := Write(patch)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("expected .env keys to be appended")
	}

	envPath := filepath.Join(dir, ".env")
	before, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the user filling in a real value, then re-running with
	// identical answers.
	filled := strings.Replace(string(before), keys[0]+"=change-me", keys[0]+"=a-real-secret", 1)
	if filled == string(before) {
		// admin/username default isn't "change-me"; fall back to touching
		// nothing so this assertion still just proves no duplication below.
		filled = string(before)
	}
	if err := os.WriteFile(envPath, []byte(filled), 0o644); err != nil {
		t.Fatal(err)
	}

	snap2 := mustLoad(t, dir)
	patch2, err := PlanSource(snap2, dir, opts)
	if err != nil {
		t.Fatalf("PlanSource (rerun): %v", err)
	}
	if patch2.HasChanges() {
		t.Fatalf("re-running add source with identical answers proposed .env changes: %+v", patch2.EnvAppends)
	}
	_, keys2, err := Write(patch2)
	if err != nil {
		t.Fatalf("Write (rerun): %v", err)
	}
	if len(keys2) != 0 {
		t.Fatalf("Write should append nothing on the idempotent rerun, appended %v", keys2)
	}

	after, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if n := strings.Count(string(after), k+"="); n != 1 {
			t.Errorf(".env key %s appears %d times after a second run, want exactly 1", k, n)
		}
	}
	if !strings.Contains(string(after), "a-real-secret") {
		t.Error("the second run clobbered the user-filled-in secret value")
	}
}
