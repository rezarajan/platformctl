package compose

import (
	"strings"
	"testing"
)

func TestExposeSourceTCPCreatesProxyAndConnection(t *testing.T) {
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	opts := ExposeOptions{
		TargetKind: "Source", TargetName: "app-db",
		Scheme: "tcp", Port: 25432,
		Provider: RefChoice{New: true},
	}
	patch, err := PlanExpose(snap, dir, opts, testResolver(t))
	if err != nil {
		t.Fatalf("PlanExpose: %v", err)
	}
	if !patch.HasChanges() {
		t.Fatal("expected changes")
	}
	got := map[string]string{}
	for _, f := range patch.Files {
		got[f.Path] = f.Content
	}
	if _, ok := got["provider-expose-proxy.yaml"]; !ok {
		t.Fatalf("expected a new proxy Provider file; got %+v", patch.Files)
	}
	connDoc, ok := got["connection-app-db-conn.yaml"]
	if !ok {
		t.Fatalf("expected a new Connection file; got %+v", patch.Files)
	}
	if !strings.Contains(connDoc, "target: db:5432") {
		t.Errorf("Connection target = %s, want \"target: db:5432\" (app-db's realizing Provider is named \"db\", postgres' standard port)", connDoc)
	}
	if !strings.Contains(connDoc, "scheme: tcp") || !strings.Contains(connDoc, "port: 25432") {
		t.Errorf("Connection doc missing scheme/port: %s", connDoc)
	}

	if _, _, err := Write(patch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	mustLoad(t, dir)
}

func TestExposeHTTPSDegradesCleanlyBeforeC8(t *testing.T) {
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	_, err := PlanExpose(snap, dir, ExposeOptions{
		TargetKind: "Source", TargetName: "app-db",
		Scheme: "https", Port: 8443,
		Provider: RefChoice{New: true},
	}, testResolver(t))
	if err == nil {
		t.Fatal("expected --scheme https to be refused (C8/ingress TLS has not merged)")
	}
	if !strings.Contains(err.Error(), "docs/adr/018") {
		t.Errorf("error = %v, want it to point at ADR 018 §C8", err)
	}
	// Must not write anything on this degrade path either.
	for _, f := range snap.Envelopes {
		if f.Metadata.Name == "expose-ingress" {
			t.Fatal("PlanExpose must not have created any resource on the https degrade path")
		}
	}
}

func TestExposeRejectsUnknownTarget(t *testing.T) {
	dir := t.TempDir()
	writeCDCToLakeFixture(t, dir)
	snap := mustLoad(t, dir)

	_, err := PlanExpose(snap, dir, ExposeOptions{
		TargetKind: "Source", TargetName: "nope", Scheme: "tcp", Port: 1,
		Provider: RefChoice{New: true},
	}, testResolver(t))
	if err == nil {
		t.Fatal("expected an error exposing a Source that does not exist")
	}
}
