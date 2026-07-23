package openziti

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func xdEnv(kind, name, ns, domain string, spec map[string]any) resource.Envelope {
	if spec == nil {
		spec = map[string]any{}
	}
	return resource.Envelope{
		GroupVersionKind: resource.GroupVersionKind{APIVersion: "datascape.io/v1alpha1", Kind: kind},
		Metadata:         resource.Metadata{Name: name, Namespace: ns, Domain: domain},
		Spec:             spec,
	}
}

// TestResolveRawMediatedTargetPicksTheTargetHostNotTheFirstEdge pins the H9
// merge-gate fix: a managed Connection's graph edge list BEGINS with its
// providerRef (the mesh Provider, same domain as the Connection), and the
// old first-edge lookup handed QualifyTargetAddress the wrong envelope —
// live symptom: a router terminator dialing a bare cross-namespace name.
// Resolution must key on the target HOST's runtime object name.
func TestResolveRawMediatedTargetPicksTheTargetHostNotTheFirstEdge(t *testing.T) {
	t.Parallel()
	mesh := xdEnv("Provider", "xd-mesh", "default", "analytics", map[string]any{"type": "openziti"})
	pg := xdEnv("Provider", "xd-pg", "default", "payments", map[string]any{"type": "postgres"})
	conn := xdEnv("Connection", "xd-conn", "default", "analytics", map[string]any{
		"providerRef": map[string]any{"name": "xd-mesh"},
		"target":      "xd-pg:5432",
	})
	resources := map[resource.Key]resource.Envelope{
		mesh.Key(): mesh, pg.Key(): pg, conn.Key(): conn,
	}
	got, ok, err := resolveRawMediatedTarget(conn, resources)
	if err != nil || !ok {
		t.Fatalf("resolveRawMediatedTarget: ok=%v err=%v", ok, err)
	}
	if got.Key() != pg.Key() {
		t.Fatalf("resolved %s, want the target Provider %s (never the providerRef mesh)", got.Key(), pg.Key())
	}

	// External target: nothing in-set matches — lenient false, no error.
	ext := xdEnv("Connection", "xd-ext", "default", "analytics", map[string]any{
		"providerRef": map[string]any{"name": "xd-mesh"},
		"target":      "db.example.com:5432",
	})
	resources[ext.Key()] = ext
	if _, ok, err := resolveRawMediatedTarget(ext, resources); err != nil || ok {
		t.Fatalf("external target: ok=%v err=%v, want false,nil", ok, err)
	}
}
