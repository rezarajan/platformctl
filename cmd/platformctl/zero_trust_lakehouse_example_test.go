package main

import (
	"strings"
	"testing"
)

// TestZeroTrustLakehouseExampleValidates guards the flagship
// examples/zero-trust-lakehouse/ manifest set — the release's proof that a
// complete zero-trust data platform (multi-source CDC, Parquet + Iceberg,
// Trino query, Nessie catalog, Marquez lineage, openziti-mediated dark
// source, label-scoped policy) is coherent. Structural validation only (no
// secret values, no Docker); the live apply + zero-trust proofs live in the
// example's own test-zero-trust.sh (README#zero-trust). This test keeps the
// example from silently rotting as schemas evolve, the same guard
// output_contract_test.go gives examples/cdc-attendance.
func TestZeroTrustLakehouseExampleValidates(t *testing.T) {
	t.Parallel()
	gates := "SchemaRegistrySupport=true,MediatedConnections=true,PolicyEngine=true,LabelScopedAccess=true,TrinoProvider=true"
	out, _, err := runSplit(t, "validate", "../../examples/zero-trust-lakehouse",
		"--policies", "../../examples/zero-trust-lakehouse/policies",
		"--feature-gates", gates)
	if err != nil {
		t.Fatalf("validate zero-trust-lakehouse: %v\n%s", err, out)
	}
	if !strings.Contains(out, "resource(s) valid") {
		t.Errorf("expected a valid resource count, got:\n%s", out)
	}
}
