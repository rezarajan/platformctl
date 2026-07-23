package archtest

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// domainScopedNetworkNaming matches the exact identifiers only
// docs/adr/022 Ring 1's translation chokepoint (internal/application/
// engine's domainRuntime, naming.NetworkName's own definition, and
// internal/adapters/runtime/kubernetes's NetworkSpec.AllowFromNetworks
// implementation) may legitimately reference. A provider under
// internal/adapters/providers referencing any of these would mean a
// provider has started computing a domain-scoped network name itself —
// the exact coupling docs/planning/08 H5 was corrected mid-task to
// eliminate: providers must keep passing spec.runtime.network's configured-
// or-default value byte-for-byte unchanged; only the engine (per resource,
// using metadata.domain) may turn that logical token into a concrete
// network/namespace name.
// The docs/adr/026 H7 additions (naming.PrivateNetworkName/EdgeNetworkName,
// graphaccess's H7-specific membership compiler, internal/domain/subnet.For)
// extend the SAME invariant to graph-scoped access: compiling a resource's
// membership set and turning it into a concrete per-owner/per-edge network
// name is exclusively the engine decorator's job
// (internal/application/engine/graphscoped.go), not a provider's — a
// provider that started calling these functions would be the identical
// class of coupling H5's own fix eliminated, one layer further in. This is
// deliberately narrower than "any graphaccess.* symbol": docs/adr/027's H6
// amendment already established a DIFFERENT, legitimate provider-side use
// of the graphaccess package (openziti calls
// DeriveEdges/CompileMediatedConnections itself to realize identity-aware
// edges) — only the H7-specific membership/grant/container-resolution
// surface is fenced here.
var domainScopedNetworkNaming = regexp.MustCompile(
	`naming\.NetworkName\(|naming\.PrivateNetworkName\(|naming\.EdgeNetworkName\(|\.Metadata\.Domain\b|resource\.NormalizeDomain\(|resource\.DefaultDomain\b|` +
		`graphaccess\.MembershipEdges\(|graphaccess\.EgressPeers\(|graphaccess\.IngressPeers\(|graphaccess\.ContainerOf\(|graphaccess\.ContainerDomain\(|graphaccess\.AccessGrant|` +
		`domain/subnet"`,
)

// providerScanDirs is internal/adapters/providers alone — the technology
// adapters this invariant is actually about. (loopback_test.go's scanDirs
// additionally covers internal/domain for a different rule; domain code
// legitimately defines metadata.domain and NormalizeDomain, so this test
// scans providers only.)
var providerScanDirs = []string{"../adapters/providers"}

// TestProvidersContainNoDomainScopedNetworkNaming pins docs/planning/08 H5's
// owner-directed correction: core-facility changes (network routing, access
// policy) must require zero provider changes. Every provider keeps calling
// its own network(cfg)/providerkit.Network(cfg) exactly as before domains
// existed — internal/application/engine's domainRuntime decorator is the
// ONE place a logical platform-network token becomes a domain-scoped
// concrete name, transparently, per resolveRequest call. If this test ever
// fails, a provider has started reading metadata.domain or calling
// naming.NetworkName/resource.NormalizeDomain itself — move that logic back
// into the engine's decorator instead.
func TestProvidersContainNoDomainScopedNetworkNaming(t *testing.T) {
	t.Parallel()
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var violations []string
	for _, dir := range providerScanDirs {
		abs := filepath.Join(root, dir)
		walkErr := filepath.WalkDir(abs, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			return scanFileForDomainNaming(path, &violations)
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("provider file(s) reference domain-scoped network-naming machinery — this belongs exclusively in internal/application/engine's domainRuntime decorator (docs/planning/08 H5's zero-provider-diff invariant):\n%s", strings.Join(violations, "\n"))
	}
}

func scanFileForDomainNaming(path string, violations *[]string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		if !domainScopedNetworkNaming.MatchString(line) {
			continue
		}
		*violations = append(*violations, path+":"+strconv.Itoa(lineNo)+": "+strings.TrimSpace(line))
	}
	return sc.Err()
}

// TestScanFileForDomainNamingDetectsViolation proves the detector itself
// works — a rule with no positive-case coverage can silently rot into a
// no-op (the same discipline loopback_test.go's own
// TestScanFileDetectsAndExemptsCorrectly holds itself to).
func TestScanFileForDomainNamingDetectsViolation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	content := `package fixture

import "github.com/rezarajan/platformctl/internal/domain/naming"

func network(cfg provider.Provider, env resource.Envelope) string {
	base := "datascape"
	return naming.NetworkName(base, env.Metadata.Domain)
}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var violations []string
	if err := scanFileForDomainNaming(path, &violations); err != nil {
		t.Fatal(err)
	}
	if len(violations) != 1 {
		t.Fatalf("got %d violations, want exactly 1 (both naming.NetworkName( and .Metadata.Domain on the same line, one violation per matching line): %v", len(violations), violations)
	}
}

// TestScanFileForDomainNamingDetectsGraphScopedViolation pins docs/adr/026
// H7's extension of the same invariant: a provider that imports graphaccess
// or calls the new per-owner/per-edge naming functions must be caught
// exactly like a pre-H7 domain-naming violation.
func TestScanFileForDomainNamingDetectsGraphScopedViolation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	content := `package fixture

import "github.com/rezarajan/platformctl/internal/application/graphaccess"

func peers(edges []graphaccess.Edge, self resource.Key, resources map[resource.Key]resource.Envelope) []resource.Key {
	return graphaccess.MembershipEdges(edges, self, resources)
}
`
	// (Deliberately calls the H7-specific MembershipEdges, not H6's
	// legitimate-for-providers CompileMediatedConnections, to exercise the
	// narrower fence this test pins.)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var violations []string
	if err := scanFileForDomainNaming(path, &violations); err != nil {
		t.Fatal(err)
	}
	if len(violations) == 0 {
		t.Fatal("expected at least one violation for a provider file importing graphaccess")
	}
}

// TestScanFileForDomainNamingClean proves an ordinary provider file — one
// that reads spec.runtime.network the pre-H5 way and never touches domains
// — passes cleanly, so this test can't have accidentally started flagging
// legitimate, unrelated code.
func TestScanFileForDomainNamingClean(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.go")
	content := `package fixture

func network(cfg provider.Provider) string {
	if n, ok := cfg.RuntimeConfig["network"].(string); ok && n != "" {
		return n
	}
	return "datascape"
}
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var violations []string
	if err := scanFileForDomainNaming(path, &violations); err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("got %d violations on an ordinary pre-H5 network(cfg) helper, want 0: %v", len(violations), violations)
	}
}
