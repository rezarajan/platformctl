package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeManifestFiles writes each name->content pair into a fresh temp dir
// and returns the dir. Mirrors loadOne (schema_test.go) but for a full
// project: more than one file, one of which is datascape.yaml.
func writeManifestFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const projectDockerYAML = "apiVersion: datascape.io/v1alpha1\nkind: Project\nmetadata: {name: orders-platform}\n" +
	"spec:\n  runtime:\n    type: docker\n    network: orders-net\n"

// TestProjectResolvesProviderRuntime is the M1 accept criterion: "an
// example with no per-Provider runtime applies on the project runtime."
func TestProjectResolvesProviderRuntime(t *testing.T) {
	t.Parallel()
	dir := writeManifestFiles(t, map[string]string{
		ProjectFileName: projectDockerYAML,
		"providers.yaml": "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: pg}\n" +
			"spec:\n  type: noop\n---\n" +
			"apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: mq}\n" +
			"spec:\n  type: noop\n",
	})

	envelopes, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(envelopes) != 2 {
		t.Fatalf("got %d envelopes, want 2", len(envelopes))
	}
	for _, e := range envelopes {
		rt, ok := e.Spec["runtime"].(map[string]any)
		if !ok {
			t.Fatalf("Provider %q: spec.runtime not populated", e.Metadata.Name)
		}
		if rt["type"] != "docker" {
			t.Errorf("Provider %q: runtime.type = %v, want docker", e.Metadata.Name, rt["type"])
		}
		if rt["network"] != "orders-net" {
			t.Errorf("Provider %q: runtime.network = %v, want orders-net", e.Metadata.Name, rt["network"])
		}
	}

	// The two Providers must not alias the same underlying map: mutating
	// one's resolved runtime must never leak into the other's.
	rt0 := envelopes[0].Spec["runtime"].(map[string]any)
	rt1 := envelopes[1].Spec["runtime"].(map[string]any)
	rt0["network"] = "mutated"
	if rt1["network"] == "mutated" {
		t.Fatal("Providers alias the same inherited runtime map")
	}
}

// TestProjectPartialRuntimeOverrideInheritsType is the M7 portability fix:
// a Provider may override runtime FIELDS (resources) without a type, so it
// inherits the project's type/network and the same plane is portable across
// runtimes. The Provider's own fields win the merge; project fields it omits
// are inherited.
func TestProjectPartialRuntimeOverrideInheritsType(t *testing.T) {
	t.Parallel()
	dir := writeManifestFiles(t, map[string]string{
		ProjectFileName: projectDockerYAML,
		"providers.yaml": "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: heavy}\n" +
			"spec:\n  type: noop\n  runtime:\n    resources: {memory: 2Gi}\n",
	})

	envelopes, err := Load(dir)
	if err != nil {
		t.Fatalf("Load refused a type-less resources override: %v", err)
	}
	rt := envelopes[0].Spec["runtime"].(map[string]any)
	if rt["type"] != "docker" {
		t.Errorf("runtime.type = %v, want inherited docker", rt["type"])
	}
	if rt["network"] != "orders-net" {
		t.Errorf("runtime.network = %v, want inherited orders-net", rt["network"])
	}
	res, ok := rt["resources"].(map[string]any)
	if !ok || res["memory"] != "2Gi" {
		t.Errorf("runtime.resources = %v, want the Provider's own {memory: 2Gi}", rt["resources"])
	}
}

// TestProjectRefusesMismatchedOverride is the M1 accept criterion: "a
// mixed-runtime inventory is refused with a clear message."
func TestProjectRefusesMismatchedOverride(t *testing.T) {
	t.Parallel()
	dir := writeManifestFiles(t, map[string]string{
		ProjectFileName: projectDockerYAML,
		"providers.yaml": "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: strays}\n" +
			"spec:\n  type: noop\n  runtime: {type: kubernetes}\n",
	})

	_, err := Load(dir)
	if err == nil {
		t.Fatal("Load accepted a Provider overriding to a different runtime family")
	}
	for _, want := range []string{
		`Provider "strays"`,
		"declares runtime kubernetes",
		"project runtime is docker",
		"own project folder",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err.Error(), want)
		}
	}
}

// TestProjectAllowsMatchingOverride: an explicit spec.runtime whose type
// matches the project's is a legal override — kept verbatim, not merged
// with the project's other runtime fields.
func TestProjectAllowsMatchingOverride(t *testing.T) {
	t.Parallel()
	dir := writeManifestFiles(t, map[string]string{
		ProjectFileName: projectDockerYAML,
		"providers.yaml": "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: pinned}\n" +
			"spec:\n  type: noop\n  runtime: {type: docker, network: custom-net}\n",
	})

	envelopes, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	rt := envelopes[0].Spec["runtime"].(map[string]any)
	if rt["network"] != "custom-net" {
		t.Errorf("override's own network field was clobbered: got %v", rt["network"])
	}
}

// TestNoProjectFileBackwardCompat pins docs/planning/08 M1's backward-
// compat accept criterion: "existing per-Provider-runtime manifests still
// work (override)" — with no datascape.yaml present, behavior must be
// byte-identical to before M1: an explicit per-Provider runtime works,
// and a Provider omitting spec.runtime still fails exactly as today.
func TestNoProjectFileBackwardCompat(t *testing.T) {
	t.Parallel()

	t.Run("explicit runtime still works, no project file", func(t *testing.T) {
		t.Parallel()
		dir := writeManifestFiles(t, map[string]string{
			"providers.yaml": "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: pg}\n" +
				"spec:\n  type: noop\n  runtime: {type: docker}\n",
		})
		envelopes, err := Load(dir)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		rt := envelopes[0].Spec["runtime"].(map[string]any)
		if rt["type"] != "docker" {
			t.Errorf("runtime.type = %v, want docker", rt["type"])
		}
	})

	t.Run("mixed runtimes with no project file still work", func(t *testing.T) {
		// The exact shape examples/cdc-attendance/provider-lineage-fake.yaml
		// relies on (mixed docker + fake, no datascape.yaml), exercised
		// live by cmd/platformctl/acceptance_integration_test.go.
		t.Parallel()
		dir := writeManifestFiles(t, map[string]string{
			"providers.yaml": "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: real}\n" +
				"spec:\n  type: noop\n  runtime: {type: docker}\n---\n" +
				"apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: stand-in}\n" +
				"spec:\n  type: noop\n  runtime: {type: fake}\n",
		})
		if _, err := Load(dir); err != nil {
			t.Fatalf("Load refused a mixed-runtime inventory with no project file: %v", err)
		}
	})

	t.Run("provider without runtime still refused, no project file", func(t *testing.T) {
		t.Parallel()
		dir := writeManifestFiles(t, map[string]string{
			"providers.yaml": "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: p}\n" +
				"spec:\n  type: noop\n",
		})
		_, err := Load(dir)
		if err == nil {
			t.Fatal("Load accepted a Provider with no spec.runtime and no project file")
		}
		if !strings.Contains(err.Error(), `Provider "p": spec.runtime.type is required`) {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// TestLoadProjectAbsent pins LoadProject's own nil,nil contract.
func TestLoadProjectAbsent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	proj, err := LoadProject(dir)
	if err != nil {
		t.Fatalf("LoadProject: %v", err)
	}
	if proj != nil {
		t.Fatalf("expected nil Project, got %+v", proj)
	}
}

// TestLoadProjectSingleFilePath: path naming a single manifest file (not a
// directory) looks for datascape.yaml in its PARENT directory ("the
// manifest path's top level").
func TestLoadProjectSingleFilePath(t *testing.T) {
	t.Parallel()
	dir := writeManifestFiles(t, map[string]string{
		ProjectFileName: projectDockerYAML,
		"manifests.yaml": "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: p}\n" +
			"spec:\n  type: noop\n",
	})
	proj, err := LoadProject(filepath.Join(dir, "manifests.yaml"))
	if err != nil {
		t.Fatalf("LoadProject: %v", err)
	}
	if proj == nil || proj.Runtime.Type != "docker" {
		t.Fatalf("got %+v, want a docker Project", proj)
	}
}

// TestLoadProjectZeroTrustDefaultAndParse: M1 only needs to parse/store
// spec.zeroTrust (M4 consumes it later) — pin the default-true and the
// explicit-false parse.
func TestLoadProjectZeroTrustDefaultAndParse(t *testing.T) {
	t.Parallel()

	t.Run("defaults true when omitted", func(t *testing.T) {
		t.Parallel()
		dir := writeManifestFiles(t, map[string]string{ProjectFileName: projectDockerYAML})
		proj, err := LoadProject(dir)
		if err != nil {
			t.Fatalf("LoadProject: %v", err)
		}
		if !proj.ZeroTrust {
			t.Error("ZeroTrust should default to true")
		}
	})

	t.Run("explicit false is honored", func(t *testing.T) {
		t.Parallel()
		dir := writeManifestFiles(t, map[string]string{
			ProjectFileName: "apiVersion: datascape.io/v1alpha1\nkind: Project\nmetadata: {name: p}\n" +
				"spec:\n  runtime: {type: docker}\n  zeroTrust: false\n",
		})
		proj, err := LoadProject(dir)
		if err != nil {
			t.Fatalf("LoadProject: %v", err)
		}
		if proj.ZeroTrust {
			t.Error("ZeroTrust should be false when explicitly set")
		}
	})
}

// TestLoadProjectMalformed: the schema-level checks LoadProject inherits
// from validateAgainstSchema (schemas/v1alpha1/project.json).
func TestLoadProjectMalformed(t *testing.T) {
	t.Parallel()

	t.Run("missing runtime", func(t *testing.T) {
		t.Parallel()
		dir := writeManifestFiles(t, map[string]string{
			ProjectFileName: "apiVersion: datascape.io/v1alpha1\nkind: Project\nmetadata: {name: p}\nspec: {}\n",
		})
		if _, err := LoadProject(dir); err == nil {
			t.Fatal("expected a schema validation error for a Project with no spec.runtime")
		}
	})

	t.Run("wrong kind", func(t *testing.T) {
		t.Parallel()
		dir := writeManifestFiles(t, map[string]string{
			ProjectFileName: "apiVersion: datascape.io/v1alpha1\nkind: Provider\nmetadata: {name: p}\n" +
				"spec: {type: noop, runtime: {type: docker}}\n",
		})
		if _, err := LoadProject(dir); err == nil {
			t.Fatal("expected an error for datascape.yaml carrying the wrong kind")
		}
	})
}

// TestDatascapeYAMLExcludedFromManifestSet: datascape.yaml must never be
// treated as an ordinary governed-set document even when the directory
// has no other manifests to combine it with (regression for collectFiles'
// exclusion).
func TestDatascapeYAMLExcludedFromManifestSet(t *testing.T) {
	t.Parallel()
	dir := writeManifestFiles(t, map[string]string{ProjectFileName: projectDockerYAML})
	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), "no manifest files") {
		t.Fatalf("expected 'no manifest files', got: %v", err)
	}
}

// TestLoadIncludeMembers pins the Helm/Kustomize include-members model
// (docs/adr/035 / M7): a root datascape.yaml's spec.resources names members —
// files and/or directories — and a directory member composes recursively via
// its OWN datascape.yaml's spec.resources. Nothing is auto-discovered: a
// sibling policies/ directory (the --policies channel) is NEVER loaded as a
// manifest because it is not a named member, and declared order is preserved.
func TestLoadIncludeMembers(t *testing.T) {
	dir := t.TempDir()
	mustWrite := func(rel, body string) {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	res := func(name string) string {
		return "apiVersion: datascape.io/v1alpha1\nkind: SecretReference\nmetadata: {name: " + name + "}\nspec: {backend: env, keys: [k]}\n"
	}
	// Root includes: one directory member (platform/, composed via its own
	// datascape.yaml) and one direct file member (sources/db.yaml).
	mustWrite(ProjectFileName, "apiVersion: datascape.io/v1alpha1\nkind: Project\nmetadata: {name: p}\n"+
		"spec:\n  runtime: {type: docker}\n  resources: [platform, sources/db.yaml]\n")
	mustWrite("platform/"+ProjectFileName, "apiVersion: datascape.io/v1alpha1\nkind: Project\nmetadata: {name: platform}\n"+
		"spec:\n  resources: [secrets.yaml]\n")
	mustWrite("platform/secrets.yaml", res("a"))
	mustWrite("sources/db.yaml", res("b"))
	// A sibling policies/ dir that is NOT a declared member — must be ignored.
	mustWrite("policies/policy.yaml", "apiVersion: datascape.io/v1alpha1\nkind: Policy\nmetadata: {name: pol}\nspec: {}\n")

	envs, err := Load(dir)
	if err != nil {
		t.Fatalf("Load an include-members project: %v", err)
	}
	if len(envs) != 2 {
		t.Fatalf("loaded %d resources, want 2 (platform/secrets.yaml + sources/db.yaml; policies/ not a member)", len(envs))
	}
	for _, e := range envs {
		if e.Kind == "Policy" {
			t.Error("policies/ was loaded as a manifest — it is not a declared member (the --policies channel)")
		}
	}

	t.Run("directory member without its own datascape.yaml is refused", func(t *testing.T) {
		bad := t.TempDir()
		if err := os.WriteFile(filepath.Join(bad, ProjectFileName),
			[]byte("apiVersion: datascape.io/v1alpha1\nkind: Project\nmetadata: {name: p}\nspec:\n  runtime: {type: docker}\n  resources: [cdc]\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(bad, "cdc"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bad, "cdc", "x.yaml"), []byte(res("x")), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := Load(bad)
		if err == nil || !strings.Contains(err.Error(), "has no "+ProjectFileName) {
			t.Fatalf("expected a missing-include-file error, got: %v", err)
		}
	})

	t.Run("included member declaring its own runtime is refused", func(t *testing.T) {
		bad := t.TempDir()
		if err := os.WriteFile(filepath.Join(bad, ProjectFileName),
			[]byte("apiVersion: datascape.io/v1alpha1\nkind: Project\nmetadata: {name: p}\nspec:\n  runtime: {type: docker}\n  resources: [plane]\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(bad, "plane"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bad, "plane", ProjectFileName),
			[]byte("apiVersion: datascape.io/v1alpha1\nkind: Project\nmetadata: {name: plane}\nspec:\n  runtime: {type: fake}\n  resources: [y.yaml]\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bad, "plane", "y.yaml"), []byte(res("y")), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := Load(bad)
		if err == nil || !strings.Contains(err.Error(), "must not declare spec.runtime") {
			t.Fatalf("expected a runtime-override refusal, got: %v", err)
		}
	})
}
