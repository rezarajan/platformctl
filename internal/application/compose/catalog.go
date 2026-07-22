package compose

import "fmt"

// CatalogOptions is `platformctl add catalog`'s flag-mode input. v1 ships
// exactly one CatalogCapableProvider (nessie), so Engine is always
// "nessie" today; the field exists for forward compatibility rather than
// present-day choice.
type CatalogOptions struct {
	Name         string    // required: names the Catalog resource
	Engine       string    // default: "nessie" (the only shipped CatalogCapableProvider)
	Provider     RefChoice // default: new
	ProviderName string    // used when Provider.New; default "<Name>-provider"
}

// PlanCatalog computes the manifest patch for `add catalog`.
func PlanCatalog(snap Snapshot, dir string, opts CatalogOptions) (Patch, error) {
	const command = "add catalog"
	if opts.Name == "" {
		return Patch{}, fmt.Errorf("--name is required")
	}
	engine := opts.Engine
	if engine == "" {
		engine = "nessie"
	}
	if engine != "nessie" {
		return Patch{}, fmt.Errorf("engine %q is not a composable catalog engine (known: nessie)", engine)
	}

	patch := Patch{Command: command, Dir: dir}
	var providerName string
	if opts.Provider.New {
		providerName = opts.ProviderName
		if providerName == "" {
			providerName = opts.Name + "-provider"
		}
		op, err := resolveFile(dir, snap, "Provider", providerName, renderCatalogProvider(command, providerName))
		if err != nil {
			return Patch{}, err
		}
		patch.Files = append(patch.Files, op)
		patch.Notes = append(patch.Notes, fmt.Sprintf("creating new nessie Provider %q", providerName))
	} else {
		if opts.Provider.Name == "" {
			return Patch{}, fmt.Errorf("--provider requires \"new\" or \"existing:<name>\"")
		}
		if !hasCandidate(snap.ProviderCandidates("nessie"), opts.Provider.Name) {
			return Patch{}, fmt.Errorf("--provider existing:%s: no such nessie Provider (candidates: %s)", opts.Provider.Name, candidateNames(snap.ProviderCandidates("nessie")))
		}
		providerName = opts.Provider.Name
		patch.Notes = append(patch.Notes, fmt.Sprintf("reusing nessie Provider %q", providerName))
	}

	catalogOp, err := resolveFile(dir, snap, "Catalog", opts.Name, renderCatalog(command, opts.Name, providerName))
	if err != nil {
		return Patch{}, err
	}
	patch.Files = append(patch.Files, catalogOp)
	return patch, nil
}

func renderCatalogProvider(command, name string) string {
	explain := "Managed Nessie — the Iceberg REST catalog realizing the Catalog below."
	lines := []string{"type: nessie", "runtime:", "  type: docker"}
	return renderDoc(command, explain, "Provider", name, lines)
}

func renderCatalog(command, name, providerName string) string {
	lines := []string{"engine: nessie"}
	lines = append(lines, refBlock("providerRef", providerName)...)
	return renderDoc(command, "", "Catalog", name, lines)
}
