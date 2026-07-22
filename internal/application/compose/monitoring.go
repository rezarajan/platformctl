package compose

import "fmt"

// MonitoringOptions is `platformctl add monitoring`'s flag-mode input: a
// standalone managed Prometheus. No attachment points — scrape targets are
// auto-discovered by the engine at apply time from every currently-
// published metrics endpoint (internal/application/engine.
// resolveMetricsTargets), not manifest-declared, so there is nothing for
// this composite to wire.
type MonitoringOptions struct {
	Name string // required: names the Provider
}

// PlanMonitoring computes the manifest patch for `add monitoring`.
func PlanMonitoring(snap Snapshot, dir string, opts MonitoringOptions) (Patch, error) {
	const command = "add monitoring"
	if opts.Name == "" {
		return Patch{}, fmt.Errorf("--name is required")
	}
	op, err := resolveFile(dir, snap, "Provider", opts.Name, renderMonitoringProvider(command, opts.Name))
	if err != nil {
		return Patch{}, err
	}
	return Patch{Command: command, Dir: dir, Files: []FileOp{op}}, nil
}

func renderMonitoringProvider(command, name string) string {
	explain := "Managed Prometheus. Scrape targets are auto-discovered at apply time\nfrom every currently-published metrics endpoint — nothing to wire here."
	lines := []string{"type: prometheus", "runtime:", "  type: docker"}
	return renderDoc(command, explain, "Provider", name, lines)
}
