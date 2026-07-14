package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	envsecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/env"
	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/application/compatibility"
	"github.com/rezarajan/platformctl/internal/application/docsgen"
	"github.com/rezarajan/platformctl/internal/application/engine"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/manifest"
	planpkg "github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/clock"
)

type wiringFunc func(*featuregate.Registry) *registry.Registry

type app struct {
	stateFile    string
	featureGates string
	output       string
	wire         wiringFunc

	gates *featuregate.Registry
	reg   *registry.Registry
}

func (a *app) init() error {
	a.gates = featuregate.NewRegistry()
	a.reg = a.wire(a.gates)
	return a.gates.Apply(a.featureGates)
}

func newRootCmd(wire wiringFunc) *cobra.Command {
	a := &app{wire: wire}

	root := &cobra.Command{
		Use:           "platformctl",
		Short:         "Datascape: declarative data infrastructure on container runtimes",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			return a.init()
		},
	}
	root.PersistentFlags().StringVar(&a.stateFile, "state-file", ".datascape/state.json", "path to the state file")
	root.PersistentFlags().StringVar(&a.featureGates, "feature-gates", "", "comma-separated Name=true|false overrides")
	root.PersistentFlags().StringVarP(&a.output, "output", "o", "table", "output format: table|json|yaml")

	root.AddCommand(
		newValidateCmd(a),
		newPlanCmd(a),
		newApplyCmd(a),
		newDestroyCmd(a),
		newStatusCmd(a),
		newDriftCmd(a),
		newImportCmd(a),
		newGraphCmd(a),
		newDocsCmd(),
	)
	return root
}

func newDocsCmd() *cobra.Command {
	docs := &cobra.Command{
		Use:   "docs",
		Short: "Generate and serve the resource reference from schemas/",
	}
	var outDir string
	build := &cobra.Command{
		Use:   "build",
		Short: "Render the reference (one markdown file per Kind + index) from the embedded schemas",
		RunE: func(cmd *cobra.Command, _ []string) error {
			pages, err := docsgen.Build()
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			for name, content := range pages {
				if err := os.WriteFile(filepath.Join(outDir, name), []byte(content), 0o644); err != nil {
					return cliutil.Exit(cliutil.ExitExecution, err)
				}
			}
			fmt.Fprintf(cmd.OutOrStdout(), "wrote %d page(s) to %s\n", len(pages), outDir)
			return nil
		},
	}
	build.Flags().StringVar(&outDir, "out", "docs/reference", "output directory")

	var addr string
	serve := &cobra.Command{
		Use:   "serve",
		Short: "Serve the generated reference over HTTP (markdown rendered as plain text)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			pages, err := docsgen.Build()
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				name := strings.TrimPrefix(r.URL.Path, "/")
				if name == "" {
					name = "index.md"
				}
				content, ok := pages[name]
				if !ok {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
				fmt.Fprint(w, content)
			})
			fmt.Fprintf(cmd.OutOrStdout(), "serving resource reference on http://%s (index.md, provider.md, ...)\n", addr)
			return http.ListenAndServe(addr, mux) //nolint:gosec
		},
	}
	serve.Flags().StringVar(&addr, "addr", "127.0.0.1:8180", "listen address")

	docs.AddCommand(build, serve)
	return docs
}

// checkExternalGate rejects manifest sets that declare External resources
// while the ExternalResourceConfiguration gate is off.
func (a *app) checkExternalGate(envelopes []resource.Envelope) error {
	for _, e := range envelopes {
		if ext, _ := e.Spec["external"].(bool); ext {
			if err := a.gates.Require("ExternalResourceConfiguration"); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("%s declares spec.external: %w", e.Key(), err))
			}
		}
	}
	return nil
}

// loadAndValidate runs the full validate pipeline: manifests → kind
// validation → graph (cycles) → compatibility. Returns envelopes + graph.
func (a *app) loadAndValidate(path string) ([]resource.Envelope, *graph.Graph, error) {
	envelopes, err := manifest.Load(path)
	if err != nil {
		return nil, nil, cliutil.Exit(cliutil.ExitValidation, err)
	}
	g, err := graph.Build(envelopes)
	if err != nil {
		return nil, nil, cliutil.Exit(cliutil.ExitValidation, err)
	}
	if err := compatibility.Check(envelopes, a.reg.Provider); err != nil {
		return nil, nil, cliutil.Exit(cliutil.ExitValidation, err)
	}
	if err := a.checkExternalGate(envelopes); err != nil {
		return nil, nil, err
	}
	return envelopes, g, nil
}

func (a *app) newEngine() *engine.Engine {
	return &engine.Engine{
		Registry:    a.reg,
		StateStore:  localfile.New(a.stateFile),
		SecretStore: envsecrets.New(),
		Clock:       clock.Real{},
		Log: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, format+"\n", args...)
		},
	}
}

func newValidateCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "validate [path]",
		Short: "Schema + graph + compatibility validation only; no state, no runtime calls",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envelopes, _, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%d resource(s) valid\n", len(envelopes))
			return nil
		},
	}
}

func newPlanCmd(a *app) *cobra.Command {
	var detectDriftOnly bool
	cmd := &cobra.Command{
		Use:   "plan [path]",
		Short: "Compute and print the plan; never mutates",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envelopes, g, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			store := localfile.New(a.stateFile)
			st, err := store.Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			p, err := planpkg.Compute(envelopes, st, g)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if err := printPlan(cmd, a.output, p); err != nil {
				return err
			}
			if p.HasChanges() && !detectDriftOnly {
				return cliutil.Exit(cliutil.ExitPlanChanges, nil)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&detectDriftOnly, "detect-drift-only", false, "exit 0 even when the plan has changes")
	return cmd
}

func newApplyCmd(a *app) *cobra.Command {
	var autoApprove bool
	var haltOnError bool
	cmd := &cobra.Command{
		Use:   "apply [path]",
		Short: "Compute the plan, then execute it",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envelopes, g, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			store := localfile.New(a.stateFile)
			unlock, err := store.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			st, err := store.Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			p, err := planpkg.Compute(envelopes, st, g)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			healDrift := a.gates.Enabled("DriftDetection")
			if !p.HasChanges() {
				if !healDrift {
					fmt.Fprintln(cmd.OutOrStdout(), "no changes; nothing to apply")
					return nil
				}
				// Healing only re-converges resources to the spec already in
				// state, so no approval prompt: nothing new is being applied.
				fmt.Fprintln(cmd.OutOrStdout(), "no changes; probing for drift")
			} else {
				if err := printPlan(cmd, a.output, p); err != nil {
					return err
				}
				if !autoApprove {
					fmt.Fprint(cmd.OutOrStdout(), "\nApply these changes? Only 'yes' is accepted: ")
					var answer string
					fmt.Fscanln(cmd.InOrStdin(), &answer) //nolint:errcheck
					if answer != "yes" {
						fmt.Fprintln(cmd.OutOrStdout(), "apply cancelled")
						return nil
					}
				}
			}
			eng := a.newEngine()
			eng.HaltOnError = haltOnError
			eng.HealDrift = healDrift
			result, err := eng.Apply(cmd.Context(), p, envelopes, g)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "applied: %d succeeded, %d failed, %d skipped\n",
				len(result.Succeeded), len(result.Failed), len(result.Skipped))
			return nil
		},
	}
	cmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "skip the interactive confirmation (for CI)")
	cmd.Flags().BoolVar(&haltOnError, "halt-on-error", false, "stop the whole apply on the first failure")
	return cmd
}

func newDestroyCmd(a *app) *cobra.Command {
	var autoApprove, includeExternal, includeImported, destructiveOK bool
	cmd := &cobra.Command{
		Use:   "destroy [path]",
		Short: "Plan and execute teardown of managed resources",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if includeExternal && !destructiveOK {
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("--include-external additionally requires --yes-i-understand-this-is-destructive"))
			}
			envelopes, g, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			store := localfile.New(a.stateFile)
			unlock, err := store.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			st, err := store.Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			p, err := planpkg.ComputeDestroy(envelopes, st, g, includeExternal, includeImported)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if err := printPlan(cmd, a.output, p); err != nil {
				return err
			}
			if !p.HasChanges() {
				fmt.Fprintln(cmd.OutOrStdout(), "nothing to destroy")
				return nil
			}
			if !autoApprove {
				fmt.Fprint(cmd.OutOrStdout(), "\nDestroy these resources? Only 'yes' is accepted: ")
				var answer string
				fmt.Fscanln(cmd.InOrStdin(), &answer) //nolint:errcheck
				if answer != "yes" {
					fmt.Fprintln(cmd.OutOrStdout(), "destroy cancelled")
					return nil
				}
			}
			eng := a.newEngine()
			eng.AllowDestructive = includeExternal && destructiveOK
			result, err := eng.Destroy(cmd.Context(), p, envelopes, g)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "destroyed: %d succeeded, %d failed, %d skipped\n", len(result.Succeeded), len(result.Failed), len(result.Skipped))
			return nil
		},
	}
	cmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "skip the interactive confirmation (for CI)")
	cmd.Flags().BoolVar(&includeExternal, "include-external", false, "also tear down External-lifecycle resources")
	cmd.Flags().BoolVar(&includeImported, "include-imported", false, "also tear down Imported-lifecycle resources")
	cmd.Flags().BoolVar(&destructiveOK, "yes-i-understand-this-is-destructive", false, "required with --include-external")
	return cmd
}

func newDriftCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "drift [path]",
		Short: "Probe live infrastructure and report drift against recorded state",
		Long: "Probes every applied resource against the actual runtime (containers, topics,\n" +
			"connectors, buckets) and records the observed Ready/DriftDetected conditions\n" +
			"into state, so a subsequent `status` reflects reality. Never mutates\n" +
			"infrastructure; run `apply` to heal reported drift. Exits 1 when drift is found.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := a.gates.Require("DriftDetection"); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			envelopes, _, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			store := localfile.New(a.stateFile)
			unlock, err := store.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			results, err := a.newEngine().Probe(cmd.Context(), envelopes)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}

			rows := [][]string{{"RESOURCE", "READY", "DRIFT", "REASON"}}
			type driftRow struct {
				Resource string `json:"resource"`
				Ready    string `json:"ready"`
				Drift    string `json:"drift"`
				Reason   string `json:"reason"`
			}
			var data []driftRow
			drifted := 0
			for _, r := range results {
				row := driftRow{Resource: r.Key.String(), Ready: "Unknown", Drift: "Unknown"}
				if c, ok := r.Status.Condition(status.Ready); ok {
					row.Ready = string(c.Status)
				}
				if c, ok := r.Status.Condition(status.DriftDetected); ok {
					row.Drift = string(c.Status)
					row.Reason = c.Reason
				}
				if engine.HasDrift(r.Status) {
					drifted++
				}
				data = append(data, row)
				rows = append(rows, []string{row.Resource, row.Ready, row.Drift, row.Reason})
			}
			if err := cliutil.WriteOutput(cmd.OutOrStdout(), a.output, data, rows); err != nil {
				return err
			}
			if drifted > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "\ndrift detected on %d resource(s); run apply to reconcile\n", drifted)
				return cliutil.Exit(cliutil.ExitPlanChanges, nil)
			}
			fmt.Fprintln(cmd.OutOrStdout(), "\nno drift detected")
			return nil
		},
	}
}

func newImportCmd(a *app) *cobra.Command {
	var from string
	cmd := &cobra.Command{
		Use:   "import <Kind>/<name> [path]",
		Short: "Adopt an existing, out-of-band-created resource into state as Imported",
		Long: "Probes the pre-existing backing object (never creating anything) and records\n" +
			"the resource in state as Imported with the manifest's current spec hash — a\n" +
			"subsequent apply plans a no-op, not a create. v1 adopts by name: --from must\n" +
			"equal metadata.name, the name providers derive backing objects from.",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := a.gates.Require("ImportedResources"); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			kind, name, ok := strings.Cut(args[0], "/")
			if !ok || kind == "" || name == "" {
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("first argument must be <Kind>/<name>, got %q", args[0]))
			}
			envelopes, _, err := a.loadAndValidate(pathArg(args[1:]))
			if err != nil {
				return err
			}
			store := localfile.New(a.stateFile)
			unlock, err := store.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			key := resource.Key{Kind: kind, Name: name}
			probed, err := a.newEngine().Import(cmd.Context(), envelopes, key, from)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			reason := ""
			if c, ok := probed.Condition(status.Ready); ok {
				reason = c.Reason
			}
			fmt.Fprintf(cmd.OutOrStdout(), "imported %s (Ready: %s)\n", key, reason)
			return nil
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "name of the existing backing object to adopt (must equal metadata.name in v1)")
	_ = cmd.MarkFlagRequired("from")
	return cmd
}

func newStatusCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "status [path]",
		Short: "Print conditions per resource, rolled up to overall health",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envelopes, _, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			st, err := localfile.New(a.stateFile).Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			rows := [][]string{{"RESOURCE", "READY", "DRIFT", "REASON", "LIFECYCLE"}}
			type statusRow struct {
				Resource  string `json:"resource"`
				Ready     string `json:"ready"`
				Drift     string `json:"drift"`
				Reason    string `json:"reason"`
				Lifecycle string `json:"lifecycle"`
			}
			var data []statusRow
			for _, e := range envelopes {
				key := e.Key()
				row := statusRow{Resource: key.String(), Ready: "Unknown", Drift: "-", Lifecycle: resource.LifecycleOf(e, false).String()}
				if rs, ok := st.Resources[key]; ok {
					row.Lifecycle = rs.Lifecycle
					if c, found := rs.Status.Condition("Ready"); found {
						row.Ready = string(c.Status)
						row.Reason = c.Reason
					}
					// Recorded by the last `drift` probe; "-" means never probed.
					if c, found := rs.Status.Condition(status.DriftDetected); found {
						row.Drift = string(c.Status)
					}
				} else {
					row.Reason = "NotApplied"
				}
				data = append(data, row)
				rows = append(rows, []string{row.Resource, row.Ready, row.Drift, row.Reason, row.Lifecycle})
			}
			return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, data, rows)
		},
	}
}

func newGraphCmd(a *app) *cobra.Command {
	var format string
	cmd := &cobra.Command{
		Use:   "graph [path]",
		Short: "Print the dependency graph",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, g, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			keys := make([]resource.Key, 0, len(g.Nodes))
			for k := range g.Nodes {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
			switch format {
			case "dot":
				fmt.Fprintln(out, "digraph datascape {")
				for _, from := range keys {
					for _, to := range g.Edges[from] {
						fmt.Fprintf(out, "  %q -> %q;\n", from, to)
					}
				}
				fmt.Fprintln(out, "}")
			case "mermaid":
				fmt.Fprintln(out, "flowchart TD")
				for _, from := range keys {
					for _, to := range g.Edges[from] {
						fmt.Fprintf(out, "  %s --> %s\n", mermaidID(from), mermaidID(to))
					}
				}
			default:
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("unknown graph format %q (allowed: dot, mermaid)", format))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&format, "format", "dot", "graph output format: dot|mermaid")
	return cmd
}

func printPlan(cmd *cobra.Command, output string, p planpkg.Plan) error {
	rows := [][]string{{"RESOURCE", "ACTION", "REASON"}}
	for _, e := range p.Entries {
		rows = append(rows, []string{e.Key.String(), string(e.Action), e.Reason})
	}
	return cliutil.WriteOutput(cmd.OutOrStdout(), output, p, rows)
}

func mermaidID(k resource.Key) string {
	return strings.NewReplacer("/", "_", "-", "_").Replace(k.String())
}

func pathArg(args []string) string {
	if len(args) > 0 {
		return args[0]
	}
	return "."
}
