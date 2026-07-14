package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/application/compatibility"
	"github.com/rezarajan/platformctl/internal/application/engine"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/manifest"
	planpkg "github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
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
		newGraphCmd(a),
	)
	return root
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
	return envelopes, g, nil
}

func (a *app) newEngine() *engine.Engine {
	return &engine.Engine{
		Registry:   a.reg,
		StateStore: localfile.New(a.stateFile),
		Clock:      clock.Real{},
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
			if !p.HasChanges() {
				fmt.Fprintln(cmd.OutOrStdout(), "no changes; nothing to apply")
				return nil
			}
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
			eng := a.newEngine()
			eng.HaltOnError = haltOnError
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
			result, err := a.newEngine().Destroy(cmd.Context(), p, envelopes)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "destroyed: %d succeeded, %d failed\n", len(result.Succeeded), len(result.Failed))
			return nil
		},
	}
	cmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "skip the interactive confirmation (for CI)")
	cmd.Flags().BoolVar(&includeExternal, "include-external", false, "also tear down External-lifecycle resources")
	cmd.Flags().BoolVar(&includeImported, "include-imported", false, "also tear down Imported-lifecycle resources")
	cmd.Flags().BoolVar(&destructiveOK, "yes-i-understand-this-is-destructive", false, "required with --include-external")
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
			rows := [][]string{{"RESOURCE", "READY", "REASON", "LIFECYCLE"}}
			type statusRow struct {
				Resource  string `json:"resource"`
				Ready     string `json:"ready"`
				Reason    string `json:"reason"`
				Lifecycle string `json:"lifecycle"`
			}
			var data []statusRow
			for _, e := range envelopes {
				key := e.Key()
				row := statusRow{Resource: key.String(), Ready: "Unknown", Lifecycle: resource.LifecycleOf(e, false).String()}
				if rs, ok := st.Resources[key]; ok {
					row.Lifecycle = rs.Lifecycle
					if c, found := rs.Status.Condition("Ready"); found {
						row.Ready = string(c.Status)
						row.Reason = c.Reason
					}
				} else {
					row.Reason = "NotApplied"
				}
				data = append(data, row)
				rows = append(rows, []string{row.Resource, row.Ready, row.Reason, row.Lifecycle})
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
