package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	applint "github.com/rezarajan/platformctl/internal/application/lint"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// runLint computes design-lint findings for envelopes/g (docs/adr/020):
// best-effort state — a missing/unreadable state file never fails lint
// (DL021 simply doesn't fire, exactly as if no state existed yet), matching
// the one-line validate summary's need to never turn a clean validate into
// a hard failure just because lint's plan-aware code couldn't load state.
func (a *app) runLint(ctx context.Context, envelopes []resource.Envelope, g *graph.Graph) ([]lint.Finding, error) {
	var st *state.State
	if store, err := a.stateStore(); err == nil {
		if loaded, err := store.Load(ctx); err == nil {
			st = &loaded
		}
	}
	opts := applint.Options{HighAvailabilityEnabled: a.gates.Enabled("HighAvailability"), State: st}
	return applint.Run(envelopes, g, a.reg.Provider, opts)
}

func newLintCmd(a *app) *cobra.Command {
	var strict bool
	cmd := &cobra.Command{
		Use:   "lint [path]",
		Short: "Report design-quality findings (ADR 020): detects, never blocks by default",
		Long: "Runs the built-in design-lint set plus every provider-contributed lint\n" +
			"(reconciler.DesignLinter) against a validated manifest set: duplicate CDC\n" +
			"capture, sink collisions, unreferenced/orphaned resources, missing\n" +
			"deletionPolicy/protect, and technology-specific hazards. Findings are pure —\n" +
			"no live infrastructure is touched — and never block by default; --strict\n" +
			"exits nonzero when any unwaived warning-severity finding exists. Waive a\n" +
			"finding with metadata.annotations[\"lint.datascape.io/waive\"]: \"<code>:" +
			" <reason>\" (reason mandatory) — waived findings still print, marked waived.\n" +
			"Run `platformctl explain <code>` for any finding's meaning and remedies.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envelopes, g, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			findings, err := a.runLint(cmd.Context(), envelopes, g)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if err := printLintFindings(cmd, a.output, findings); err != nil {
				return err
			}
			if strict && hasUnwaivedWarning(findings) {
				return cliutil.Exit(cliutil.ExitPlanChanges, nil)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&strict, "strict", false, "exit nonzero if any unwaived warning-severity finding exists")
	return cmd
}

func hasUnwaivedWarning(findings []lint.Finding) bool {
	for _, f := range findings {
		if f.Severity == lint.Warning && !f.Waived {
			return true
		}
	}
	return false
}

// lintFindingOutput mirrors lint.Finding for -o json|yaml — Resource is
// rendered as its string form ("namespace/Kind/name") rather than the
// struct, matching every other command's resource-key rendering
// (planpkg.Entry, driftRow, ...).
type lintFindingOutput struct {
	Code         string `json:"code" yaml:"code"`
	Severity     string `json:"severity" yaml:"severity"`
	Resource     string `json:"resource" yaml:"resource"`
	Message      string `json:"message" yaml:"message"`
	Waived       bool   `json:"waived" yaml:"waived"`
	WaiverReason string `json:"waiverReason,omitempty" yaml:"waiverReason,omitempty"`
}

type lintOutput struct {
	Findings []lintFindingOutput `json:"findings" yaml:"findings"`
}

func printLintFindings(cmd *cobra.Command, output string, findings []lint.Finding) error {
	data := make([]lintFindingOutput, len(findings))
	rows := [][]string{{"SEVERITY", "CODE", "RESOURCE", "WAIVED", "MESSAGE"}}
	for i, f := range findings {
		data[i] = lintFindingOutput{
			Code: f.Code, Severity: string(f.Severity), Resource: f.Resource.String(),
			Message: f.Message, Waived: f.Waived, WaiverReason: f.WaiverReason,
		}
		waived := "-"
		if f.Waived {
			waived = "yes"
		}
		rows = append(rows, []string{string(f.Severity), f.Code, f.Resource.String(), waived, f.Message})
	}
	if isStructured(output) {
		return cliutil.WriteOutput(cmd.OutOrStdout(), output, lintOutput{Findings: data}, nil)
	}
	if len(findings) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "no design findings")
		return nil
	}
	return cliutil.WriteOutput(cmd.OutOrStdout(), output, data, rows)
}
