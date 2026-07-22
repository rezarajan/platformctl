package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	applint "github.com/rezarajan/platformctl/internal/application/lint"
	"github.com/rezarajan/platformctl/internal/application/manifest"
	planpkg "github.com/rezarajan/platformctl/internal/application/plan"
	apppolicy "github.com/rezarajan/platformctl/internal/application/policy"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	domainpolicy "github.com/rezarajan/platformctl/internal/domain/policy"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// conventionalPoliciesDir is the ADR 021 §1 default location a policy
// directory is discovered at when --policies is not given.
const conventionalPoliciesDir = ".datascape/policies"

// policiesDir resolves which directory of Policy documents (if any) to
// load: the explicit --policies flag first, else the conventional
// .datascape/policies/ directory if it exists, else "" (no policies).
func (a *app) policiesDir() string {
	if a.policies != "" {
		return a.policies
	}
	if info, err := os.Stat(conventionalPoliciesDir); err == nil && info.IsDir() {
		return conventionalPoliciesDir
	}
	return ""
}

// evaluatePolicies is the match+assert/matchFinding half of ADR 021's
// evaluator, gated behind PolicyEngine (Alpha, disabled by default):
// disabled means a full no-op — (nil, nil), no directory stat, no file
// read, no evaluation at all (docs/planning/08 H3's "gate-disabled = no
// evaluation with clear off-switch semantics"). Lint findings are computed
// best-effort (mirroring runLint's own doc comment: a missing/unreadable
// state file never fails this) with context.Background() rather than a
// caller-supplied context, since loadAndValidate — this function's primary
// caller — has no context of its own to thread through without widening
// every one of its 13 call sites' signatures for a best-effort side input.
func (a *app) evaluatePolicies(envelopes []resource.Envelope, g *graph.Graph) ([]apppolicy.Decision, error) {
	if !a.gates.Enabled("PolicyEngine") {
		return nil, nil
	}
	dir := a.policiesDir()
	if dir == "" {
		return nil, nil
	}
	policies, err := apppolicy.LoadDir(dir)
	if err != nil {
		return nil, cliutil.Exit(cliutil.ExitValidation, err)
	}
	if len(policies) == 0 {
		return nil, nil
	}
	findings, _ := a.runLint(context.Background(), envelopes, g)
	decisions, err := apppolicy.Run(policies, envelopes, g, findings)
	if err != nil {
		return nil, cliutil.Exit(cliutil.ExitExecution, err)
	}
	return decisions, nil
}

// enforcePolicies is evaluatePolicies plus ADR 021 §3's deny-wins blocking
// behavior — wired into loadAndValidate after compatibility + lint: a
// non-exempted deny fails via the standard validation-error exit path,
// naming every offending rule id, message, and resource.
func (a *app) enforcePolicies(envelopes []resource.Envelope, g *graph.Graph) error {
	decisions, err := a.evaluatePolicies(envelopes, g)
	if err != nil {
		return err
	}
	return denyError(decisions)
}

// enforcePlanPolicies is the matchPlan half: evaluated once a plan actually
// exists (plan/apply/destroy only — matchPlan rules never fire from
// loadAndValidate/evaluatePolicies, since no plan exists yet at validate
// time). Same off-switch and directory-resolution rules as
// evaluatePolicies.
func (a *app) enforcePlanPolicies(envelopes []resource.Envelope, entries []planpkg.Entry) error {
	if !a.gates.Enabled("PolicyEngine") {
		return nil
	}
	dir := a.policiesDir()
	if dir == "" {
		return nil
	}
	policies, err := apppolicy.LoadDir(dir)
	if err != nil {
		return cliutil.Exit(cliutil.ExitValidation, err)
	}
	if len(policies) == 0 {
		return nil
	}
	decisions, err := apppolicy.RunPlan(policies, envelopes, entries)
	if err != nil {
		return cliutil.Exit(cliutil.ExitExecution, err)
	}
	return denyError(decisions)
}

// denyError builds the standard validation-error exit path (ADR 021 §3) out
// of every non-exempted deny decision, naming each rule id, message, and
// resource; nil when there are none (warn decisions and exempted denies
// never fail a command — they are reported, not blocked).
func denyError(decisions []apppolicy.Decision) error {
	var denied []string
	for _, d := range decisions {
		if d.Effect != domainpolicy.Deny || d.Exempted {
			continue
		}
		denied = append(denied, fmt.Sprintf("%s (rule %s): %s", d.Resource, d.RuleID, d.Message))
	}
	if len(denied) == 0 {
		return nil
	}
	return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("denied by policy:\n  %s", strings.Join(denied, "\n  ")))
}

func newPolicyCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "Author and test typed governance policies (docs/adr/021)",
	}
	cmd.AddCommand(newPolicyTestCmd(a), newPolicyInitCmd(a))
	return cmd
}

// policyDecisionOutput mirrors lintFindingOutput's shape for -o json|yaml —
// Resource rendered as its string form, matching every other command's
// resource-key rendering.
type policyDecisionOutput struct {
	RuleID       string `json:"ruleId" yaml:"ruleId"`
	Effect       string `json:"effect" yaml:"effect"`
	Resource     string `json:"resource" yaml:"resource"`
	Message      string `json:"message" yaml:"message"`
	Exempted     bool   `json:"exempted" yaml:"exempted"`
	ExemptReason string `json:"exemptReason,omitempty" yaml:"exemptReason,omitempty"`
}

type policyTestOutput struct {
	Decisions []policyDecisionOutput `json:"decisions" yaml:"decisions"`
}

func printPolicyDecisions(cmd *cobra.Command, output string, decisions []apppolicy.Decision) error {
	data := make([]policyDecisionOutput, len(decisions))
	rows := [][]string{{"EFFECT", "RULE", "RESOURCE", "EXEMPTED", "MESSAGE"}}
	for i, d := range decisions {
		data[i] = policyDecisionOutput{
			RuleID: d.RuleID, Effect: string(d.Effect), Resource: d.Resource.String(),
			Message: d.Message, Exempted: d.Exempted, ExemptReason: d.ExemptReason,
		}
		exempted := "-"
		if d.Exempted {
			exempted = "yes"
		}
		rows = append(rows, []string{string(d.Effect), d.RuleID, d.Resource.String(), exempted, d.Message})
	}
	if isStructured(output) {
		return cliutil.WriteOutput(cmd.OutOrStdout(), output, policyTestOutput{Decisions: data}, nil)
	}
	if len(decisions) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "no policy decisions")
		return nil
	}
	return cliutil.WriteOutput(cmd.OutOrStdout(), output, data, rows)
}

func newPolicyTestCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test [path]",
		Short: "Evaluate policies against a manifest set without running the rest of validate",
		Long: "Loads the manifest set at path (schema + graph only — no compatibility\n" +
			"checks, no gate checks, no kubernetes preflight) and every design lint,\n" +
			"then evaluates every loaded policy (--policies, or the conventional\n" +
			".datascape/policies/ directory) against it: a fast, CI-friendly authoring\n" +
			"loop for policy files themselves (docs/adr/021 §3). Requires the\n" +
			"PolicyEngine feature gate. Exits nonzero when any unexempted deny-effect\n" +
			"decision fires — the same rule id/message/resource shape `validate`\n" +
			"itself would deny with.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := a.gates.Require("PolicyEngine"); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			envelopes, err := manifest.Load(pathArg(args))
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			g, err := graph.Build(envelopes)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			dir := a.policiesDir()
			if dir == "" {
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("no policies to test: pass --policies <dir> or create %s/", conventionalPoliciesDir))
			}
			policies, err := apppolicy.LoadDir(dir)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			findings, err := applint.Run(envelopes, g, a.reg.Provider, applint.Options{HighAvailabilityEnabled: a.gates.Enabled("HighAvailability")})
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			decisions, err := apppolicy.Run(policies, envelopes, g, findings)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if err := printPolicyDecisions(cmd, a.output, decisions); err != nil {
				return err
			}
			return denyError(decisions)
		},
	}
	return cmd
}

type policyInitOutput struct {
	Pack  string   `json:"pack" yaml:"pack"`
	Dir   string   `json:"dir" yaml:"dir"`
	Files []string `json:"files" yaml:"files"`
}

func newPolicyInitCmd(a *app) *cobra.Command {
	var dir string
	var force bool
	cmd := &cobra.Command{
		Use:   "init <pack>",
		Short: "Write a built-in policy pack for local tailoring (blueprint pattern; known: " + strings.Join(apppolicy.PackNames(), ", ") + ")",
		Long: "Writes the named built-in policy pack (docs/adr/021 §4) to --dir (default:\n" +
			conventionalPoliciesDir + "/) for local tailoring — every rule cites the\n" +
			"mechanism ADR/doc it enforces. Enable evaluation with --feature-gates\n" +
			"PolicyEngine=true (Alpha, disabled by default); review and tailor the\n" +
			"written rules (especially any REPLACE_ME placeholder) before enabling in\n" +
			"CI. Run `platformctl policy test` to preview what the pack would deny.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			targetDir := dir
			if targetDir == "" {
				targetDir = conventionalPoliciesDir
			}
			files, err := apppolicy.WritePack(args[0], targetDir, force)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			if isStructured(a.output) {
				return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, policyInitOutput{Pack: args[0], Dir: targetDir, Files: files}, nil)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "wrote policy pack %q to %s (%d file(s))\n", args[0], targetDir, len(files))
			for _, f := range files {
				fmt.Fprintf(cmd.ErrOrStderr(), "  %s\n", f)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "\nNext: review %s, then `platformctl policy test` and `--feature-gates PolicyEngine=true` to enable evaluation\n", targetDir)
			return nil
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "", "output directory (default: "+conventionalPoliciesDir+")")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite files already present in --dir")
	return cmd
}
