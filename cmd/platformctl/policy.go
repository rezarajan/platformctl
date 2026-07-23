package main

import (
	"context"
	"fmt"
	"log/slog"
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
	decisions, err := apppolicy.Run(policies, envelopes, g, findings, a.gates.Enabled("LabelScopedAccess"))
	if err != nil {
		return nil, cliutil.Exit(cliutil.ExitExecution, err)
	}
	return decisions, nil
}

// enforcePolicies is evaluatePolicies plus ADR 021 §3's deny-wins blocking
// behavior — wired into loadAndValidate after compatibility + lint: a
// non-exempted deny fails via the standard validation-error exit path,
// naming every offending rule id, message, and resource. logger is the K5
// structured-decision-event seam (docs/planning/08 K5, ADR 033 decision
// 5): every decision Run produced — deny or warn, exempted or not — is
// logged, in the SAME deterministic order Run/policy.SortDecisions already
// establish, before a deny can turn into the returned error.
func (a *app) enforcePolicies(envelopes []resource.Envelope, g *graph.Graph, logger *slog.Logger) error {
	decisions, err := a.evaluatePolicies(envelopes, g)
	if err != nil {
		return err
	}
	logPolicyDecisions(logger, decisions)
	return denyError(decisions)
}

// enforcePlanPolicies is the matchPlan half: evaluated once a plan actually
// exists (plan/apply/destroy only — matchPlan rules never fire from
// loadAndValidate/evaluatePolicies, since no plan exists yet at validate
// time). Same off-switch, directory-resolution, and K5 decision-logging
// rules as enforcePolicies.
func (a *app) enforcePlanPolicies(envelopes []resource.Envelope, entries []planpkg.Entry, logger *slog.Logger) error {
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
	logPolicyDecisions(logger, decisions)
	return denyError(decisions)
}

// logPolicyDecisions emits one structured slog event per decision — the
// docs/planning/08 K5 / ADR 033 decision 5 audit trail: "strictly
// moderated must be provable after the fact." Mirrors
// internal/application/engine.Engine.logAction's exact shape (message
// carries the full byte-compatible prose text mode renders verbatim; attrs
// carry the SAME facts structured for --log-format json) so a policy
// decision event and a reconciliation-action event share one seam and one
// on-disk/stdout shape. decisions is assumed already deterministically
// ordered (Run/RunPlan both return output already passed through
// policy.SortDecisions), so iterating in place preserves that order in
// both text and json log streams.
func logPolicyDecisions(logger *slog.Logger, decisions []apppolicy.Decision) {
	if logger == nil {
		return
	}
	for _, d := range decisions {
		outcome := "warn"
		switch {
		case d.Exempted:
			outcome = "exempt"
		case d.Effect == domainpolicy.Deny:
			outcome = "deny"
		}
		msg := fmt.Sprintf("policy %s %s (rule %s): %s", outcome, d.Resource, d.RuleID, d.Message)
		attrs := []slog.Attr{
			slog.String("resource", d.Resource.String()),
			slog.String("rule", d.RuleID),
			slog.String("effect", string(d.Effect)),
			slog.String("outcome", outcome),
			slog.Bool("exempted", d.Exempted),
		}
		if d.ExemptReason != "" {
			attrs = append(attrs, slog.String("exemptReason", d.ExemptReason))
		}
		logger.LogAttrs(context.Background(), slog.LevelInfo, msg, attrs...)
	}
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
	cmd.AddCommand(newPolicyTestCmd(a), newPolicyInitCmd(a), newPolicyAuditCmd(a))
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
			decisions, err := apppolicy.Run(policies, envelopes, g, findings, a.gates.Enabled("LabelScopedAccess"))
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

// policyAuditEdgeOutput is one `platformctl policy audit` row's -o
// json|yaml shape (A7 harness): every declared edge in the CURRENT
// manifests — a Binding, a connectionRef consumption, or a spec.access
// grant — plus its admission verdict and WHY (docs/planning/08 K5, ADR 033
// decision 5). RuleID is empty exactly when Justification is
// "no-matching-deny" or "grant" — there is no rule to name; the absence of
// one, or the grant declaration itself, IS the justification.
type policyAuditEdgeOutput struct {
	Owner         string `json:"owner" yaml:"owner"`
	From          string `json:"from" yaml:"from"`
	To            string `json:"to" yaml:"to"`
	Kind          string `json:"kind" yaml:"kind"`
	Verdict       string `json:"verdict" yaml:"verdict"`
	Justification string `json:"justification" yaml:"justification"`
	RuleID        string `json:"ruleId,omitempty" yaml:"ruleId,omitempty"`
	Detail        string `json:"detail" yaml:"detail"`
	ExemptReason  string `json:"exemptReason,omitempty" yaml:"exemptReason,omitempty"`
}

type policyAuditOutput struct {
	Edges []policyAuditEdgeOutput `json:"edges" yaml:"edges"`
}

func printPolicyAudit(cmd *cobra.Command, output string, audits []apppolicy.EdgeAudit) error {
	data := make([]policyAuditEdgeOutput, len(audits))
	rows := [][]string{{"VERDICT", "KIND", "OWNER", "FROM", "TO", "JUSTIFICATION", "RULE", "DETAIL"}}
	for i, a := range audits {
		data[i] = policyAuditEdgeOutput{
			Owner: a.Owner.String(), From: a.From, To: a.To,
			Kind: string(a.Kind), Verdict: string(a.Verdict), Justification: string(a.Justification),
			RuleID: a.RuleID, Detail: a.Detail, ExemptReason: a.ExemptReason,
		}
		rows = append(rows, []string{
			string(a.Verdict), string(a.Kind), a.Owner.String(), a.From, a.To,
			string(a.Justification), a.RuleID, a.Detail,
		})
	}
	if isStructured(output) {
		return cliutil.WriteOutput(cmd.OutOrStdout(), output, policyAuditOutput{Edges: data}, nil)
	}
	if len(audits) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "no declared edges to audit")
		return nil
	}
	return cliutil.WriteOutput(cmd.OutOrStdout(), output, data, rows)
}

// newPolicyAuditCmd implements docs/planning/08 K5 / ADR 033 decision 5:
// "every edge decision is auditable ... platformctl policy audit naming
// rule/selector/grant for any permitted edge." Unlike `policy test`
// (authoring loop; errors outright with no --policies dir), audit renders
// a row for every declared edge REGARDLESS of whether any policy is
// loaded at all — "no policies loaded" is itself an honest, auditable
// state (every edge's justification becomes "no-matching-deny"/"grant").
// Never fails on a denied edge (unlike validate/plan/apply, whose job is
// to BLOCK): audit's job is to REPORT, including the ADR 021 severing
// amendment's in-between state — a denied-but-standing edge from a prior
// apply shows up here as Denied, evaluated purely against the CURRENT
// manifests + policies (no state/runtime read at all).
func newPolicyAuditCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit [path]",
		Short: "Render the permitted/denied-edge justification table for every declared edge (docs/adr/033 decision 5)",
		Long: "Loads the manifest set at path (schema + graph only, like `policy test`) and\n" +
			"every loaded policy (--policies, or the conventional .datascape/policies/\n" +
			"directory — an empty/absent policy set is itself a valid, reportable state),\n" +
			"then names, for EVERY declared edge — a Binding, a connectionRef consumption,\n" +
			"or a spec.access wide/selector grant — why it is permitted (no matching deny\n" +
			"rule, an exemption, or the grant itself) or why it is denied (the specific\n" +
			"deny rule). Read-only and non-blocking: exits 0 regardless of verdicts (run\n" +
			"`validate`/`plan` to enforce). Requires the PolicyEngine feature gate.",
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
			var policies []domainpolicy.Policy
			if dir := a.policiesDir(); dir != "" {
				policies, err = apppolicy.LoadDir(dir)
				if err != nil {
					return cliutil.Exit(cliutil.ExitValidation, err)
				}
			}
			audits := apppolicy.Audit(policies, envelopes, g, a.gates.Enabled("LabelScopedAccess"))
			return printPolicyAudit(cmd, a.output, audits)
		},
	}
	return cmd
}
