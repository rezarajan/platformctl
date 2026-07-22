// compose_shared.go holds the glue every `add`/`wire`/`expose` leaf
// command shares (docs/planning/08 E9, docs/adr/024-interactive-
// composition.md): loading the tolerant snapshot, the --dry-run/-o
// json|yaml output contract (A7), and the "non-TTY + incomplete flags is a
// hard error listing exactly the missing flags" rule. The composition
// *engine* lives in internal/application/compose and holds no CLI/TUI
// imports; this file (and cmd/platformctl generally) is where compose
// meets cobra and — via internal/cliutil — huh.
package main

import (
	"fmt"
	"sort"
	"strings"

	huh "charm.land/huh/v2"
	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/application/compose"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

// composeFileResult is one file's outcome in a compose command's -o
// json|yaml document.
type composeFileResult struct {
	Path   string `json:"path" yaml:"path"`
	Status string `json:"status" yaml:"status"` // created | unchanged | would-create
}

// composeOutput is the exactly-one-document -o json|yaml contract (A7)
// every add/wire/expose command emits.
type composeOutput struct {
	Command  string              `json:"command" yaml:"command"`
	Dir      string              `json:"dir" yaml:"dir"`
	DryRun   bool                `json:"dryRun" yaml:"dryRun"`
	Changed  bool                `json:"changed" yaml:"changed"`
	Files    []composeFileResult `json:"files" yaml:"files"`
	EnvKeys  []string            `json:"envKeys,omitempty" yaml:"envKeys,omitempty"`
	Notes    []string            `json:"notes,omitempty" yaml:"notes,omitempty"`
	Warnings []string            `json:"warnings,omitempty" yaml:"warnings,omitempty"`
}

// loadComposeSnapshot is every compose command's first step: the tolerant
// loadAndValidate front-end (ADR 024 "Graph-aware reuse"). A degraded
// snapshot is not an error — a.reg.Provider mirrors exactly what
// a.loadAndValidate itself passes to compatibility.Check.
func (a *app) loadComposeSnapshot(dir string) (compose.Snapshot, error) {
	snap, err := compose.LoadTolerant(dir, a.reg.Provider)
	if err != nil {
		return compose.Snapshot{}, cliutil.Exit(cliutil.ExitValidation, err)
	}
	return snap, nil
}

// runCompose applies the shared output contract to a computed Patch:
// --dry-run prints the exact files/diffs and writes nothing; otherwise it
// writes the patch and reports what changed. Structured output (-o
// json|yaml) emits exactly one composeOutput document on stdout; prose
// (notes/warnings/file listing in human mode) goes to stderr, matching
// every other command's isStructured branch in this package.
func runCompose(cmd *cobra.Command, a *app, patch compose.Patch, dryRun bool, snapWarning string) error {
	out := composeOutput{Command: patch.Command, Dir: patch.Dir, DryRun: dryRun, Notes: patch.Notes}
	if snapWarning != "" {
		out.Warnings = append(out.Warnings, snapWarning)
	}

	if dryRun {
		out.Changed = patch.HasChanges()
		for _, f := range patch.Files {
			status := "unchanged"
			if f.New {
				status = "would-create"
			}
			out.Files = append(out.Files, composeFileResult{Path: f.Path, Status: status})
		}
		for _, e := range patch.EnvAppends {
			if e.Pending {
				out.EnvKeys = append(out.EnvKeys, e.Key)
			}
		}
		if isStructured(a.output) {
			return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, out, nil)
		}
		printComposeNotes(cmd, out)
		return printComposeDryRun(cmd, patch)
	}

	written, envKeys, err := compose.Write(patch)
	if err != nil {
		return cliutil.Exit(cliutil.ExitExecution, err)
	}
	out.Changed = len(written) > 0 || len(envKeys) > 0
	writtenSet := make(map[string]bool, len(written))
	for _, w := range written {
		writtenSet[w] = true
	}
	for _, f := range patch.Files {
		status := "unchanged"
		if writtenSet[f.Path] {
			status = "created"
		}
		out.Files = append(out.Files, composeFileResult{Path: f.Path, Status: status})
	}
	out.EnvKeys = envKeys

	if isStructured(a.output) {
		return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, out, nil)
	}
	printComposeNotes(cmd, out)
	if !out.Changed {
		fmt.Fprintf(cmd.OutOrStdout(), "%s: no changes (already up to date)\n", patch.Command)
		return nil
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%s: wrote %d file(s), appended %d .env key(s)\n", patch.Command, len(written), len(envKeys))
	for _, f := range written {
		fmt.Fprintf(cmd.ErrOrStderr(), "  %s\n", f)
	}
	if len(envKeys) > 0 {
		fmt.Fprintf(cmd.ErrOrStderr(), "\nNext: fill in the appended .env key(s) in %s/.env, then `platformctl validate %s`\n", patch.Dir, patch.Dir)
	}
	return nil
}

func printComposeNotes(cmd *cobra.Command, out composeOutput) {
	for _, w := range out.Warnings {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s\n", w)
	}
	for _, n := range out.Notes {
		fmt.Fprintf(cmd.ErrOrStderr(), "note: %s\n", n)
	}
}

func printComposeDryRun(cmd *cobra.Command, patch compose.Patch) error {
	newCount, pendingEnv := 0, 0
	for _, f := range patch.Files {
		if f.New {
			newCount++
		}
	}
	for _, e := range patch.EnvAppends {
		if e.Pending {
			pendingEnv++
		}
	}
	fmt.Fprintf(cmd.OutOrStdout(), "dry run: %s would write %d file(s), append %d .env key(s); nothing written\n", patch.Command, newCount, pendingEnv)
	for _, f := range patch.Files {
		switch {
		case f.New:
			fmt.Fprintf(cmd.OutOrStdout(), "\n+++ %s (new)\n%s", f.Path, f.Content)
		default:
			fmt.Fprintf(cmd.OutOrStdout(), "%s: unchanged\n", f.Path)
		}
	}
	for _, e := range patch.EnvAppends {
		if e.Pending {
			fmt.Fprintf(cmd.OutOrStdout(), "+++ .env: %s=%s\n", e.Key, e.Default)
		}
	}
	return nil
}

// missingFlags collects the names (without leading "--") of every entry in
// required whose value is still empty/zero, in declaration order — the
// exact list the non-TTY hard error names (docs/planning/08 E9: "non-TTY +
// incomplete flags = hard error listing exactly the missing flags").
type flagCheck struct {
	name    string
	missing bool
}

func requireFlags(command string, checks ...flagCheck) error {
	var missing []string
	for _, c := range checks {
		if c.missing {
			missing = append(missing, "--"+c.name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("%s: missing required flag(s) in non-interactive mode: %s", command, strings.Join(missing, ", ")))
}

// resolveRefChoice is the flag/prompt "same machine" (ADR 024) for one
// attachment point: if flagValue is already set ("new" or
// "existing:<name>"), parse and return it. Otherwise, in a TTY, render an
// interactive select over candidates (plus "create new"); outside a TTY,
// the caller's non-TTY flag-completeness check catches the empty flag
// before this is ever called for a required attachment point.
func resolveRefChoice(flagName, flagValue string, candidates []compose.Candidate) (compose.RefChoice, error) {
	if flagValue != "" {
		return compose.ParseRefChoice(flagName, flagValue)
	}
	if !cliutil.Interactive() {
		return compose.RefChoice{}, fmt.Errorf("--%s is required (non-interactive)", flagName)
	}
	options := make([]huh.Option[string], 0, len(candidates)+1)
	for _, c := range candidates {
		options = append(options, cliutil.Option(c.Name+" — "+c.Summary, c.Name))
	}
	options = append(options, cliutil.Option("create new…", "\x00new"))
	choice, err := cliutil.SelectString("Select "+flagName, options)
	if err != nil {
		return compose.RefChoice{}, err
	}
	if choice == "\x00new" {
		return compose.RefChoice{New: true}, nil
	}
	return compose.RefChoice{Name: choice}, nil
}

// resolveRefChoiceReuseFirst is resolveRefChoice's "missing glue" variant
// (wire's and expose's --provider): when the flag is unset and running
// non-interactively, reuse-first decides without asking — zero candidates
// means there is nothing to reuse (create new), exactly one means reuse it
// unambiguously; two or more is genuinely ambiguous and still requires an
// explicit flag (or, in a TTY, the ordinary select, which always offers
// "create new…" too).
func resolveRefChoiceReuseFirst(flagName, flagValue string, candidates []compose.Candidate) (compose.RefChoice, error) {
	if flagValue == "" && !cliutil.Interactive() {
		switch len(candidates) {
		case 0:
			return compose.RefChoice{New: true}, nil
		case 1:
			return compose.RefChoice{Name: candidates[0].Name}, nil
		default:
			names := make([]string, len(candidates))
			for i, c := range candidates {
				names[i] = c.Name
			}
			return compose.RefChoice{}, fmt.Errorf("--%s is required (non-interactive; candidates: %s)", flagName, strings.Join(names, ", "))
		}
	}
	return resolveRefChoice(flagName, flagValue, candidates)
}

// promptString fills in a missing required string flag interactively, or
// returns an error if not running against a TTY (the caller's
// requireFlags check will normally have already caught this).
func promptString(flagName, flagValue, title, defaultValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if !cliutil.Interactive() {
		return "", fmt.Errorf("--%s is required (non-interactive)", flagName)
	}
	return cliutil.InputString(title, defaultValue, true)
}
