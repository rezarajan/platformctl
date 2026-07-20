package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/application/blueprint"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

// initListOutput is the -o json|yaml document for `platformctl init
// --list` (docs/planning/08 §E1): exactly one machine-readable document on
// stdout, mirroring the output-contract pattern in output_contract_test.go.
type initListOutput struct {
	Blueprints []blueprint.Info `json:"blueprints" yaml:"blueprints"`
}

// initWriteOutput is the -o json|yaml document for a successful `init
// <blueprint>`.
type initWriteOutput struct {
	Blueprint string   `json:"blueprint" yaml:"blueprint"`
	Dir       string   `json:"dir" yaml:"dir"`
	Files     []string `json:"files" yaml:"files"`
}

func newInitCmd(a *app) *cobra.Command {
	var dir string
	var list bool
	var force bool
	cmd := &cobra.Command{
		Use:   "init [blueprint]",
		Short: "Scaffold a ready-to-apply manifest set from an embedded blueprint",
		Long: "Writes a tailored starting point — manifests, a .env template naming every\n" +
			"secret key the blueprint's providers need, and a README — so `platformctl\n" +
			"validate` succeeds immediately with no manifest edits (providers' default\n" +
			"images and auto-assigned host ports are used throughout). Fill in the\n" +
			"written .env before `platformctl apply`; any key left unset is reported by\n" +
			"name at apply time (secret Preflight), not as an opaque mid-apply failure.\n\n" +
			"Use --list to enumerate the shipped blueprints instead of writing one.\n\n" +
			"Blueprints: " + strings.Join(blueprint.Names(), ", "),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if list {
				return runInitList(cmd, a)
			}
			if len(args) == 0 {
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("init requires a blueprint name or --list; known blueprints: %s", strings.Join(blueprint.Names(), ", ")))
			}
			return runInitWrite(cmd, a, args[0], dir, force)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "", "output directory (default: ./<blueprint>)")
	cmd.Flags().BoolVar(&list, "list", false, "list available blueprints instead of writing one")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite files already present in --dir")
	return cmd
}

func runInitList(cmd *cobra.Command, a *app) error {
	infos := blueprint.List()
	if isStructured(a.output) {
		return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, initListOutput{Blueprints: infos}, nil)
	}
	for _, b := range infos {
		fmt.Fprintf(cmd.OutOrStdout(), "%-16s %s\n", b.Name, b.Summary)
	}
	return nil
}

func runInitWrite(cmd *cobra.Command, a *app, name, dir string, force bool) error {
	targetDir := dir
	if targetDir == "" {
		targetDir = name
	}
	files, err := blueprint.Write(name, targetDir, force)
	if err != nil {
		return cliutil.Exit(cliutil.ExitValidation, err)
	}
	if isStructured(a.output) {
		return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, initWriteOutput{Blueprint: name, Dir: targetDir, Files: files}, nil)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "wrote blueprint %q to %s (%d file(s))\n", name, targetDir, len(files))
	for _, f := range files {
		fmt.Fprintf(cmd.ErrOrStderr(), "  %s\n", f)
	}
	fmt.Fprintf(cmd.ErrOrStderr(), "\nNext: fill in %s/.env, then `platformctl validate %s`\n", targetDir, targetDir)
	return nil
}
