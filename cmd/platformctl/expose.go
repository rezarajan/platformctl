package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/application/compose"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

func newExposeCmd(a *app) *cobra.Command {
	var scheme string
	var port int
	var connectionName, provider, providerName, dir string
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "expose <Kind>/<name>",
		Short: "Emit a stable entrypoint (Connection + realizing Provider) to an existing block",
		Long: "expose <Kind>/<name> --scheme <tcp|http|https> --port <port> emits a\n" +
			"Connection and its realizing Provider: tcp -> proxy, http/https ->\n" +
			"ingress. --scheme https requires ingress TLS support (docs/adr/018\n" +
			"§C8); until that merges, it fails with a clear message rather than\n" +
			"emitting a Connection apply would reject\n" +
			"(docs/adr/024-interactive-composition.md).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			kind, name, err := parseKindNameSelector(args[0])
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			snap, err := a.loadComposeSnapshot(dir)
			if err != nil {
				return err
			}
			if !cliutil.Interactive() {
				if err := requireFlags("expose",
					flagCheck{"scheme", scheme == ""},
					flagCheck{"port", port == 0},
				); err != nil {
					return err
				}
			}
			if scheme, err = promptString("scheme", scheme, "Scheme (tcp|http|https)", "tcp"); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}

			var providerCandidates []compose.Candidate
			if realizingType, rtErr := compose.RealizingProviderType(scheme); rtErr == nil {
				providerCandidates = snap.ProviderCandidates(realizingType)
			}
			providerChoice, err := resolveRefChoiceReuseFirst("provider", provider, providerCandidates)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}

			opts := compose.ExposeOptions{
				TargetKind: kind, TargetName: name,
				Scheme: scheme, Port: port,
				ConnectionName: connectionName,
				Provider:       providerChoice,
				ProviderName:   providerName,
			}
			patch, err := compose.PlanExpose(snap, dir, opts, a.reg.Provider)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			return runCompose(cmd, a, patch, dryRun, snap.Warning)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", ".", "the manifest set directory")
	cmd.Flags().StringVar(&scheme, "scheme", "", "tcp | http | https (required)")
	cmd.Flags().IntVar(&port, "port", 0, "the Connection's listen port (required)")
	cmd.Flags().StringVar(&connectionName, "connection-name", "", "Connection name (default \"<name>-conn\"; also drives the http(s) routed host, \"<name>.localhost\")")
	cmd.Flags().StringVar(&provider, "provider", "", `realizing Provider: "new" or "existing:<name>" (reuse-first: auto-selected if exactly one candidate exists)`)
	cmd.Flags().StringVar(&providerName, "provider-name", "", "new Provider name (--provider new only; default \"expose-<proxy|ingress>\")")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the exact files/diffs and write nothing")
	return cmd
}

func parseKindNameSelector(value string) (kind, name string, err error) {
	kind, name, ok := strings.Cut(value, "/")
	if !ok || kind == "" || name == "" {
		return "", "", fmt.Errorf("expose target must be <Kind>/<name>, got %q", value)
	}
	return kind, name, nil
}
