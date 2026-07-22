package main

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/application/compose"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

func newWireCmd(a *app) *cobra.Command {
	var from, to string
	var provider, providerName, providerType string
	var broker, brokerName string
	var partitions int
	var retention, tablesFlag, snapshotMode, name string
	var dryRun bool
	var dir string

	cmd := &cobra.Command{
		Use:   "wire <mode>",
		Short: "Connect two existing blocks with a Binding (+ any missing glue: worker Provider, EventStream)",
		Long: "wire <cdc|sink|ingest> --from <Kind/name> --to <Kind/name> emits the\n" +
			"Binding connecting two *existing* resources, plus any missing glue\n" +
			"reuse-first: a realizing worker Provider, and — cdc mode only — the\n" +
			"EventStream target itself when it doesn't exist yet\n" +
			"(docs/adr/024-interactive-composition.md).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mode := args[0]
			command := "wire " + mode
			snap, err := a.loadComposeSnapshot(dir)
			if err != nil {
				return err
			}
			if !cliutil.Interactive() {
				if err := requireFlags(command,
					flagCheck{"from", from == ""},
					flagCheck{"to", to == ""},
				); err != nil {
					return err
				}
			}
			if from, err = promptString("from", from, "From (Kind/name)", ""); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			if to, err = promptString("to", to, "To (Kind/name)", ""); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}

			providerChoice, err := resolveRefChoiceReuseFirst("provider", provider, workerCandidatesForMode(snap, mode))
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			if providerChoice.New && providerType == "" {
				if providerType, err = promptString("provider-type", providerType, "New Provider type (e.g. debezium, s3sink, jdbcsink, s3source)", ""); err != nil {
					return cliutil.Exit(cliutil.ExitValidation, err)
				}
			}

			var tables []string
			if tablesFlag != "" {
				tables = strings.Split(tablesFlag, ",")
			}

			brokerChoice, err := parseOptionalRefChoice(broker)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			opts := compose.WireOptions{
				Mode: mode, From: from, To: to,
				Provider: providerChoice, ProviderName: providerName, ProviderType: providerType,
				Broker: brokerChoice, BrokerName: brokerName,
				Partitions: partitions, Retention: retention,
				Tables: tables, SnapshotMode: snapshotMode,
				Name: name,
			}
			patch, err := compose.PlanWire(snap, dir, opts)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			return runCompose(cmd, a, patch, dryRun, snap.Warning)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", ".", "the manifest set directory")
	cmd.Flags().StringVar(&from, "from", "", "existing resource (Kind/name) the Binding originates from (required)")
	cmd.Flags().StringVar(&to, "to", "", "existing resource (Kind/name) the Binding targets — cdc mode may name a not-yet-existing EventStream, created as missing glue (required)")
	cmd.Flags().StringVar(&provider, "provider", "", `realizing worker Provider: "new" or "existing:<name>" (reuse-first: auto-selected if exactly one candidate exists)`)
	cmd.Flags().StringVar(&providerName, "provider-name", "", "new Provider name (--provider new only; default \"<from>-<mode>\")")
	cmd.Flags().StringVar(&providerType, "provider-type", "", "new Provider's technology, e.g. debezium, s3sink, jdbcsink, s3source (--provider new only, required)")
	cmd.Flags().StringVar(&broker, "broker", "", `broker Provider for a missing EventStream target: "new" or "existing:<name>" (reuse-first: auto-selected if exactly one exists)`)
	cmd.Flags().StringVar(&brokerName, "broker-name", "", "new broker Provider name (--broker new only)")
	cmd.Flags().IntVar(&partitions, "partitions", 0, "new EventStream partitions (default 6)")
	cmd.Flags().StringVar(&retention, "retention", "", "new EventStream retention duration (default 7d)")
	cmd.Flags().StringVar(&tablesFlag, "tables", "", "cdc mode: comma-separated captured table list (default: records)")
	cmd.Flags().StringVar(&snapshotMode, "snapshot-mode", "", "cdc mode: Debezium snapshot mode (default: initial)")
	cmd.Flags().StringVar(&name, "name", "", "Binding name (default \"<from>-to-<to>\")")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the exact files/diffs and write nothing")
	return cmd
}

// parseOptionalRefChoice parses an optional RefChoice flag (wire's
// --broker: unset means "let the engine reuse-first auto-select"). Unlike
// resolveRefChoice, an empty value is not an error — but a non-empty,
// malformed one still is, so a typo'd flag never silently falls through to
// auto-select.
func parseOptionalRefChoice(value string) (compose.RefChoice, error) {
	if value == "" {
		return compose.RefChoice{}, nil
	}
	return compose.ParseRefChoice("broker", value)
}

// workerCandidatesForMode picks the reuse candidate list matching mode's
// realizing capability, for wire's --provider interactive select.
func workerCandidatesForMode(snap compose.Snapshot, mode string) []compose.Candidate {
	switch mode {
	case "cdc":
		return snap.CDCWorkerCandidates()
	case "sink":
		return snap.SinkWorkerCandidates()
	default:
		return nil
	}
}
