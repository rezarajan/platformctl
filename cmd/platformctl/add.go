package main

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/application/compose"
	"github.com/rezarajan/platformctl/internal/cliutil"
)

func newAddCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add <composite>",
		Short: "Add a building block to an existing manifest set (docs/planning/08 E9)",
		Long: "Composes new manifests (and, where compatible, reuses existing\n" +
			"infrastructure — an existing broker, Dataset, lake, ...) into the\n" +
			"current directory's manifest set. Composition only ever writes files;\n" +
			"`validate -> lint -> plan -> apply` is unchanged (docs/adr/024-" +
			"interactive-composition.md).",
	}
	cmd.AddCommand(
		newAddSourceCmd(a),
		newAddPipelineCmd(a),
		newAddSinkCmd(a),
		newAddCatalogCmd(a),
		newAddMonitoringCmd(a),
	)
	return cmd
}

func newAddSourceCmd(a *app) *cobra.Command {
	var name, engine, database string
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "source [path]",
		Short: "Add a standalone managed database (Provider + Source + credentials)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := pathArg(args)
			snap, err := a.loadComposeSnapshot(dir)
			if err != nil {
				return err
			}
			if !cliutil.Interactive() {
				if err := requireFlags("add source",
					flagCheck{"name", name == ""},
					flagCheck{"engine", engine == ""},
				); err != nil {
					return err
				}
			}
			if name, err = promptString("name", name, "Source name", ""); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			if engine, err = promptString("engine", engine, "Engine (postgres|mysql|mariadb)", "postgres"); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			patch, err := compose.PlanSource(snap, dir, compose.SourceOptions{Name: name, Engine: engine, Database: database})
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			return runCompose(cmd, a, patch, dryRun, snap.Warning)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Source name (required)")
	cmd.Flags().StringVar(&engine, "engine", "", "database engine: postgres | mysql | mariadb (required)")
	cmd.Flags().StringVar(&database, "database", "", "database name (default: --name)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the exact files/diffs and write nothing")
	return cmd
}

func newAddPipelineCmd(a *app) *cobra.Command {
	var name, engine, database, tablesFlag, snapshotMode string
	var broker, brokerName string
	var sink, sinkPrefix, lake, lakeName, sinkName, bucket, prefix, format string
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "pipeline [path]",
		Short: "Add a source with CDC into a (possibly reused) broker, sunk into a (possibly reused) Dataset",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := pathArg(args)
			snap, err := a.loadComposeSnapshot(dir)
			if err != nil {
				return err
			}
			if !cliutil.Interactive() {
				if err := requireFlags("add pipeline",
					flagCheck{"name", name == ""},
					flagCheck{"engine", engine == ""},
					flagCheck{"broker", broker == ""},
					flagCheck{"sink", sink == ""},
				); err != nil {
					return err
				}
			}
			if name, err = promptString("name", name, "Pipeline name", ""); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			if engine, err = promptString("engine", engine, "Engine (postgres|mysql|mariadb)", "postgres"); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			brokerChoice, err := resolveRefChoice("broker", broker, snap.BrokerCandidates())
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			sinkChoice, err := resolveRefChoice("sink", sink, datasetCandidatesAsGeneric(snap))
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			attach := compose.SinkAttachment{
				Sink: sinkChoice, SinkPrefix: sinkPrefix,
				LakeName: lakeName, SinkName: sinkName, Bucket: bucket, Prefix: prefix, Format: format,
			}
			if sinkChoice.New {
				lakeChoice, err := resolveRefChoice("lake", lake, snap.LakeCandidates())
				if err != nil {
					return cliutil.Exit(cliutil.ExitValidation, err)
				}
				attach.Lake = lakeChoice
			}
			var tables []string
			if tablesFlag != "" {
				tables = strings.Split(tablesFlag, ",")
			}
			opts := compose.PipelineOptions{
				Name: name, Engine: engine, Database: database, Tables: tables, SnapshotMode: snapshotMode,
				Broker: brokerChoice, BrokerName: brokerName,
				SinkAttachment: attach,
			}
			patch, err := compose.PlanPipeline(snap, dir, opts)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			return runCompose(cmd, a, patch, dryRun, snap.Warning)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "base name every generated resource derives from (required)")
	cmd.Flags().StringVar(&engine, "engine", "", "database engine: postgres | mysql | mariadb (required)")
	cmd.Flags().StringVar(&database, "database", "", "database name (default: --name)")
	cmd.Flags().StringVar(&tablesFlag, "tables", "", "comma-separated captured table list (default: records)")
	cmd.Flags().StringVar(&snapshotMode, "snapshot-mode", "", "Debezium snapshot mode (default: initial)")
	cmd.Flags().StringVar(&broker, "broker", "", `broker Provider: "new" or "existing:<name>" (required)`)
	cmd.Flags().StringVar(&brokerName, "broker-name", "", "new broker Provider name (--broker new only; default \"<name>-broker\")")
	cmd.Flags().StringVar(&sink, "sink", "", `sink Dataset: "new" or "existing:<name>" (required)`)
	cmd.Flags().StringVar(&sinkPrefix, "sink-prefix", "", "prefix override when reusing an existing Dataset (emits a second sink Binding at this prefix)")
	cmd.Flags().StringVar(&lake, "lake", "", `lake Provider: "new" or "existing:<name>" (--sink new only)`)
	cmd.Flags().StringVar(&lakeName, "lake-name", "", "new lake Provider name (--lake new only)")
	cmd.Flags().StringVar(&sinkName, "sink-name", "", "new sink worker Provider name (--sink new only)")
	cmd.Flags().StringVar(&bucket, "bucket", "", "object-store bucket (--sink new only, required)")
	cmd.Flags().StringVar(&prefix, "prefix", "", "object-store prefix (--sink new only)")
	cmd.Flags().StringVar(&format, "format", "", "Dataset format (--sink new only; default json)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the exact files/diffs and write nothing")
	return cmd
}

func newAddSinkCmd(a *app) *cobra.Command {
	var name, stream, sink, sinkPrefix, lake, lakeName, sinkName, bucket, prefix, format string
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "sink [path]",
		Short: "Wire an existing EventStream into a (possibly reused) Dataset",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := pathArg(args)
			snap, err := a.loadComposeSnapshot(dir)
			if err != nil {
				return err
			}
			if !cliutil.Interactive() {
				if err := requireFlags("add sink",
					flagCheck{"name", name == ""},
					flagCheck{"stream", stream == ""},
					flagCheck{"sink", sink == ""},
				); err != nil {
					return err
				}
			}
			if name, err = promptString("name", name, "Sink name", ""); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			if stream, err = promptString("stream", stream, "Existing EventStream to sink", ""); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			sinkChoice, err := resolveRefChoice("sink", sink, datasetCandidatesAsGeneric(snap))
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			attach := compose.SinkAttachment{
				Sink: sinkChoice, SinkPrefix: sinkPrefix,
				LakeName: lakeName, SinkName: sinkName, Bucket: bucket, Prefix: prefix, Format: format,
			}
			if sinkChoice.New {
				lakeChoice, err := resolveRefChoice("lake", lake, snap.LakeCandidates())
				if err != nil {
					return cliutil.Exit(cliutil.ExitValidation, err)
				}
				attach.Lake = lakeChoice
			}
			patch, err := compose.PlanSink(snap, dir, compose.SinkOptions{Name: name, Stream: stream, SinkAttachment: attach})
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			return runCompose(cmd, a, patch, dryRun, snap.Warning)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "names the sink Binding and any newly-created Dataset (required)")
	cmd.Flags().StringVar(&stream, "stream", "", "an existing EventStream name (required)")
	cmd.Flags().StringVar(&sink, "sink", "", `sink Dataset: "new" or "existing:<name>" (required)`)
	cmd.Flags().StringVar(&sinkPrefix, "sink-prefix", "", "prefix override when reusing an existing Dataset")
	cmd.Flags().StringVar(&lake, "lake", "", `lake Provider: "new" or "existing:<name>" (--sink new only)`)
	cmd.Flags().StringVar(&lakeName, "lake-name", "", "new lake Provider name (--lake new only)")
	cmd.Flags().StringVar(&sinkName, "sink-name", "", "new sink worker Provider name (--sink new only)")
	cmd.Flags().StringVar(&bucket, "bucket", "", "object-store bucket (--sink new only, required)")
	cmd.Flags().StringVar(&prefix, "prefix", "", "object-store prefix (--sink new only)")
	cmd.Flags().StringVar(&format, "format", "", "Dataset format (--sink new only; default json)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the exact files/diffs and write nothing")
	return cmd
}

func newAddCatalogCmd(a *app) *cobra.Command {
	var name, engine, provider, providerName string
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "catalog [path]",
		Short: "Add an Iceberg catalog (Catalog + realizing Provider)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := pathArg(args)
			snap, err := a.loadComposeSnapshot(dir)
			if err != nil {
				return err
			}
			if !cliutil.Interactive() {
				if err := requireFlags("add catalog", flagCheck{"name", name == ""}); err != nil {
					return err
				}
			}
			if name, err = promptString("name", name, "Catalog name", ""); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			providerChoice, err := resolveRefChoiceReuseFirst("provider", provider, snap.ProviderCandidates("nessie"))
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			patch, err := compose.PlanCatalog(snap, dir, compose.CatalogOptions{Name: name, Engine: engine, Provider: providerChoice, ProviderName: providerName})
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			return runCompose(cmd, a, patch, dryRun, snap.Warning)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Catalog resource name (required)")
	cmd.Flags().StringVar(&engine, "engine", "", "catalog engine (default: nessie, the only shipped one)")
	cmd.Flags().StringVar(&provider, "provider", "", `realizing Provider: "new" or "existing:<name>" (reuse-first: auto-selected if exactly one nessie Provider exists)`)
	cmd.Flags().StringVar(&providerName, "provider-name", "", "new Provider name (--provider new only; default \"<name>-provider\")")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the exact files/diffs and write nothing")
	return cmd
}

func newAddMonitoringCmd(a *app) *cobra.Command {
	var name string
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "monitoring [path]",
		Short: "Add a standalone managed Prometheus (scrape targets auto-discovered at apply time)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := pathArg(args)
			snap, err := a.loadComposeSnapshot(dir)
			if err != nil {
				return err
			}
			if !cliutil.Interactive() {
				if err := requireFlags("add monitoring", flagCheck{"name", name == ""}); err != nil {
					return err
				}
			}
			if name, err = promptString("name", name, "Provider name", "monitoring"); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			patch, err := compose.PlanMonitoring(snap, dir, compose.MonitoringOptions{Name: name})
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			return runCompose(cmd, a, patch, dryRun, snap.Warning)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Provider name (required)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the exact files/diffs and write nothing")
	return cmd
}

// datasetCandidatesAsGeneric adapts Snapshot.DatasetCandidates() (which
// carries extra reuse fields the --sink flag doesn't need) into the plain
// compose.Candidate list resolveRefChoice's interactive select renders.
func datasetCandidatesAsGeneric(snap compose.Snapshot) []compose.Candidate {
	ds := snap.DatasetCandidates()
	out := make([]compose.Candidate, len(ds))
	for i, d := range ds {
		out[i] = d.Candidate
	}
	return out
}
