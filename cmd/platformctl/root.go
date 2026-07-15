package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	envsecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/env"
	filesecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/file"
	secretrouter "github.com/rezarajan/platformctl/internal/adapters/secrets/router"
	vaultsecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/vault"
	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/application/archview"
	"github.com/rezarajan/platformctl/internal/application/compatibility"
	"github.com/rezarajan/platformctl/internal/application/docsgen"
	"github.com/rezarajan/platformctl/internal/application/engine"
	"github.com/rezarajan/platformctl/internal/application/featuregate"
	"github.com/rezarajan/platformctl/internal/application/manifest"
	planpkg "github.com/rezarajan/platformctl/internal/application/plan"
	"github.com/rezarajan/platformctl/internal/application/registry"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/graph"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/secret"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/clock"
)

type wiringFunc func(*featuregate.Registry) *registry.Registry

type app struct {
	stateFile    string
	featureGates string
	output       string
	envFile      string
	wire         wiringFunc

	gates *featuregate.Registry
	reg   *registry.Registry
}

func (a *app) init() error {
	a.gates = featuregate.NewRegistry()
	a.reg = a.wire(a.gates)
	if a.envFile != "" {
		if err := loadEnvFile(a.envFile); err != nil {
			return cliutil.Exit(cliutil.ExitValidation, err)
		}
	}
	return a.gates.Apply(a.featureGates)
}

// loadEnvFile reads KEY=VALUE lines (dotenv style: blank lines and #
// comments ignored, optional surrounding quotes stripped, optional leading
// "export ") into the process environment. A variable already set in the
// shell environment is left untouched, so an explicit export always wins.
func loadEnvFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("--env-file: %w", err)
	}
	for i, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("--env-file %s: line %d is not KEY=VALUE: %q", path, i+1, raw)
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if len(val) >= 2 && (val[0] == '"' && val[len(val)-1] == '"' || val[0] == '\'' && val[len(val)-1] == '\'') {
			val = val[1 : len(val)-1]
		}
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, val); err != nil {
				return fmt.Errorf("--env-file: setting %s: %w", key, err)
			}
		}
	}
	return nil
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
	root.PersistentFlags().StringVar(&a.envFile, "env-file", "", "load KEY=VALUE lines from a file into the environment before resolving secrets (shell environment wins on conflict)")

	root.AddCommand(
		newValidateCmd(a),
		newPlanCmd(a),
		newApplyCmd(a),
		newDestroyCmd(a),
		newStatusCmd(a),
		newDriftCmd(a),
		newImportCmd(a),
		newGraphCmd(a),
		newInventoryCmd(a),
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
	secrets := secretrouter.New().
		Register(secret.BackendEnv, envsecrets.New()).
		Register(secret.BackendFile, filesecrets.New())
	if a.gates.Enabled("VaultSecretBackend") {
		secrets.Register(secret.BackendVault, vaultsecrets.New())
	}
	return &engine.Engine{
		Registry:    a.reg,
		StateStore:  localfile.New(a.stateFile),
		SecretStore: secrets,
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
	var parallelism int
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
			// Fail fast before touching any infrastructure: every declared
			// secret must resolve, or the platform would half-apply.
			eng := a.newEngine()
			if err := eng.PreflightSecrets(cmd.Context(), envelopes); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
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
			if parallelism > 1 {
				if err := a.gates.Require("ParallelReconciliation"); err != nil {
					return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("--parallelism: %w", err))
				}
			}
			eng.HaltOnError = haltOnError
			eng.HealDrift = healDrift
			eng.Parallelism = parallelism
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
	cmd.Flags().IntVar(&parallelism, "parallelism", 1, "max concurrent reconciliations within a dependency level (>1 requires the ParallelReconciliation gate)")
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

			eng := a.newEngine()
			if err := eng.PreflightSecrets(cmd.Context(), envelopes); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			key := resource.Key{Kind: kind, Name: name}
			probed, err := eng.Import(cmd.Context(), envelopes, key, from)
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

func newInventoryCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:     "inventory [path]",
		Aliases: []string{"services", "endpoints"},
		Short:   "List the service endpoints of an applied platform (for configuring external tools)",
		Long: "Surfaces the reachable endpoints each applied component publishes — the stable\n" +
			"access identifiers you point orchestrators (Dagster), BI tools (Metabase), or a\n" +
			"psql/mc client at — with the SecretReference that holds each one's credentials.\n" +
			"Reads recorded state; components not yet applied show no endpoints.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envelopes, _, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			st, err := localfile.New(a.stateFile).Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			creds := credentialRefs(envelopes)

			type invRow struct {
				Component string `json:"component"`
				Endpoint  string `json:"endpoint"`
				Scheme    string `json:"scheme"`
				Host      string `json:"host"`
				InNetwork string `json:"inNetwork"`
				Secret    string `json:"credentials"`
			}
			rows := [][]string{{"COMPONENT", "ENDPOINT", "SCHEME", "HOST (from your machine)", "IN-NETWORK", "CREDENTIALS"}}
			var data []invRow
			for _, e := range envelopes {
				rs, ok := st.Resources[e.Key()]
				if !ok {
					continue
				}
				for _, ep := range endpoint.FromState(rs.Provider[endpoint.Key]) {
					host := ep.Host
					if host == "" {
						host = "(in-network only)"
					}
					r := invRow{
						Component: e.Key().String(),
						Endpoint:  ep.Name,
						Scheme:    ep.Scheme,
						Host:      host,
						InNetwork: ep.Internal,
						Secret:    creds[e.Key()],
					}
					data = append(data, r)
					rows = append(rows, []string{r.Component, r.Endpoint, r.Scheme, r.Host, r.InNetwork, dash(r.Secret)})
				}
			}
			if len(data) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no service endpoints recorded — apply the platform first")
				return nil
			}
			return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, data, rows)
		},
	}
}

// credentialRefs maps each resource to the SecretReference(s) holding its
// credentials, so the inventory can tell a user which secret to read.
func credentialRefs(envelopes []resource.Envelope) map[resource.Key]string {
	out := map[resource.Key]string{}
	for _, e := range envelopes {
		var refs []string
		if list, ok := e.Spec["secretRefs"].([]any); ok {
			for _, r := range list {
				if s, ok := r.(string); ok {
					refs = append(refs, s)
				}
			}
		}
		if ref, ok := e.Spec["secretRef"].(map[string]any); ok {
			if n, _ := ref["name"].(string); n != "" {
				refs = append(refs, n)
			}
		}
		if len(refs) > 0 {
			out[e.Key()] = strings.Join(refs, ", ")
		}
	}
	return out
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
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
	return &cobra.Command{
		Use:   "graph [path]",
		Short: "Render the platform architecture (data flow + technology layer)",
		Long: "Renders the architecture the manifests describe — data-movement pipelines\n" +
			"(Bindings collapse into labelled source→target edges) and the technology layer\n" +
			"(which Provider realizes each asset, and how external systems are reached).\n" +
			"This is the picture you configure orchestrators against, not the internal\n" +
			"reconcile ordering. Choose the format with -o: tree (default), dot, mermaid, json.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envelopes, _, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			view := archview.Build(envelopes)
			if err := view.Render(cmd.OutOrStdout(), a.output); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			return nil
		},
	}
}

func printPlan(cmd *cobra.Command, output string, p planpkg.Plan) error {
	rows := [][]string{{"RESOURCE", "ACTION", "REASON"}}
	for _, e := range p.Entries {
		rows = append(rows, []string{e.Key.String(), string(e.Action), e.Reason})
	}
	return cliutil.WriteOutput(cmd.OutOrStdout(), output, p, rows)
}

func pathArg(args []string) string {
	if len(args) > 0 {
		return args[0]
	}
	return "."
}
