package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	envsecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/env"
	filesecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/file"
	secretrouter "github.com/rezarajan/platformctl/internal/adapters/secrets/router"
	vaultsecrets "github.com/rezarajan/platformctl/internal/adapters/secrets/vault"
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

	// Shared/remote state backend (docs/design/003, gated
	// SharedStateBackend). stateFile above remains the "local" backend's
	// path; these are only consulted when stateBackend != "local".
	stateBackend   string
	stateBucket    string
	statePrefix    string
	stateEndpoint  string
	stateRegion    string
	stateSecretRef string
	stateInsecure  bool
	stateLockTTL   time.Duration

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
	root.PersistentFlags().StringVar(&a.stateFile, "state-file", ".datascape/state.json", "path to the state file (local backend only)")
	root.PersistentFlags().StringVar(&a.featureGates, "feature-gates", "", "comma-separated Name=true|false overrides")
	root.PersistentFlags().StringVarP(&a.output, "output", "o", "table", "output format: table|json|yaml")
	root.PersistentFlags().StringVar(&a.envFile, "env-file", "", "load KEY=VALUE lines from a file into the environment before resolving secrets (shell environment wins on conflict)")
	root.PersistentFlags().StringVar(&a.stateBackend, "state-backend", "local", "state backend: local|s3 (s3 requires the SharedStateBackend gate, see docs/design/003-shared-state.md)")
	root.PersistentFlags().StringVar(&a.stateBucket, "state-bucket", "", "s3 backend: bucket holding state.json and the lock object")
	root.PersistentFlags().StringVar(&a.statePrefix, "state-prefix", "", "s3 backend: object key prefix (e.g. \"team-a/\")")
	root.PersistentFlags().StringVar(&a.stateEndpoint, "state-endpoint", "", "s3 backend: endpoint host:port (MinIO or S3-compatible; empty = AWS S3 default)")
	root.PersistentFlags().StringVar(&a.stateRegion, "state-region", "", "s3 backend: region")
	root.PersistentFlags().StringVar(&a.stateSecretRef, "state-secret-ref", "", "s3 backend: env-backend SecretReference name providing accessKey/secretKey (DATASCAPE_SECRET_<NAME>_{ACCESSKEY,SECRETKEY})")
	root.PersistentFlags().BoolVar(&a.stateInsecure, "state-insecure", false, "s3 backend: use plain HTTP instead of TLS (local MinIO testing)")
	root.PersistentFlags().DurationVar(&a.stateLockTTL, "state-lock-ttl", 0, "s3 backend: lock lease TTL (default 15m — must outlast the longest apply/destroy run)")

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
		newGCCmd(a),
		newStateCmd(a),
	)
	return root
}

func newDocsCmd() *cobra.Command {
	docs := &cobra.Command{
		Use:   "docs",
		Short: "Generate and serve the resource reference from schemas/",
	}
	var outDir string
	var asHTML bool
	build := &cobra.Command{
		Use:   "build",
		Short: "Render the reference from the embedded schemas (markdown, or --html for a static site)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if asHTML {
				site, err := docsgen.Site()
				if err != nil {
					return cliutil.Exit(cliutil.ExitExecution, err)
				}
				p := filepath.Join(outDir, "index.html")
				if err := os.WriteFile(p, []byte(site), 0o644); err != nil {
					return cliutil.Exit(cliutil.ExitExecution, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "wrote self-contained docs site to %s\n", p)
				return nil
			}
			pages, err := docsgen.Build()
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			for name, content := range pages {
				if err := os.WriteFile(filepath.Join(outDir, name), []byte(content), 0o644); err != nil {
					return cliutil.Exit(cliutil.ExitExecution, err)
				}
			}
			fmt.Fprintf(cmd.OutOrStdout(), "wrote %d markdown page(s) to %s\n", len(pages), outDir)
			return nil
		},
	}
	build.Flags().StringVar(&outDir, "out", "docs/reference", "output directory")
	build.Flags().BoolVar(&asHTML, "html", false, "render a single self-contained HTML site (index.html) with search instead of markdown")

	var addr string
	serve := &cobra.Command{
		Use:   "serve",
		Short: "Serve the reference as a searchable HTML site",
		RunE: func(cmd *cobra.Command, _ []string) error {
			site, err := docsgen.Site()
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				fmt.Fprint(w, site)
			})
			fmt.Fprintf(cmd.OutOrStdout(), "serving searchable resource reference on http://%s\n", addr)
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

func (a *app) newEngine() (*engine.Engine, error) {
	secrets := secretrouter.New().
		Register(secret.BackendEnv, envsecrets.New()).
		Register(secret.BackendFile, filesecrets.New())
	if a.gates.Enabled("VaultSecretBackend") {
		secrets.Register(secret.BackendVault, vaultsecrets.New())
	}
	store, err := a.stateStore()
	if err != nil {
		return nil, cliutil.Exit(cliutil.ExitValidation, err)
	}
	return &engine.Engine{
		Registry:    a.reg,
		StateStore:  store,
		SecretStore: secrets,
		Clock:       clock.Real{},
		Log: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, format+"\n", args...)
		},
	}, nil
}

type validateOutput struct {
	Valid     bool `json:"valid" yaml:"valid"`
	Resources int  `json:"resources" yaml:"resources"`
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
			if isStructured(a.output) {
				return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, validateOutput{Valid: true, Resources: len(envelopes)}, nil)
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
			store, err := a.stateStore()
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
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
	var includeExternal, destructiveOK, includeImportedDeletes bool
	cmd := &cobra.Command{
		Use:   "apply [path]",
		Short: "Compute the plan, then execute it",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if includeExternal && !destructiveOK {
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("--include-external additionally requires --yes-i-understand-this-is-destructive"))
			}
			envelopes, g, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			eng, err := a.newEngine()
			if err != nil {
				return err
			}
			unlock, err := eng.StateStore.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			st, err := eng.StateStore.Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			// Fail fast before touching any infrastructure: every declared
			// secret must resolve, or the platform would half-apply.
			if err := eng.PreflightSecrets(cmd.Context(), envelopes); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			secretHashes, err := eng.SecretHashes(cmd.Context(), envelopes)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			p, err := planpkg.ComputeWithSecretHashes(envelopes, st, g, secretHashes)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			healDrift := a.gates.Enabled("DriftDetection")
			if !p.HasChanges() {
				if !healDrift {
					if isStructured(a.output) {
						return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, applyOutput{Plan: p}, nil)
					}
					fmt.Fprintln(cmd.OutOrStdout(), "no changes; nothing to apply")
					return nil
				}
				// Healing only re-converges resources to the spec already in
				// state, so no approval prompt: nothing new is being applied.
				fmt.Fprintln(humanWriter(cmd, a.output), "no changes; probing for drift")
			} else {
				if isStructured(a.output) {
					if err := printPlanTo(humanWriter(cmd, a.output), "table", p); err != nil {
						return err
					}
				} else {
					if err := printPlan(cmd, a.output, p); err != nil {
						return err
					}
				}
				if !autoApprove {
					fmt.Fprint(humanWriter(cmd, a.output), "\nApply these changes? Only 'yes' is accepted: ")
					var answer string
					fmt.Fscanln(cmd.InOrStdin(), &answer) //nolint:errcheck
					if answer != "yes" {
						if isStructured(a.output) {
							return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, applyOutput{Plan: p, Cancelled: true}, nil)
						}
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
			eng.AllowDestructive = includeExternal && destructiveOK
			eng.AllowImportedDeletes = includeImportedDeletes
			// Stream ordered, countable progress to stderr; the reporter owns
			// per-step output, so silence the raw log to avoid duplicate lines.
			eng.Log = nil
			eng.Reporter = cliutil.NewProgressReporter(cmd.ErrOrStderr(), isTTY(cmd.ErrOrStderr()))
			result, err := eng.Apply(cmd.Context(), p, envelopes, g)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if isStructured(a.output) {
				return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, applyOutput{Plan: p, Result: resultPayload(result)}, nil)
			}
			// The reporter (stderr) already printed the streamed steps and a
			// summary; success is also conveyed by the zero exit code.
			return nil
		},
	}
	cmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "skip the interactive confirmation (for CI)")
	cmd.Flags().BoolVar(&haltOnError, "halt-on-error", false, "stop the whole apply on the first failure")
	cmd.Flags().IntVar(&parallelism, "parallelism", 1, "max concurrent reconciliations within a dependency level (>1 requires the ParallelReconciliation gate)")
	cmd.Flags().BoolVar(&includeExternal, "include-external", false, "also delete absent External-lifecycle resources during authoritative apply")
	cmd.Flags().BoolVar(&includeImportedDeletes, "include-imported-deletes", false, "also delete absent Imported-lifecycle resources during authoritative apply")
	cmd.Flags().BoolVar(&destructiveOK, "yes-i-understand-this-is-destructive", false, "required with --include-external")
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
			eng, err := a.newEngine()
			if err != nil {
				return err
			}
			unlock, err := eng.StateStore.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			st, err := eng.StateStore.Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			p, err := planpkg.ComputeDestroy(envelopes, st, g, includeExternal, includeImported)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if isStructured(a.output) {
				if err := printPlanTo(cmd.ErrOrStderr(), "table", p); err != nil {
					return err
				}
			} else {
				if err := printPlan(cmd, a.output, p); err != nil {
					return err
				}
			}
			if !p.HasChanges() {
				if isStructured(a.output) {
					return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, destroyOutput{Plan: p}, nil)
				}
				fmt.Fprintln(cmd.OutOrStdout(), "nothing to destroy")
				return nil
			}
			if !autoApprove {
				fmt.Fprint(humanWriter(cmd, a.output), "\nDestroy these resources? Only 'yes' is accepted: ")
				var answer string
				fmt.Fscanln(cmd.InOrStdin(), &answer) //nolint:errcheck
				if answer != "yes" {
					if isStructured(a.output) {
						return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, destroyOutput{Plan: p, Cancelled: true}, nil)
					}
					fmt.Fprintln(cmd.OutOrStdout(), "destroy cancelled")
					return nil
				}
			}
			eng.AllowDestructive = includeExternal && destructiveOK
			result, err := eng.Destroy(cmd.Context(), p, envelopes, g)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if isStructured(a.output) {
				return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, destroyOutput{Plan: p, Result: resultPayload(result)}, nil)
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
			eng, err := a.newEngine()
			if err != nil {
				return err
			}
			unlock, err := eng.StateStore.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			results, err := eng.Probe(cmd.Context(), envelopes)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}

			rows := [][]string{{"RESOURCE", "READY", "DRIFT", "REASON", "MESSAGE"}}
			type driftRow struct {
				Resource string `json:"resource"`
				Ready    string `json:"ready"`
				Drift    string `json:"drift"`
				Reason   string `json:"reason"`
				// Message carries the observed-vs-desired detail a Reason
				// enum can't (e.g. which connector config keys drifted, or
				// "wal_level is replica, want logical") — probes set it,
				// but every consumer before this dropped it on the floor.
				Message string `json:"message,omitempty"`
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
					row.Message = c.Message
				}
				if engine.HasDrift(r.Status) {
					drifted++
				}
				data = append(data, row)
				rows = append(rows, []string{row.Resource, row.Ready, row.Drift, row.Reason, row.Message})
			}
			payload := driftOutput{Resources: data, Drifted: drifted}
			if isStructured(a.output) {
				if err := cliutil.WriteOutput(cmd.OutOrStdout(), a.output, payload, nil); err != nil {
					return err
				}
			} else if err := cliutil.WriteOutput(cmd.OutOrStdout(), a.output, data, rows); err != nil {
				return err
			}
			if drifted > 0 {
				if !isStructured(a.output) {
					fmt.Fprintf(cmd.OutOrStdout(), "\ndrift detected on %d resource(s); run apply to reconcile\n", drifted)
				}
				return cliutil.Exit(cliutil.ExitPlanChanges, nil)
			}
			if !isStructured(a.output) {
				fmt.Fprintln(cmd.OutOrStdout(), "\nno drift detected")
			}
			return nil
		},
	}
}

func newImportCmd(a *app) *cobra.Command {
	var from string
	var namespace string
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
			key, err := resource.ParseSelector(args[0], namespace)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			envelopes, _, err := a.loadAndValidate(pathArg(args[1:]))
			if err != nil {
				return err
			}
			eng, err := a.newEngine()
			if err != nil {
				return err
			}
			unlock, err := eng.StateStore.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			if err := eng.PreflightSecrets(cmd.Context(), envelopes); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			probed, err := eng.Import(cmd.Context(), envelopes, key, from)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			reason := ""
			if c, ok := probed.Condition(status.Ready); ok {
				reason = c.Reason
			}
			if isStructured(a.output) {
				return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, importOutput{Key: key, Ready: reason}, nil)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "imported %s (Ready: %s)\n", key, reason)
			return nil
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "name of the existing backing object to adopt (must equal metadata.name in v1)")
	cmd.Flags().StringVar(&namespace, "namespace", resource.DefaultNamespace, "namespace for the <Kind>/<name> selector")
	_ = cmd.MarkFlagRequired("from")
	return cmd
}

func newInventoryCmd(a *app) *cobra.Command {
	var forTool string
	cmd := &cobra.Command{
		Use:     "inventory [path]",
		Aliases: []string{"services", "endpoints"},
		Short:   "List the service endpoints of an applied platform (for configuring external tools)",
		Long: "Surfaces the reachable endpoints each applied component publishes — the stable\n" +
			"access identifiers you point orchestrators (Dagster), BI tools (Metabase), or a\n" +
			"psql/mc client at — with the SecretReference that holds each one's credentials.\n" +
			"Reads recorded state; components not yet applied show no endpoints.\n" +
			"--for renders a paste-ready config snippet for a specific tool (" + toolNames() + ")\n" +
			"from the same recorded endpoints; secret values are never rendered, only the\n" +
			"SecretReference names holding them.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envelopes, _, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			store, err := a.stateStore()
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			st, err := store.Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			creds := credentialRefs(envelopes)
			if forTool != "" {
				if isStructured(a.output) {
					snippet, err := renderToolConfigString(forTool, gatherToolFacts(envelopes, st, creds))
					if err != nil {
						return cliutil.Exit(cliutil.ExitValidation, err)
					}
					return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, toolConfigOutput{Tool: forTool, Config: snippet}, nil)
				}
				if err := renderToolConfig(cmd.OutOrStdout(), forTool, gatherToolFacts(envelopes, st, creds)); err != nil {
					return cliutil.Exit(cliutil.ExitValidation, err)
				}
				return nil
			}

			type invRow struct {
				Component string `json:"component"`
				Endpoint  string `json:"endpoint"`
				Scheme    string `json:"scheme"`
				Host      string `json:"host"`
				InNetwork string `json:"inNetwork"`
				Secret    string `json:"credentials"`
				// Insecure surfaces plaintext (no-TLS) endpoints explicitly —
				// local-dev defaults are insecure and must say so
				// (docs/planning/07 §2.3/§2.5).
				Insecure bool `json:"insecure,omitempty"`
			}
			rows := [][]string{{"COMPONENT", "ENDPOINT", "SCHEME", "HOST (from your machine)", "IN-NETWORK", "CREDENTIALS", "SECURITY"}}
			data := []invRow{}
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
						Insecure:  ep.Insecure,
					}
					security := "tls"
					if r.Insecure {
						security = "plaintext (local only)"
					}
					data = append(data, r)
					rows = append(rows, []string{r.Component, r.Endpoint, r.Scheme, r.Host, r.InNetwork, dash(r.Secret), security})
				}
			}
			if len(data) == 0 {
				if isStructured(a.output) {
					return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, inventoryOutput{Endpoints: data}, nil)
				}
				fmt.Fprintln(cmd.OutOrStdout(), "no service endpoints recorded — apply the platform first")
				return nil
			}
			if isStructured(a.output) {
				return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, inventoryOutput{Endpoints: data}, nil)
			}
			return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, data, rows)
		},
	}
	cmd.Flags().StringVar(&forTool, "for", "", "render a paste-ready config snippet for a tool: "+toolNames())
	return cmd
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
			store, err := a.stateStore()
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			st, err := store.Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			rows := [][]string{{"RESOURCE", "READY", "DRIFT", "REASON", "MESSAGE", "LIFECYCLE"}}
			type statusRow struct {
				Resource string `json:"resource"`
				Ready    string `json:"ready"`
				Drift    string `json:"drift"`
				Reason   string `json:"reason"`
				// Message carries the observed-vs-desired detail a Reason
				// enum can't (e.g. "wal_level is replica, want logical").
				Message   string `json:"message,omitempty"`
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
						row.Message = c.Message
					}
					// Recorded by the last `drift` probe; "-" means never probed.
					if c, found := rs.Status.Condition(status.DriftDetected); found {
						row.Drift = string(c.Status)
					}
				} else {
					row.Reason = "NotApplied"
				}
				data = append(data, row)
				rows = append(rows, []string{row.Resource, row.Ready, row.Drift, row.Reason, row.Message, row.Lifecycle})
			}
			return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, data, rows)
		},
	}
}

func newGraphCmd(a *app) *cobra.Command {
	var graphFormat string
	cmd := &cobra.Command{
		Use:   "graph [path]",
		Short: "Render the platform architecture (data flow + technology layer)",
		Long: "Renders the architecture the manifests describe — data-movement pipelines\n" +
			"(Bindings collapse into labelled source→target edges) and the technology layer\n" +
			"(which Provider realizes each asset, and how external systems are reached).\n" +
			"This is the picture you configure orchestrators against, not the internal\n" +
			"reconcile ordering. Choose the graph format with --format: tree (default), dot, mermaid, json.\n" +
			"-o json|yaml overrides --format with a structured node/edge document (the root\n" +
			"output contract: exactly one parseable document on stdout).",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envelopes, _, err := a.loadAndValidate(pathArg(args))
			if err != nil {
				return err
			}
			view := archview.Build(envelopes)
			if isStructured(a.output) {
				if cmd.Flags().Changed("format") && graphFormat != "json" {
					fmt.Fprintf(cmd.ErrOrStderr(), "warning: -o %s overrides --format %s for graph\n", a.output, graphFormat)
				}
				if err := cliutil.WriteOutput(cmd.OutOrStdout(), a.output, graphView(view), nil); err != nil {
					return cliutil.Exit(cliutil.ExitValidation, err)
				}
				return nil
			}
			if err := view.Render(cmd.OutOrStdout(), graphFormat); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&graphFormat, "format", "tree", "graph format: tree|dot|mermaid|json")
	return cmd
}

// graphViewOutput mirrors archview's own JSON shape (renderJSON) so -o
// json|yaml produces the identical document --format json already does,
// without archview needing to expose an encoder for each format.
type graphViewOutput struct {
	Nodes []archview.Node `json:"nodes" yaml:"nodes"`
	Edges []graphEdge     `json:"edges" yaml:"edges"`
}

type graphEdge struct {
	From, To, Kind, Label string
}

func graphView(v *archview.View) graphViewOutput {
	out := graphViewOutput{Nodes: v.Nodes}
	for _, e := range v.Edges {
		out.Edges = append(out.Edges, graphEdge{e.From.String(), e.To.String(), string(e.Kind), e.Label})
	}
	return out
}

func printPlan(cmd *cobra.Command, output string, p planpkg.Plan) error {
	return printPlanTo(cmd.OutOrStdout(), output, p)
}

func printPlanTo(w io.Writer, output string, p planpkg.Plan) error {
	rows := [][]string{{"RESOURCE", "ACTION", "REASON"}}
	for _, e := range p.Entries {
		rows = append(rows, []string{e.Key.String(), string(e.Action), e.Reason})
	}
	return cliutil.WriteOutput(w, output, p, rows)
}

func pathArg(args []string) string {
	if len(args) > 0 {
		return args[0]
	}
	return "."
}

type applyOutput struct {
	Plan      planpkg.Plan  `json:"plan" yaml:"plan"`
	Result    *resultOutput `json:"result,omitempty" yaml:"result,omitempty"`
	Cancelled bool          `json:"cancelled,omitempty" yaml:"cancelled,omitempty"`
}

type destroyOutput struct {
	Plan      planpkg.Plan  `json:"plan" yaml:"plan"`
	Result    *resultOutput `json:"result,omitempty" yaml:"result,omitempty"`
	Cancelled bool          `json:"cancelled,omitempty" yaml:"cancelled,omitempty"`
}

type resultOutput struct {
	Succeeded []resource.Key    `json:"succeeded" yaml:"succeeded"`
	Failed    map[string]string `json:"failed,omitempty" yaml:"failed,omitempty"`
	Skipped   []resource.Key    `json:"skipped,omitempty" yaml:"skipped,omitempty"`
}

type driftOutput struct {
	Resources any `json:"resources" yaml:"resources"`
	Drifted   int `json:"drifted" yaml:"drifted"`
}

type inventoryOutput struct {
	Endpoints any `json:"endpoints" yaml:"endpoints"`
}

type toolConfigOutput struct {
	Tool   string `json:"tool" yaml:"tool"`
	Config string `json:"config" yaml:"config"`
}

type importOutput struct {
	Key   resource.Key `json:"key" yaml:"key"`
	Ready string       `json:"ready" yaml:"ready"`
}

func isStructured(output string) bool {
	return output == "json" || output == "yaml"
}

func humanWriter(cmd *cobra.Command, output string) io.Writer {
	if isStructured(output) {
		return cmd.ErrOrStderr()
	}
	return cmd.OutOrStdout()
}

func resultPayload(result engine.Result) *resultOutput {
	out := &resultOutput{
		Succeeded: result.Succeeded,
		Skipped:   result.Skipped,
	}
	if len(result.Failed) > 0 {
		out.Failed = make(map[string]string, len(result.Failed))
		for key, err := range result.Failed {
			out.Failed[key.String()] = err.Error()
		}
	}
	return out
}

// isTTY reports whether w is an interactive terminal (a character device),
// so colour is only emitted when a human is watching. Honours NO_COLOR.
func isTTY(w io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
