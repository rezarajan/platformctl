package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
	"github.com/rezarajan/platformctl/internal/ports/state"
)

// This file implements docs/planning/07 §1.4 / docs/planning/08 A3: state
// inspection and repair. `state inspect` dumps the normalized state
// (read-only); `state doctor` reports defect classes without changing
// anything; `state repair` applies doctor's safe fixes. The migration chain
// itself (formalized scaffolding for future format changes) lives in
// internal/ports/state/state.go, not here.

type stateEntryOutput struct {
	Key            string `json:"key" yaml:"key"`
	SpecHash       string `json:"specHash" yaml:"specHash"`
	Lifecycle      string `json:"lifecycle" yaml:"lifecycle"`
	Imported       bool   `json:"imported,omitempty" yaml:"imported,omitempty"`
	HasLastApplied bool   `json:"hasLastApplied" yaml:"hasLastApplied"`
}

type stateInspectOutput struct {
	Version   int                `json:"version" yaml:"version"`
	Resources []stateEntryOutput `json:"resources" yaml:"resources"`
}

// doctorFindings is state doctor's typed diagnosis. Keys stay typed
// (resource.Key) here so repair can act on them directly; CLI output
// marshals them to their string form via report().
type doctorFindings struct {
	FileVersion    int
	CurrentVersion int
	LegacyOrphans  []resource.Key // state.ResourceState.LastApplied == nil — same predicate plan.computeApplyDeletes uses
	CorruptEntries []resource.Key // the map key and rs.LastApplied.Key() disagree
	GoneObjects    []resource.Key // Kind == "Provider", LastApplied != nil, but the runtime reports no such container
}

func (f doctorFindings) Healthy() bool {
	return f.FileVersion >= f.CurrentVersion &&
		len(f.LegacyOrphans) == 0 && len(f.CorruptEntries) == 0 && len(f.GoneObjects) == 0
}

type stateDoctorReport struct {
	FileVersion    int      `json:"fileVersion" yaml:"fileVersion"`
	CurrentVersion int      `json:"currentVersion" yaml:"currentVersion"`
	StaleFormat    bool     `json:"staleFormat" yaml:"staleFormat"`
	LegacyOrphans  []string `json:"legacyOrphans,omitempty" yaml:"legacyOrphans,omitempty"`
	CorruptEntries []string `json:"corruptEntries,omitempty" yaml:"corruptEntries,omitempty"`
	GoneObjects    []string `json:"goneObjects,omitempty" yaml:"goneObjects,omitempty"`
}

func (f doctorFindings) report() stateDoctorReport {
	toStrings := func(keys []resource.Key) []string {
		if len(keys) == 0 {
			return nil
		}
		out := make([]string, len(keys))
		for i, k := range keys {
			out[i] = k.String()
		}
		return out
	}
	return stateDoctorReport{
		FileVersion:    f.FileVersion,
		CurrentVersion: f.CurrentVersion,
		StaleFormat:    f.FileVersion < f.CurrentVersion,
		LegacyOrphans:  toStrings(f.LegacyOrphans),
		CorruptEntries: toStrings(f.CorruptEntries),
		GoneObjects:    toStrings(f.GoneObjects),
	}
}

// readFileVersion peeks at the on-disk version without going through
// State.Normalize, which always reports CurrentVersion once loaded into
// memory — doctor needs to know whether the *file* still carries a stale
// format, i.e. whether a migration ran in memory but was never persisted.
func readFileVersion(path string) (int, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return state.CurrentVersion, nil // nothing written yet — nothing to migrate
	}
	if err != nil {
		return 0, fmt.Errorf("read state file %s: %w", path, err)
	}
	var probe struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return 0, fmt.Errorf("parse state file %s: %w", path, err)
	}
	if probe.Version == 0 {
		return 1, nil // v1 files never wrote a version field
	}
	return probe.Version, nil
}

// stateDoctor loads state and runs every doctor check. Returns the loaded
// state too so repair can act on it without a second Load.
func (a *app) stateDoctor(ctx context.Context, runtimeType string) (doctorFindings, state.State, error) {
	fileVersion, err := readFileVersion(a.stateFile)
	if err != nil {
		return doctorFindings{}, state.State{}, err
	}
	st, err := localfile.New(a.stateFile).Load(ctx)
	if err != nil {
		return doctorFindings{}, state.State{}, err
	}
	findings := doctorFindings{FileVersion: fileVersion, CurrentVersion: state.CurrentVersion}

	keys := make([]resource.Key, 0, len(st.Resources))
	for k := range st.Resources {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	// Constructed lazily, once, only when at least one Provider entry needs
	// checking — a state file with none never dials the runtime at all.
	var rt runtime.ContainerRuntime
	for _, key := range keys {
		rs := st.Resources[key]
		if rs.LastApplied == nil {
			findings.LegacyOrphans = append(findings.LegacyOrphans, key)
			continue
		}
		if rs.LastApplied.Key() != key {
			findings.CorruptEntries = append(findings.CorruptEntries, key)
			continue
		}
		if key.Kind != "Provider" {
			continue
		}
		if rt == nil {
			rt, err = a.reg.Runtime(runtimeType, map[string]any{})
			if err != nil {
				return findings, st, err
			}
		}
		_, found, err := rt.Inspect(ctx, key.Name)
		if err != nil {
			return findings, st, err
		}
		if !found {
			findings.GoneObjects = append(findings.GoneObjects, key)
		}
	}
	return findings, st, nil
}

func doctorRows(report stateDoctorReport) [][]string {
	rows := [][]string{{"CHECK", "FINDING"}}
	rows = append(rows, []string{"format version",
		fmt.Sprintf("file=v%d current=v%d stale=%v", report.FileVersion, report.CurrentVersion, report.StaleFormat)})
	for _, k := range report.LegacyOrphans {
		rows = append(rows, []string{"legacy orphan (no last-applied manifest)", k})
	}
	for _, k := range report.CorruptEntries {
		rows = append(rows, []string{"corrupt (state key disagrees with its manifest's own key)", k})
	}
	for _, k := range report.GoneObjects {
		rows = append(rows, []string{"runtime object gone", k})
	}
	return rows
}

func newStateCmd(a *app) *cobra.Command {
	stateCmd := &cobra.Command{
		Use:   "state",
		Short: "Inspect and repair the state file",
	}

	inspectCmd := &cobra.Command{
		Use:   "inspect",
		Short: "Dump the normalized state (read-only)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			st, err := localfile.New(a.stateFile).Load(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			keys := make([]resource.Key, 0, len(st.Resources))
			for k := range st.Resources {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

			out := stateInspectOutput{Version: st.Version}
			rows := [][]string{{"KEY", "SPEC HASH", "LIFECYCLE", "IMPORTED", "HAS LAST-APPLIED"}}
			for _, k := range keys {
				rs := st.Resources[k]
				e := stateEntryOutput{
					Key:            k.String(),
					SpecHash:       rs.SpecHash,
					Lifecycle:      rs.Lifecycle,
					Imported:       rs.Imported,
					HasLastApplied: rs.LastApplied != nil,
				}
				out.Resources = append(out.Resources, e)
				rows = append(rows, []string{e.Key, e.SpecHash, e.Lifecycle, fmt.Sprintf("%v", e.Imported), fmt.Sprintf("%v", e.HasLastApplied)})
			}
			return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, out, rows)
		},
	}

	var doctorRuntime string
	doctorCmd := &cobra.Command{
		Use:   "doctor",
		Short: "Report state defects without changing anything",
		Long: "Checks: the on-disk format version (stale means a migration ran in memory but\n" +
			"was never persisted), legacy orphan entries (no last-applied manifest — the same\n" +
			"class apply refuses to delete), corrupt entries (a state key that disagrees with\n" +
			"its own manifest's key), and Provider entries whose backing container the runtime\n" +
			"reports gone. Exits 1 when any check finds something; run `state repair` to fix.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			findings, _, err := a.stateDoctor(cmd.Context(), doctorRuntime)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			report := findings.report()
			if err := cliutil.WriteOutput(cmd.OutOrStdout(), a.output, report, doctorRows(report)); err != nil {
				return err
			}
			if !findings.Healthy() {
				return cliutil.Exit(cliutil.ExitPlanChanges, nil)
			}
			return nil
		},
	}
	doctorCmd.Flags().StringVar(&doctorRuntime, "runtime", "docker", "runtime type to check Provider liveness against (docker|kubernetes)")

	var repairRuntime string
	var autoApprove bool
	repairCmd := &cobra.Command{
		Use:   "repair",
		Short: "Apply doctor's safe fixes",
		Long: "Persists a migrated state format when the on-disk file is stale, and drops state\n" +
			"entries for Provider objects doctor confirmed the runtime no longer has (asks for\n" +
			"confirmation unless --yes). Never touches legacy-orphan or corrupt entries — those\n" +
			"have no safe automatic fix; destroy or a manual state edit remains the remedy.\n" +
			"A no-op (no write) when state is already healthy.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			findings, st, err := a.stateDoctor(cmd.Context(), repairRuntime)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if findings.Healthy() {
				if !isStructured(a.output) {
					fmt.Fprintln(cmd.OutOrStdout(), "state is healthy; nothing to repair")
				}
				return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, repairOutput{}, [][]string{{"ACTION", "DETAIL"}})
			}

			store := localfile.New(a.stateFile)
			unlock, err := store.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			var applied []repairAction
			if len(findings.GoneObjects) > 0 {
				if !autoApprove {
					fmt.Fprintf(humanWriter(cmd, a.output), "Drop %d state entry(ies) for confirmed-gone Provider objects? Only 'yes' is accepted: ", len(findings.GoneObjects))
					var answer string
					fmt.Fscanln(cmd.InOrStdin(), &answer) //nolint:errcheck
					if answer != "yes" {
						if !isStructured(a.output) {
							fmt.Fprintln(cmd.OutOrStdout(), "repair cancelled")
						}
						return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, repairOutput{Cancelled: true}, [][]string{{"ACTION", "DETAIL"}})
					}
				}
				for _, key := range findings.GoneObjects {
					delete(st.Resources, key)
					applied = append(applied, repairAction{Action: "dropped-gone-object", Detail: key.String()})
				}
			}
			if err := store.Save(cmd.Context(), st); err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if findings.FileVersion < findings.CurrentVersion {
				applied = append(applied, repairAction{Action: "migrated-format", Detail: fmt.Sprintf("v%d -> v%d", findings.FileVersion, findings.CurrentVersion)})
			}

			rows := [][]string{{"ACTION", "DETAIL"}}
			for _, act := range applied {
				rows = append(rows, []string{act.Action, act.Detail})
			}
			return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, repairOutput{Applied: applied}, rows)
		},
	}
	repairCmd.Flags().StringVar(&repairRuntime, "runtime", "docker", "runtime type to check Provider liveness against (docker|kubernetes)")
	repairCmd.Flags().BoolVar(&autoApprove, "yes", false, "skip the interactive confirmation for dropping gone-object entries (for CI)")

	stateCmd.AddCommand(inspectCmd, doctorCmd, repairCmd)
	return stateCmd
}

type repairAction struct {
	Action string `json:"action" yaml:"action"`
	Detail string `json:"detail" yaml:"detail"`
}

type repairOutput struct {
	Applied   []repairAction `json:"applied,omitempty" yaml:"applied,omitempty"`
	Cancelled bool           `json:"cancelled,omitempty" yaml:"cancelled,omitempty"`
}
