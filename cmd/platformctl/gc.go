package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/adapters/state/localfile"
	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// This file implements docs/planning/07 §1.3 / docs/planning/08 A2: garbage
// collection and orphan inspection. Ownership labels are already on every
// created object (runtime.ManagedLabels); `gc plan` diffs every labeled
// runtime object against state.Resources and reports exactly what state
// does not account for, grouped by the namespace/kind/name the labels
// carry — never anything unlabeled, which the runtime port's ownership
// guards already refuse to touch on the removal side.

// gcOrphan is one runtime object (container/network/volume) carrying this
// project's ownership labels that no state entry accounts for.
type gcOrphan struct {
	Object    string `json:"object" yaml:"object"` // "container:<name>" | "network:<name>" | "volume:<name>"
	Namespace string `json:"namespace" yaml:"namespace"`
	Kind      string `json:"kind" yaml:"kind"`
	Name      string `json:"name" yaml:"name"`
}

type gcPlanOutput struct {
	Orphans []gcOrphan `json:"orphans" yaml:"orphans"`
}

type gcApplyOutput struct {
	Removed []gcOrphan        `json:"removed" yaml:"removed"`
	Failed  map[string]string `json:"failed,omitempty" yaml:"failed,omitempty"`
}

func newGCCmd(a *app) *cobra.Command {
	gc := &cobra.Command{
		Use:   "gc",
		Short: "Inspect and clean up runtime objects state no longer accounts for",
		Long: "Ownership labels are on every object platformctl creates. `gc plan` lists every\n" +
			"labeled container, network, and volume whose namespace/kind/name has no matching\n" +
			"state entry — e.g. left behind by a crash before state was written. `gc apply`\n" +
			"removes exactly that list. Neither command touches unlabeled objects.",
	}

	var planRuntime string
	planCmd := &cobra.Command{
		Use:   "plan",
		Short: "List orphaned managed objects (read-only)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			orphans, err := a.gcOrphans(cmd.Context(), planRuntime)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			rows := [][]string{{"OBJECT", "NAMESPACE", "KIND", "NAME"}}
			for _, o := range orphans {
				rows = append(rows, []string{o.Object, o.Namespace, o.Kind, o.Name})
			}
			return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, gcPlanOutput{Orphans: orphans}, rows)
		},
	}
	planCmd.Flags().StringVar(&planRuntime, "runtime", "docker", "runtime type to scan (docker|kubernetes)")

	var applyRuntime string
	var destructiveOK bool
	applyCmd := &cobra.Command{
		Use:   "apply",
		Short: "Remove exactly the objects gc plan lists",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !destructiveOK {
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("gc apply requires --yes-i-understand-this-is-destructive"))
			}
			orphans, err := a.gcOrphans(cmd.Context(), applyRuntime)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			rt, err := a.reg.Runtime(applyRuntime, map[string]any{})
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			out := gcApplyOutput{}
			for _, o := range orphans {
				kind, name, _ := strings.Cut(o.Object, ":")
				var rmErr error
				switch kind {
				case "container":
					rmErr = rt.Remove(cmd.Context(), name)
				case "network":
					rmErr = rt.RemoveNetwork(cmd.Context(), name)
				case "volume":
					rmErr = rt.RemoveVolume(cmd.Context(), name)
				default:
					rmErr = fmt.Errorf("unknown object kind %q", kind)
				}
				if rmErr != nil {
					if out.Failed == nil {
						out.Failed = map[string]string{}
					}
					out.Failed[o.Object] = rmErr.Error()
					continue
				}
				out.Removed = append(out.Removed, o)
			}
			rows := [][]string{{"OBJECT", "STATUS"}}
			for _, o := range out.Removed {
				rows = append(rows, []string{o.Object, "removed"})
			}
			for obj, errMsg := range out.Failed {
				rows = append(rows, []string{obj, "failed: " + errMsg})
			}
			if err := cliutil.WriteOutput(cmd.OutOrStdout(), a.output, out, rows); err != nil {
				return err
			}
			if len(out.Failed) > 0 {
				return cliutil.Exit(cliutil.ExitExecution, fmt.Errorf("%d object(s) failed to remove", len(out.Failed)))
			}
			return nil
		},
	}
	applyCmd.Flags().StringVar(&applyRuntime, "runtime", "docker", "runtime type to scan (docker|kubernetes)")
	applyCmd.Flags().BoolVar(&destructiveOK, "yes-i-understand-this-is-destructive", false, "required — gc apply deletes runtime objects")

	gc.AddCommand(planCmd, applyCmd)
	return gc
}

// gcOrphans lists every labeled container/network/volume whose
// namespace/kind/name (read from its ownership labels) has no matching
// entry in state.Resources.
func (a *app) gcOrphans(ctx context.Context, runtimeType string) ([]gcOrphan, error) {
	rt, err := a.reg.Runtime(runtimeType, map[string]any{})
	if err != nil {
		return nil, err
	}
	st, err := localfile.New(a.stateFile).Load(ctx)
	if err != nil {
		return nil, err
	}
	accounted := func(namespace, kind, name string) bool {
		if kind == "" || name == "" {
			// Not one of our labeled objects in the first place — never
			// reported (ListManaged*/RemoveX already scope to labeled
			// objects only, but be defensive against a malformed label set).
			return true
		}
		key := resource.Key{Namespace: resource.NormalizeNamespace(namespace), Kind: kind, Name: name}
		_, ok := st.Resources[key]
		return ok
	}

	var orphans []gcOrphan
	containers, err := rt.ListManaged(ctx)
	if err != nil {
		return nil, fmt.Errorf("list managed containers: %w", err)
	}
	for _, c := range containers {
		ns, kind, name := c.Labels[runtime.LabelNamespace], c.Labels[runtime.LabelKind], c.Labels[runtime.LabelName]
		if !accounted(ns, kind, name) {
			orphans = append(orphans, gcOrphan{Object: "container:" + c.Name, Namespace: ns, Kind: kind, Name: name})
		}
	}
	nets, err := rt.ListManagedNetworks(ctx)
	if err != nil {
		return nil, fmt.Errorf("list managed networks: %w", err)
	}
	for _, n := range nets {
		ns, kind, name := n.Labels[runtime.LabelNamespace], n.Labels[runtime.LabelKind], n.Labels[runtime.LabelName]
		if !accounted(ns, kind, name) {
			orphans = append(orphans, gcOrphan{Object: "network:" + n.Name, Namespace: ns, Kind: kind, Name: name})
		}
	}
	vols, err := rt.ListManagedVolumes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list managed volumes: %w", err)
	}
	for _, v := range vols {
		ns, kind, name := v.Labels[runtime.LabelNamespace], v.Labels[runtime.LabelKind], v.Labels[runtime.LabelName]
		if !accounted(ns, kind, name) {
			orphans = append(orphans, gcOrphan{Object: "volume:" + v.Name, Namespace: ns, Kind: kind, Name: name})
		}
	}

	sort.Slice(orphans, func(i, j int) bool { return orphans[i].Object < orphans[j].Object })
	return orphans, nil
}
