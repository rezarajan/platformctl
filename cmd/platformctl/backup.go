package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// newBackupCmd implements docs/planning/08 C6: stream a data-bearing
// resource's contents to an object-store destination via its provider's
// reconciler.BackupCapableProvider. Gated (Alpha, disabled by default) —
// see docs/design/007-backup-restore.md for the seam this is built on.
func newBackupCmd(a *app) *cobra.Command {
	var to, credentialsSecretRef, namespace string
	cmd := &cobra.Command{
		Use:   "backup <Kind/name> [path]",
		Short: "Stream a data-bearing resource's contents to an object-store destination (Alpha; BackupRestore gate)",
		Long: "Resolves the named resource's realizing Provider and, if it implements\n" +
			"BackupCapableProvider (postgres, mysql, s3 in v1), streams its data to --to.\n" +
			"Scheduling stays external (cron/CI) — this is the primitive, not a scheduler.",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := a.gates.Require("BackupRestore"); err != nil {
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
			dest, err := eng.ResolveObjectStoreLocation(cmd.Context(), envelopes, to, credentialsSecretRef, "", namespace)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			manifest, err := eng.Backup(cmd.Context(), envelopes, key, dest)
			if err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if isStructured(a.output) {
				return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, manifest, nil)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "backed up %s to %s/%s (format: %s)\n", key, manifest.Destination.Bucket, manifest.Destination.Key, manifest.Format)
			return nil
		},
	}
	cmd.Flags().StringVar(&to, "to", "", "backup destination: Kind/name of a Dataset, or a scheme://host[:port]/bucket[/prefix] URL")
	cmd.Flags().StringVar(&credentialsSecretRef, "credentials-secret-ref", "", "SecretReference providing accessKey/secretKey for a URL --to; ignored for a Dataset --to")
	cmd.Flags().StringVar(&namespace, "namespace", resource.DefaultNamespace, "namespace for the <Kind>/<name> selector and a Dataset --to")
	_ = cmd.MarkFlagRequired("to")
	return cmd
}

// newRestoreCmd implements docs/planning/08 C6's restore-over-existing-data
// safety: restore always overwrites the target's current data, so it
// refuses outright — before touching any infrastructure, state, or secret
// store — unless --yes-i-understand-this-overwrites-existing-data is passed
// (the NFR-3-style opt-in, mirroring destroy's
// --yes-i-understand-this-is-destructive/--include-external pair).
func newRestoreCmd(a *app) *cobra.Command {
	var from, credentialsSecretRef, object, namespace string
	var confirmOverwrite bool
	cmd := &cobra.Command{
		Use:   "restore <Kind/name> [path]",
		Short: "Stream an object-store backup back into a data-bearing resource, overwriting its current data (Alpha; BackupRestore gate)",
		Long: "Resolves the named resource's realizing Provider and, if it implements\n" +
			"BackupCapableProvider (postgres, mysql, s3 in v1), streams --from back into it.\n" +
			"This always overwrites whatever data the resource currently holds — it refuses\n" +
			"outright without --yes-i-understand-this-overwrites-existing-data, before any\n" +
			"infrastructure, state, or secret store is touched.",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !confirmOverwrite {
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("restore always overwrites the target's existing data; re-run with --yes-i-understand-this-overwrites-existing-data"))
			}
			if err := a.gates.Require("BackupRestore"); err != nil {
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
			eng.AllowOverwrite = confirmOverwrite
			unlock, err := eng.StateStore.Lock(cmd.Context())
			if err != nil {
				return cliutil.Exit(cliutil.ExitLockHeld, err)
			}
			defer unlock() //nolint:errcheck

			if err := eng.PreflightSecrets(cmd.Context(), envelopes); err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			src, err := eng.ResolveObjectStoreLocation(cmd.Context(), envelopes, from, credentialsSecretRef, object, namespace)
			if err != nil {
				return cliutil.Exit(cliutil.ExitValidation, err)
			}
			if err := eng.Restore(cmd.Context(), envelopes, key, src); err != nil {
				return cliutil.Exit(cliutil.ExitExecution, err)
			}
			if isStructured(a.output) {
				return cliutil.WriteOutput(cmd.OutOrStdout(), a.output, restoreOutput{Key: key, RestoredFrom: backup.RefOf(src, src.Prefix)}, nil)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "restored %s from %s/%s\n", key, src.Bucket, src.Prefix)
			return nil
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "restore source: Kind/name of a Dataset (combine with --object), or a scheme://host[:port]/bucket/key URL")
	cmd.Flags().StringVar(&object, "object", "", "exact object key to restore, relative to --from Dataset's prefix (required when --from is a Dataset; ignored for a URL, whose path already names the object)")
	cmd.Flags().StringVar(&credentialsSecretRef, "credentials-secret-ref", "", "SecretReference providing accessKey/secretKey for a URL --from; ignored for a Dataset --from")
	cmd.Flags().StringVar(&namespace, "namespace", resource.DefaultNamespace, "namespace for the <Kind>/<name> selector and a Dataset --from")
	cmd.Flags().BoolVar(&confirmOverwrite, "yes-i-understand-this-overwrites-existing-data", false, "required: restore always overwrites the target's existing data")
	_ = cmd.MarkFlagRequired("from")
	return cmd
}

type restoreOutput struct {
	Key          resource.Key `json:"key" yaml:"key"`
	RestoredFrom backup.Ref   `json:"restoredFrom" yaml:"restoredFrom"`
}
