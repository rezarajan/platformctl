package compose

import "fmt"

// SourceOptions is `platformctl add source`'s flag-mode input: a
// standalone managed database (Provider + Source + admin/replication
// SecretReferences). No CDC/broker wiring — that is `add pipeline` or a
// follow-up `wire cdc`.
type SourceOptions struct {
	Name     string // required: names the Source and derives every other resource name
	Engine   string // required: postgres | mysql | mariadb
	Database string // default: Name
}

// PlanSource computes the manifest patch for `add source`.
func PlanSource(snap Snapshot, dir string, opts SourceOptions) (Patch, error) {
	const command = "add source"
	if opts.Name == "" {
		return Patch{}, fmt.Errorf("--name is required")
	}
	spec, err := lookupDBEngine(opts.Engine)
	if err != nil {
		return Patch{}, err
	}
	database := opts.Database
	if database == "" {
		database = opts.Name
	}

	providerName := opts.Name + "-db"
	adminSecret := opts.Name + "-admin-creds"
	replSecret := opts.Name + "-replication-creds"

	providerDoc := renderDBProvider(command, spec, providerName, adminSecret, replSecret)
	sourceDoc := renderSource(command, opts.Engine, opts.Name, providerName, database)
	adminDoc, replDoc, envAppends := renderSecretPair(command, adminSecret, spec.AdminUser, replSecret)

	patch := Patch{Command: command, Dir: dir}
	for _, f := range []struct{ kind, name, content string }{
		{"Provider", providerName, providerDoc},
		{"Source", opts.Name, sourceDoc},
		{"SecretReference", adminSecret, adminDoc},
		{"SecretReference", replSecret, replDoc},
	} {
		op, err := resolveFile(dir, snap, f.kind, f.name, f.content)
		if err != nil {
			return Patch{}, err
		}
		patch.Files = append(patch.Files, op)
	}

	pending, err := resolveEnvAppends(dir, envAppends)
	if err != nil {
		return Patch{}, err
	}
	patch.EnvAppends = pending
	return patch, nil
}
