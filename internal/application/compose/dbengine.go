package compose

import (
	"fmt"
	"sort"
	"strings"
)

// dbEngineSpec captures the small, closed set of facts genuinely specific
// to each database engine compose knows how to scaffold a managed instance
// for: the Provider type (identical to the engine name for every shipped
// engine today), its default pinned version, and the Provider.spec.
// configuration field naming the admin-credentials SecretReference
// (postgres and mysql/mariadb spell this differently —
// superuserSecretRef vs rootSecretRef). Everything else (secret plumbing,
// CDC worker wiring, file layout) is engine-agnostic and lives in
// source.go/pipeline.go.
type dbEngineSpec struct {
	ProviderType     string
	DefaultVersion   string
	AdminSecretField string
	AdminUser        string
}

var dbEngines = map[string]dbEngineSpec{
	"postgres": {ProviderType: "postgres", DefaultVersion: "16", AdminSecretField: "superuserSecretRef", AdminUser: "admin"},
	"mysql":    {ProviderType: "mysql", DefaultVersion: "8.4", AdminSecretField: "rootSecretRef", AdminUser: "root"},
	"mariadb":  {ProviderType: "mariadb", DefaultVersion: "11", AdminSecretField: "rootSecretRef", AdminUser: "root"},
}

func lookupDBEngine(engine string) (dbEngineSpec, error) {
	spec, ok := dbEngines[engine]
	if !ok {
		known := make([]string, 0, len(dbEngines))
		for k := range dbEngines {
			known = append(known, k)
		}
		sort.Strings(known)
		return dbEngineSpec{}, fmt.Errorf("engine %q is not a composable database engine (known: %s)", engine, strings.Join(known, ", "))
	}
	return spec, nil
}

// renderDBProvider renders a managed database Provider (postgres/mysql/
// mariadb): version pinned, admin+replication SecretReferences wired.
func renderDBProvider(command string, spec dbEngineSpec, providerName, adminSecretRef, replSecretRef string) string {
	explain := fmt.Sprintf(
		"Managed %s — the database captured by the CDC worker below.\n"+
			"spec.configuration.image is omitted: the provider selects its default\n"+
			"image for the pinned version. The host port is omitted too: auto-\n"+
			"assigned from the resource name.",
		spec.ProviderType)
	lines := []string{
		"type: " + spec.ProviderType,
		"runtime:",
		"  type: docker",
		"configuration:",
		fmt.Sprintf("  version: %q", spec.DefaultVersion),
		fmt.Sprintf("  %s: %s", spec.AdminSecretField, adminSecretRef),
		"  replicationSecretRef: " + replSecretRef,
		"secretRefs: " + flowList([]string{adminSecretRef, replSecretRef}),
	}
	return renderDoc(command, explain, "Provider", providerName, lines)
}

// renderSource renders a managed Source for engine, whose spec.<engine>
// block currently carries just spec.database (every shipped engine's
// convention — see internal/adapters/providers/{postgres,mysql}).
func renderSource(command, engine, name, providerName, database string) string {
	lines := []string{
		"engine: " + engine,
	}
	lines = append(lines, refBlock("providerRef", providerName)...)
	lines = append(lines, engine+":", "  database: "+database)
	return renderDoc(command, "", "Source", name, lines)
}

// renderSecret renders one env-backend SecretReference carrying a
// username/password pair, plus the two .env keys to append (env backend,
// DATASCAPE_SECRET_<REF>_<KEY> convention — see
// internal/adapters/secrets/env).
func renderSecret(command, name, defaultUser string) (doc string, envAppends []EnvAppend) {
	doc = renderDoc(command, "", "SecretReference", name, []string{
		"backend: env",
		"keys: " + flowList([]string{"username", "password"}),
	})
	envAppends = []EnvAppend{
		{Key: envVarName(name, "username"), Default: defaultUser},
		{Key: envVarName(name, "password"), Default: "change-me"},
	}
	return doc, envAppends
}

// renderSecretPair renders the admin+replication SecretReference pair
// every managed database engine needs.
func renderSecretPair(command, adminName, adminUser, replName string) (adminDoc, replDoc string, envAppends []EnvAppend) {
	adminDoc, adminEnv := renderSecret(command, adminName, adminUser)
	replDoc, replEnv := renderSecret(command, replName, "replicator")
	return adminDoc, replDoc, append(adminEnv, replEnv...)
}

// envVarName computes the env backend's resolution key for SecretReference
// refName's key (internal/adapters/secrets/env's exact convention:
// DATASCAPE_SECRET_<REF>_<KEY>, uppercased, dashes -> underscores).
func envVarName(refName, key string) string {
	ref := strings.ToUpper(strings.ReplaceAll(refName, "-", "_"))
	k := strings.ToUpper(strings.ReplaceAll(key, "-", "_"))
	return "DATASCAPE_SECRET_" + ref + "_" + k
}
