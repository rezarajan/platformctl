// Package schemas embeds the JSON Schema files that define every Kind's
// manifest shape — the single source of truth for validate-time schema
// checking and `platformctl docs build`. A schema change here requires a
// matching update to docs/planning/03-resource-model-reference.md.
package schemas

import "embed"

//go:embed v1alpha1/*.json
var FS embed.FS

// KindFiles maps each Kind to its schema path within FS, per apiVersion.
var KindFiles = map[string]map[string]string{
	"datascape.io/v1alpha1": {
		"Provider":        "v1alpha1/provider.json",
		"Source":          "v1alpha1/source.json",
		"EventStream":     "v1alpha1/eventstream.json",
		"Binding":         "v1alpha1/binding.json",
		"Dataset":         "v1alpha1/dataset.json",
		"SecretReference": "v1alpha1/secretreference.json",
		"Catalog":         "v1alpha1/catalog.json",
		"Connection":      "v1alpha1/connection.json",
	},
}

// PolicyFS embeds the Policy kind's schema (docs/adr/021-policy-engine-zero-
// trust.md §1) — a deliberately parallel directory, never merged into FS/
// KindFiles above: Policy is not a datascape.io/v1alpha1 resource kind, and
// must never be validated as one (a policy governing the manifest set can't
// also be a member of the set it governs).
//
//go:embed policy/v1alpha1/*.json
var PolicyFS embed.FS

// PolicyKindFiles maps the one Policy kind to its schema path within
// PolicyFS, per its own apiVersion — the policy-schema counterpart of
// KindFiles, kept as a separate map on purpose (see PolicyFS's doc comment).
var PolicyKindFiles = map[string]map[string]string{
	"policy.datascape.io/v1alpha1": {
		"Policy": "policy/v1alpha1/policy.json",
	},
}

// FragmentFS embeds the provider-owned JSON-Schema fragments (docs/planning/
// 08 E5): each fragment validates one open-ended, discriminator-keyed block
// that the core Kind schemas above deliberately leave open
// (additionalProperties: true/an open engine block) — Provider
// spec.configuration (keyed by spec.type), Source/Catalog spec.<engine>
// (keyed by spec.engine), and Binding spec.options (keyed by
// "<mode>-<providerType>"). Composed into validation by
// internal/application/manifest (never by the core schemas above — the core
// Kind shape and its provider-specific interior stay independently
// evolvable) and rendered into docs/reference by docsgen. A provider without
// a registered fragment here simply gets no fragment-level check (the
// pre-E5 behavior; ValidateSpec/ValidateBindingOptions Go code still runs
// regardless).
//
//go:embed v1alpha1/fragments/provider/*.json v1alpha1/fragments/source/*.json v1alpha1/fragments/catalog/*.json v1alpha1/fragments/binding/*.json
var FragmentFS embed.FS

// ProviderConfigFragments maps a Provider's spec.type to its
// spec.configuration fragment's path within FragmentFS. mysql/mariadb (one
// adapter, two provider types) and s3/minio (ditto) intentionally share one
// file, exactly like their RegisterProvider constructors in
// cmd/platformctl/main.go share one constructor. noop/container (test-only,
// never a "shipped provider" per provider.json's own description) have no
// fragment and never will.
var ProviderConfigFragments = map[string]string{
	"redpanda":    "v1alpha1/fragments/provider/redpanda.json",
	"postgres":    "v1alpha1/fragments/provider/postgres.json",
	"mysql":       "v1alpha1/fragments/provider/mysql.json",
	"mariadb":     "v1alpha1/fragments/provider/mysql.json",
	"debezium":    "v1alpha1/fragments/provider/debezium.json",
	"s3":          "v1alpha1/fragments/provider/s3.json",
	"minio":       "v1alpha1/fragments/provider/s3.json",
	"s3sink":      "v1alpha1/fragments/provider/s3sink.json",
	"jdbcsink":    "v1alpha1/fragments/provider/jdbcsink.json",
	"s3source":    "v1alpha1/fragments/provider/s3source.json",
	"nessie":      "v1alpha1/fragments/provider/nessie.json",
	"openlineage": "v1alpha1/fragments/provider/openlineage.json",
	"proxy":       "v1alpha1/fragments/provider/proxy.json",
	"prometheus":  "v1alpha1/fragments/provider/prometheus.json",
	"grafana":     "v1alpha1/fragments/provider/grafana.json",
	"ingress":     "v1alpha1/fragments/provider/ingress.json",
	"trino":       "v1alpha1/fragments/provider/trino.json",
	"wireguard":   "v1alpha1/fragments/provider/wireguard.json",
	"openziti":    "v1alpha1/fragments/provider/openziti.json",
}

// SourceEngineFragments maps a Source's spec.engine to its spec.<engine>
// block's fragment path within FragmentFS.
var SourceEngineFragments = map[string]string{
	"postgres": "v1alpha1/fragments/source/postgres.json",
	"mysql":    "v1alpha1/fragments/source/mysql.json",
	"mariadb":  "v1alpha1/fragments/source/mariadb.json",
}

// CatalogEngineFragments maps a Catalog's spec.engine to its spec.<engine>
// block's fragment path within FragmentFS.
var CatalogEngineFragments = map[string]string{
	"nessie": "v1alpha1/fragments/catalog/nessie.json",
}

// BindingOptionsFragments maps a Binding's "<spec.mode>-<providerRef's
// resolved spec.type>" to its spec.options fragment's path within
// FragmentFS. Only fires when providerRef resolves cleanly to a Provider in
// the same manifest set (an unresolvable ref is left to
// application/compatibility's own clearer, graph-aware error).
var BindingOptionsFragments = map[string]string{
	"cdc-debezium":    "v1alpha1/fragments/binding/cdc-debezium.json",
	"sink-s3sink":     "v1alpha1/fragments/binding/sink-s3sink.json",
	"sink-jdbcsink":   "v1alpha1/fragments/binding/sink-jdbcsink.json",
	"ingest-s3source": "v1alpha1/fragments/binding/ingest-s3source.json",
}
