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
