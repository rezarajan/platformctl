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
	},
}
