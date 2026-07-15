// Package versionprofile is the immutable, pinned definition of a technology
// version — the image *and* the version-specific internals that must move
// with it (data mount path, data directory, env pins). Coupling a free-form
// image to hard-coded internals is the defect this fixes: postgres:16 stores
// data under /var/lib/postgresql/data, postgres:18 under /var/lib/postgresql,
// so an image swapped without the matching mount silently breaks persistence.
// A provider ships a Catalog of tested profiles; the manifest references a
// version, never a raw image with implicit internals — the Helm/Terraform
// discipline of versioned, referenced definitions.
package versionprofile

import (
	"fmt"
	"sort"
	"strings"
)

// Profile is one technology version's pinned, immutable definition.
type Profile struct {
	// Version is the immutable identifier referenced from
	// configuration.version, e.g. "16".
	Version string
	// Image is the container image this version ships with.
	Image string
	// DataMount is the container path the persistent data volume mounts at —
	// the value that differs across major versions and must never be paired
	// with the wrong image.
	DataMount string
	// Env are version-specific environment pins layered onto the provider's
	// own env (optional).
	Env map[string]string
}

// Catalog is a provider's set of supported version profiles.
type Catalog struct {
	Default  string
	Profiles map[string]Profile
}

// Versions returns the supported version identifiers, sorted.
func (c Catalog) Versions() []string {
	out := make([]string, 0, len(c.Profiles))
	for v := range c.Profiles {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// Resolve returns the profile for version, or the default when version is
// empty. An unknown version is an error naming the supported set — the
// validate-time guard against an unstable, unpinned deployment.
func (c Catalog) Resolve(version string) (Profile, error) {
	if version == "" {
		version = c.Default
	}
	p, ok := c.Profiles[version]
	if !ok {
		return Profile{}, fmt.Errorf("unsupported version %q (supported: %s)", version, strings.Join(c.Versions(), ", "))
	}
	return p, nil
}

// ValidateConfig enforces the versioned-provider contract on a provider's
// configuration map: the version (if given) must be supported, and an image
// override may not appear without a version — the internals cannot be
// inferred from a bare image, which is exactly the silent mismatch this
// prevents (an image swapped without its matching data mount).
func (c Catalog) ValidateConfig(configuration map[string]any) error {
	version, _ := configuration["version"].(string)
	image, _ := configuration["image"].(string)
	if image != "" && version == "" {
		return fmt.Errorf("configuration.image is set without configuration.version — pin a version so the version-specific internals (e.g. the data mount path) travel with the image (supported versions: %s)", strings.Join(c.Versions(), ", "))
	}
	_, err := c.Resolve(version)
	return err
}
