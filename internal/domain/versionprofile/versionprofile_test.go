package versionprofile

import (
	"strings"
	"testing"
)

var cat = Catalog{
	Default: "16",
	Profiles: map[string]Profile{
		"16": {Version: "16", Image: "postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20", DataMount: "/var/lib/postgresql/data"},
		"18": {Version: "18", Image: "postgres:18@sha256:3a82e1f56c8f0f5616a11103ac3d47e632c3938698946a7ad26da0df1334744a", DataMount: "/var/lib/postgresql"},
	},
}

func TestResolveDefaultAndKnown(t *testing.T) {
	t.Parallel()
	def, err := cat.Resolve("")
	if err != nil || def.Version != "16" {
		t.Fatalf("default resolve = %+v, %v", def, err)
	}
	p18, err := cat.Resolve("18")
	if err != nil {
		t.Fatal(err)
	}
	if p18.DataMount != "/var/lib/postgresql" || p18.Image != "postgres:18@sha256:3a82e1f56c8f0f5616a11103ac3d47e632c3938698946a7ad26da0df1334744a" {
		t.Errorf("18 profile = %+v", p18)
	}
}

func TestResolveUnknownErrors(t *testing.T) {
	t.Parallel()
	_, err := cat.Resolve("42")
	if err == nil || !strings.Contains(err.Error(), "supported: 16, 18") {
		t.Errorf("unexpected: %v", err)
	}
}

func TestValidateConfig(t *testing.T) {
	t.Parallel()
	// valid version
	if err := cat.ValidateConfig(map[string]any{"version": "18"}); err != nil {
		t.Errorf("valid version rejected: %v", err)
	}
	// no version → default, ok
	if err := cat.ValidateConfig(map[string]any{}); err != nil {
		t.Errorf("empty config rejected: %v", err)
	}
	// image without version → rejected (the reported failure mode)
	err := cat.ValidateConfig(map[string]any{"image": "postgres:18@sha256:3a82e1f56c8f0f5616a11103ac3d47e632c3938698946a7ad26da0df1334744a"})
	if err == nil || !strings.Contains(err.Error(), "without configuration.version") {
		t.Errorf("image-without-version not rejected: %v", err)
	}
	// image with version → allowed (mirror of the same version)
	if err := cat.ValidateConfig(map[string]any{"image": "mirror/postgres:16", "version": "16"}); err != nil {
		t.Errorf("image+version rejected: %v", err)
	}
	// unknown version → rejected
	if err := cat.ValidateConfig(map[string]any{"version": "9"}); err == nil {
		t.Error("unknown version accepted")
	}
}
