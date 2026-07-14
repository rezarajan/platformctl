// Package dataset defines the Dataset kind.
// See docs/planning/02-architecture.md §3.5.
package dataset

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

type Dataset struct {
	ProviderRef string // an s3/minio-typed Provider
	Bucket      string
	Prefix      string
	Format      string // "parquet" | "json" | "avro" — validated against the sink provider's SupportedSinkFormats()
	External    bool
}

func FromEnvelope(e resource.Envelope) (Dataset, error) {
	d := Dataset{}
	if ref, ok := e.Spec["providerRef"].(map[string]any); ok {
		d.ProviderRef, _ = ref["name"].(string)
	}
	d.Bucket, _ = e.Spec["bucket"].(string)
	d.Prefix, _ = e.Spec["prefix"].(string)
	d.Format, _ = e.Spec["format"].(string)
	if ext, ok := e.Spec["external"].(bool); ok {
		d.External = ext
	}
	return d, d.validate(e.Metadata.Name)
}

func (d Dataset) validate(name string) error {
	if !d.External && d.ProviderRef == "" {
		return fmt.Errorf("Dataset %q: spec.providerRef is required", name)
	}
	if d.Bucket == "" {
		return fmt.Errorf("Dataset %q: spec.bucket is required", name)
	}
	if d.Format == "" {
		return fmt.Errorf("Dataset %q: spec.format is required", name)
	}
	return nil
}
