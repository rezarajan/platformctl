// Package dataset defines the Dataset kind.
// See docs/planning/02-architecture.md §3.5.
package dataset

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// Deletion policies for data-bearing resources (Dataset, Source) — what
// destroying the platform's *record* of the resource does to the *data*
// (docs/planning/07 §2.2). Kubernetes-reclaim-policy-shaped: retain is the
// default because data loss must be opted into, never implied.
const (
	DeletionRetain = "retain"
	DeletionDelete = "delete"
)

type Dataset struct {
	ProviderRef    string // an s3/minio-typed Provider
	Bucket         string
	Prefix         string
	Format         string // "parquet" | "json" | "avro" — validated against the sink provider's SupportedSinkFormats()
	External       bool
	DeletionPolicy string // DeletionRetain (default) | DeletionDelete
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
	d.DeletionPolicy, _ = e.Spec["deletionPolicy"].(string)
	if d.DeletionPolicy == "" {
		d.DeletionPolicy = DeletionRetain
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
	if d.DeletionPolicy != DeletionRetain && d.DeletionPolicy != DeletionDelete {
		return fmt.Errorf("Dataset %q: spec.deletionPolicy must be %q or %q, got %q", name, DeletionRetain, DeletionDelete, d.DeletionPolicy)
	}
	return nil
}
