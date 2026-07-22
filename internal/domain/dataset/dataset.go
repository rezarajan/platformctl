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

// Versioning states for Dataset.spec.lifecycle.versioning — S3 bucket
// versioning's own vocabulary (minio-go's Enabled/Suspended), lowercased to
// match this model's other enabled|suspended-shaped fields (docs/planning/08
// D7).
const (
	VersioningEnabled   = "enabled"
	VersioningSuspended = "suspended"
)

type Dataset struct {
	ProviderRef    string // an s3/minio-typed Provider
	Bucket         string
	Prefix         string
	Format         string // "parquet" | "json" | "avro" — validated against the sink provider's SupportedSinkFormats()
	External       bool
	DeletionPolicy string // DeletionRetain (default) | DeletionDelete
	Lifecycle      Lifecycle
}

// Lifecycle is Dataset.spec.lifecycle (docs/planning/08 D7): expiration and
// versioning, reconciled by the realizing s3/minio provider via the S3 API
// (bucket lifecycle rules + bucket versioning), never touched when unset —
// omitting spec.lifecycle entirely leaves any out-of-band bucket lifecycle
// config alone, exactly like every other optional-and-unmanaged-when-absent
// field in this model.
type Lifecycle struct {
	// ExpireAfterDays, when > 0, wants exactly one lifecycle rule expiring
	// objects under this Dataset's prefix after this many days. 0 (unset)
	// means "no expiration rule managed" — not "expire immediately".
	ExpireAfterDays int
	// Versioning, when non-empty, wants the bucket's versioning state to be
	// VersioningEnabled or VersioningSuspended. Empty means "don't manage
	// versioning".
	Versioning string
}

func (l Lifecycle) HasExpiration() bool { return l.ExpireAfterDays > 0 }
func (l Lifecycle) HasVersioning() bool { return l.Versioning != "" }

// Empty reports whether spec.lifecycle was omitted entirely — the
// provider's cue to skip lifecycle reconciliation/probing altogether rather
// than actively un-managing anything.
func (l Lifecycle) Empty() bool { return !l.HasExpiration() && !l.HasVersioning() }

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
	if lc, ok := e.Spec["lifecycle"].(map[string]any); ok {
		switch n := lc["expireAfterDays"].(type) {
		case int:
			d.Lifecycle.ExpireAfterDays = n
		case float64:
			d.Lifecycle.ExpireAfterDays = int(n)
		}
		d.Lifecycle.Versioning, _ = lc["versioning"].(string)
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
	if d.Lifecycle.ExpireAfterDays < 0 {
		return fmt.Errorf("Dataset %q: spec.lifecycle.expireAfterDays must be a positive integer, got %d", name, d.Lifecycle.ExpireAfterDays)
	}
	if v := d.Lifecycle.Versioning; v != "" && v != VersioningEnabled && v != VersioningSuspended {
		return fmt.Errorf("Dataset %q: spec.lifecycle.versioning must be %q or %q, got %q", name, VersioningEnabled, VersioningSuspended, v)
	}
	return nil
}
