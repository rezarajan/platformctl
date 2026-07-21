// Package backup defines the value types the BackupCapableProvider
// capability (docs/planning/08 C6) passes across the reconciler.Request
// boundary: an object-store Location a Backup/Restore call reads from or
// writes to, and the Manifest a successful Backup returns.
//
// Both types are deliberately credential-shaped only where required
// (Location) and never where recorded (Manifest): a Manifest is what lands
// in a plan/apply -o json document, gets echoed back to a terminal, or is
// handed to a caller who may log or persist it — so it carries no field
// capable of holding a secret value. This is the mechanical half of the
// "backups never embed plaintext credentials" accept criterion; Location's
// AccessKey/SecretKey exist only in memory for the duration of one
// Backup/Restore call, exactly like reconciler.Request.Secrets.
package backup

import "time"

// Location is an object-store destination (Backup) or source (Restore): a
// bucket/prefix at an S3-API-compatible endpoint, plus already-resolved
// credentials for it. Callers build one either from a Dataset (resolving its
// realizing s3/minio Provider, mirroring how every other capability method
// receives already-resolved inputs) or from a raw URL plus a
// SecretReference — never from a bare providerRef/secretRef the provider
// method itself would have to resolve.
type Location struct {
	// Endpoint is the S3 API base address ("http://name:9000",
	// "https://s3.amazonaws.com") reachable from the job container's
	// network — never a bare host with an implied scheme.
	Endpoint string
	Bucket   string
	// Prefix is the key prefix under which objects are read/written; may be
	// empty (bucket root).
	Prefix string
	// Insecure marks a plain-HTTP endpoint (self-hosted MinIO without TLS,
	// the same convention the s3 provider's own endpoint facts use) so a
	// consumer knows not to demand certificate verification.
	Insecure bool
	// Network is the runtime network the job container must additionally
	// join to resolve Endpoint by its internal DNS name — set when Location
	// was resolved from a Dataset realized by an in-platform s3 Provider;
	// empty when Endpoint is externally routable (a raw URL destination),
	// meaning no extra network join is needed.
	Network string
	// AccessKey/SecretKey are resolved credentials, held only for the
	// duration of the call — never serialized into a Manifest, state, or
	// log line.
	AccessKey string
	SecretKey string
}

// Ref is Location with its credentials and network-join details stripped —
// the credential-free shape a Manifest records, safe to print, log, or
// persist.
type Ref struct {
	Endpoint string `json:"endpoint" yaml:"endpoint"`
	Bucket   string `json:"bucket" yaml:"bucket"`
	// Key is the full object key a postgres/mysql dump landed at (Prefix
	// plus the generated filename); empty for an s3 bucket-sync backup,
	// which instead records Prefix as the tree that was synced.
	Key    string `json:"key,omitempty" yaml:"key,omitempty"`
	Prefix string `json:"prefix,omitempty" yaml:"prefix,omitempty"`
}

// RefOf strips a Location down to its credential-free Ref.
func RefOf(loc Location, key string) Ref {
	return Ref{Endpoint: loc.Endpoint, Bucket: loc.Bucket, Key: key, Prefix: loc.Prefix}
}

// Manifest is what a successful Backup call returns: where the data landed
// and how, never the credentials used to get it there.
type Manifest struct {
	Kind         string `json:"kind" yaml:"kind"`
	Name         string `json:"name" yaml:"name"`
	Namespace    string `json:"namespace" yaml:"namespace"`
	ProviderType string `json:"providerType" yaml:"providerType"`
	// Format identifies the engine-specific dump shape, e.g.
	// "postgres/pg_dump-plain", "mysql/mysqldump-sql", "s3/sync" — enough
	// for a future `restore` (or a human) to know how to read it back.
	Format      string    `json:"format" yaml:"format"`
	Destination Ref       `json:"destination" yaml:"destination"`
	StartedAt   time.Time `json:"startedAt" yaml:"startedAt"`
	CompletedAt time.Time `json:"completedAt" yaml:"completedAt"`
}
