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

import (
	"strings"
	"time"
)

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
	// RuntimeName/ContainerPort are the F4 runtime facts (docs/planning/08
	// F4; docs/adr/007-backup-restore.md) behind Endpoint, for a caller
	// dialing from *outside* the runtime (this CLI process, not a job
	// container on the shared network): the exact (runtime object name,
	// container port) the realizing provider passed to
	// ContainerRuntime.EnsureContainer, letting that caller resolve a
	// currently-dialable address via ContainerRuntime.EnsureReachable /
	// runtime.WithReachable instead of dialing Endpoint directly — Endpoint
	// is only valid from inside the runtime's own network (a job container
	// that joined Network), never from the CLI host itself. Empty for a raw
	// URL Location (external S3, real AWS, ...), whose Endpoint is already
	// externally routable and needs no runtime resolution.
	RuntimeName   string
	ContainerPort int
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

// RefOf strips a Location down to its credential-free Ref. key is the exact
// object a Backup/Restore call landed on or read from (its full key,
// including any prefix) or "" for a whole-prefix sync (s3's own Backup,
// which never lands at one object). Prefix is derived from key's directory
// portion, not simply copied from loc.Prefix — a restore's src.Prefix
// already *is* the full key by the time it reaches here (Restore needs the
// exact object to read, not a directory), so reusing loc.Prefix verbatim
// made a Ref's Key and Prefix carry the same value, redundantly (C6 review
// finding 5b). key == "" leaves Prefix as loc.Prefix unchanged (the s3
// bucket-sync case, where Prefix legitimately means "the tree that was
// synced," not "this key's directory").
func RefOf(loc Location, key string) Ref {
	prefix := loc.Prefix
	if key != "" {
		prefix = ""
		if idx := strings.LastIndex(key, "/"); idx >= 0 {
			prefix = key[:idx]
		}
	}
	return Ref{Endpoint: loc.Endpoint, Bucket: loc.Bucket, Key: key, Prefix: prefix}
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
	// Checksum is "sha256:<hex>" of the exact bytes streamed through the
	// dbjob pipeline (docs/planning/08 I12; docs/adr/007-backup-restore.md
	// addendum) — computed producer-side (whichever role that is: the
	// database's dump tool for Backup, mc for Restore), never by reading
	// the whole payload back into this process. Restore verifies a
	// downloaded object's checksum against this field before trusting it.
	Checksum string `json:"checksum" yaml:"checksum"`
	// Bytes is the byte count of the same stream Checksum was computed
	// over.
	Bytes int64 `json:"bytes" yaml:"bytes"`
}
