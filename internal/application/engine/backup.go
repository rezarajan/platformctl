package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/naming"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// Backup resolves key's realizing Provider, checks it implements
// reconciler.BackupCapableProvider, and streams its data to dest. Callers
// build dest via ResolveObjectStoreLocation.
func (e *Engine) Backup(ctx context.Context, envelopes []resource.Envelope, key resource.Key, dest backup.Location) (backup.Manifest, error) {
	bp, req, err := e.backupCapable(ctx, envelopes, key)
	if err != nil {
		return backup.Manifest{}, err
	}
	return bp.Backup(ctx, req, dest)
}

// Restore resolves key's realizing Provider, checks it implements
// reconciler.BackupCapableProvider, and streams src back into it,
// unconditionally overwriting whatever data is already there.
//
// AllowOverwrite is the engine half of NFR-3-style safety, mirroring
// Engine.AllowDestructive: Restore refuses before touching any
// infrastructure unless the caller (the CLI, only after
// --yes-i-understand-this-overwrites-existing-data was passed) set it. This
// is deliberately unconditional — not "only when a probe finds existing
// data" — because probing first would mean live infrastructure I/O runs
// before the safety check can even fire, and a probe that fails open (a
// transient error reads as "no data") must never silently allow an
// overwrite it can't actually rule out (docs/design/007).
func (e *Engine) Restore(ctx context.Context, envelopes []resource.Envelope, key resource.Key, src backup.Location) error {
	if !e.AllowOverwrite {
		return fmt.Errorf("%s: restore always overwrites existing data; re-run with --yes-i-understand-this-overwrites-existing-data", key)
	}
	bp, req, err := e.backupCapable(ctx, envelopes, key)
	if err != nil {
		return err
	}
	return bp.Restore(ctx, req, src)
}

func (e *Engine) backupCapable(ctx context.Context, envelopes []resource.Envelope, key resource.Key) (reconciler.BackupCapableProvider, reconciler.Request, error) {
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, env := range envelopes {
		byKey[env.Key()] = env
	}
	env, ok := byKey[key]
	if !ok {
		return nil, reconciler.Request{}, fmt.Errorf("%s is not declared in the manifest set", key)
	}
	prov, req, err := e.resolveRequest(ctx, env, byKey)
	if err != nil {
		return nil, reconciler.Request{}, err
	}
	bp, ok := prov.(reconciler.BackupCapableProvider)
	if !ok {
		return nil, reconciler.Request{}, fmt.Errorf("%s: provider type %q does not implement backup/restore (BackupCapableProvider)", key, prov.Type())
	}
	return bp, req, nil
}

// ResolveObjectStoreLocation turns a --to/--from CLI argument into a
// backup.Location: either "Kind/name" naming a Dataset already declared in
// envelopes (credentials resolved from its own realizing s3/minio Provider,
// exactly like any other capability call), or a raw "http://host:port/
// bucket[/prefix...]" URL paired with credentialsSecretRef naming a
// SecretReference declared in envelopes providing accessKey/secretKey.
//
// object is appended under the resolved Dataset's own prefix — required by
// Restore (which needs the exact object to read back, not just a bucket/
// prefix directory) and left empty by Backup (which lets the provider
// generate its own filename under the resolved prefix). A raw URL's path
// already names the exact object it needs to; object is ignored for that
// form.
func (e *Engine) ResolveObjectStoreLocation(ctx context.Context, envelopes []resource.Envelope, ref, credentialsSecretRef, object, namespace string) (backup.Location, error) {
	if strings.Contains(ref, "://") {
		return e.resolveURLLocation(ctx, envelopes, ref, credentialsSecretRef, namespace)
	}
	return e.resolveDatasetLocation(ctx, envelopes, ref, object, namespace)
}

// resolveDatasetLocation resolves a "Dataset/name" selector into a Location
// by resolving its own providerRef the same way resolveRequest does for any
// resource — the Dataset's provider secretRefs are already resolved onto
// req.Secrets by the time this reads them.
func (e *Engine) resolveDatasetLocation(ctx context.Context, envelopes []resource.Envelope, ref, object, namespace string) (backup.Location, error) {
	key, err := resource.ParseSelector(ref, namespace)
	if err != nil {
		return backup.Location{}, fmt.Errorf("--to/--from %q: %w (expected Kind/name or a URL)", ref, err)
	}
	if key.Kind != "Dataset" {
		return backup.Location{}, fmt.Errorf("--to/--from %q: object-store destinations/sources must be a Dataset or a URL, got kind %q", ref, key.Kind)
	}
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, env := range envelopes {
		byKey[env.Key()] = env
	}
	env, ok := byKey[key]
	if !ok {
		return backup.Location{}, fmt.Errorf("%s is not declared in the manifest set", key)
	}
	_, req, err := e.resolveRequest(ctx, env, byKey)
	if err != nil {
		return backup.Location{}, err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return backup.Location{}, err
	}
	// Only the s3/minio adapter realizes a Dataset in v1 (ObjectStoreProvider
	// is the only IngestCapableProvider-shaped store); resolving a Location
	// requires knowing the provider's own admin-credential and endpoint
	// conventions, which is why this is a small, explicit convention here
	// rather than an application→adapter import (layering: engine may not
	// import internal/adapters/providers/s3 — see CLAUDE.md).
	if cfg.Type != "s3" && cfg.Type != "minio" {
		return backup.Location{}, fmt.Errorf("Dataset %q: backup/restore destinations must resolve to an s3/minio Provider, got %q", env.Metadata.Name, cfg.Type)
	}
	ds, err := dataset.FromEnvelope(env)
	if err != nil {
		return backup.Location{}, err
	}
	refName, _ := cfg.Configuration["rootSecretRef"].(string)
	if refName == "" && len(cfg.SecretRefs) > 0 {
		refName = cfg.SecretRefs[0]
	}
	creds, ok := req.Secrets[refName]
	if !ok {
		return backup.Location{}, fmt.Errorf("Dataset %q: no resolved credentials for secretRef %q", env.Metadata.Name, refName)
	}
	netName := "datascape"
	if rt, ok := req.Provider.Spec["runtime"].(map[string]any); ok {
		if n, _ := rt["network"].(string); n != "" {
			netName = n
		}
	}
	name := naming.RuntimeObjectName(req.Provider)
	return backup.Location{
		Endpoint:  fmt.Sprintf("http://%s:9000", name),
		Bucket:    ds.Bucket,
		Prefix:    joinPrefix(ds.Prefix, object),
		Insecure:  true,
		Network:   netName,
		AccessKey: creds["username"],
		SecretKey: creds["password"],
	}, nil
}

// resolveURLLocation resolves a raw "scheme://host[:port]/bucket[/key...]"
// URL plus a SecretReference naming accessKey/secretKey credentials. No
// extra network join is added: the endpoint is assumed externally routable
// from the job container's own default network path (real AWS S3, or any
// other publicly reachable S3-compatible endpoint) — see docs/design/007
// for the follow-up this defers (an explicit "join this network too" flag
// for a self-hosted destination not fronted by a Dataset).
func (e *Engine) resolveURLLocation(ctx context.Context, envelopes []resource.Envelope, raw, credentialsSecretRef, namespace string) (backup.Location, error) {
	if credentialsSecretRef == "" {
		return backup.Location{}, fmt.Errorf("--to/--from %q is a URL: --credentials-secret-ref is required to name where its access/secret key resolve from", raw)
	}
	scheme, rest, ok := strings.Cut(raw, "://")
	if !ok {
		return backup.Location{}, fmt.Errorf("--to/--from %q: not a valid URL (expected scheme://host[:port]/bucket[/key...])", raw)
	}
	if scheme != "http" && scheme != "https" {
		return backup.Location{}, fmt.Errorf("--to/--from %q: unsupported scheme %q (allowed: http, https)", raw, scheme)
	}
	hostAndPath := rest
	host, path, _ := strings.Cut(hostAndPath, "/")
	if host == "" || path == "" {
		return backup.Location{}, fmt.Errorf("--to/--from %q: expected scheme://host[:port]/bucket[/key...]", raw)
	}
	bucket, prefix, _ := strings.Cut(path, "/")

	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, env := range envelopes {
		byKey[env.Key()] = env
	}
	refKey := resource.Key{Namespace: resource.NormalizeNamespace(namespace), Kind: "SecretReference", Name: credentialsSecretRef}
	refEnv, ok := byKey[refKey]
	if !ok {
		return backup.Location{}, fmt.Errorf("--credentials-secret-ref %q does not resolve to a SecretReference in namespace %q", credentialsSecretRef, refKey.Namespace)
	}
	if e.SecretStore == nil {
		return backup.Location{}, fmt.Errorf("--credentials-secret-ref %q declared, but no secret store is configured", credentialsSecretRef)
	}
	creds, err := e.SecretStore.Resolve(ctx, secretRefFrom(refEnv))
	if err != nil {
		return backup.Location{}, err
	}
	if creds["accessKey"] == "" || creds["secretKey"] == "" {
		return backup.Location{}, fmt.Errorf("SecretReference %q must provide accessKey and secretKey keys", credentialsSecretRef)
	}
	return backup.Location{
		Endpoint:  scheme + "://" + host,
		Bucket:    bucket,
		Prefix:    prefix,
		Insecure:  scheme == "http",
		AccessKey: creds["accessKey"],
		SecretKey: creds["secretKey"],
	}, nil
}

func joinPrefix(prefix, object string) string {
	prefix = strings.Trim(prefix, "/")
	object = strings.TrimPrefix(object, "/")
	switch {
	case prefix == "":
		return object
	case object == "":
		return prefix
	default:
		return prefix + "/" + object
	}
}
