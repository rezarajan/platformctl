package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
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
// overwrite it can't actually rule out (docs/adr/007-backup-restore.md).
//
// metadata.protect is a second, independent gate checked below: it refuses
// even *with* AllowOverwrite set — protect exists to make "this resource's
// data must not be destroyed" true regardless of which destructive verb is
// used (the same safe default `destroy` already gives a protected resource;
// see internal/application/plan's isProtected), not something a single flag
// can waive.
func (e *Engine) Restore(ctx context.Context, envelopes []resource.Envelope, key resource.Key, src backup.Location) error {
	if !e.AllowOverwrite {
		return fmt.Errorf("%s: restore always overwrites existing data; re-run with --yes-i-understand-this-overwrites-existing-data", key)
	}
	bp, req, err := e.backupCapable(ctx, envelopes, key)
	if err != nil {
		return err
	}
	if req.Resource.Metadata.Protect {
		return fmt.Errorf("%s: metadata.protect is true; restore refuses to overwrite a protected resource's data even with --yes-i-understand-this-overwrites-existing-data — set metadata.protect: false and re-apply to lift the block, then retry", key)
	}
	return bp.Restore(ctx, req, src)
}

// backupCapable resolves key's realizing Provider and Request the same way
// every other capability call does, then checks two additional things
// Backup/Restore specifically need: the provider implements
// BackupCapableProvider, and the resolved runtime is Docker — the
// job-container-plus-FIFO-volume mechanism (internal/adapters/providers/
// dbjob) and s3's own read-after-exit sentinel-file protocol have no
// Kubernetes equivalent (no Deployment maps onto a short-lived,
// exit-code-observable Job the way a Docker container does); see
// docs/adr/007-backup-restore.md's Known limitations.
func (e *Engine) backupCapable(ctx context.Context, envelopes []resource.Envelope, key resource.Key) (reconciler.BackupCapableProvider, reconciler.Request, error) {
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, env := range envelopes {
		byKey[env.Key()] = env
	}
	env, ok := byKey[key]
	if !ok {
		return nil, reconciler.Request{}, fmt.Errorf("%s is not declared in the manifest set", key)
	}
	prov, req, err := e.resolveRequest(ctx, env, byKey, nil) // nil state: SchemaRegistryURL is Binding-only, backup targets are Source/Dataset
	if err != nil {
		return nil, reconciler.Request{}, err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return nil, reconciler.Request{}, err
	}
	if cfg.RuntimeType != "docker" {
		return nil, reconciler.Request{}, fmt.Errorf("%s: backup/restore only supports the docker runtime in v1 (resolved runtime type %q) — see docs/adr/007-backup-restore.md's Known limitations", key, cfg.RuntimeType)
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
// req.Secrets by the time this reads them. The address itself comes from the
// realizing Provider's own PUBLISHED endpoint fact (objectStoreEndpointFact,
// below) — this function must not know a technology's private port/scheme/
// network conventions (docs/planning/08 F4; C6 review finding 3;
// docs/adr/007-backup-restore.md). What remains an explicit, minimal
// coupling: the s3/minio adapter's root-credential SecretReference shape
// (username/password keys) — the same shape postgres's superuser and
// mysql's root password already use platform-wide, not an s3-only guess;
// there is no fact-based equivalent for a credential shape the way there is
// for a network address.
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
	_, req, err := e.resolveRequest(ctx, env, byKey, nil) // nil state: SchemaRegistryURL is Binding-only, backup targets are Source/Dataset
	if err != nil {
		return backup.Location{}, err
	}
	cfg, err := provider.FromEnvelope(req.Provider)
	if err != nil {
		return backup.Location{}, err
	}
	ds, err := dataset.FromEnvelope(env)
	if err != nil {
		return backup.Location{}, err
	}
	ep, err := e.objectStoreEndpointFact(ctx, req.Provider)
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
	scheme := ep.Scheme
	if scheme == "" {
		scheme = "http"
	}
	return backup.Location{
		Endpoint:      scheme + "://" + ep.Internal,
		Bucket:        ds.Bucket,
		Prefix:        joinPrefix(ds.Prefix, object),
		Insecure:      ep.Insecure,
		Network:       ep.Network,
		RuntimeName:   ep.RuntimeName,
		ContainerPort: ep.ContainerPort,
		AccessKey:     creds["username"],
		SecretKey:     creds["password"],
	}, nil
}

// objectStoreEndpointFact resolves providerEnv's own "s3" endpoint fact —
// the S3-API address it published on its own last successful reconcile —
// from PERSISTED STATE, not from providerEnv itself: backup/restore run as
// a separate CLI invocation from apply, and a manifest envelope never
// carries a status block at all (manifest.Load refuses one if hand-authored
// — status is Datascape-written only), so providerEnv.Status is always
// empty here regardless of what apply already recorded. Reading
// e.StateStore's persisted state.ResourceState.Status.ProviderState is the
// only place the real, already-realized fact lives (the same field
// reconcileOne itself writes as st.Resources[key] = e.resourceState(...)).
// A missing Provider entry or a missing "s3" fact means the store was never
// applied (or predates F4) — this fails with a clear, named prerequisite
// rather than falling back to a guessed port/scheme/network the way this
// once did (C6 review finding 3).
func (e *Engine) objectStoreEndpointFact(ctx context.Context, providerEnv resource.Envelope) (endpoint.Endpoint, error) {
	st, err := e.StateStore.Load(ctx)
	if err != nil {
		return endpoint.Endpoint{}, err
	}
	rs, ok := st.Resources[providerEnv.Key()]
	if !ok {
		return endpoint.Endpoint{}, fmt.Errorf("Provider %q has not been applied yet — backup/restore needs its persisted endpoint facts to resolve an address; run apply first", providerEnv.Metadata.Name)
	}
	for _, ep := range endpoint.FromState(rs.Status.ProviderState[endpoint.Key]) {
		if ep.Name != "s3" {
			continue
		}
		if ep.RuntimeName == "" || ep.ContainerPort == 0 || ep.Internal == "" {
			return endpoint.Endpoint{}, fmt.Errorf("Provider %q's %q endpoint fact is missing its runtime object name/port/address; re-apply it", providerEnv.Metadata.Name, "s3")
		}
		return ep, nil
	}
	return endpoint.Endpoint{}, fmt.Errorf("Provider %q has not published an %q endpoint fact (its own S3-API address) — backup/restore destinations/sources must resolve to a provider that publishes one (e.g. s3/minio)", providerEnv.Metadata.Name, "s3")
}

// resolveURLLocation resolves a raw "scheme://host[:port]/bucket[/key...]"
// URL plus a SecretReference naming accessKey/secretKey credentials. No
// extra network join is added: the endpoint is assumed externally routable
// from the job container's own default network path (real AWS S3, or any
// other publicly reachable S3-compatible endpoint) — see
// docs/adr/007-backup-restore.md for the follow-up this defers (an explicit
// "join this network too" flag for a self-hosted destination not fronted by
// a Dataset).
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
