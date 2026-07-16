package docker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/errdefs"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// ensureImage makes the image available per the spec's pull policy. The
// default (PullIfNotPresent) pulls only when the image isn't present
// locally — keeping EnsureContainer idempotent and offline-friendly once
// images are cached. Digest-pinned refs ("repo@sha256:...") work through
// the same inspect/pull calls unchanged.
func (r *Runtime) ensureImage(ctx context.Context, ref, pullPolicy string) error {
	switch pullPolicy {
	case runtime.PullNever:
		if _, err := r.cli.ImageInspect(ctx, ref); err != nil {
			if errdefs.IsNotFound(err) {
				return fmt.Errorf("image %q is not present locally and pull policy is %q", ref, runtime.PullNever)
			}
			return fmt.Errorf("inspect image %q: %w", ref, err)
		}
		return nil
	case runtime.PullAlways:
		return r.pullImage(ctx, ref)
	case runtime.PullIfNotPresent:
		_, err := r.cli.ImageInspect(ctx, ref)
		if err == nil {
			return nil
		}
		if !errdefs.IsNotFound(err) {
			return fmt.Errorf("inspect image %q: %w", ref, err)
		}
		return r.pullImage(ctx, ref)
	default:
		return fmt.Errorf("unknown image pull policy %q (allowed: %q, %q, %q)", pullPolicy, runtime.PullIfNotPresent, runtime.PullAlways, runtime.PullNever)
	}
}

func (r *Runtime) pullImage(ctx context.Context, ref string) error {
	rc, err := r.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull image %q: %w", ref, err)
	}
	defer rc.Close()
	// Drain the progress stream; the pull completes when it EOFs.
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return fmt.Errorf("pull image %q: %w", ref, err)
	}
	return nil
}

// specHash produces a stable fingerprint of a ContainerSpec, stored as a
// label so EnsureContainer can detect an already-matching container.
func specHash(spec runtime.ContainerSpec) string {
	data, _ := json.Marshal(spec)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
