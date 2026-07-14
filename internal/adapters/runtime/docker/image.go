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

// ensureImage pulls the image only when it isn't present locally — keeping
// EnsureContainer idempotent and offline-friendly once images are cached.
func (r *Runtime) ensureImage(ctx context.Context, ref string) error {
	_, err := r.cli.ImageInspect(ctx, ref)
	if err == nil {
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return fmt.Errorf("inspect image %q: %w", ref, err)
	}
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
