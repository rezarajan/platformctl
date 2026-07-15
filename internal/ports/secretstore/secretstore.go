// Package secretstore defines the SecretStore port.
// See docs/planning/02-architecture.md §4.4.
package secretstore

import (
	"context"

	"github.com/rezarajan/platformctl/internal/domain/secret"
)

type SecretStore interface {
	Resolve(ctx context.Context, ref secret.SecretReference) (map[string]string, error)
	// Preflight reports whether ref can be resolved right now, without
	// returning (or holding) the secret values. It is the fail-fast check
	// run before any infrastructure is touched: a manifest set whose
	// secrets are not all resolvable must never half-apply. Implementations
	// report every missing key for the reference in a single error so the
	// caller can aggregate a complete list across all references.
	Preflight(ctx context.Context, ref secret.SecretReference) error
}
