// Package secretstore defines the SecretStore port.
// See docs/planning/02-architecture.md §4.4.
package secretstore

import (
	"context"

	"github.com/rezarajan/platformctl/internal/domain/secret"
)

type SecretStore interface {
	Resolve(ctx context.Context, ref secret.SecretReference) (map[string]string, error)
}
