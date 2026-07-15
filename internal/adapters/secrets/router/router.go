// Package router dispatches SecretReference resolution to the store
// registered for the reference's backend, failing fast with a clear message
// for declared-but-unavailable backends (schema-accepted futures like
// kubernetes, or gated ones like vault).
package router

import (
	"context"
	"fmt"
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/secret"
	"github.com/rezarajan/platformctl/internal/ports/secretstore"
)

type Router struct {
	stores map[secret.Backend]secretstore.SecretStore
}

func New() *Router {
	return &Router{stores: make(map[secret.Backend]secretstore.SecretStore)}
}

func (r *Router) Register(backend secret.Backend, store secretstore.SecretStore) *Router {
	r.stores[backend] = store
	return r
}

func (r *Router) Resolve(ctx context.Context, ref secret.SecretReference) (map[string]string, error) {
	store, ok := r.stores[ref.Backend]
	if !ok {
		names := make([]string, 0, len(r.stores))
		for b := range r.stores {
			names = append(names, string(b))
		}
		sort.Strings(names)
		return nil, fmt.Errorf("SecretReference %q: backend %q is not available in this configuration (available: %v)", ref.Name, ref.Backend, names)
	}
	return store.Resolve(ctx, ref)
}

func (r *Router) Preflight(ctx context.Context, ref secret.SecretReference) error {
	store, ok := r.stores[ref.Backend]
	if !ok {
		names := make([]string, 0, len(r.stores))
		for b := range r.stores {
			names = append(names, string(b))
		}
		sort.Strings(names)
		return fmt.Errorf("SecretReference %q: backend %q is not available in this configuration (available: %v)", ref.Name, ref.Backend, names)
	}
	return store.Preflight(ctx, ref)
}
