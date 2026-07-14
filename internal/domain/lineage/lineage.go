// Package lineage defines the LineageEndpoint value type.
//
// LineageEndpoint is a connection fact, nothing more. It carries no notion of
// Job, Run, Dataset, or event — those are the lineage backend's and the
// consuming tool's concepts, not Datascape's.
// See docs/planning/02-architecture.md §3.6.
package lineage

import "github.com/rezarajan/platformctl/internal/domain/secret"

type LineageEndpoint struct {
	URL       string
	Namespace string                  // optional; some backends want a namespace hint
	AuthRef   *secret.SecretReference // optional
}
