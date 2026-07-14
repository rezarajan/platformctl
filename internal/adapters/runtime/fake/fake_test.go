package fake

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/runtime/conformance"
)

// Mutations implements conformance.MutationCounter.
func (r *Runtime) Mutations() int { return r.MutationCount }

func TestConformance(t *testing.T) {
	conformance.Run(t, New(), "fake-conf")
}
