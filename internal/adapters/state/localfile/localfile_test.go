package localfile

import (
	"path/filepath"
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/state"
	"github.com/rezarajan/platformctl/internal/ports/state/conformance"
)

func TestConformance(t *testing.T) {
	conformance.Run(t, func(t *testing.T) state.StateStore {
		return New(filepath.Join(t.TempDir(), "state.json"))
	})
}
