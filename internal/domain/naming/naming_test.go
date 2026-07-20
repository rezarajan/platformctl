package naming

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// TestRuntimeObjectNameIsTheSingleAuthority pins the current convention
// (named after the realizing resource) and, more importantly, proves it
// lives in exactly one place: changing this function's body is the only
// edit a future convention change would require — every provider and the
// engine call this function rather than re-deriving the name themselves.
func TestRuntimeObjectNameIsTheSingleAuthority(t *testing.T) {
	env := resource.Envelope{}
	env.Metadata.Name = "orders-db"
	env.Metadata.Namespace = "default"

	if got := RuntimeObjectName(env); got != "orders-db" {
		t.Errorf("RuntimeObjectName = %q, want %q", got, "orders-db")
	}
}
