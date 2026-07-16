//go:build integration

package kubernetes

import (
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/runtime/conformance"
)

func TestConformance(t *testing.T) {
	rt, err := New(nil)
	if err != nil {
		t.Fatalf("connect to kubernetes: %v", err)
	}
	conformance.Run(t, rt, "datascape-k8s-conf")
}
