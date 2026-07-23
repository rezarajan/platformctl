package cliutil

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func TestProgressReporterStreamsOrderedSteps(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	r := NewProgressReporter(&buf, false) // no color for stable assertions
	k1 := resource.Key{Kind: "Provider", Name: "pg"}
	k2 := resource.Key{Kind: "Source", Name: "db"}

	r.Begin(2)
	r.StepStarted(1, 2, k1, "create")
	r.StepFinished(1, 2, k1, "create", 2700*time.Millisecond, nil)
	r.StepStarted(2, 2, k2, "create")
	r.StepFinished(2, 2, k2, "create", 50*time.Millisecond, errors.New("boom"))
	r.End(1, 1, 0)

	out := buf.String()
	for _, want := range []string{
		"Reconciling 2 resources:",
		"[1/2]", "create default/Provider/pg", "(2.7s)",
		"[2/2]", "default/Source/db", "boom",
		"1 applied", "1 failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("progress output missing %q:\n%s", want, out)
		}
	}
}

func TestProgressReporterHealingBeyondPlan(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	r := NewProgressReporter(&buf, false)
	r.Begin(0) // drift-probe apply: no planned changes
	r.StepHealing(resource.Key{Kind: "Provider", Name: "minio"}, "InstanceUnhealthy")
	r.StepStarted(1, 0, resource.Key{Kind: "Provider", Name: "minio"}, "update")
	r.StepFinished(1, 0, resource.Key{Kind: "Provider", Name: "minio"}, "update", time.Second, nil)
	r.End(1, 0, 0)

	out := buf.String()
	if !strings.Contains(out, "drift default/Provider/minio") {
		t.Errorf("missing healing line:\n%s", out)
	}
	if !strings.Contains(out, "[1]") { // seq beyond total renders as [n]
		t.Errorf("healing step should use [n] counter:\n%s", out)
	}
}

func TestProgressReporterSilentOnNoChanges(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	r := NewProgressReporter(&buf, false)
	r.Begin(0)
	r.End(0, 0, 0)
	if buf.Len() != 0 {
		t.Errorf("expected no output for an empty apply, got:\n%s", buf.String())
	}
}
