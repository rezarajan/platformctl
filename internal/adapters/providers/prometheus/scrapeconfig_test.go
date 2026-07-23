package prometheus

import (
	"reflect"
	"strings"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

func TestTargetsFromMetrics(t *testing.T) {
	t.Parallel()
	metrics := []reconciler.MetricsTarget{
		{JobName: "redpanda", Endpoint: endpoint.Endpoint{Name: "metrics", Internal: "http://redpanda:9644/public_metrics"}},
		{JobName: "minio", Endpoint: endpoint.Endpoint{Name: "metrics", Internal: "http://minio:9000/minio/v2/metrics/cluster"}},
		// No path: falls back to the client-library default "/metrics".
		{JobName: "no-path", Endpoint: endpoint.Endpoint{Name: "metrics", Internal: "http://no-path:9999"}},
		// Malformed/empty Internal is skipped, not fatal.
		{JobName: "broken", Endpoint: endpoint.Endpoint{Name: "metrics", Internal: ""}},
	}
	got := targetsFromMetrics(metrics)
	want := []ScrapeTarget{
		{Job: "minio", Target: "minio:9000", Path: "/minio/v2/metrics/cluster"},
		{Job: "no-path", Target: "no-path:9999", Path: "/metrics"},
		{Job: "redpanda", Target: "redpanda:9644", Path: "/public_metrics"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("targetsFromMetrics = %+v, want %+v", got, want)
	}
}

func TestRenderScrapeConfigDeterministic(t *testing.T) {
	t.Parallel()
	targets := []ScrapeTarget{
		{Job: "minio", Target: "minio:9000", Path: "/minio/v2/metrics/cluster"},
		{Job: "redpanda", Target: "redpanda:9644", Path: "/public_metrics"},
	}
	a, err := RenderScrapeConfig(targets, "")
	if err != nil {
		t.Fatalf("RenderScrapeConfig: %v", err)
	}
	b, err := RenderScrapeConfig(targets, "")
	if err != nil {
		t.Fatalf("RenderScrapeConfig: %v", err)
	}
	if string(a) != string(b) {
		t.Errorf("RenderScrapeConfig is not deterministic:\n%s\n---\n%s", a, b)
	}
	if !strings.Contains(string(a), "scrape_interval: 15s") {
		t.Errorf("missing default scrape_interval:\n%s", a)
	}
	if !strings.Contains(string(a), "job_name: minio") || !strings.Contains(string(a), "job_name: redpanda") {
		t.Errorf("missing expected job names:\n%s", a)
	}
}

func TestRenderScrapeConfigCustomInterval(t *testing.T) {
	t.Parallel()
	out, err := RenderScrapeConfig(nil, "5s")
	if err != nil {
		t.Fatalf("RenderScrapeConfig: %v", err)
	}
	if !strings.Contains(string(out), "scrape_interval: 5s") {
		t.Errorf("custom scrape_interval not honored:\n%s", out)
	}
}

func TestParseScrapeConfigRoundTrip(t *testing.T) {
	t.Parallel()
	targets := []ScrapeTarget{
		{Job: "minio", Target: "minio:9000", Path: "/minio/v2/metrics/cluster"},
		{Job: "redpanda", Target: "redpanda:9644", Path: "/public_metrics"},
	}
	rendered, err := RenderScrapeConfig(targets, "")
	if err != nil {
		t.Fatalf("RenderScrapeConfig: %v", err)
	}
	parsed, err := ParseScrapeConfig(rendered)
	if err != nil {
		t.Fatalf("ParseScrapeConfig: %v", err)
	}
	if !reflect.DeepEqual(parsed, targets) {
		t.Errorf("round trip = %+v, want %+v", parsed, targets)
	}
}

func TestDiffScrapeConfig(t *testing.T) {
	t.Parallel()
	base := []ScrapeTarget{
		{Job: "minio", Target: "minio:9000", Path: "/minio/v2/metrics/cluster"},
		{Job: "redpanda", Target: "redpanda:9644", Path: "/public_metrics"},
	}
	t.Run("identical", func(t *testing.T) {
		if got := diffScrapeConfig(base, base); got != nil {
			t.Errorf("diffScrapeConfig(identical) = %v, want nil", got)
		}
	})
	t.Run("target changed", func(t *testing.T) {
		live := []ScrapeTarget{
			{Job: "minio", Target: "minio:9001", Path: "/minio/v2/metrics/cluster"}, // different port
			{Job: "redpanda", Target: "redpanda:9644", Path: "/public_metrics"},
		}
		got := diffScrapeConfig(base, live)
		want := []string{"minio"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("diffScrapeConfig(target changed) = %v, want %v", got, want)
		}
	})
	t.Run("job added to desired", func(t *testing.T) {
		desired := append(append([]ScrapeTarget{}, base...), ScrapeTarget{Job: "new-thing", Target: "new-thing:1234", Path: "/metrics"})
		got := diffScrapeConfig(desired, base)
		want := []string{"new-thing"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("diffScrapeConfig(job added) = %v, want %v", got, want)
		}
	})
	t.Run("job removed from desired", func(t *testing.T) {
		desired := base[:1] // only minio
		got := diffScrapeConfig(desired, base)
		want := []string{"redpanda"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("diffScrapeConfig(job removed) = %v, want %v", got, want)
		}
	})
}
