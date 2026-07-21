package prometheus

import (
	"fmt"
	"net/url"
	"sort"

	"gopkg.in/yaml.v3"

	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// ScrapeTarget is one Prometheus scrape job's already-resolved target: a job
// name, a dial address ("host:port"), and a metrics path. Every field is
// derived from an already-published endpoint fact (targetsFromMetrics) —
// RenderScrapeConfig/ParseScrapeConfig never resolve or construct an
// address themselves (ADR 015: scrape targets come from published endpoint
// facts, never provider-constructed).
type ScrapeTarget struct {
	Job    string
	Target string
	Path   string
}

// scrapeConfigFile is the subset of Prometheus's YAML config file shape this
// package generates and parses back — global.scrape_interval plus one
// static_configs job per scrape target. Decoding Prometheus's own
// /api/v1/status/config effective-config text (which additionally carries
// every field Prometheus fills in with a default) into this narrow struct
// silently drops what this package doesn't care about; yaml.v3 does not
// error on unknown fields by default.
type scrapeConfigFile struct {
	Global        scrapeGlobal    `yaml:"global,omitempty"`
	ScrapeConfigs []scrapeJobSpec `yaml:"scrape_configs"`
}

type scrapeGlobal struct {
	ScrapeInterval string `yaml:"scrape_interval,omitempty"`
}

type scrapeJobSpec struct {
	JobName       string              `yaml:"job_name"`
	MetricsPath   string              `yaml:"metrics_path,omitempty"`
	StaticConfigs []scrapeStaticEntry `yaml:"static_configs"`
}

type scrapeStaticEntry struct {
	Targets []string `yaml:"targets"`
}

// defaultMetricsPath is Prometheus's own client-library default; used when
// a published metrics endpoint's Internal URL carries no path.
const defaultMetricsPath = "/metrics"

// defaultScrapeInterval applies when configuration.scrapeInterval is unset.
const defaultScrapeInterval = "15s"

// targetsFromMetrics resolves req.MetricsTargets (engine-published per
// endpoint fact, ADR 015) into the ScrapeTarget list RenderScrapeConfig
// consumes — parsing each endpoint's already-published Internal URL for its
// host:port and path; it never constructs either. A target with no
// host:port (an endpoint fact declaring one only for host audience, or
// malformed) is silently skipped rather than failing the whole reconcile —
// consistent with "scrape whatever carries a metrics-capable endpoint" being
// best-effort over currently-published state, not a hard dependency.
func targetsFromMetrics(metrics []reconciler.MetricsTarget) []ScrapeTarget {
	out := make([]ScrapeTarget, 0, len(metrics))
	for _, m := range metrics {
		u, err := url.Parse(m.Endpoint.Internal)
		if err != nil || u.Host == "" {
			continue
		}
		path := u.Path
		if path == "" {
			path = defaultMetricsPath
		}
		out = append(out, ScrapeTarget{Job: m.JobName, Target: u.Host, Path: path})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Job < out[j].Job })
	return out
}

// RenderScrapeConfig renders targets into a complete Prometheus config file
// (global + scrape_configs) in deterministic (sorted-by-job) order, so two
// calls with the same targets produce byte-identical output — required for
// EnsureContainer's spec-hash idempotency (ContainerSpec.Files.Content
// participates in the hash; a call that isn't deterministic would recreate
// the container on every reconcile even with unchanged targets). This is
// the exact renderer `platformctl inventory --for prometheus`
// (cmd/platformctl/toolconfig.go) also calls: "rendered from the same
// facts" means literally the same code, not two hand-synced templates.
// scrapeInterval defaults to defaultScrapeInterval when empty.
func RenderScrapeConfig(targets []ScrapeTarget, scrapeInterval string) ([]byte, error) {
	if scrapeInterval == "" {
		scrapeInterval = defaultScrapeInterval
	}
	cfg := scrapeConfigFile{Global: scrapeGlobal{ScrapeInterval: scrapeInterval}}
	for _, t := range targets {
		cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, scrapeJobSpec{
			JobName:       t.Job,
			MetricsPath:   t.Path,
			StaticConfigs: []scrapeStaticEntry{{Targets: []string{t.Target}}},
		})
	}
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("render scrape config: %w", err)
	}
	return out, nil
}

// ParseScrapeConfig parses a Prometheus config file's YAML (the file this
// package generated, or Prometheus's own /api/v1/status/config
// effective-config text) back into the ScrapeTarget list it encodes — the
// other half of the Probe-time drift check (diffScrapeConfig compares this
// against a freshly-regenerated desired list, the debezium
// connectorConfigDrift bar).
func ParseScrapeConfig(data []byte) ([]ScrapeTarget, error) {
	var cfg scrapeConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse scrape config: %w", err)
	}
	var out []ScrapeTarget
	for _, job := range cfg.ScrapeConfigs {
		path := job.MetricsPath
		if path == "" {
			path = defaultMetricsPath
		}
		for _, static := range job.StaticConfigs {
			for _, target := range static.Targets {
				out = append(out, ScrapeTarget{Job: job.JobName, Target: target, Path: path})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Job < out[j].Job })
	return out, nil
}

// diffScrapeConfig reports the job names whose scrape target differs
// between desired and live — a job present on only one side, or present on
// both with a different target/path — sorted and deduplicated, naming keys
// only and never a target value (the debezium connectorConfigDrift bar:
// "report drifted key names, never values").
func diffScrapeConfig(desired, live []ScrapeTarget) []string {
	toMap := func(ts []ScrapeTarget) map[string]ScrapeTarget {
		m := make(map[string]ScrapeTarget, len(ts))
		for _, t := range ts {
			m[t.Job] = t
		}
		return m
	}
	d, l := toMap(desired), toMap(live)
	var drifted []string
	for job, dt := range d {
		if lt, ok := l[job]; !ok || lt.Target != dt.Target || lt.Path != dt.Path {
			drifted = append(drifted, job)
		}
	}
	for job := range l {
		if _, ok := d[job]; !ok {
			drifted = append(drifted, job)
		}
	}
	// No job name can appear in both loops above (the first only visits
	// jobs present in desired; the second only visits jobs absent from
	// desired), so drifted is already duplicate-free — sort for a
	// deterministic message.
	sort.Strings(drifted)
	return drifted
}
