package compose

import (
	"sort"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// Candidate is one reuse option surfaced to an interactive select or
// matched against a --flag "existing:<name>" value.
type Candidate struct {
	Name    string
	Summary string
}

func sortCandidates(c []Candidate) []Candidate {
	sort.Slice(c, func(i, j int) bool { return c[i].Name < c[j].Name })
	return c
}

// BrokerCandidates returns every Provider capable of hosting an
// EventStream (broker-shaped) in the loaded set — Redpanda today, the only
// shipped broker-type Provider (docs/planning/03 §5).
func (s Snapshot) BrokerCandidates() []Candidate {
	var out []Candidate
	for _, e := range s.byKindType("Provider", "redpanda") {
		out = append(out, Candidate{Name: e.Metadata.Name, Summary: "redpanda broker"})
	}
	return sortCandidates(out)
}

// LakeCandidates returns every object-store Provider (s3/minio) — the
// backing store a Dataset's providerRef names.
func (s Snapshot) LakeCandidates() []Candidate {
	var out []Candidate
	for _, kind := range []string{"minio", "s3"} {
		for _, e := range s.byKindType("Provider", kind) {
			out = append(out, Candidate{Name: e.Metadata.Name, Summary: kind + " object store"})
		}
	}
	return sortCandidates(out)
}

// SinkWorkerCandidates returns every Kafka-Connect-worker Provider capable
// of realizing a sink Binding into a Dataset (s3sink today).
func (s Snapshot) SinkWorkerCandidates() []Candidate {
	var out []Candidate
	for _, e := range s.byKindType("Provider", "s3sink") {
		out = append(out, Candidate{Name: e.Metadata.Name, Summary: "s3sink Connect worker"})
	}
	return sortCandidates(out)
}

// CDCWorkerCandidates returns every Kafka-Connect-worker Provider capable
// of realizing a cdc Binding (debezium today).
func (s Snapshot) CDCWorkerCandidates() []Candidate {
	var out []Candidate
	for _, e := range s.byKindType("Provider", "debezium") {
		out = append(out, Candidate{Name: e.Metadata.Name, Summary: "debezium Connect worker"})
	}
	return sortCandidates(out)
}

// EventStreamCandidates returns every EventStream in the loaded set.
func (s Snapshot) EventStreamCandidates() []Candidate {
	var out []Candidate
	for _, e := range s.byKindType("EventStream", "") {
		out = append(out, Candidate{Name: e.Metadata.Name, Summary: "EventStream"})
	}
	return sortCandidates(out)
}

// DatasetCandidate is a Dataset already wired to a sink Binding, carrying
// everything a reuse needs to add a second Binding to the same
// infrastructure at a different location (ADR 024's "prefix-override"
// scenario): the Dataset's own bucket/format, its backing lake Provider,
// and the worker Provider realizing its existing sink Binding.
type DatasetCandidate struct {
	Candidate
	Bucket       string
	Prefix       string
	Format       string
	LakeProvider string
	SinkProvider string // the worker Provider realizing the existing sink Binding into this Dataset
}

// DatasetCandidates returns every Dataset that has at least one sink
// Binding targeting it, so its lake/worker Providers are known and
// reusable. A Dataset with no sink Binding yet is not offered as a reuse
// candidate — nothing names which worker would realize a second Binding
// into it.
func (s Snapshot) DatasetCandidates() []DatasetCandidate {
	var out []DatasetCandidate
	for _, ds := range s.byKindType("Dataset", "") {
		lakeProvider := resource.RefName(ds.Spec, "providerRef")
		bucket, _ := ds.Spec["bucket"].(string)
		prefix, _ := ds.Spec["prefix"].(string)
		format, _ := ds.Spec["format"].(string)
		sinkProvider := s.firstSinkWorkerFor(ds.Metadata.Name)
		if sinkProvider == "" {
			continue
		}
		out = append(out, DatasetCandidate{
			Candidate:    Candidate{Name: ds.Metadata.Name, Summary: "Dataset (bucket=" + bucket + ", prefix=" + prefix + ")"},
			Bucket:       bucket,
			Prefix:       prefix,
			Format:       format,
			LakeProvider: lakeProvider,
			SinkProvider: sinkProvider,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// firstSinkWorkerFor returns the providerRef of the first sink-mode
// Binding whose targetRef names datasetName, or "" if none exists.
func (s Snapshot) firstSinkWorkerFor(datasetName string) string {
	for _, b := range s.byKindType("Binding", "") {
		mode, _ := b.Spec["mode"].(string)
		if mode != "sink" {
			continue
		}
		if resource.RefName(b.Spec, "targetRef") != datasetName {
			continue
		}
		return resource.RefName(b.Spec, "providerRef")
	}
	return ""
}

// DatasetCandidateByName finds a DatasetCandidate by name, for resolving a
// --sink existing:<name> flag against the reuse-eligible set.
func (s Snapshot) DatasetCandidateByName(name string) (DatasetCandidate, bool) {
	for _, c := range s.DatasetCandidates() {
		if c.Name == name {
			return c, true
		}
	}
	return DatasetCandidate{}, false
}

// ProviderCandidates returns every Provider of the given spec.type.
func (s Snapshot) ProviderCandidates(specType string) []Candidate {
	var out []Candidate
	for _, e := range s.byKindType("Provider", specType) {
		out = append(out, Candidate{Name: e.Metadata.Name, Summary: specType + " Provider"})
	}
	return sortCandidates(out)
}
