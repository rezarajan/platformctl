package compose

import (
	"fmt"
	"strings"
)

// SinkAttachment is the sink attachment point ADR 024 describes: reuse an
// existing Dataset (optionally at a new prefix, reusing its lake+worker
// Providers into a fresh Dataset) or create a whole new sink chain. Shared
// between `add pipeline` (PipelineOptions) and the standalone `add sink`
// (SinkOptions).
type SinkAttachment struct {
	Sink       RefChoice // required
	SinkPrefix string    // optional; only meaningful with Sink pointing at an existing Dataset

	// The following are only consulted when Sink.New:
	Lake     RefChoice
	LakeName string // used when Lake.New; default "<base>-lake-store"
	SinkName string // worker Provider name; default "<base>-sink"
	Bucket   string // required when Sink.New
	Prefix   string // default ""
	Format   string // default "json"
}

// PipelineOptions is `platformctl add pipeline`'s flag-mode input: a new
// source with CDC into a (possibly reused) broker, sunk into a (possibly
// reused) Dataset — the owner's "source-with-cdc-and-sink" (ADR 024).
type PipelineOptions struct {
	Name         string   // required: base name every generated resource derives from
	Engine       string   // required: postgres | mysql | mariadb
	Database     string   // default: Name
	Tables       []string // default: ["records"]
	SnapshotMode string   // default: "initial"

	Broker     RefChoice // required
	BrokerName string    // used when Broker.New; default "<Name>-broker"

	SinkAttachment
}

// PlanPipeline computes the manifest patch for `add pipeline`.
func PlanPipeline(snap Snapshot, dir string, opts PipelineOptions) (Patch, error) {
	const command = "add pipeline"
	if opts.Name == "" {
		return Patch{}, fmt.Errorf("--name is required")
	}
	dbSpec, err := lookupDBEngine(opts.Engine)
	if err != nil {
		return Patch{}, err
	}
	database := opts.Database
	if database == "" {
		database = opts.Name
	}
	tables := opts.Tables
	if len(tables) == 0 {
		tables = []string{"records"}
	}
	snapshotMode := opts.SnapshotMode
	if snapshotMode == "" {
		snapshotMode = "initial"
	}

	patch := Patch{Command: command, Dir: dir}
	var pieces []piece
	var envAppends []EnvAppend

	// --- source side: Provider(db) + Source + admin/replication secrets ---
	dbProviderName := opts.Name + "-db"
	adminSecret := opts.Name + "-admin-creds"
	replSecret := opts.Name + "-replication-creds"
	pieces = append(pieces,
		piece{"Provider", dbProviderName, renderDBProvider(command, dbSpec, dbProviderName, adminSecret, replSecret)},
		piece{"Source", opts.Name, renderSource(command, opts.Engine, opts.Name, dbProviderName, database)},
	)
	adminDoc, replDoc, secretEnv := renderSecretPair(command, adminSecret, dbSpec.AdminUser, replSecret)
	pieces = append(pieces,
		piece{"SecretReference", adminSecret, adminDoc},
		piece{"SecretReference", replSecret, replDoc},
	)
	envAppends = append(envAppends, secretEnv...)

	// --- broker: reuse or create ---
	brokerName, note, err := resolveBroker(snap, opts.Broker, opts.BrokerName, opts.Name, command, &pieces)
	if err != nil {
		return Patch{}, err
	}
	patch.Notes = append(patch.Notes, note)

	// --- CDC: worker + EventStream + Binding, always new (one per source) ---
	cdcWorkerName := opts.Name + "-cdc"
	streamName := opts.Name + "-events"
	cdcBindingName := opts.Name + "-to-events"
	pieces = append(pieces,
		piece{"Provider", cdcWorkerName, renderCDCWorkerProvider(command, cdcWorkerName, replSecret)},
		piece{"EventStream", streamName, renderEventStream(command, streamName, brokerName, 6, "7d")},
		piece{"Binding", cdcBindingName, renderCDCBinding(command, cdcBindingName, opts.Name, streamName, cdcWorkerName, tables, snapshotMode)},
	)

	// --- sink: reuse or create ---
	sinkTarget, sinkWorker, sinkNote, sinkPieces, sinkEnv, err := resolveSink(snap, opts.Name, opts.SinkAttachment, command)
	if err != nil {
		return Patch{}, err
	}
	patch.Notes = append(patch.Notes, sinkNote)
	pieces = append(pieces, sinkPieces...)
	envAppends = append(envAppends, sinkEnv...)

	sinkBindingName := opts.Name + "-to-lake"
	pieces = append(pieces, piece{"Binding", sinkBindingName, renderSinkBinding(command, sinkBindingName, streamName, sinkTarget, sinkWorker)})

	for _, p := range pieces {
		op, err := resolveFile(dir, snap, p.kind, p.name, p.content)
		if err != nil {
			return Patch{}, err
		}
		patch.Files = append(patch.Files, op)
	}
	pending, err := resolveEnvAppends(dir, envAppends)
	if err != nil {
		return Patch{}, err
	}
	patch.EnvAppends = pending
	return patch, nil
}

// resolveBroker implements the broker attachment point: reuse an existing
// redpanda Provider by name, or create a new one.
func resolveBroker(snap Snapshot, choice RefChoice, brokerName, baseName, command string, pieces *[]piece) (name, note string, err error) {
	if !choice.New {
		if choice.Name == "" {
			return "", "", fmt.Errorf("--broker requires \"new\" or \"existing:<name>\"")
		}
		if !hasCandidate(snap.BrokerCandidates(), choice.Name) {
			return "", "", fmt.Errorf("--broker existing:%s: no such broker Provider (candidates: %s)", choice.Name, candidateNames(snap.BrokerCandidates()))
		}
		return choice.Name, fmt.Sprintf("reusing broker Provider %q", choice.Name), nil
	}
	name = brokerName
	if name == "" {
		name = baseName + "-broker"
	}
	*pieces = append(*pieces, piece{"Provider", name, renderBrokerProvider(command, name)})
	return name, fmt.Sprintf("creating new broker Provider %q", name), nil
}

// resolveSink implements the sink attachment point: reuse an existing
// Dataset (optionally at a new prefix — a new Dataset reusing the same
// lake+worker Providers) or create a whole new sink chain. base names the
// derived resources (the composite's --name).
func resolveSink(snap Snapshot, base string, attach SinkAttachment, command string) (targetName, workerName, note string, pieces []piece, envAppends []EnvAppend, err error) {
	if !attach.Sink.New {
		if attach.Sink.Name == "" {
			return "", "", "", nil, nil, fmt.Errorf("--sink requires \"new\" or \"existing:<name>\"")
		}
		ds, ok := snap.DatasetCandidateByName(attach.Sink.Name)
		if !ok {
			return "", "", "", nil, nil, fmt.Errorf("--sink existing:%s: no such reusable Dataset (candidates: %s)", attach.Sink.Name, candidateNames(datasetCandidatesAsCandidates(snap)))
		}
		prefix := attach.SinkPrefix
		if prefix == "" || prefix == ds.Prefix {
			note = fmt.Sprintf("reusing Dataset %q and sink worker Provider %q (bucket=%s, prefix=%s)", ds.Name, ds.SinkProvider, ds.Bucket, ds.Prefix)
			return ds.Name, ds.SinkProvider, note, nil, nil, nil
		}
		newDatasetName := base + "-lake"
		pieces = append(pieces, piece{"Dataset", newDatasetName, renderDataset(command, newDatasetName, ds.LakeProvider, ds.Bucket, prefix, ds.Format)})
		note = fmt.Sprintf("reusing lake Provider %q and sink worker Provider %q; new Dataset %q at bucket=%s prefix=%s", ds.LakeProvider, ds.SinkProvider, newDatasetName, ds.Bucket, prefix)
		return newDatasetName, ds.SinkProvider, note, pieces, nil, nil
	}

	// New sink chain: lake (reuse or new) + sink worker (always new) + Dataset (always new).
	if attach.Bucket == "" {
		return "", "", "", nil, nil, fmt.Errorf("--sink new requires --bucket")
	}
	format := attach.Format
	if format == "" {
		format = "json"
	}

	var lakeName, lakeSecretRef string
	var lakeNote string
	if attach.Lake.New {
		lakeName = attach.LakeName
		if lakeName == "" {
			lakeName = base + "-lake-store"
		}
		lakeSecretRef = base + "-lake-creds"
		pieces = append(pieces, piece{"Provider", lakeName, renderLakeProvider(command, lakeName, lakeSecretRef)})
		secretDoc, secretEnv := renderSecret(command, lakeSecretRef, "minioadmin")
		pieces = append(pieces, piece{"SecretReference", lakeSecretRef, secretDoc})
		envAppends = append(envAppends, secretEnv...)
		lakeNote = fmt.Sprintf("creating new lake Provider %q", lakeName)
	} else {
		if attach.Lake.Name == "" {
			return "", "", "", nil, nil, fmt.Errorf("--sink new requires --lake \"new\" or \"existing:<name>\"")
		}
		lakeEnv, ok := snap.byName("Provider", attach.Lake.Name)
		if !ok || !hasCandidate(snap.LakeCandidates(), attach.Lake.Name) {
			return "", "", "", nil, nil, fmt.Errorf("--lake existing:%s: no such lake Provider (candidates: %s)", attach.Lake.Name, candidateNames(snap.LakeCandidates()))
		}
		lakeName = attach.Lake.Name
		if cfg, ok := lakeEnv.Spec["configuration"].(map[string]any); ok {
			lakeSecretRef, _ = cfg["rootSecretRef"].(string)
		}
		lakeNote = fmt.Sprintf("reusing lake Provider %q", lakeName)
	}

	sinkWorkerName := attach.SinkName
	if sinkWorkerName == "" {
		sinkWorkerName = base + "-sink"
	}
	pieces = append(pieces, piece{"Provider", sinkWorkerName, renderSinkWorkerProvider(command, sinkWorkerName, lakeSecretRef)})

	datasetName := base + "-lake"
	pieces = append(pieces, piece{"Dataset", datasetName, renderDataset(command, datasetName, lakeName, attach.Bucket, attach.Prefix, format)})

	note = lakeNote + fmt.Sprintf("; creating new sink worker Provider %q and Dataset %q", sinkWorkerName, datasetName)
	return datasetName, sinkWorkerName, note, pieces, envAppends, nil
}

func hasCandidate(cands []Candidate, name string) bool {
	for _, c := range cands {
		if c.Name == name {
			return true
		}
	}
	return false
}

func candidateNames(cands []Candidate) string {
	names := make([]string, len(cands))
	for i, c := range cands {
		names[i] = c.Name
	}
	if len(names) == 0 {
		return "(none)"
	}
	return strings.Join(names, ", ")
}

func datasetCandidatesAsCandidates(snap Snapshot) []Candidate {
	dcs := snap.DatasetCandidates()
	out := make([]Candidate, len(dcs))
	for i, c := range dcs {
		out[i] = c.Candidate
	}
	return out
}
