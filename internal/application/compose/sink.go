package compose

import "fmt"

// SinkOptions is `platformctl add sink`'s flag-mode input: wires an
// existing EventStream into a (possibly reused) Dataset, standalone (no
// new source/CDC — that's `add pipeline` or a prior `add source` + `wire
// cdc`).
type SinkOptions struct {
	Name   string // required: names the sink Binding ("<Name>-to-lake") and any newly-created Dataset ("<Name>-lake")
	Stream string // required: an existing EventStream name

	SinkAttachment
}

// PlanSink computes the manifest patch for `add sink`.
func PlanSink(snap Snapshot, dir string, opts SinkOptions) (Patch, error) {
	const command = "add sink"
	if opts.Name == "" {
		return Patch{}, fmt.Errorf("--name is required")
	}
	if opts.Stream == "" {
		return Patch{}, fmt.Errorf("--stream is required")
	}
	if !hasCandidate(snap.EventStreamCandidates(), opts.Stream) {
		return Patch{}, fmt.Errorf("--stream %s: no such EventStream (candidates: %s)", opts.Stream, candidateNames(snap.EventStreamCandidates()))
	}

	patch := Patch{Command: command, Dir: dir}
	sinkTarget, sinkWorker, note, pieces, envAppends, err := resolveSink(snap, opts.Name, opts.SinkAttachment, command)
	if err != nil {
		return Patch{}, err
	}
	patch.Notes = append(patch.Notes, note)

	bindingName := opts.Name + "-to-lake"
	pieces = append(pieces, piece{"Binding", bindingName, renderSinkBinding(command, bindingName, opts.Stream, sinkTarget, sinkWorker)})

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
