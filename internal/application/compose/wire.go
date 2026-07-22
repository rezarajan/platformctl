package compose

import (
	"fmt"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/binding"
)

// WireOptions is `platformctl wire <mode>`'s flag-mode input: connects two
// *existing* blocks with a Binding, emitting any missing glue (a worker
// Provider, and — cdc mode only — the EventStream target itself) reuse-
// first (ADR 024).
type WireOptions struct {
	Mode string // cdc | sink | ingest
	From string // "Kind/name" — must already exist
	To   string // "Kind/name" — must already exist, except a cdc Binding's EventStream target (missing glue)

	Provider     RefChoice // the worker Provider realizing the Binding; required
	ProviderName string    // used when Provider.New; default "<from>-<mode>"
	ProviderType string    // required when Provider.New: the Provider technology (debezium, s3sink, jdbcsink, s3source, ...)

	// Consulted only when --to names an EventStream that does not exist yet
	// (cdc mode's missing glue):
	Broker     RefChoice
	BrokerName string
	Partitions int    // default 6
	Retention  string // default "7d"

	Tables       []string // cdc only; default ["records"]
	SnapshotMode string   // cdc only; default "initial"

	Name string // Binding name; default "<from>-to-<to>"
}

// PlanWire computes the manifest patch for `wire <mode>`.
func PlanWire(snap Snapshot, dir string, opts WireOptions) (Patch, error) {
	command := "wire " + opts.Mode
	mode := binding.Mode(opts.Mode)
	pairs, ok := binding.AllowedKindPairs[mode]
	if !ok {
		return Patch{}, fmt.Errorf("mode %q is not a wireable Binding mode (known: cdc, sink, ingest)", opts.Mode)
	}

	fromKind, fromName, err := parseKindName("--from", opts.From)
	if err != nil {
		return Patch{}, err
	}
	toKind, toName, err := parseKindName("--to", opts.To)
	if err != nil {
		return Patch{}, err
	}
	if !snap.NameExists(fromKind, fromName) {
		return Patch{}, fmt.Errorf("--from %s/%s does not exist in the manifest set (wire connects two existing blocks)", fromKind, fromName)
	}

	matched := false
	var allowed []string
	for _, p := range pairs {
		allowed = append(allowed, p.SourceKind+"->"+p.TargetKind)
		if p.SourceKind == fromKind && p.TargetKind == toKind {
			matched = true
		}
	}
	if !matched {
		return Patch{}, fmt.Errorf("mode %q does not connect %s to %s (allowed pairings: %s)", opts.Mode, fromKind, toKind, strings.Join(allowed, ", "))
	}

	patch := Patch{Command: command, Dir: dir}
	var pieces []piece
	toExists := snap.NameExists(toKind, toName)
	if !toExists {
		if mode != binding.ModeCDC || toKind != "EventStream" {
			return Patch{}, fmt.Errorf("--to %s/%s does not exist in the manifest set; wire only creates missing glue for a cdc Binding's EventStream target", toKind, toName)
		}
		broker, err := autoSelectBroker(snap, opts.Broker)
		if err != nil {
			return Patch{}, err
		}
		brokerName, note, err := resolveBroker(snap, broker, opts.BrokerName, toName, command, &pieces)
		if err != nil {
			return Patch{}, err
		}
		patch.Notes = append(patch.Notes, note)
		partitions := opts.Partitions
		if partitions == 0 {
			partitions = 6
		}
		retention := opts.Retention
		if retention == "" {
			retention = "7d"
		}
		pieces = append(pieces, piece{"EventStream", toName, renderEventStream(command, toName, brokerName, partitions, retention)})
		patch.Notes = append(patch.Notes, fmt.Sprintf("creating missing EventStream %q", toName))
	}

	var providerName string
	if opts.Provider.New {
		if opts.ProviderType == "" {
			return Patch{}, fmt.Errorf("--provider new requires --provider-type")
		}
		providerName = opts.ProviderName
		if providerName == "" {
			providerName = fromName + "-" + opts.Mode
		}
		pieces = append(pieces, piece{"Provider", providerName, renderGenericWorkerProvider(command, providerName, opts.ProviderType)})
		patch.Notes = append(patch.Notes, fmt.Sprintf("creating new %s Provider %q", opts.ProviderType, providerName))
	} else {
		if opts.Provider.Name == "" {
			return Patch{}, fmt.Errorf("--provider requires \"new\" or \"existing:<name>\"")
		}
		if !snap.NameExists("Provider", opts.Provider.Name) {
			return Patch{}, fmt.Errorf("--provider existing:%s: no such Provider in the manifest set", opts.Provider.Name)
		}
		providerName = opts.Provider.Name
		patch.Notes = append(patch.Notes, fmt.Sprintf("reusing Provider %q", providerName))
	}

	bindingName := opts.Name
	if bindingName == "" {
		bindingName = fromName + "-to-" + toName
	}
	var bindingDoc string
	switch mode {
	case binding.ModeCDC:
		tables := opts.Tables
		if len(tables) == 0 {
			tables = []string{"records"}
		}
		snapshotMode := opts.SnapshotMode
		if snapshotMode == "" {
			snapshotMode = "initial"
		}
		bindingDoc = renderCDCBinding(command, bindingName, fromName, toName, providerName, tables, snapshotMode)
	default: // sink, ingest: same source/target/providerRef shape, mode-parameterized
		bindingDoc = renderBinding(command, string(mode), bindingName, fromName, toName, providerName)
	}
	pieces = append(pieces, piece{"Binding", bindingName, bindingDoc})

	for _, p := range pieces {
		op, err := resolveFile(dir, snap, p.kind, p.name, p.content)
		if err != nil {
			return Patch{}, err
		}
		patch.Files = append(patch.Files, op)
	}
	return patch, nil
}

// autoSelectBroker implements the "reuse-first" half of wire's missing-glue
// EventStream: when --broker was not given at all (the zero RefChoice),
// auto-pick the sole existing broker candidate; with zero or more than one
// candidate, the caller must disambiguate with an explicit --broker flag —
// exactly the same "reuse existing candidates, else ask" rule the
// interactive select renders, just without a TTY to ask through.
func autoSelectBroker(snap Snapshot, choice RefChoice) (RefChoice, error) {
	if choice.New || choice.Name != "" {
		return choice, nil
	}
	candidates := snap.BrokerCandidates()
	if len(candidates) == 1 {
		return RefChoice{Name: candidates[0].Name}, nil
	}
	return RefChoice{}, fmt.Errorf("--broker is required to create the missing EventStream (candidates: %s; or --broker new)", candidateNames(candidates))
}

// parseKindName parses a "Kind/name" selector (the same shape
// resource.ParseSelector validates, reimplemented locally to avoid forcing
// a namespace argument compose has no use for).
func parseKindName(flag, value string) (kind, name string, err error) {
	kind, name, ok := strings.Cut(value, "/")
	if !ok || kind == "" || name == "" {
		return "", "", fmt.Errorf("%s must be <Kind>/<name>, got %q", flag, value)
	}
	return kind, name, nil
}

// renderGenericWorkerProvider renders a minimal Provider skeleton for a
// technology this package has no specific rendering knowledge of (wire's
// --provider new --provider-type <type> escape hatch) — a comment points
// at the resource reference doc for the fields spec.configuration needs.
func renderGenericWorkerProvider(command, name, providerType string) string {
	explain := fmt.Sprintf(
		"A new %s Provider (docs/planning/03-resource-model-reference.md\n"+
			"has its spec.configuration fields) — fill in whatever this technology\n"+
			"requires before `platformctl apply`.", providerType)
	lines := []string{"type: " + providerType, "runtime:", "  type: docker"}
	return renderDoc(command, explain, "Provider", name, lines)
}
