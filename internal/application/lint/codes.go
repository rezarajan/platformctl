package lint

// Built-in lint codes — docs/adr/020-design-lints.md §4's table. Only these
// 12 codes exist; "DL001-DL022" names the addressing range the table
// reserves (DL005-DL009 and DL015-DL019 are gaps left for future codes),
// not 22 distinct lints.
const (
	CodeDuplicateCapture    = "DL001" // warning: overlapping cdc capture on one sourceRef
	CodeSinkCollision       = "DL002" // warning: sink Bindings target the same bucket+prefix/table
	CodeObserverNotConsumed = "DL003" // warning: observers names a Provider with no LineageAware
	CodePlaintextBoundary   = "DL004" // warning: a plaintext Connection where a TLS realization exists

	CodeOrphanedEventStream     = "DL010" // info: no Binding reads or writes it
	CodeUnreferencedCatalog     = "DL011" // info: nothing consumes it
	CodeUnusedResource          = "DL012" // info: SecretReference/Connection/Provider nothing resolves
	CodeDeadEndPipeline         = "DL013" // info: cdc Binding's EventStream has no downstream
	CodeSingleReplicaWithHAGate = "DL014" // info: brokers/workers/nodes = 1 with HighAvailability on

	CodeDeletionPolicyUnset = "DL020" // warning: deletionPolicy unset on Dataset/Source
	CodeProtectUnset        = "DL021" // warning: protect unset where authoritative deletes are in play
	CodeNamespaceWideGrant  = "DL022" // warning: spec.access grant entry has no selector (docs/adr/033 decision 3)
)

// CodeMalformedWaiver is not in ADR 020 §4's table (it's about the waiver
// mechanism itself, not a design hazard) — ADR 020 §2 requires it without
// giving it a code: "empty reason = the waiver itself is a warning". DL000
// is otherwise unused in the reserved range.
const CodeMalformedWaiver = "DL000"

// BuiltinCodes lists every code this package's Run can produce, for the E4
// explain-catalog completeness guard (cmd/platformctl/lint_catalog_test.go)
// — kept in this one file, next to the constants above, so adding a code
// and forgetting to register it here fails loudly rather than silently.
var BuiltinCodes = []string{
	CodeMalformedWaiver,
	CodeDuplicateCapture,
	CodeSinkCollision,
	CodeObserverNotConsumed,
	CodePlaintextBoundary,
	CodeOrphanedEventStream,
	CodeUnreferencedCatalog,
	CodeUnusedResource,
	CodeDeadEndPipeline,
	CodeSingleReplicaWithHAGate,
	CodeDeletionPolicyUnset,
	CodeProtectUnset,
	CodeNamespaceWideGrant,
}
