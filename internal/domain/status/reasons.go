package status

// Condition Reason catalog (docs/planning/08 G4). Every `status.Condition`
// constructed anywhere outside this package must set Reason to one of these
// constants — internal/archtest/reason_literal_test.go fails the build
// otherwise. This is a prerequisite for E4's `explain` catalog, which walks
// this file to enumerate every reason a user can see.
//
// These are plain untyped string constants (not a named Reason type): the
// Condition.Reason field is a bare string, and a named type would force a
// cast at every construction site for no behavioral benefit.
//
// A handful of reasons below are combined with runtime-observed data before
// being assigned to Condition.Reason (e.g. redpanda's PartitionCountMismatch
// carries the observed/wanted counts, debezium/s3sink's ConnectorState
// carries the live connector state). Each such site is commented at its
// call site explaining why the reason stays partially dynamic; the constant
// here still names the stable, greppable prefix.

// --- Generic reconcile/probe lifecycle -------------------------------------
// Used by the engine and by every provider's Reconcile/Probe: these three
// carry no technology-specific meaning.
const (
	ReasonReconcileComplete = "ReconcileComplete"
	ReasonNoDrift           = "NoDrift"
	ReasonProbeFailed       = "ProbeFailed"
)

// --- Secrets (internal/application/engine SecretReference handling) -------
const (
	ReasonSecretUnresolvable = "SecretUnresolvable"
	ReasonSecretResolvable   = "SecretResolvable"
	ReasonSecretChanged      = "SecretChanged"
)

// --- External/connection (internal/application/engine External binding) --
const (
	ReasonExternalConnectionUnresolvable = "ExternalConnectionUnresolvable"
	ReasonExternalConnectionResolvable   = "ExternalConnectionResolvable"
	ReasonExternalEndpointUnreachable    = "ExternalEndpointUnreachable"
	// ReasonExternalEndpointUnreachableInNetwork is the in-network-audience
	// counterpart of ExternalEndpointUnreachable (docs/planning/08 C10): the
	// endpoint answers from the host but not from the network a consuming
	// Binding will dial it from (or vice versa) — the two vantage points are
	// probed and reported distinctly, never folded together.
	ReasonExternalEndpointUnreachableInNetwork = "ExternalEndpointUnreachableInNetwork"
	ReasonExternalEndpointReachable            = "ExternalEndpointReachable"
)

// --- Lineage (docs/planning/02-architecture.md §5.5) -----------------------

// ReasonLineageNotConsumed is the informational reason recorded when a
// resource declares observers but its provider does not implement
// LineageAware. Never blocks Ready.
const ReasonLineageNotConsumed = "LineageEndpointDeclaredNotConsumed"

// --- Shared instance lifecycle ---------------------------------------------
// postgres, mysql, nessie, and s3 each provision a base "Instance"-shaped
// container/service before layering their technology-specific kind on top;
// all four report its health with these two reasons verbatim.
const (
	ReasonInstanceHealthy   = "InstanceHealthy"
	ReasonInstanceUnhealthy = "InstanceUnhealthy"
)

// --- Shared CDC source (postgres Source, mysql Source) ---------------------
// postgres and mysql both implement CDCCapableProvider over a Source kind
// and share these four reasons verbatim; each also has technology-specific
// precondition reasons declared in its own section below (WALNotLogical vs
// BinlogNotRowFormat name different settings and are deliberately not
// unified — docs/planning/08 G4).
const (
	ReasonSourceProvisioned             = "SourceProvisioned"
	ReasonDatabaseMissing               = "DatabaseMissing"
	ReasonReplicationCredentialsInvalid = "ReplicationCredentialsInvalid"
	ReasonSourceHealthy                 = "SourceHealthy"
)

// --- Shared Kafka Connect connector lifecycle (debezium Binding, s3sink
// Binding) -------------------------------------------------------------
// Both providers reconcile a Kafka Connect connector and share these
// reasons verbatim.
const (
	ReasonConnectWorkerHealthy   = "ConnectWorkerHealthy"
	ReasonConnectWorkerUnhealthy = "ConnectWorkerUnhealthy"
	ReasonConnectorRunning       = "ConnectorRunning"
	ReasonConnectorNotRunning    = "ConnectorNotRunning"
	ReasonConnectorMissing       = "ConnectorMissing"
	ReasonConnectorConfigDrift   = "ConnectorConfigDrift"
	// ReasonConnectorState is a prefix, not a complete reason: both
	// providers append the live Kafka Connect connector state (e.g.
	// "PAUSED", "FAILED") to it at the call site so the reason names the
	// exact observed state without a separate Message. See debezium.go and
	// s3sink.go for the call sites.
	ReasonConnectorState = "ConnectorState"
)

// --- noop provider (internal/adapters/providers/noop; test/dev only) ------
const (
	ReasonNoopReconciled = "NoopReconciled"
	ReasonNoopHealthy    = "NoopHealthy"
)

// --- placeholder provider ---------------------------------------------------
const (
	ReasonHealthCheckPassed  = "HealthCheckPassed"
	ReasonContainerMissing   = "ContainerMissing"
	ReasonContainerUnhealthy = "ContainerUnhealthy"
)

// --- redpanda (EventStream broker, Topic) -----------------------------------
const (
	ReasonBrokerHealthy   = "BrokerHealthy"
	ReasonBrokerUnhealthy = "BrokerUnhealthy"
	ReasonTopicReconciled = "TopicReconciled"
	ReasonTopicHealthy    = "TopicHealthy"
	// ReasonTopicMissing, ReasonPartitionCountMismatch, and
	// ReasonRetentionMismatch are probeTopic's drift reasons
	// (internal/adapters/providers/redpanda/kafka.go). The latter two are
	// combined with the observed/wanted values via fmt.Sprintf at the call
	// site so the reason carries the mismatch detail; the constant here
	// names the stable, greppable prefix.
	ReasonTopicMissing           = "TopicMissing"
	ReasonPartitionCountMismatch = "PartitionCountMismatch"
	ReasonRetentionMismatch      = "RetentionMismatch"
	// ReasonBrokerMissing, ReasonBrokerNotJoined, and
	// ReasonReplicationFactorMismatch are the multi-broker drift reasons
	// (docs/adr/017 §a.6): a set ordinal absent/stopped at the runtime, a
	// broker present but not a cluster member per the admin API, and a
	// topic whose observed replication factor differs from
	// spec.replication. Same constant-prefix + dynamic-detail convention
	// as PartitionCountMismatch above.
	ReasonBrokerMissing             = "BrokerMissing"
	ReasonBrokerNotJoined           = "BrokerNotJoined"
	ReasonReplicationFactorMismatch = "ReplicationFactorMismatch"
)

// --- postgres-specific probe reasons ----------------------------------------
const ReasonWALNotLogical = "WALNotLogical"

// --- mysql-specific probe reasons -------------------------------------------
const ReasonBinlogNotRowFormat = "BinlogNotRowFormat"

// --- openlineage --------------------------------------------------------
const (
	ReasonLineageBackendHealthy   = "LineageBackendHealthy"
	ReasonLineageBackendUnhealthy = "LineageBackendUnhealthy"
)

// --- proxy ------------------------------------------------------------------
const (
	ReasonEntrypointSurfaceReady = "EntrypointSurfaceReady"
	ReasonForwarding             = "Forwarding"
	ReasonForwarderDown          = "ForwarderDown"
	ReasonUpstreamUnreachable    = "UpstreamUnreachable"
)

// --- nessie ------------------------------------------------------------
const (
	ReasonCatalogProvisioned = "CatalogProvisioned"
	ReasonCatalogHealthy     = "CatalogHealthy"
	// ReasonBranchMissing and ReasonCatalogUnreachable are Catalog probe's
	// two possible drift reasons (internal/adapters/providers/nessie/
	// nessie.go); selected between, not interpolated, so they stay plain
	// constants.
	ReasonBranchMissing      = "BranchMissing"
	ReasonCatalogUnreachable = "CatalogUnreachable"
)

// --- s3 (Dataset) ------------------------------------------------------
const (
	ReasonDatasetProvisioned = "DatasetProvisioned"
	ReasonDatasetHealthy     = "DatasetHealthy"
	ReasonBucketMissing      = "BucketMissing"
	ReasonPrefixUnlistable   = "PrefixUnlistable"
)

// --- trino (compute-engine provider; docs/planning/08 D10) -----------------
const (
	// ReasonCoordinatorHealthy/Unhealthy: the coordinator container's own
	// health (reuses the shared-instance shape's naming convention, but
	// declared separately since a Ready trino Provider blocks on both the
	// coordinator AND the worker set below — a single ReasonInstanceHealthy
	// couldn't distinguish which).
	ReasonCoordinatorHealthy   = "CoordinatorHealthy"
	ReasonCoordinatorUnhealthy = "CoordinatorUnhealthy"
	// ReasonWorkerCountMismatch: ContainerState.ReadyReplicas does not
	// (yet) match the declared spec.configuration.workers count.
	ReasonWorkerCountMismatch = "WorkerCountMismatch"
	// ReasonCatalogConfigMissing: configuration.catalogRef is set but the
	// referenced Catalog (or its resolved warehouse Provider) has not yet
	// published the facts etc/catalog/lakehouse.properties needs
	// (Request.CatalogFacts is nil) — distinct from CatalogConfigDrift
	// below, which is the file existing but disagreeing with the current
	// facts.
	ReasonCatalogConfigMissing = "CatalogConfigMissing"
	// ReasonCatalogConfigDrift: the live etc/catalog/lakehouse.properties
	// (read back via ContainerRuntime.ReadFile) no longer matches the file
	// regenerated from the currently-published catalog/warehouse facts —
	// the same regenerate-and-diff-by-key bar as prometheus's
	// ScrapeConfigDrift / debezium's ConnectorConfigDrift.
	ReasonCatalogConfigDrift = "CatalogConfigDrift"
)

// --- prometheus (managed monitoring stack; docs/planning/08 C9) ------------
// The base container reuses ReasonInstanceHealthy/ReasonInstanceUnhealthy
// above (the shared single-container-instance shape). These two are
// prometheus-specific probe outcomes once the container itself is healthy.
const (
	// ReasonScrapeTargetsIncomplete: Ready requires /api/v1/targets'
	// activeTargets count to match the number of configured scrape
	// targets — per-target up-ness is Prometheus's own concern, not
	// Ready-blocking (docs/planning/08 C9).
	ReasonScrapeTargetsIncomplete = "ScrapeTargetsIncomplete"
	// ReasonScrapeConfigDrift: the live scrape config (fetched via
	// /api/v1/status/config) no longer matches the config regenerated from
	// currently-published metrics endpoint facts.
	ReasonScrapeConfigDrift = "ScrapeConfigDrift"
)
