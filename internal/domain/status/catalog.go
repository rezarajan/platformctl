package status

// CatalogEntry is one `platformctl explain` (docs/planning/08 E4) entry:
// either a Condition Type (Ready, Progressing, Degraded, DriftDetected) or
// a Condition Reason declared in reasons.go. Meaning generally reuses that
// constant's doc comment verbatim (reasons.go's comments are the source of
// truth); Causes/Remedies are authored here since reasons.go's comments
// document the mechanism, not the operator playbook.
//
// Catalog is this package's enumerable source of truth for explain: every
// Reason* constant in reasons.go must have exactly one entry here, and
// internal/archtest's completeness test fails naming any that don't.
type CatalogEntry struct {
	// Token is the string explain matches: a ConditionType's value or a
	// Reason constant's value.
	Token string
	// Area groups entries the way reasons.go's own section comments do
	// (e.g. "redpanda", "secrets") — used to organize the rendered docs
	// page and to label candidates in explain's fallback output.
	Area string
	// Kind is "conditionType" or "reason".
	Kind string
	// Prefix marks a dynamic reason: the provider appends observed detail
	// (a live state, a mismatch count) to Token at the call site — see
	// reasons.go's per-reason doc comments (ReasonConnectorState,
	// ReasonPartitionCountMismatch, ...). explain matches these by prefix
	// as well as by exact token, since the token a user actually sees in
	// `status`/`drift` output carries the appended detail.
	Prefix bool
	// Meaning is what the condition/reason means.
	Meaning string
	// Causes lists likely root causes, most-common first. Empty/"N/A" for
	// reasons that indicate success rather than a problem.
	Causes []string
	// Remedies lists remedy commands or actions.
	Remedies []string
}

// healthyCauses/healthyRemedies are the standard filler for reasons that
// indicate success or health rather than a problem — explain still emits a
// complete entry (every field populated) rather than omitting Causes/
// Remedies for the roughly half of the catalog that isn't actionable.
var (
	healthyCauses   = []string{"N/A — this reason indicates a healthy or successful state, not a problem."}
	healthyRemedies = []string{"None required."}
)

// Catalog is the full E4 catalog: 4 ConditionType entries followed by every
// Reason constant in reasons.go, grouped in the same order as that file's
// section comments.
var Catalog = []CatalogEntry{
	// --- Condition Types --------------------------------------------------
	{
		Token: string(Ready), Area: "conditionType", Kind: "conditionType",
		Meaning: "Whether the resource is fully reconciled and healthy from the runtime's point of view — the roll-up condition every Reason ultimately feeds. Ready=True means the last Reconcile succeeded and the live health probe passed.",
		Causes: []string{
			"Reconcile has not run yet (resource declared but never applied).",
			"Reconcile ran but returned an error.",
			"A dependency's health check has not passed yet (Progressing).",
			"A technology-specific precondition is unmet — see the resource's actual Reason for which one.",
		},
		Remedies: []string{
			"platformctl status <path> to see the Reason/Message for the affected resource.",
			"platformctl explain <reason> for the specific Reason shown.",
			"platformctl apply <path> to (re)converge.",
		},
	},
	{
		Token: string(Progressing), Area: "conditionType", Kind: "conditionType",
		Meaning: "The resource is mid-reconcile — a multi-step create/update still in flight. Expected to resolve to Ready or Degraded on its own without user action, given enough time.",
		Causes: []string{
			"A multi-step provider action (e.g. multi-node cluster formation, image pull) has not finished yet.",
			"A container/service is still starting and has not passed its health check.",
		},
		Remedies: []string{
			"platformctl status <path> to poll current state.",
			"platformctl drift <path> once it should have settled, to confirm it did.",
		},
	},
	{
		Token: string(Degraded), Area: "conditionType", Kind: "conditionType",
		Meaning: "The resource reconciled but is running in an unhealthy or partially-healthy state — distinct from Ready=False (never reconciled) and DriftDetected (config diverged from spec): Degraded means the live system itself is unwell right now.",
		Causes: []string{
			"A dependency (broker, connect worker, upstream) is down.",
			"A connector or process entered a failed/paused state at the technology level.",
		},
		Remedies: []string{
			"platformctl status <path> for the Degraded condition's Reason/Message.",
			"Check the underlying container/service logs named in Message (docker logs <container>).",
			"platformctl apply <path> after fixing the underlying cause.",
		},
	},
	{
		Token: string(DriftDetected), Area: "conditionType", Kind: "conditionType",
		Meaning: "The live infrastructure no longer matches the spec or state platformctl last applied — set by `platformctl drift`, healed by `platformctl apply`.",
		Causes: []string{
			"An out-of-band change (someone edited or reconfigured the resource directly).",
			"A resource was deleted, stopped, or killed outside platformctl.",
			"A previous apply was interrupted, leaving divergent live state.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the diverged resource(s) and the observed Reason/Message.",
			"platformctl apply <path> to heal drift back to the declared spec.",
		},
	},

	// --- Generic reconcile/probe lifecycle ---------------------------------
	{
		Token: ReasonReconcileComplete, Area: "lifecycle", Kind: "reason",
		Meaning:  "Generic success reason set on Ready=True after a provider's Reconcile finishes without error. Carries no technology-specific detail by design (docs/planning/08 G4) — every provider's Reconcile/Probe uses it identically.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonNoDrift, Area: "lifecycle", Kind: "reason",
		Meaning:  "Generic success reason set on DriftDetected=False: the last `platformctl drift` probe found the live resource matching recorded state.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonProbeFailed, Area: "lifecycle", Kind: "reason",
		Meaning: "The provider's Probe call itself errored (a transport/auth/unexpected-response failure talking to the backing service), as opposed to the probe succeeding and reporting the service unhealthy — that distinction matters because a probe failure usually means the runtime or credentials are wrong, not the service's own health.",
		Causes: []string{
			"The backing container/service is unreachable (network, stopped, wrong port).",
			"Credentials used to probe (admin API, DB connection) are invalid or rotated.",
			"The runtime (Docker/Kubernetes) itself is unreachable.",
		},
		Remedies: []string{
			"platformctl status <path> for the Message (usually the underlying transport error).",
			"Verify the runtime is reachable (docker ps / kubectl get pods) and credentials are current.",
			"platformctl apply <path> to retry once the underlying issue is fixed.",
		},
	},

	// --- Secrets ------------------------------------------------------------
	{
		Token: ReasonSecretUnresolvable, Area: "secrets", Kind: "reason",
		Meaning: "A SecretReference the resource depends on could not be resolved from its configured secret backend (env, file, vault, kubernetes).",
		Causes: []string{
			"The referenced secret name/key does not exist in the backend.",
			"The secret backend (Vault, Kubernetes Secret store) is unreachable or misconfigured.",
			"An environment-backend secret's env var is unset (see docs/planning `--env-file`).",
		},
		Remedies: []string{
			"platformctl apply runs a secret Preflight before touching infrastructure — its error names the exact missing key.",
			"Set the missing key (env var, --env-file, Vault path, or Kubernetes Secret) and re-run.",
		},
	},
	{
		Token: ReasonSecretResolvable, Area: "secrets", Kind: "reason",
		Meaning:  "The resource's SecretReference(s) resolved successfully from their backend.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonSecretChanged, Area: "secrets", Kind: "reason",
		Meaning: "A SecretReference's resolved value now hashes differently than what was recorded at the last apply — the secret was rotated (or its backend content changed) out of band.",
		Causes: []string{
			"The secret was rotated in its backend (Vault, Kubernetes Secret, file) without a platformctl apply.",
			"A different secret backend/environment is now resolving the same reference to a different value.",
		},
		Remedies: []string{
			"platformctl apply <path> to re-apply with the new secret value (most providers pick it up on next reconcile).",
			"If the rotation was unintentional, restore the prior secret value instead.",
		},
	},

	// --- External/connection -------------------------------------------------
	{
		Token: ReasonExternalConnectionUnresolvable, Area: "external", Kind: "reason",
		Meaning: "A Connection or Provider declared external:true references a connectionRef (Connection or bare SecretReference) that could not be resolved.",
		Causes: []string{
			"The referenced Connection/SecretReference does not exist in the manifest set or secret backend.",
			"A typo in connectionRef.name.",
		},
		Remedies: []string{
			"platformctl validate <path> to see the exact resolution error.",
			"Fix the connectionRef name or add the missing Connection/SecretReference.",
		},
	},
	{
		Token: ReasonExternalConnectionResolvable, Area: "external", Kind: "reason",
		Meaning:  "The external resource's connectionRef resolved successfully.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonExternalEndpointUnreachable, Area: "external", Kind: "reason",
		Meaning: "An external endpoint (a Connection pointing at infrastructure platformctl does not manage) did not answer a reachability probe from the host running platformctl.",
		Causes: []string{
			"The external service is down or the address/port is wrong.",
			"A firewall/security-group rule blocks the host's access.",
			"TLS/auth misconfiguration causes the probe to fail before it can confirm reachability.",
		},
		Remedies: []string{
			"Verify the endpoint independently (curl/telnet/nc from the same host).",
			"Check the Connection's host/port and credentials.",
			"platformctl drift <path> once the endpoint is confirmed reachable, to clear the condition.",
		},
	},
	{
		Token: ReasonExternalEndpointUnreachableInNetwork, Area: "external", Kind: "reason",
		Meaning: "The in-network-audience counterpart of ExternalEndpointUnreachable (docs/planning/08 C10): the endpoint answers from the host running platformctl but not from the network a consuming Binding will actually dial it from (or vice versa) — the two vantage points are probed and reported distinctly, never folded together.",
		Causes: []string{
			"The managed runtime network (Docker network / Kubernetes cluster) cannot route to the external endpoint even though the host can (or vice versa).",
			"DNS resolves differently from inside the managed network than from the host.",
			"A network policy or firewall rule scoped to one vantage point but not the other.",
		},
		Remedies: []string{
			"Confirm which vantage point failed in the condition's Message.",
			"Test connectivity from inside the managed network (e.g. a throwaway container on the same Docker network, or a pod in-cluster) versus from the host.",
			"Fix routing/DNS/firewall for the failing vantage point, then platformctl drift <path> to re-probe both.",
		},
	},
	{
		Token: ReasonExternalEndpointReachable, Area: "external", Kind: "reason",
		Meaning:  "The external endpoint answered the reachability probe successfully (from every vantage point checked).",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},

	// --- Lineage --------------------------------------------------------------
	{
		Token: ReasonLineageNotConsumed, Area: "lineage", Kind: "reason",
		Meaning: "Informational reason recorded when a resource declares metadata.observers but its provider does not implement LineageAware. Never blocks Ready (docs/planning/02-architecture.md §5.5).",
		Causes: []string{
			"The resource's provider does not implement the LineageAware capability interface yet.",
			"metadata.observers names a provider that cannot consume forwarded lineage events for this resource's kind.",
		},
		Remedies: []string{
			"Informational only — no action needed if lineage for this resource is not expected.",
			"If lineage IS expected, confirm the target provider implements LineageAware (docs/planning/02-architecture.md §5.5) and its version supports it.",
		},
	},

	// --- Shared instance lifecycle ---------------------------------------------
	{
		Token: ReasonInstanceHealthy, Area: "instance", Kind: "reason",
		Meaning:  "The base Instance-shaped container/service (postgres, mysql, nessie, s3, prometheus, trino coordinator all report this verbatim) passed its health check.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonInstanceUnhealthy, Area: "instance", Kind: "reason",
		Meaning: "The base Instance-shaped container/service failed its health check.",
		Causes: []string{
			"The container is not running (crashed, OOM-killed, image pull failure).",
			"The service process is up but not yet ready (still initializing).",
			"A configured health-check port/path is wrong.",
		},
		Remedies: []string{
			"docker inspect / docker logs <container> (or kubectl describe/logs for the Kubernetes runtime) for the failure detail.",
			"platformctl apply <path> to retry once the underlying cause is fixed.",
		},
	},

	// --- Shared CDC source ------------------------------------------------------
	{
		Token: ReasonSourceProvisioned, Area: "cdc-source", Kind: "reason",
		Meaning:  "The CDC Source's database/replication-user provisioning step (postgres or mysql) completed.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonDatabaseMissing, Area: "cdc-source", Kind: "reason",
		Meaning: "The database named in the Source's spec does not exist on the target instance.",
		Causes: []string{
			"The database was never created (typo in spec.database, or the instance is fresh).",
			"The database was dropped out of band.",
		},
		Remedies: []string{
			"platformctl apply <path> — a managed instance's Source creates the database if absent.",
			"For an external instance, create the database out of band, then platformctl apply <path>.",
		},
	},
	{
		Token: ReasonReplicationCredentialsInvalid, Area: "cdc-source", Kind: "reason",
		Meaning: "The replication user/role platformctl provisioned (or was told to use) cannot authenticate or lacks replication privileges.",
		Causes: []string{
			"The replication user's password was rotated out of band.",
			"The user lacks REPLICATION privilege (postgres) or REPLICATION SLAVE/CLIENT (mysql).",
			"The referenced SecretReference now resolves to a different value than what was provisioned.",
		},
		Remedies: []string{
			"platformctl apply <path> to re-provision the replication user/grant with the current secret.",
			"For an external instance, verify the user's privileges and password out of band.",
		},
	},
	{
		Token: ReasonSourceHealthy, Area: "cdc-source", Kind: "reason",
		Meaning:  "The CDC Source's preconditions (WAL/binlog mode, replication user, database) are all satisfied.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},

	// --- Shared Kafka Connect connector lifecycle --------------------------------
	{
		Token: ReasonConnectWorkerHealthy, Area: "connect", Kind: "reason",
		Meaning:  "The Kafka Connect worker (debezium or s3sink Binding) answered its REST API health check.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonConnectWorkerUnhealthy, Area: "connect", Kind: "reason",
		Meaning: "The Kafka Connect worker's REST API did not answer or reported unhealthy.",
		Causes: []string{
			"The Connect worker container is not running or still starting.",
			"The worker cannot reach the Kafka brokers it depends on.",
		},
		Remedies: []string{
			"docker logs <connect-worker-container> for the startup/connection error.",
			"platformctl status <path> for the broker Binding this Connect worker depends on.",
			"platformctl apply <path> to retry.",
		},
	},
	{
		Token: ReasonConnectorRunning, Area: "connect", Kind: "reason",
		Meaning:  "The Kafka Connect connector (debezium source or s3sink) is in RUNNING state on the worker.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonConnectorNotRunning, Area: "connect", Kind: "reason",
		Meaning: "The connector exists on the worker but is not in RUNNING state (see ReasonConnectorState for the exact observed state).",
		Causes: []string{
			"The connector is PAUSED (deliberately, or by a prior failed apply).",
			"The connector is FAILED (see its trace via the Connect REST API).",
			"A task within the connector failed while the connector itself shows a different status.",
		},
		Remedies: []string{
			"platformctl explain ConnectorState for how to read the appended live state.",
			"platformctl status <path> for the Message (usually includes the Connect REST API's failure trace).",
			"platformctl apply <path> to restart/reconfigure the connector.",
		},
	},
	{
		Token: ReasonConnectorMissing, Area: "connect", Kind: "reason",
		Meaning: "The connector was expected on the Connect worker but is absent — created out of band deletion, or a worker restart lost unpersisted config.",
		Causes: []string{
			"The connector was deleted directly via the Connect REST API or UI.",
			"The Connect worker's config storage topic was lost or the worker was pointed at a fresh Kafka cluster.",
		},
		Remedies: []string{
			"platformctl apply <path> to recreate the connector from spec.",
		},
	},
	{
		Token: ReasonConnectorConfigDrift, Area: "connect", Kind: "reason",
		Meaning: "The connector exists and is running, but its live configuration (fetched from the Connect REST API) no longer matches the configuration platformctl's spec would generate.",
		Causes: []string{
			"The connector's config was edited directly via the Connect REST API.",
			"The Binding's spec changed but a prior apply did not complete.",
		},
		Remedies: []string{
			"platformctl drift <path> to see exactly which config keys differ.",
			"platformctl apply <path> to reconcile the live config back to spec.",
		},
	},
	{
		Token: ReasonConnectorState, Area: "connect", Kind: "reason", Prefix: true,
		Meaning: "Prefix, not a complete reason: both debezium and s3sink append the live Kafka Connect connector state (e.g. \"ConnectorStatePAUSED\", \"ConnectorStateFAILED\") so the reason names the exact observed state without a separate Message lookup.",
		Causes: []string{
			"PAUSED: the connector was paused (deliberately, or by tooling) and needs resuming.",
			"FAILED: the connector or one of its tasks threw an unrecoverable error — see the Connect REST API's /connectors/<name>/status trace, or the condition's Message.",
			"UNASSIGNED/RESTARTING: transient — the worker is still redistributing tasks.",
		},
		Remedies: []string{
			"platformctl status <path> for the Message, which usually includes the failure trace for FAILED.",
			"platformctl apply <path> to have platformctl resume/recreate the connector.",
			"For a stubborn FAILED task, check the target/source system's own health (DB replication slot, S3 bucket permissions) named in the trace.",
		},
	},
	{
		Token: ReasonConnectWorkerMissing, Area: "connect", Kind: "reason", Prefix: true,
		Meaning: "Prefix, not a complete reason (docs/planning/08 C3, mirrors redpanda's BrokerMissing): a declared spec.configuration.workers > 1 Connect-worker set whose per-ordinal Probe finds one or more ordinals absent/stopped appends the missing ordinal names to this prefix.",
		Causes: []string{
			"One or more Connect worker containers/pods were stopped or killed out of band.",
			"A prior apply that was supposed to scale the worker set up did not complete.",
		},
		Remedies: []string{
			"The condition's Message/appended detail names the missing ordinal(s).",
			"platformctl apply <path> to recreate the missing worker(s).",
		},
	},

	// --- noop provider (test/dev only) --------------------------------------------
	{
		Token: ReasonNoopReconciled, Area: "noop", Kind: "reason",
		Meaning:  "Test/dev-only noop provider: Reconcile ran (no real infrastructure touched).",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonNoopHealthy, Area: "noop", Kind: "reason",
		Meaning:  "Test/dev-only noop provider: Probe reports healthy (always true — there is no real backing infrastructure to check).",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},

	// --- placeholder provider -----------------------------------------------------
	{
		Token: ReasonHealthCheckPassed, Area: "placeholder", Kind: "reason",
		Meaning:  "The placeholder provider's container passed its configured health check.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonContainerMissing, Area: "placeholder", Kind: "reason",
		Meaning: "The placeholder provider's container is absent from the runtime.",
		Causes: []string{
			"The container was removed out of band.",
			"A prior apply failed before the container was created.",
		},
		Remedies: []string{
			"platformctl apply <path> to (re)create it.",
		},
	},
	{
		Token: ReasonContainerUnhealthy, Area: "placeholder", Kind: "reason",
		Meaning: "The placeholder provider's container is present but failing its health check.",
		Causes: []string{
			"The process inside the container crashed or is still starting.",
			"The configured health-check command/port is wrong for this image.",
		},
		Remedies: []string{
			"docker logs <container> for the failure detail.",
			"platformctl apply <path> to retry.",
		},
	},

	// --- redpanda (EventStream broker, Topic) ---------------------------------------
	{
		Token: ReasonBrokerHealthy, Area: "redpanda", Kind: "reason",
		Meaning:  "The redpanda broker (or every broker in a multi-broker set) answered its admin API health check.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonBrokerUnhealthy, Area: "redpanda", Kind: "reason",
		Meaning: "The redpanda broker did not answer its admin API health check.",
		Causes: []string{
			"The broker container is not running or still starting.",
			"The admin API port is unreachable (network/firewall).",
		},
		Remedies: []string{
			"docker logs <broker-container> for the startup error.",
			"platformctl apply <path> to retry.",
		},
	},
	{
		Token: ReasonTopicReconciled, Area: "redpanda", Kind: "reason",
		Meaning:  "The Topic's create/update against the redpanda admin API completed.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonTopicHealthy, Area: "redpanda", Kind: "reason",
		Meaning:  "The Topic's live partition count and retention match spec, and it is not drifted.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonTopicMissing, Area: "redpanda", Kind: "reason",
		Meaning: "The Topic does not exist on the redpanda cluster.",
		Causes: []string{
			"The topic was deleted out of band.",
			"A prior apply failed before the topic was created.",
		},
		Remedies: []string{
			"platformctl apply <path> to recreate it.",
		},
	},
	{
		Token: ReasonPartitionCountMismatch, Area: "redpanda", Kind: "reason", Prefix: true,
		Meaning: "probeTopic's drift reason (internal/adapters/providers/redpanda/kafka.go): the topic's live partition count differs from spec.partitions. Combined with the observed/wanted counts via fmt.Sprintf at the call site (e.g. \"PartitionCountMismatch(3!=5)\").",
		Causes: []string{
			"Partitions were added directly via the Kafka admin API/CLI out of band.",
			"spec.partitions was lowered — redpanda (like Kafka) cannot shrink partition count; that is a refused plan, not drift, if attempted through platformctl.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the exact observed vs. wanted counts.",
			"platformctl apply <path> to raise the live count to match spec (increases only).",
		},
	},
	{
		Token: ReasonRetentionMismatch, Area: "redpanda", Kind: "reason", Prefix: true,
		Meaning: "probeTopic's drift reason: the topic's live retention.ms differs from spec. Combined with the observed/wanted values via fmt.Sprintf at the call site (e.g. \"RetentionMismatch(86400000!=604800000)\").",
		Causes: []string{
			"Retention was changed directly via the Kafka admin API/CLI out of band.",
			"spec.retention changed but a prior apply did not complete.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the exact observed vs. wanted retention.",
			"platformctl apply <path> to reconcile it back to spec.",
		},
	},
	{
		Token: ReasonBrokerMissing, Area: "redpanda", Kind: "reason", Prefix: true,
		Meaning: "Multi-broker drift reason (docs/adr/017 §a.6): a set ordinal absent/stopped at the runtime. The missing ordinal name(s) are appended to this prefix.",
		Causes: []string{
			"One or more broker containers/pods were stopped or killed out of band.",
			"A prior apply that was supposed to scale the broker set up did not complete.",
		},
		Remedies: []string{
			"The condition's appended detail names the missing broker ordinal(s).",
			"platformctl apply <path> to recreate the missing broker(s) — this requires the HighAvailability gate for brokers > 1.",
		},
	},
	{
		Token: ReasonBrokerNotJoined, Area: "redpanda", Kind: "reason", Prefix: true,
		Meaning: "Multi-broker drift reason: a broker is present and running at the runtime level but the admin API does not report it as a cluster member. Combined with the observed/wanted joined-broker counts (e.g. \"BrokerNotJoined(2!=3)\").",
		Causes: []string{
			"The broker started but has not finished joining the raft group yet (can be transient).",
			"A network partition between brokers is preventing cluster formation.",
			"Broker configuration (seed servers, advertised addresses) is inconsistent across the set.",
		},
		Remedies: []string{
			"platformctl drift <path> again after a short wait — this can be transient during startup.",
			"docker logs <broker-container> for raft/cluster-join errors if it persists.",
			"platformctl apply <path> once the underlying network/config issue is fixed.",
		},
	},
	{
		Token: ReasonReplicationFactorMismatch, Area: "redpanda", Kind: "reason", Prefix: true,
		Meaning: "Multi-broker drift reason: a topic's observed replication factor differs from spec.replication. Combined with the observed/wanted values via fmt.Sprintf at the call site.",
		Causes: []string{
			"The topic was created (or its replicas reassigned) directly via the Kafka admin API out of band.",
			"spec.replication changed but a prior apply did not complete a reassignment.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the exact observed vs. wanted replication factor.",
			"platformctl apply <path> to reconcile it back to spec.",
		},
	},

	// --- postgres-specific probe reasons ---------------------------------------
	{
		Token: ReasonWALNotLogical, Area: "postgres", Kind: "reason",
		Meaning: "The postgres instance's wal_level server setting is not \"logical\", which logical replication (and therefore CDC via debezium) requires.",
		Causes: []string{
			"The instance was provisioned or configured without wal_level=logical.",
			"An external postgres instance's operator has not enabled logical replication.",
		},
		Remedies: []string{
			"Managed instance: platformctl provisions wal_level=logical by default — check for a configuration override in the Provider spec.",
			"External instance: set wal_level=logical (postgresql.conf or ALTER SYSTEM) and restart postgres, then platformctl apply <path>.",
		},
	},

	// --- mysql-specific probe reasons ---------------------------------------------
	{
		Token: ReasonBinlogNotRowFormat, Area: "mysql", Kind: "reason",
		Meaning: "The mysql/mariadb instance's binlog_format server variable is not ROW, which row-based CDC via debezium requires.",
		Causes: []string{
			"The instance was provisioned or configured without binlog_format=ROW.",
			"An external mysql instance's operator has not set row-based binary logging.",
		},
		Remedies: []string{
			"Managed instance: platformctl provisions binlog_format=ROW by default — check for a configuration override in the Provider spec.",
			"External instance: SET GLOBAL binlog_format=ROW (or set it in the server config and restart), then platformctl apply <path>.",
		},
	},

	// --- openlineage -----------------------------------------------------------
	{
		Token: ReasonLineageBackendHealthy, Area: "openlineage", Kind: "reason",
		Meaning:  "The openlineage backend (Marquez) answered its health check.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonLineageBackendUnhealthy, Area: "openlineage", Kind: "reason",
		Meaning: "The openlineage backend did not answer its health check.",
		Causes: []string{
			"The backend container is not running or still starting.",
			"Its own storage dependency (e.g. postgres backing Marquez) is unavailable.",
		},
		Remedies: []string{
			"docker logs <lineage-backend-container> for the failure detail.",
			"platformctl apply <path> to retry.",
		},
	},

	// --- proxy -------------------------------------------------------------------
	{
		Token: ReasonEntrypointSurfaceReady, Area: "proxy", Kind: "reason",
		Meaning:  "The proxy provider's own entrypoint container is up.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonForwarding, Area: "proxy", Kind: "reason",
		Meaning:  "The proxy is actively forwarding traffic for this route to its upstream.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonForwarderDown, Area: "proxy", Kind: "reason",
		Meaning: "The proxy's forwarding process for this route is not running.",
		Causes: []string{
			"The proxy container itself is down.",
			"The forwarder subprocess/socat instance crashed.",
		},
		Remedies: []string{
			"docker logs <proxy-container> for the failure detail.",
			"platformctl apply <path> to retry.",
		},
	},
	{
		Token: ReasonUpstreamUnreachable, Area: "proxy", Kind: "reason",
		Meaning: "The proxy is forwarding correctly but the upstream it forwards to does not answer.",
		Causes: []string{
			"The upstream service is down or not yet ready.",
			"The upstream's address/port changed without the proxy's config being updated.",
		},
		Remedies: []string{
			"platformctl status <path> for the upstream resource's own Ready condition.",
			"platformctl apply <path> once the upstream is healthy, to clear the condition.",
		},
	},

	// --- nessie --------------------------------------------------------------------
	{
		Token: ReasonCatalogProvisioned, Area: "nessie", Kind: "reason",
		Meaning:  "The Catalog's default branch/config against the nessie instance was provisioned.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonCatalogHealthy, Area: "nessie", Kind: "reason",
		Meaning:  "The nessie catalog instance answered its health check and the Catalog's branch exists.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonBranchMissing, Area: "nessie", Kind: "reason",
		Meaning: "Catalog probe drift reason (internal/adapters/providers/nessie/nessie.go): the branch platformctl provisioned no longer exists on the nessie instance.",
		Causes: []string{
			"The branch was deleted directly via the nessie API/CLI out of band.",
			"A prior apply failed before the branch was created.",
		},
		Remedies: []string{
			"platformctl apply <path> to recreate it.",
		},
	},
	{
		Token: ReasonCatalogUnreachable, Area: "nessie", Kind: "reason",
		Meaning: "Catalog probe drift reason: the nessie instance did not answer.",
		Causes: []string{
			"The nessie container is not running or still starting.",
			"Network/firewall issue between platformctl and the instance.",
		},
		Remedies: []string{
			"docker logs <nessie-container> for the failure detail.",
			"platformctl apply <path> to retry.",
		},
	},

	// --- s3 (Dataset) -----------------------------------------------------------
	{
		Token: ReasonDatasetProvisioned, Area: "s3-dataset", Kind: "reason",
		Meaning:  "The Dataset's bucket/prefix/lifecycle rule/versioning setting was provisioned against the object store.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonDatasetHealthy, Area: "s3-dataset", Kind: "reason",
		Meaning:  "The Dataset's bucket exists, its prefix is listable, and its lifecycle/versioning settings match spec.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonBucketMissing, Area: "s3-dataset", Kind: "reason",
		Meaning: "The Dataset's bucket does not exist on the object store.",
		Causes: []string{
			"The bucket was deleted out of band.",
			"For an external Provider, the bucket was never created (v1 does not create buckets on external stores it does not manage — check the Dataset's ExternalConfigurer behavior).",
		},
		Remedies: []string{
			"Managed store: platformctl apply <path> to recreate it.",
			"External store: create the bucket out of band, then platformctl apply <path>.",
		},
	},
	{
		Token: ReasonPrefixUnlistable, Area: "s3-dataset", Kind: "reason",
		Meaning: "The bucket exists but the Dataset's prefix could not be listed (a permissions or connectivity problem distinct from the bucket being entirely absent).",
		Causes: []string{
			"The credentials used lack list/read permission on that prefix.",
			"A bucket policy denies access to this prefix.",
		},
		Remedies: []string{
			"Verify the credentials referenced by spec.secretRef have s3:ListBucket on the prefix.",
			"platformctl apply <path> to retry once permissions are fixed.",
		},
	},
	{
		Token: ReasonLifecycleRuleDrift, Area: "s3-dataset", Kind: "reason",
		Meaning: "Dataset probe's lifecycle-management drift reason (docs/planning/08 D7): the live bucket's managed lifecycle rule (matched by its deterministic ID) no longer matches spec.lifecycle — including an out-of-band change to it.",
		Causes: []string{
			"The lifecycle rule was edited or deleted directly via the object-store console/API.",
			"spec.lifecycle changed but a prior apply did not complete.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the exact observed vs. wanted rule.",
			"platformctl apply <path> to reconcile it back to spec.",
		},
	},
	{
		Token: ReasonVersioningDrift, Area: "s3-dataset", Kind: "reason",
		Meaning: "Dataset probe's lifecycle-management drift reason: the live bucket's versioning state no longer matches spec.lifecycle.versioning.",
		Causes: []string{
			"Versioning was toggled directly via the object-store console/API.",
			"spec.lifecycle.versioning changed but a prior apply did not complete.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the observed vs. wanted versioning state.",
			"platformctl apply <path> to reconcile it back to spec.",
		},
	},

	// --- s3 (Provider, StableIdentity node set) ----------------------------------
	{
		Token: ReasonNodeMissing, Area: "s3-nodes", Kind: "reason", Prefix: true,
		Meaning: "Mirrors redpanda's BrokerMissing for a distributed MinIO node set: a missing/stopped ordinal is drift the runtime can report even with the whole cluster otherwise healthy. The missing ordinal name(s) are appended to this prefix.",
		Causes: []string{
			"One or more MinIO node containers/pods were stopped or killed out of band.",
			"A prior apply that was supposed to scale the node set up did not complete.",
		},
		Remedies: []string{
			"The condition's appended detail names the missing node ordinal(s).",
			"platformctl apply <path> to recreate the missing node(s) — this requires the HighAvailability gate for nodes > 1.",
		},
	},
	{
		Token: ReasonNodeUnreachable, Area: "s3-nodes", Kind: "reason",
		Meaning: "Every ordinal in the MinIO node set is present, but none of them answer — a network partition, not a per-ordinal absence (distinct from NodeMissing).",
		Causes: []string{
			"A network partition between platformctl and the node set (or between nodes themselves).",
			"All nodes are still starting simultaneously (can be transient right after apply).",
		},
		Remedies: []string{
			"platformctl drift <path> again after a short wait.",
			"Check the runtime network (docker network inspect / kubectl get pods) if it persists.",
		},
	},

	// --- ingress (managed HTTP Connection routing) -----------------------------
	{
		Token: ReasonProxySurfaceReady, Area: "ingress", Kind: "reason",
		Meaning:  "The shared reverse-proxy container (Docker) or the ingress provider's Provider-level anchor (Kubernetes — no central container) is up.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonProxySurfaceDown, Area: "ingress", Kind: "reason",
		Meaning: "The shared reverse-proxy surface is down.",
		Causes: []string{
			"The Caddy (Docker) container is not running or still starting.",
			"Kubernetes: the ingress controller/anchor is not ready.",
		},
		Remedies: []string{
			"docker logs <caddy-container> (Docker) or kubectl describe (Kubernetes) for the failure detail.",
			"platformctl apply <path> to retry.",
		},
	},
	{
		Token: ReasonRouteHealthy, Area: "ingress", Kind: "reason",
		Meaning:  "The Connection's route answers through the entrypoint (Docker: dialed through Caddy with the route's Host header; Kubernetes: the Ingress object matches spec).",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonRouteMissing, Area: "ingress", Kind: "reason",
		Meaning: "The route was removed out-of-band (Docker: no matching @id in Caddy's live config; Kubernetes: the Ingress object is gone) — a drift condition, healed by re-reconciling.",
		Causes: []string{
			"The route was deleted directly via the Caddy admin API or by deleting the Kubernetes Ingress object.",
		},
		Remedies: []string{
			"platformctl apply <path> to recreate it.",
		},
	},
	{
		Token: ReasonRouteConfigDrift, Area: "ingress", Kind: "reason",
		Meaning: "The route exists but its live Host match or upstream target differs from what the Connection's spec generates — drifted names, never target values, matching the debezium/s3sink/prometheus config-drift bar.",
		Causes: []string{
			"The route's Caddy config or Kubernetes Ingress object was edited directly.",
			"The Connection's spec changed but a prior apply did not complete.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the exact observed vs. wanted route config.",
			"platformctl apply <path> to reconcile it back to spec.",
		},
	},
	{
		Token: ReasonIngressUpstreamUnreachable, Area: "ingress", Kind: "reason",
		Meaning: "The route is correctly configured but the upstream it forwards to does not answer. Named distinctly from proxy's own UpstreamUnreachable (same concept, different provider).",
		Causes: []string{
			"The upstream service is down or not yet ready.",
			"The upstream's address/port changed without the route being updated.",
		},
		Remedies: []string{
			"platformctl status <path> for the upstream resource's own Ready condition.",
			"platformctl apply <path> once the upstream is healthy, to clear the condition.",
		},
	},

	// --- trino (compute-engine provider) -----------------------------------------
	{
		Token: ReasonCoordinatorHealthy, Area: "trino", Kind: "reason",
		Meaning:  "The trino coordinator container's own health check passed. Declared separately from the shared InstanceHealthy shape since a Ready trino Provider blocks on both the coordinator AND the worker set.",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonCoordinatorUnhealthy, Area: "trino", Kind: "reason",
		Meaning: "The trino coordinator container failed its health check.",
		Causes: []string{
			"The coordinator container is not running or still starting.",
			"The coordinator's configured catalog files are invalid, preventing startup.",
		},
		Remedies: []string{
			"docker logs <trino-coordinator-container> for the failure detail.",
			"platformctl apply <path> to retry.",
		},
	},
	{
		Token: ReasonWorkerCountMismatch, Area: "trino", Kind: "reason",
		Meaning: "ContainerState.ReadyReplicas does not (yet) match the declared spec.configuration.workers count.",
		Causes: []string{
			"One or more worker containers/pods were stopped or killed out of band.",
			"Workers are still starting right after a scale-up apply (can be transient).",
			"A prior apply that was supposed to scale the worker set did not complete.",
		},
		Remedies: []string{
			"platformctl drift <path> again after a short wait if this just followed an apply.",
			"platformctl apply <path> to recreate missing workers — requires the HighAvailability gate for workers > 1.",
		},
	},
	{
		Token: ReasonCatalogConfigMissing, Area: "trino", Kind: "reason",
		Meaning: "configuration.catalogRef is set but the referenced Catalog (or its resolved warehouse Provider) has not yet published the facts etc/catalog/lakehouse.properties needs. Distinct from CatalogConfigDrift below, which is the file existing but disagreeing with current facts.",
		Causes: []string{
			"The referenced Catalog/warehouse Provider has not been applied yet, or its own apply is still in progress.",
			"The referenced Catalog/warehouse Provider is itself not Ready.",
		},
		Remedies: []string{
			"platformctl status <path> for the referenced Catalog/warehouse Provider's own Ready condition.",
			"platformctl apply <path> again once the dependency is Ready (dependency ordering usually handles this automatically within one apply).",
		},
	},
	{
		Token: ReasonCatalogConfigDrift, Area: "trino", Kind: "reason",
		Meaning: "The live etc/catalog/lakehouse.properties (read back via ContainerRuntime.ReadFile) no longer matches the file regenerated from the currently-published catalog/warehouse facts — the same regenerate-and-diff-by-key bar as prometheus's ScrapeConfigDrift / debezium's ConnectorConfigDrift.",
		Causes: []string{
			"The catalog config file was edited directly inside the container.",
			"The referenced Catalog/warehouse Provider's published facts (endpoint, credentials) changed but trino was not re-applied.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the exact observed vs. wanted catalog config.",
			"platformctl apply <path> to regenerate and reconcile it.",
		},
	},

	// --- prometheus (managed monitoring stack) -------------------------------------
	{
		Token: ReasonScrapeTargetsIncomplete, Area: "prometheus", Kind: "reason",
		Meaning: "Ready requires /api/v1/targets' activeTargets count to match the number of configured scrape targets — per-target up-ness is Prometheus's own concern, not Ready-blocking (docs/planning/08 C9).",
		Causes: []string{
			"A scrape target's metrics endpoint is not yet published (its resource hasn't been applied/reconciled).",
			"Prometheus has not finished its first scrape-config reload after a config change (can be transient).",
		},
		Remedies: []string{
			"platformctl status <path> for the targets each scrape config expects vs. what's configured.",
			"platformctl drift <path> again after a short wait if this just followed an apply.",
		},
	},
	{
		Token: ReasonScrapeConfigDrift, Area: "prometheus", Kind: "reason",
		Meaning: "The live scrape config (fetched via /api/v1/status/config) no longer matches the config regenerated from currently-published metrics endpoint facts.",
		Causes: []string{
			"The scrape config was edited directly on the Prometheus container/config file.",
			"A monitored resource's published metrics endpoint changed but prometheus was not re-applied.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the exact observed vs. wanted scrape config.",
			"platformctl apply <path> to regenerate and reconcile it.",
		},
	},

	// --- wireguard (tunnel provider on the Connection seam; docs/planning/08 D5, docs/adr/023) ---
	{
		Token: ReasonTunnelSurfaceReady, Area: "wireguard", Kind: "reason",
		Meaning:  "The shared tunnel container (Provider kind) is up and healthy (its wg0 interface exists).",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonTunnelDown, Area: "wireguard", Kind: "reason",
		Meaning: "The shared tunnel container is missing or unhealthy — every Connection realized through it is unreachable.",
		Causes: []string{
			"The tunnel container is not running or still starting.",
			"configuration.peerNetwork does not exist or is unreachable.",
		},
		Remedies: []string{
			"docker logs <wireguard-provider-container> for the failure detail.",
			"platformctl apply <path> to retry.",
		},
	},
	{
		Token: ReasonTunnelUp, Area: "wireguard", Kind: "reason",
		Meaning:  "A Connection's forwarder answers through the tunnel (a live TCP accept through its iptables DNAT rule).",
		Causes:   healthyCauses,
		Remedies: healthyRemedies,
	},
	{
		Token: ReasonHandshakeStale, Area: "wireguard", Kind: "reason",
		Meaning: "The WireGuard peer's latest handshake is older than 3x the configured PersistentKeepalive — logged as drift, not failed outright, since a stale reading can still coexist with a functioning tunnel.",
		Causes: []string{
			"The peer (the WireGuard responder) is unreachable or has stopped responding.",
			"configuration.peerEndpoint or configuration.peerPublicKey no longer match the peer's actual configuration.",
			"Normal idle roaming behavior — a stale reading with a still-successful upstream dial is not necessarily broken.",
		},
		Remedies: []string{
			"platformctl status <path> to check whether the upstream dial through the tunnel is still succeeding.",
			"Verify the peer (responder) is up and reachable on configuration.peerNetwork independently of platformctl.",
		},
	},
	{
		Token: ReasonTunnelUpstreamUnreachable, Area: "wireguard", Kind: "reason",
		Meaning: "The tunnel and its forwarder rule are in place but the upstream (spec.target) does not answer through it. Named distinctly from proxy's own UpstreamUnreachable (same concept, different provider).",
		Causes: []string{
			"The upstream (spec.target) is down or not listening on the declared port.",
			"spec.target is not actually within configuration.allowedIPs — the tunnel has no route to it.",
			"The WireGuard peer (responder) is not forwarding/NATing traffic into its own private network.",
		},
		Remedies: []string{
			"platformctl drift <path> to see the exact observed vs. wanted forwarder config.",
			"Verify spec.target is reachable from the peer's own network independently of platformctl.",
		},
	},
}
