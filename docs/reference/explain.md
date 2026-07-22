# Condition & reason catalog

Generated from `internal/domain/status.Catalog` by `platformctl docs build` — do not
edit by hand. The same content is available interactively via
`platformctl explain <ConditionType|reason|error-token>`, which resolves an
exact match first, then a case-insensitive prefix/substring fallback.

## conditionType

### `Ready` (conditionType)

Whether the resource is fully reconciled and healthy from the runtime's point of view — the roll-up condition every Reason ultimately feeds. Ready=True means the last Reconcile succeeded and the live health probe passed.

Likely causes:

- Reconcile has not run yet (resource declared but never applied).
- Reconcile ran but returned an error.
- A dependency's health check has not passed yet (Progressing).
- A technology-specific precondition is unmet — see the resource's actual Reason for which one.

Remedies:

- platformctl status <path> to see the Reason/Message for the affected resource.
- platformctl explain <reason> for the specific Reason shown.
- platformctl apply <path> to (re)converge.

### `Progressing` (conditionType)

The resource is mid-reconcile — a multi-step create/update still in flight. Expected to resolve to Ready or Degraded on its own without user action, given enough time.

Likely causes:

- A multi-step provider action (e.g. multi-node cluster formation, image pull) has not finished yet.
- A container/service is still starting and has not passed its health check.

Remedies:

- platformctl status <path> to poll current state.
- platformctl drift <path> once it should have settled, to confirm it did.

### `Degraded` (conditionType)

The resource reconciled but is running in an unhealthy or partially-healthy state — distinct from Ready=False (never reconciled) and DriftDetected (config diverged from spec): Degraded means the live system itself is unwell right now.

Likely causes:

- A dependency (broker, connect worker, upstream) is down.
- A connector or process entered a failed/paused state at the technology level.

Remedies:

- platformctl status <path> for the Degraded condition's Reason/Message.
- Check the underlying container/service logs named in Message (docker logs <container>).
- platformctl apply <path> after fixing the underlying cause.

### `DriftDetected` (conditionType)

The live infrastructure no longer matches the spec or state platformctl last applied — set by `platformctl drift`, healed by `platformctl apply`.

Likely causes:

- An out-of-band change (someone edited or reconfigured the resource directly).
- A resource was deleted, stopped, or killed outside platformctl.
- A previous apply was interrupted, leaving divergent live state.

Remedies:

- platformctl drift <path> to see the diverged resource(s) and the observed Reason/Message.
- platformctl apply <path> to heal drift back to the declared spec.

## lifecycle

### `ReconcileComplete` (reason)

Generic success reason set on Ready=True after a provider's Reconcile finishes without error. Carries no technology-specific detail by design (docs/planning/08 G4) — every provider's Reconcile/Probe uses it identically.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `NoDrift` (reason)

Generic success reason set on DriftDetected=False: the last `platformctl drift` probe found the live resource matching recorded state.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `ProbeFailed` (reason)

The provider's Probe call itself errored (a transport/auth/unexpected-response failure talking to the backing service), as opposed to the probe succeeding and reporting the service unhealthy — that distinction matters because a probe failure usually means the runtime or credentials are wrong, not the service's own health.

Likely causes:

- The backing container/service is unreachable (network, stopped, wrong port).
- Credentials used to probe (admin API, DB connection) are invalid or rotated.
- The runtime (Docker/Kubernetes) itself is unreachable.

Remedies:

- platformctl status <path> for the Message (usually the underlying transport error).
- Verify the runtime is reachable (docker ps / kubectl get pods) and credentials are current.
- platformctl apply <path> to retry once the underlying issue is fixed.

## secrets

### `SecretUnresolvable` (reason)

A SecretReference the resource depends on could not be resolved from its configured secret backend (env, file, vault, kubernetes).

Likely causes:

- The referenced secret name/key does not exist in the backend.
- The secret backend (Vault, Kubernetes Secret store) is unreachable or misconfigured.
- An environment-backend secret's env var is unset (see docs/planning `--env-file`).

Remedies:

- platformctl apply runs a secret Preflight before touching infrastructure — its error names the exact missing key.
- Set the missing key (env var, --env-file, Vault path, or Kubernetes Secret) and re-run.

### `SecretResolvable` (reason)

The resource's SecretReference(s) resolved successfully from their backend.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `SecretChanged` (reason)

A SecretReference's resolved value now hashes differently than what was recorded at the last apply — the secret was rotated (or its backend content changed) out of band.

Likely causes:

- The secret was rotated in its backend (Vault, Kubernetes Secret, file) without a platformctl apply.
- A different secret backend/environment is now resolving the same reference to a different value.

Remedies:

- platformctl apply <path> to re-apply with the new secret value (most providers pick it up on next reconcile).
- If the rotation was unintentional, restore the prior secret value instead.

## external

### `ExternalConnectionUnresolvable` (reason)

A Connection or Provider declared external:true references a connectionRef (Connection or bare SecretReference) that could not be resolved.

Likely causes:

- The referenced Connection/SecretReference does not exist in the manifest set or secret backend.
- A typo in connectionRef.name.

Remedies:

- platformctl validate <path> to see the exact resolution error.
- Fix the connectionRef name or add the missing Connection/SecretReference.

### `ExternalConnectionResolvable` (reason)

The external resource's connectionRef resolved successfully.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `ExternalEndpointUnreachable` (reason)

An external endpoint (a Connection pointing at infrastructure platformctl does not manage) did not answer a reachability probe from the host running platformctl.

Likely causes:

- The external service is down or the address/port is wrong.
- A firewall/security-group rule blocks the host's access.
- TLS/auth misconfiguration causes the probe to fail before it can confirm reachability.

Remedies:

- Verify the endpoint independently (curl/telnet/nc from the same host).
- Check the Connection's host/port and credentials.
- platformctl drift <path> once the endpoint is confirmed reachable, to clear the condition.

### `ExternalEndpointUnreachableInNetwork` (reason)

The in-network-audience counterpart of ExternalEndpointUnreachable (docs/planning/08 C10): the endpoint answers from the host running platformctl but not from the network a consuming Binding will actually dial it from (or vice versa) — the two vantage points are probed and reported distinctly, never folded together.

Likely causes:

- The managed runtime network (Docker network / Kubernetes cluster) cannot route to the external endpoint even though the host can (or vice versa).
- DNS resolves differently from inside the managed network than from the host.
- A network policy or firewall rule scoped to one vantage point but not the other.

Remedies:

- Confirm which vantage point failed in the condition's Message.
- Test connectivity from inside the managed network (e.g. a throwaway container on the same Docker network, or a pod in-cluster) versus from the host.
- Fix routing/DNS/firewall for the failing vantage point, then platformctl drift <path> to re-probe both.

### `ExternalEndpointReachable` (reason)

The external endpoint answered the reachability probe successfully (from every vantage point checked).

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

## lineage

### `LineageEndpointDeclaredNotConsumed` (reason)

Informational reason recorded when a resource declares metadata.observers but its provider does not implement LineageAware. Never blocks Ready (docs/planning/02-architecture.md §5.5).

Likely causes:

- The resource's provider does not implement the LineageAware capability interface yet.
- metadata.observers names a provider that cannot consume forwarded lineage events for this resource's kind.

Remedies:

- Informational only — no action needed if lineage for this resource is not expected.
- If lineage IS expected, confirm the target provider implements LineageAware (docs/planning/02-architecture.md §5.5) and its version supports it.

## instance

### `InstanceHealthy` (reason)

The base Instance-shaped container/service (postgres, mysql, nessie, s3, prometheus, trino coordinator all report this verbatim) passed its health check.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `InstanceUnhealthy` (reason)

The base Instance-shaped container/service failed its health check.

Likely causes:

- The container is not running (crashed, OOM-killed, image pull failure).
- The service process is up but not yet ready (still initializing).
- A configured health-check port/path is wrong.

Remedies:

- docker inspect / docker logs <container> (or kubectl describe/logs for the Kubernetes runtime) for the failure detail.
- platformctl apply <path> to retry once the underlying cause is fixed.

## cdc-source

### `SourceProvisioned` (reason)

The CDC Source's database/replication-user provisioning step (postgres or mysql) completed.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `DatabaseMissing` (reason)

The database named in the Source's spec does not exist on the target instance.

Likely causes:

- The database was never created (typo in spec.database, or the instance is fresh).
- The database was dropped out of band.

Remedies:

- platformctl apply <path> — a managed instance's Source creates the database if absent.
- For an external instance, create the database out of band, then platformctl apply <path>.

### `ReplicationCredentialsInvalid` (reason)

The replication user/role platformctl provisioned (or was told to use) cannot authenticate or lacks replication privileges.

Likely causes:

- The replication user's password was rotated out of band.
- The user lacks REPLICATION privilege (postgres) or REPLICATION SLAVE/CLIENT (mysql).
- The referenced SecretReference now resolves to a different value than what was provisioned.

Remedies:

- platformctl apply <path> to re-provision the replication user/grant with the current secret.
- For an external instance, verify the user's privileges and password out of band.

### `SourceHealthy` (reason)

The CDC Source's preconditions (WAL/binlog mode, replication user, database) are all satisfied.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

## connect

### `ConnectWorkerHealthy` (reason)

The Kafka Connect worker (debezium or s3sink Binding) answered its REST API health check.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `ConnectWorkerUnhealthy` (reason)

The Kafka Connect worker's REST API did not answer or reported unhealthy.

Likely causes:

- The Connect worker container is not running or still starting.
- The worker cannot reach the Kafka brokers it depends on.

Remedies:

- docker logs <connect-worker-container> for the startup/connection error.
- platformctl status <path> for the broker Binding this Connect worker depends on.
- platformctl apply <path> to retry.

### `ConnectorRunning` (reason)

The Kafka Connect connector (debezium source or s3sink) is in RUNNING state on the worker.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `ConnectorNotRunning` (reason)

The connector exists on the worker but is not in RUNNING state (see ReasonConnectorState for the exact observed state).

Likely causes:

- The connector is PAUSED (deliberately, or by a prior failed apply).
- The connector is FAILED (see its trace via the Connect REST API).
- A task within the connector failed while the connector itself shows a different status.

Remedies:

- platformctl explain ConnectorState for how to read the appended live state.
- platformctl status <path> for the Message (usually includes the Connect REST API's failure trace).
- platformctl apply <path> to restart/reconfigure the connector.

### `ConnectorMissing` (reason)

The connector was expected on the Connect worker but is absent — created out of band deletion, or a worker restart lost unpersisted config.

Likely causes:

- The connector was deleted directly via the Connect REST API or UI.
- The Connect worker's config storage topic was lost or the worker was pointed at a fresh Kafka cluster.

Remedies:

- platformctl apply <path> to recreate the connector from spec.

### `ConnectorConfigDrift` (reason)

The connector exists and is running, but its live configuration (fetched from the Connect REST API) no longer matches the configuration platformctl's spec would generate.

Likely causes:

- The connector's config was edited directly via the Connect REST API.
- The Binding's spec changed but a prior apply did not complete.

Remedies:

- platformctl drift <path> to see exactly which config keys differ.
- platformctl apply <path> to reconcile the live config back to spec.

### `ConnectorState*` (reason)

Prefix, not a complete reason: both debezium and s3sink append the live Kafka Connect connector state (e.g. "ConnectorStatePAUSED", "ConnectorStateFAILED") so the reason names the exact observed state without a separate Message lookup.

Likely causes:

- PAUSED: the connector was paused (deliberately, or by tooling) and needs resuming.
- FAILED: the connector or one of its tasks threw an unrecoverable error — see the Connect REST API's /connectors/<name>/status trace, or the condition's Message.
- UNASSIGNED/RESTARTING: transient — the worker is still redistributing tasks.

Remedies:

- platformctl status <path> for the Message, which usually includes the failure trace for FAILED.
- platformctl apply <path> to have platformctl resume/recreate the connector.
- For a stubborn FAILED task, check the target/source system's own health (DB replication slot, S3 bucket permissions) named in the trace.

### `ConnectWorkerMissing*` (reason)

Prefix, not a complete reason (docs/planning/08 C3, mirrors redpanda's BrokerMissing): a declared spec.configuration.workers > 1 Connect-worker set whose per-ordinal Probe finds one or more ordinals absent/stopped appends the missing ordinal names to this prefix.

Likely causes:

- One or more Connect worker containers/pods were stopped or killed out of band.
- A prior apply that was supposed to scale the worker set up did not complete.

Remedies:

- The condition's Message/appended detail names the missing ordinal(s).
- platformctl apply <path> to recreate the missing worker(s).

## noop

### `NoopReconciled` (reason)

Test/dev-only noop provider: Reconcile ran (no real infrastructure touched).

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `NoopHealthy` (reason)

Test/dev-only noop provider: Probe reports healthy (always true — there is no real backing infrastructure to check).

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

## placeholder

### `HealthCheckPassed` (reason)

The placeholder provider's container passed its configured health check.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `ContainerMissing` (reason)

The placeholder provider's container is absent from the runtime.

Likely causes:

- The container was removed out of band.
- A prior apply failed before the container was created.

Remedies:

- platformctl apply <path> to (re)create it.

### `ContainerUnhealthy` (reason)

The placeholder provider's container is present but failing its health check.

Likely causes:

- The process inside the container crashed or is still starting.
- The configured health-check command/port is wrong for this image.

Remedies:

- docker logs <container> for the failure detail.
- platformctl apply <path> to retry.

## redpanda

### `BrokerHealthy` (reason)

The redpanda broker (or every broker in a multi-broker set) answered its admin API health check.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `BrokerUnhealthy` (reason)

The redpanda broker did not answer its admin API health check.

Likely causes:

- The broker container is not running or still starting.
- The admin API port is unreachable (network/firewall).

Remedies:

- docker logs <broker-container> for the startup error.
- platformctl apply <path> to retry.

### `TopicReconciled` (reason)

The Topic's create/update against the redpanda admin API completed.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `TopicHealthy` (reason)

The Topic's live partition count and retention match spec, and it is not drifted.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `TopicMissing` (reason)

The Topic does not exist on the redpanda cluster.

Likely causes:

- The topic was deleted out of band.
- A prior apply failed before the topic was created.

Remedies:

- platformctl apply <path> to recreate it.

### `PartitionCountMismatch*` (reason)

probeTopic's drift reason (internal/adapters/providers/redpanda/kafka.go): the topic's live partition count differs from spec.partitions. Combined with the observed/wanted counts via fmt.Sprintf at the call site (e.g. "PartitionCountMismatch(3!=5)").

Likely causes:

- Partitions were added directly via the Kafka admin API/CLI out of band.
- spec.partitions was lowered — redpanda (like Kafka) cannot shrink partition count; that is a refused plan, not drift, if attempted through platformctl.

Remedies:

- platformctl drift <path> to see the exact observed vs. wanted counts.
- platformctl apply <path> to raise the live count to match spec (increases only).

### `RetentionMismatch*` (reason)

probeTopic's drift reason: the topic's live retention.ms differs from spec. Combined with the observed/wanted values via fmt.Sprintf at the call site (e.g. "RetentionMismatch(86400000!=604800000)").

Likely causes:

- Retention was changed directly via the Kafka admin API/CLI out of band.
- spec.retention changed but a prior apply did not complete.

Remedies:

- platformctl drift <path> to see the exact observed vs. wanted retention.
- platformctl apply <path> to reconcile it back to spec.

### `BrokerMissing*` (reason)

Multi-broker drift reason (docs/adr/017 §a.6): a set ordinal absent/stopped at the runtime. The missing ordinal name(s) are appended to this prefix.

Likely causes:

- One or more broker containers/pods were stopped or killed out of band.
- A prior apply that was supposed to scale the broker set up did not complete.

Remedies:

- The condition's appended detail names the missing broker ordinal(s).
- platformctl apply <path> to recreate the missing broker(s) — this requires the HighAvailability gate for brokers > 1.

### `BrokerNotJoined*` (reason)

Multi-broker drift reason: a broker is present and running at the runtime level but the admin API does not report it as a cluster member. Combined with the observed/wanted joined-broker counts (e.g. "BrokerNotJoined(2!=3)").

Likely causes:

- The broker started but has not finished joining the raft group yet (can be transient).
- A network partition between brokers is preventing cluster formation.
- Broker configuration (seed servers, advertised addresses) is inconsistent across the set.

Remedies:

- platformctl drift <path> again after a short wait — this can be transient during startup.
- docker logs <broker-container> for raft/cluster-join errors if it persists.
- platformctl apply <path> once the underlying network/config issue is fixed.

### `ReplicationFactorMismatch*` (reason)

Multi-broker drift reason: a topic's observed replication factor differs from spec.replication. Combined with the observed/wanted values via fmt.Sprintf at the call site.

Likely causes:

- The topic was created (or its replicas reassigned) directly via the Kafka admin API out of band.
- spec.replication changed but a prior apply did not complete a reassignment.

Remedies:

- platformctl drift <path> to see the exact observed vs. wanted replication factor.
- platformctl apply <path> to reconcile it back to spec.

## postgres

### `WALNotLogical` (reason)

The postgres instance's wal_level server setting is not "logical", which logical replication (and therefore CDC via debezium) requires.

Likely causes:

- The instance was provisioned or configured without wal_level=logical.
- An external postgres instance's operator has not enabled logical replication.

Remedies:

- Managed instance: platformctl provisions wal_level=logical by default — check for a configuration override in the Provider spec.
- External instance: set wal_level=logical (postgresql.conf or ALTER SYSTEM) and restart postgres, then platformctl apply <path>.

## mysql

### `BinlogNotRowFormat` (reason)

The mysql/mariadb instance's binlog_format server variable is not ROW, which row-based CDC via debezium requires.

Likely causes:

- The instance was provisioned or configured without binlog_format=ROW.
- An external mysql instance's operator has not set row-based binary logging.

Remedies:

- Managed instance: platformctl provisions binlog_format=ROW by default — check for a configuration override in the Provider spec.
- External instance: SET GLOBAL binlog_format=ROW (or set it in the server config and restart), then platformctl apply <path>.

## openlineage

### `LineageBackendHealthy` (reason)

The openlineage backend (Marquez) answered its health check.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `LineageBackendUnhealthy` (reason)

The openlineage backend did not answer its health check.

Likely causes:

- The backend container is not running or still starting.
- Its own storage dependency (e.g. postgres backing Marquez) is unavailable.

Remedies:

- docker logs <lineage-backend-container> for the failure detail.
- platformctl apply <path> to retry.

## proxy

### `EntrypointSurfaceReady` (reason)

The proxy provider's own entrypoint container is up.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `Forwarding` (reason)

The proxy is actively forwarding traffic for this route to its upstream.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `ForwarderDown` (reason)

The proxy's forwarding process for this route is not running.

Likely causes:

- The proxy container itself is down.
- The forwarder subprocess/socat instance crashed.

Remedies:

- docker logs <proxy-container> for the failure detail.
- platformctl apply <path> to retry.

### `UpstreamUnreachable` (reason)

The proxy is forwarding correctly but the upstream it forwards to does not answer.

Likely causes:

- The upstream service is down or not yet ready.
- The upstream's address/port changed without the proxy's config being updated.

Remedies:

- platformctl status <path> for the upstream resource's own Ready condition.
- platformctl apply <path> once the upstream is healthy, to clear the condition.

## nessie

### `CatalogProvisioned` (reason)

The Catalog's default branch/config against the nessie instance was provisioned.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `CatalogHealthy` (reason)

The nessie catalog instance answered its health check and the Catalog's branch exists.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `BranchMissing` (reason)

Catalog probe drift reason (internal/adapters/providers/nessie/nessie.go): the branch platformctl provisioned no longer exists on the nessie instance.

Likely causes:

- The branch was deleted directly via the nessie API/CLI out of band.
- A prior apply failed before the branch was created.

Remedies:

- platformctl apply <path> to recreate it.

### `CatalogUnreachable` (reason)

Catalog probe drift reason: the nessie instance did not answer.

Likely causes:

- The nessie container is not running or still starting.
- Network/firewall issue between platformctl and the instance.

Remedies:

- docker logs <nessie-container> for the failure detail.
- platformctl apply <path> to retry.

## s3-dataset

### `DatasetProvisioned` (reason)

The Dataset's bucket/prefix/lifecycle rule/versioning setting was provisioned against the object store.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `DatasetHealthy` (reason)

The Dataset's bucket exists, its prefix is listable, and its lifecycle/versioning settings match spec.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `BucketMissing` (reason)

The Dataset's bucket does not exist on the object store.

Likely causes:

- The bucket was deleted out of band.
- For an external Provider, the bucket was never created (v1 does not create buckets on external stores it does not manage — check the Dataset's ExternalConfigurer behavior).

Remedies:

- Managed store: platformctl apply <path> to recreate it.
- External store: create the bucket out of band, then platformctl apply <path>.

### `PrefixUnlistable` (reason)

The bucket exists but the Dataset's prefix could not be listed (a permissions or connectivity problem distinct from the bucket being entirely absent).

Likely causes:

- The credentials used lack list/read permission on that prefix.
- A bucket policy denies access to this prefix.

Remedies:

- Verify the credentials referenced by spec.secretRef have s3:ListBucket on the prefix.
- platformctl apply <path> to retry once permissions are fixed.

### `LifecycleRuleDrift` (reason)

Dataset probe's lifecycle-management drift reason (docs/planning/08 D7): the live bucket's managed lifecycle rule (matched by its deterministic ID) no longer matches spec.lifecycle — including an out-of-band change to it.

Likely causes:

- The lifecycle rule was edited or deleted directly via the object-store console/API.
- spec.lifecycle changed but a prior apply did not complete.

Remedies:

- platformctl drift <path> to see the exact observed vs. wanted rule.
- platformctl apply <path> to reconcile it back to spec.

### `VersioningDrift` (reason)

Dataset probe's lifecycle-management drift reason: the live bucket's versioning state no longer matches spec.lifecycle.versioning.

Likely causes:

- Versioning was toggled directly via the object-store console/API.
- spec.lifecycle.versioning changed but a prior apply did not complete.

Remedies:

- platformctl drift <path> to see the observed vs. wanted versioning state.
- platformctl apply <path> to reconcile it back to spec.

## s3-nodes

### `NodeMissing*` (reason)

Mirrors redpanda's BrokerMissing for a distributed MinIO node set: a missing/stopped ordinal is drift the runtime can report even with the whole cluster otherwise healthy. The missing ordinal name(s) are appended to this prefix.

Likely causes:

- One or more MinIO node containers/pods were stopped or killed out of band.
- A prior apply that was supposed to scale the node set up did not complete.

Remedies:

- The condition's appended detail names the missing node ordinal(s).
- platformctl apply <path> to recreate the missing node(s) — this requires the HighAvailability gate for nodes > 1.

### `NodeUnreachable` (reason)

Every ordinal in the MinIO node set is present, but none of them answer — a network partition, not a per-ordinal absence (distinct from NodeMissing).

Likely causes:

- A network partition between platformctl and the node set (or between nodes themselves).
- All nodes are still starting simultaneously (can be transient right after apply).

Remedies:

- platformctl drift <path> again after a short wait.
- Check the runtime network (docker network inspect / kubectl get pods) if it persists.

## ingress

### `ProxySurfaceReady` (reason)

The shared reverse-proxy container (Docker) or the ingress provider's Provider-level anchor (Kubernetes — no central container) is up.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `ProxySurfaceDown` (reason)

The shared reverse-proxy surface is down.

Likely causes:

- The Caddy (Docker) container is not running or still starting.
- Kubernetes: the ingress controller/anchor is not ready.

Remedies:

- docker logs <caddy-container> (Docker) or kubectl describe (Kubernetes) for the failure detail.
- platformctl apply <path> to retry.

### `CertHealthy` (reason)

A https Connection's certificate is loaded (Docker: Caddy's tls app; Kubernetes: the Ingress's referenced Secret) and structurally valid — parses, not expired, SAN matches the route's host, and (self-signed) chains to the Provider's own CA.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `CertMissing` (reason)

A https Connection has no certificate loaded yet — Docker: no matching @id in Caddy's tls app; Kubernetes: the referenced Secret does not exist, including a not-yet-issued cert-manager Secret, which is expected to converge rather than error.

Likely causes:

- First reconcile has not completed yet.
- cert-manager has not issued the referenced Secret yet (Kubernetes secretName mode).
- The tls.secretRef names a SecretReference whose backend has no cert/key material.

Remedies:

- Re-run `platformctl apply` once issuance/material is available; check `platformctl status` for convergence.
- For cert-manager mode, inspect the Certificate/Order objects with kubectl to see why issuance is pending.

### `CertInvalid*` (reason)

A certificate is loaded but fails structural validation — unparsable, expired, SAN mismatch with the route's host, or (self-signed) not chaining to the Provider's current CA; the failing check is appended after the prefix.

Likely causes:

- The certificate expired (Probe fails Ready 24h before expiry to force reissue on the next apply).
- The route's host changed after the certificate was issued (SAN mismatch).
- The Provider's self-signed CA was regenerated while an old leaf remained loaded.

Remedies:

- Provide a fresh cert/key via the tls.secretRef, or re-run `platformctl apply` to reissue self-signed leaves.
- `platformctl explain <the-full-printed-reason>` — the appended detail names the failing check.

### `CertConfigDrift` (reason)

A provided (secretRef) certificate's live value no longer matches the manifest-derived one — rotated out-of-band or the SecretReference changed; value-drift is reported (unlike RouteConfigDrift's names-only) because a provided cert's desired content is fully deterministic.

Likely causes:

- The certificate was replaced out-of-band through the mediating runtime or admin API.
- The SecretReference's backing material was rotated; the manifest now derives a different cert.

Remedies:

- Run `platformctl apply` to converge the loaded certificate to the manifest-derived value.

### `CAProvisioned` (reason)

The ingress provider generated its Provider-scoped self-signed local CA (first reconcile with tls.selfSigned Connections) or found the persisted one; the CA's public certificate is published in providerState and named by inventory so tools can trust it.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- Nothing to fix — informational. Retrieve the CA location via `platformctl inventory -o json` (certificateAuthorities) to add it to a client trust store.

### `RouteHealthy` (reason)

The Connection's route answers through the entrypoint (Docker: dialed through Caddy with the route's Host header; Kubernetes: the Ingress object matches spec).

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `RouteMissing` (reason)

The route was removed out-of-band (Docker: no matching @id in Caddy's live config; Kubernetes: the Ingress object is gone) — a drift condition, healed by re-reconciling.

Likely causes:

- The route was deleted directly via the Caddy admin API or by deleting the Kubernetes Ingress object.

Remedies:

- platformctl apply <path> to recreate it.

### `RouteConfigDrift` (reason)

The route exists but its live Host match or upstream target differs from what the Connection's spec generates — drifted names, never target values, matching the debezium/s3sink/prometheus config-drift bar.

Likely causes:

- The route's Caddy config or Kubernetes Ingress object was edited directly.
- The Connection's spec changed but a prior apply did not complete.

Remedies:

- platformctl drift <path> to see the exact observed vs. wanted route config.
- platformctl apply <path> to reconcile it back to spec.

### `IngressUpstreamUnreachable` (reason)

The route is correctly configured but the upstream it forwards to does not answer. Named distinctly from proxy's own UpstreamUnreachable (same concept, different provider).

Likely causes:

- The upstream service is down or not yet ready.
- The upstream's address/port changed without the route being updated.

Remedies:

- platformctl status <path> for the upstream resource's own Ready condition.
- platformctl apply <path> once the upstream is healthy, to clear the condition.

## trino

### `CoordinatorHealthy` (reason)

The trino coordinator container's own health check passed. Declared separately from the shared InstanceHealthy shape since a Ready trino Provider blocks on both the coordinator AND the worker set.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `CoordinatorUnhealthy` (reason)

The trino coordinator container failed its health check.

Likely causes:

- The coordinator container is not running or still starting.
- The coordinator's configured catalog files are invalid, preventing startup.

Remedies:

- docker logs <trino-coordinator-container> for the failure detail.
- platformctl apply <path> to retry.

### `WorkerCountMismatch` (reason)

ContainerState.ReadyReplicas does not (yet) match the declared spec.configuration.workers count.

Likely causes:

- One or more worker containers/pods were stopped or killed out of band.
- Workers are still starting right after a scale-up apply (can be transient).
- A prior apply that was supposed to scale the worker set did not complete.

Remedies:

- platformctl drift <path> again after a short wait if this just followed an apply.
- platformctl apply <path> to recreate missing workers — requires the HighAvailability gate for workers > 1.

### `CatalogConfigMissing` (reason)

configuration.catalogRef is set but the referenced Catalog (or its resolved warehouse Provider) has not yet published the facts etc/catalog/lakehouse.properties needs. Distinct from CatalogConfigDrift below, which is the file existing but disagreeing with current facts.

Likely causes:

- The referenced Catalog/warehouse Provider has not been applied yet, or its own apply is still in progress.
- The referenced Catalog/warehouse Provider is itself not Ready.

Remedies:

- platformctl status <path> for the referenced Catalog/warehouse Provider's own Ready condition.
- platformctl apply <path> again once the dependency is Ready (dependency ordering usually handles this automatically within one apply).

### `CatalogConfigDrift` (reason)

The live etc/catalog/lakehouse.properties (read back via ContainerRuntime.ReadFile) no longer matches the file regenerated from the currently-published catalog/warehouse facts — the same regenerate-and-diff-by-key bar as prometheus's ScrapeConfigDrift / debezium's ConnectorConfigDrift.

Likely causes:

- The catalog config file was edited directly inside the container.
- The referenced Catalog/warehouse Provider's published facts (endpoint, credentials) changed but trino was not re-applied.

Remedies:

- platformctl drift <path> to see the exact observed vs. wanted catalog config.
- platformctl apply <path> to regenerate and reconcile it.

## metrics-exporter

### `ExporterHealthy` (reason)

The opt-in (configuration.metrics: enabled) postgres_exporter/mysqld_exporter sidecar container is up and healthy alongside its database instance.

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `ExporterUnhealthy` (reason)

configuration.metrics is enabled but the exporter sidecar container is missing or unhealthy — the database instance itself may still be fine (its own health is reported separately as InstanceHealthy/InstanceUnhealthy).

Likely causes:

- The exporter container was removed or stopped out-of-band.
- The exporter cannot authenticate: the platform-managed monitoring role/user was altered or dropped directly in the database.
- The exporter container is still starting (can be transient just after apply).

Remedies:

- docker logs <instance>-exporter for the exporter's own failure detail.
- platformctl apply <path> to recreate the exporter and re-provision its monitoring role.

## grafana

### `DatasourceUnhealthy` (reason)

Grafana is up but its provisioned Prometheus datasource's own health check (Grafana's /api/datasources/uid/:uid/health) failed — Grafana cannot reach the prometheus Provider it was provisioned to query.

Likely causes:

- The prometheus Provider's container is down or unreachable on the shared network.
- The prometheus Provider was re-created at a different address after grafana's last reconcile.

Remedies:

- platformctl status <path> to check the prometheus Provider's own Ready condition first.
- platformctl apply <path> to re-provision the datasource from the currently-published endpoint fact.

### `DashboardMissing` (reason)

The starter dashboard's file-based provisioning is expected but Grafana's own API does not report it (GET /api/dashboards/uid/... is not 200) — provisioning failed or the dashboard was removed out-of-band.

Likely causes:

- The dashboard was deleted through Grafana's UI/API (provisioned dashboards can still be removed by an admin).
- Grafana's provisioning scan failed at startup (malformed provisioning file — should not happen with generated content).

Remedies:

- docker logs <grafana-container> for provisioning errors.
- platformctl apply <path> to re-reconcile; if the file content changed, the container is recreated with corrected provisioning.

### `PrometheusUnresolved` (reason)

No prometheus Provider's published endpoint fact could be resolved for grafana's datasource — configuration.prometheusRef is unset with zero or more than one candidate prometheus Provider in the namespace, or the resolved one has not reconciled/published its endpoint yet. The same next-apply-converges caveat as prometheus's own scrape targets: no graph edge orders grafana after prometheus.

Likely causes:

- The prometheus Provider has not been applied yet (fresh single apply — converges on the next apply).
- More than one prometheus Provider exists and configuration.prometheusRef does not name one explicitly.
- No prometheus Provider exists in the manifest at all.

Remedies:

- platformctl apply <path> again once the prometheus Provider is Ready (its endpoint fact publishes on its first successful reconcile).
- Set configuration.prometheusRef: {name: <prometheus-provider>} to disambiguate when several exist.

## prometheus

### `ScrapeTargetsIncomplete` (reason)

Ready requires /api/v1/targets' activeTargets count to match the number of configured scrape targets — per-target up-ness is Prometheus's own concern, not Ready-blocking (docs/planning/08 C9).

Likely causes:

- A scrape target's metrics endpoint is not yet published (its resource hasn't been applied/reconciled).
- Prometheus has not finished its first scrape-config reload after a config change (can be transient).

Remedies:

- platformctl status <path> for the targets each scrape config expects vs. what's configured.
- platformctl drift <path> again after a short wait if this just followed an apply.

### `ScrapeConfigDrift` (reason)

The live scrape config (fetched via /api/v1/status/config) no longer matches the config regenerated from currently-published metrics endpoint facts.

Likely causes:

- The scrape config was edited directly on the Prometheus container/config file.
- A monitored resource's published metrics endpoint changed but prometheus was not re-applied.

Remedies:

- platformctl drift <path> to see the exact observed vs. wanted scrape config.
- platformctl apply <path> to regenerate and reconcile it.

## wireguard

### `TunnelSurfaceReady` (reason)

The shared tunnel container (Provider kind) is up and healthy (its wg0 interface exists).

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `TunnelDown` (reason)

The shared tunnel container is missing or unhealthy — every Connection realized through it is unreachable.

Likely causes:

- The tunnel container is not running or still starting.
- configuration.peerNetwork does not exist or is unreachable.

Remedies:

- docker logs <wireguard-provider-container> for the failure detail.
- platformctl apply <path> to retry.

### `TunnelUp` (reason)

A Connection's forwarder answers through the tunnel (a live TCP accept through its iptables DNAT rule).

Likely causes:

- N/A — this reason indicates a healthy or successful state, not a problem.

Remedies:

- None required.

### `HandshakeStale` (reason)

The WireGuard peer's latest handshake is older than 3x the configured PersistentKeepalive — logged as drift, not failed outright, since a stale reading can still coexist with a functioning tunnel.

Likely causes:

- The peer (the WireGuard responder) is unreachable or has stopped responding.
- configuration.peerEndpoint or configuration.peerPublicKey no longer match the peer's actual configuration.
- Normal idle roaming behavior — a stale reading with a still-successful upstream dial is not necessarily broken.

Remedies:

- platformctl status <path> to check whether the upstream dial through the tunnel is still succeeding.
- Verify the peer (responder) is up and reachable on configuration.peerNetwork independently of platformctl.

### `TunnelUpstreamUnreachable` (reason)

The tunnel and its forwarder rule are in place but the upstream (spec.target) does not answer through it. Named distinctly from proxy's own UpstreamUnreachable (same concept, different provider).

Likely causes:

- The upstream (spec.target) is down or not listening on the declared port.
- spec.target is not actually within configuration.allowedIPs — the tunnel has no route to it.
- The WireGuard peer (responder) is not forwarding/NATing traffic into its own private network.

Remedies:

- platformctl drift <path> to see the exact observed vs. wanted forwarder config.
- Verify spec.target is reachable from the peer's own network independently of platformctl.

## lint

### `DL000` (lintCode)

A metadata.annotations["lint.datascape.io/waive"] entry names a lint code but gives no reason. ADR 020 §2 makes a waiver's reason mandatory: an empty one does not suppress the finding it names and is itself flagged as this warning.

Likely causes:

- The annotation value is just a code ("DL010") with no ": reason" suffix.
- The reason after the colon is blank or only whitespace.

Remedies:

- Add a reason: metadata.annotations["lint.datascape.io/waive"]: "DL010: <why this is intentional>".
- platformctl lint -o json to see which resource/code the malformed waiver is on.

### `DL001` (lintCode)

Duplicate capture: two or more cdc Bindings share a sourceRef with overlapping effective table sets (unset options.tables means "all", which overlaps everything) — separate replication slots/streams over the same tables.

Likely causes:

- Two Bindings were created independently against the same Source without noticing the overlap.
- A wide, unset-tables Binding coexists with a narrower one against the same Source.

Remedies:

- Consolidate into one cdc Binding with a wider options.tables list.
- If the overlap is intentional (e.g. two independent consumers), waive DL001 on each Binding with a reason.

### `DL002` (lintCode)

Sink collision: two or more sink Bindings write the same Dataset bucket+prefix (or the same Source+table for a sink-into-database pairing) — object-key or row collisions between independently-managed connectors.

Likely causes:

- Two sink Bindings were pointed at the same Dataset/table without noticing.
- A shared landing location is genuinely intended (e.g. two streams merging into one prefix).

Remedies:

- Give each sink Binding its own bucket/prefix or target table.
- If the shared target is intentional, waive DL002 on each Binding with a reason.

### `DL003` (lintCode)

A resource declares metadata.observers but its own realizing Provider implements no LineageAware capability — the forwarded lineage event is a runtime no-op (see ReasonLineageNotConsumed), predicted here at validate time instead of only discovered live.

Likely causes:

- The realizing Provider's technology has no lineage integration yet.
- metadata.observers was copied from another manifest without checking the provider type.

Remedies:

- Remove metadata.observers if lineage isn't actually expected for this resource.
- Point the Binding at a LineageAware-capable Provider (debezium, in v1) if lineage is expected.

### `DL004` (lintCode)

Plaintext boundary: a managed Connection uses a plaintext scheme while its realizing Provider also advertises a TLS-capable scheme ("https") — a safer realization exists but wasn't chosen.

Likely causes:

- The Connection was written before its Provider gained TLS support.
- Plaintext was chosen for local development and never revisited for a shared/production environment.

Remedies:

- Set spec.scheme to the TLS-capable scheme if this Connection serves non-loopback traffic.
- If plaintext is intentional (local-only), waive DL004 with a reason.

### `DL010` (lintCode)

Orphaned EventStream: no Binding reads or writes it — inert infrastructure that will be provisioned, billed, and monitored for nothing.

Likely causes:

- The EventStream was scaffolded ahead of the Binding that will use it.
- A Binding that used to reference it was removed.

Remedies:

- Add the Binding(s) that should read/write it, or remove the EventStream if it's no longer needed.
- If it's deliberately provisioned ahead of use, waive DL010 with a reason.

### `DL011` (lintCode)

Unreferenced Catalog: no catalogRef/warehouse consumer and no Connection routes to it — inert infrastructure.

Likely causes:

- No compute-engine Provider (e.g. trino) has been wired to consume it yet.
- The Catalog exists only for `platformctl inventory` to point future tooling at.

Remedies:

- Wire a consumer (a compute-engine Provider's configuration.catalogRef) to it.
- If it's deliberately provisioned ahead of a consumer, waive DL011 with a reason.

### `DL012` (lintCode)

Unused SecretReference / Connection / Provider: nothing in the manifest set resolves it.

Likely causes:

- The resource was scaffolded ahead of what will consume it.
- Something that used to reference it was removed.
- A managed Connection or Provider exists purely for host-side/external tool access, with no in-graph consumer.

Remedies:

- Wire a consumer to it, or remove it if it's no longer needed.
- If it's deliberately unreferenced in-graph (e.g. a host-access Connection), waive DL012 with a reason.

### `DL013` (lintCode)

Dead-end pipeline: a cdc Binding's EventStream has no downstream sink/ingest Binding — frequently intentional (e.g. consumed directly by an external Kafka client or orchestrator), hence info rather than warning.

Likely causes:

- The capture is consumed by something outside this manifest set (an external Kafka client, an orchestrator).
- A sink/ingest Binding that used to consume it was removed.

Remedies:

- Add the downstream Binding if delivery within this manifest set is expected.
- If external consumption is intentional, waive DL013 with a reason.

### `DL014` (lintCode)

Single-replica data path where the HA field exists (spec.configuration.brokers/workers/nodes explicitly set to 1) and the HighAvailability gate is enabled — a single replica has no failover.

Likely causes:

- HighAvailability was enabled platform-wide, but this Provider was left at its single-replica default.
- A single replica is intentional for this Provider (e.g. a scratch/dev instance).

Remedies:

- Raise the replica count if this Provider should be highly available.
- If a single replica is intentional, waive DL014 with a reason.

### `DL020` (lintCode)

spec.deletionPolicy is unset on a data-bearing kind (Dataset/Source) — the default is "retain", but explicitness is the best practice for data that can be destroyed.

Likely causes:

- The manifest was written before deletionPolicy was a habit, or copied from an older example.

Remedies:

- Set spec.deletionPolicy explicitly ("retain" or "delete").

### `DL021` (lintCode)

metadata.protect is unset on a data-bearing kind (Dataset/Source) in a manifest set whose plan would also perform an authoritative delete elsewhere (state has a resource no longer in the current set) — plan-aware.

Likely causes:

- The manifest set is mid-refactor: some resources were removed while data-bearing ones nearby were never explicitly protected.

Remedies:

- Set metadata.protect: true on the data-bearing resource if it must never be deleted by an authoritative apply/destroy.
- If the resource is genuinely safe to delete, no action is needed beyond confirming the authoritative delete is intentional.

### `DL-debezium-001` (lintCode)

N cdc Bindings, each realized by a debezium-typed Provider, capture from Source resources backed by the same physical Postgres/MySQL Provider — each Binding is a separate Debezium connector, and each connector opens its own replication slot against that one server, independent of DL001's table-overlap condition (different Source resources, so DL001 never fires here).

Likely causes:

- Several independent Source resources on one physical database instance each got their own cdc Binding.
- A migration in progress: an old and a new cdc Binding both point at the same server.

Remedies:

- Consolidate into fewer Debezium connectors (wider table lists) if these are really one capture concern.
- If independent replication slots are intentional (isolated failure domains per consumer), waive DL-debezium-001 with a reason.

### `DL-debezium-002` (lintCode)

Two cdc Bindings on the same sourceRef whose options.tables entries overlap only once Debezium's own table.include.list regex semantics are applied (e.g. a pattern like "ord.*" against a literal "orders") — DL001's generic, technology-agnostic form only compares literal table names.

Likely causes:

- One Binding uses a regex-shaped table pattern that happens to also match another Binding's explicit table list.

Remedies:

- Narrow the pattern, or consolidate into one Binding with a single wider table list.
- If the overlapping patterns are intentional, waive DL-debezium-002 with a reason.

### `DL-redpanda-001` (lintCode)

A multi-broker redpanda cluster (spec.configuration.brokers > 1) hosts an EventStream whose spec.replication is lower than the broker count — a durability shape hint, not a hazard on DL001-004's level (an intentionally low replication factor for a scratch/throwaway topic is entirely reasonable), hence info like DL014's identically-shaped single-replica hint.

Likely causes:

- The EventStream's spec.replication was left at its 1-copy default after the broker count was raised.
- A low replication factor is intentional for this topic (non-critical data, cost/throughput tradeoff).

Remedies:

- Raise spec.replication toward the broker count for better durability.
- If a low replication factor is intentional, waive DL-redpanda-001 with a reason.

### `DL-s3sink-001` (lintCode)

Refines the generic DL002 (exact bucket+prefix match): two sink Bindings realized by s3sink write into the same bucket with prefixes where one is a path-hierarchy prefix of the other (e.g. "events/" and "events/raw/") — S3 prefixes are hierarchical, so their object keys still overlap even though the prefix strings differ, which DL002's plain equality check cannot see.

Likely causes:

- Two sink Bindings were given nested prefixes under the same bucket without noticing the overlap.

Remedies:

- Give each sink Binding a disjoint prefix tree.
- If the nested layout is intentional, waive DL-s3sink-001 with a reason.

