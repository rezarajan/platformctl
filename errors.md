# examples/cdc-attendance failure — diagnosed and fixed

**Status: resolved.** The full example lifecycle (apply → CDC traffic →
objects in MinIO → idempotent re-apply → destroy, 14/14 resources) was
re-verified against a Docker daemon that had unrelated stacks holding ports
5432/8083/9000/9092.

## Original logs

```
ok   Provider/local-redpanda (create) in 2.681s
ok   Provider/test-lineage-fake (create) in 1ms
ok   SecretReference/minio-root-creds (create) in 1ms
ok   SecretReference/postgres-admin-creds (create) in 1ms
ok   SecretReference/postgres-replication-creds (create) in 1ms
ok   EventStream/attendance-events (create) in 208ms
ok   Provider/local-minio (create) in 2.573s
ok   Provider/local-postgres (create) in 2.58s
fail Provider/postgres-cdc (create) after 5.579s: container "postgres-cdc" exited before becoming healthy
ok   Provider/s3-sink (create) in 6.698s
fail Dataset/attendance-raw (create) after 1ms: check bucket "raw-events": Get "http://localhost:9000/raw-events/?location=": dial tcp [::1]:9000: connect: connection refused
fail Source/student-database (create) after 2ms: connect to postgres: failed to connect to `user=admin database=postgres`:
        [::1]:5432 (localhost): dial error: dial tcp [::1]:5432: connect: connection refused
        127.0.0.1:5432 (localhost): dial error: dial tcp 127.0.0.1:5432: connect: connection refused
skip Binding/attendance-events-to-lake: a dependency failed
skip Binding/student-db-to-events: a dependency failed
error: 3 resource(s) failed to reconcile
exit status 2
```

## Root causes and fixes

1. **`Provider/postgres-cdc` exited before healthy** — the container from a
   previous attempt was still on disk, attached to a `datascape` network that
   no longer existed (its endpoint was pruned when the network was removed).
   `EnsureContainer`'s spec-hash reuse path restarted it *without verifying
   network attachment*, so Kafka Connect came up unable to resolve
   `local-redpanda` ("No resolvable bootstrap urls") and exited.
   **Fix:** the docker adapter now checks that an existing container is
   attached to every network the spec declares before reusing it; on drift it
   replaces the container. `WaitHealthy` failures now also include the
   container's last log lines, so this class of failure is no longer a black
   box.

2. **`Source/student-database` connection refused on 5432** — the Postgres
   healthcheck was `pg_isready -U <user>`, which answers over the *unix
   socket*. The postgres image's initdb phase runs a temporary
   socket-only server, so the container reported healthy (~2.5s) before the
   real server was listening on TCP; the provider then dialed the host port
   and was refused. **Fix:** the healthcheck forces TCP
   (`pg_isready -h 127.0.0.1`), and Source provisioning additionally
   ping-waits up to 30s before issuing SQL.

3. **`Dataset/attendance-raw` connection refused on 9000** — same
   healthy-vs-reachable gap class, aggravated by dialing `localhost` (which
   can resolve to `::1` where some daemons only publish IPv4). **Fix:**
   every provider now dials `127.0.0.1` explicitly, and bucket operations
   retry for up to 30s after the store reports healthy.

4. **Default host ports collide with real machines** — 5432/8083/9000/9092
   are exactly what any existing Postgres/Connect/MinIO/Kafka occupies. The
   example now publishes on 15432 (Postgres), 18083/18084 (Connect workers),
   19000 (MinIO), 19093 (Kafka); see the example README.

Two destroy-path defects surfaced while re-verifying and are fixed too:
failures during provider/secret resolution were counted but never logged,
and a failed destroy did not block teardown of the resources it depends on
(a failed connector delete no longer lets the broker be removed out from
under the Connect worker — mirroring apply-side dependency blocking).

## External errors are not detected against state, and are thus not reconciled correctly
Manual removal of a container does not get reflected in the platformctl state check; the utility does not check against the current running status of the containers. External failures are not observed and thus impossible to reconcile. Furthermore, in this state when issuing the destroy command, containers are removed but the state does not correctly reflect that. Furthermore, assets like the Dataset and Provider/local-minio show as 'Ready' although the containers are no longer available. This is illogical since, firstly the S3 provider local-minio was killed externally, and thus there is no way for the tool to ascertain the availability of the Dataset/attendance-raw, which resides in S3 in this case.

Expected result: The status command must correctly consider the existing state against the declared/saved state. Things fail all the time, out-of-band from platformctl, and the tool must be designed to expect this. Dependencies must have a proper way of resolving state, and assessing state for assets dependent on parents.

```text
❯ bin/platformctl status examples/cdc-attendance/
RESOURCE                                    READY    REASON              LIFECYCLE
Binding/student-db-to-events                Unknown  NotApplied          Managed
Binding/attendance-events-to-lake           Unknown  NotApplied          Managed
Dataset/attendance-raw                      True     DatasetProvisioned  Managed
EventStream/attendance-events               Unknown  NotApplied          Managed
Provider/postgres-cdc                       Unknown  NotApplied          Managed
Provider/test-lineage-fake                  Unknown  NotApplied          Managed
Provider/local-minio                        True     InstanceHealthy     Managed
Provider/local-postgres                     Unknown  NotApplied          Managed
Provider/local-redpanda                     Unknown  NotApplied          Managed
Provider/s3-sink                            Unknown  NotApplied          Managed
SecretReference/postgres-admin-creds        Unknown  NotApplied          Managed
SecretReference/postgres-replication-creds  Unknown  NotApplied          Managed
SecretReference/minio-root-creds            True     SecretResolvable    Managed
Source/student-database                     Unknown  NotApplied          Managed
```
