#!/usr/bin/env bash
# Integration-test economy (docs/planning/06 §10): run the MINIMAL set of
# integration suites affected by a change, exactly once per content-state,
# serialized on the shared Docker daemon.
#
#   scripts/test-impact.sh [--base <ref>] [--print] [--force] [--full]
#   scripts/test-impact.sh --prune <days>
#
# --base <ref>  diff against <ref> (default: main) to select affected suites
# --print       list the selected suites and their ledger status; run nothing
# --force       ignore ledger hits (re-run even if this content-state passed)
# --full        select every suite regardless of the diff
# --prune <days>
#               delete ledger entries older than <days> days (by mtime) and
#               exit — a standalone maintenance action, run nothing else
#               (docs/planning/08 G7). The ledger has no automatic expiry:
#               scope-hash keys accumulate forever otherwise (harmless
#               correctness-wise — a stale key just never gets hit again once
#               its content-state is gone — but unbounded in a long-lived
#               shared git common dir). Not run automatically by CI; a
#               maintainer runs it periodically, or wires it into a
#               scheduled job.
#
# Suite selection: each suite declares the path scope that can affect it.
# Ledger: a pass is recorded under (suite, scope-hash) where scope-hash
# covers the tracked content AND uncommitted changes of the suite's scope —
# so an identical content-state never runs the same suite twice, across
# branches, worktrees, and sessions (the ledger lives in the shared git
# common dir). A change outside a suite's scope cannot invalidate its green.
#
# The suite<->scope map below is the contract; when adding a suite or moving
# files, update it in the same commit. Completeness (every integration
# Test* function matched by some suite's -run pattern, or explicitly
# exempted) is enforced by internal/archtest/test_impact_completeness_test.go
# (docs/planning/08 G7), which parses this file rather than duplicating the
# map — it always reads whatever rows exist here.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
COMMON_DIR=$(git rev-parse --git-common-dir)
LEDGER="$COMMON_DIR/platformctl-itest-ledger"
LOCK="/tmp/platformctl-itest.lock"
mkdir -p "$LEDGER"

BASE="main"; PRINT=0; FORCE=0; FULL=0; PRUNE_DAYS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --base) BASE="$2"; shift 2 ;;
    --print) PRINT=1; shift ;;
    --force) FORCE=1; shift ;;
    --full) FULL=1; shift ;;
    --prune) PRUNE_DAYS="$2"; shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# --prune is a standalone maintenance action: prune and exit, without
# touching the diff-based suite selection below.
if [[ -n "$PRUNE_DAYS" ]]; then
  removed=0
  while IFS= read -r -d '' entry; do
    rm -f -- "$entry"
    removed=$((removed+1))
  done < <(find "$LEDGER" -maxdepth 1 -type f -mtime "+$PRUNE_DAYS" -print0)
  echo "pruned $removed ledger entry(ies) older than $PRUNE_DAYS day(s) from $LEDGER"
  exit 0
fi

# suite id | scope (space-separated pathspecs) | test command
# Scopes deliberately include the shared surfaces every suite depends on
# (ports, engine wiring) only where a change there genuinely alters the
# suite's behavior.
SHARED_CORE="internal/ports internal/application/engine internal/application/plan internal/application/registry internal/domain go.mod"
suites() {
  cat <<'EOF'
docker-conformance|internal/adapters/runtime/docker internal/adapters/runtime/fake internal/ports/runtime internal/adapters/runtime/probe|go test -tags integration -count=1 -run Conformance -timeout 900s ./internal/adapters/runtime/docker/
k8s-adapter|internal/adapters/runtime/kubernetes internal/ports/runtime deploy/kubernetes/rbac internal/adapters/runtime/probe|go test -tags integration -count=1 -timeout 1800s ./internal/adapters/runtime/kubernetes/
redpanda|internal/adapters/providers/redpanda internal/adapters/providers/providerkit internal/adapters/runtime/kubernetes SHARED_CORE|go test -tags integration -count=1 -run 'TestRedpandaEndToEnd|TestRedpandaHA' -timeout 1800s ./cmd/platformctl/
cdc|internal/adapters/providers/postgres internal/adapters/providers/mysql internal/adapters/providers/debezium internal/adapters/kafkaconnect internal/adapters/providers/providerkit internal/application/compatibility internal/adapters/runtime/kubernetes SHARED_CORE|go test -tags integration -count=1 -run 'TestCDC|TestMariaDBCDCEndToEnd|TestAvroCDCEndToEnd' -timeout 2400s ./cmd/platformctl/
sink|internal/adapters/providers/s3 internal/adapters/providers/s3sink internal/adapters/kafkaconnect internal/adapters/providers/providerkit cmd/platformctl/testdata/s3sink-image SHARED_CORE|go test -tags integration -count=1 -run 'TestSinkEndToEnd|TestParquetSinkEndToEnd' -timeout 2400s ./cmd/platformctl/
connect-ha-dlq|internal/adapters/providers/debezium internal/adapters/providers/s3sink internal/adapters/kafkaconnect internal/adapters/providers/providerkit internal/domain/binding internal/application/compatibility cmd/platformctl/testdata/s3sink-image cmd/platformctl/testdata/connect-ha-dlq-scenario SHARED_CORE|go test -tags integration -count=1 -run 'TestConnectWorkersHAAndDeadLetterQueue' -timeout 1800s ./cmd/platformctl/
acceptance|examples/cdc-attendance internal/application SHARED_CORE|go test -tags integration -count=1 -run 'TestAcceptance' -timeout 1800s ./cmd/platformctl/
lakehouse|internal/adapters/providers/nessie internal/adapters/providers/openlineage internal/adapters/providers/proxy internal/adapters/providers/mysql examples/lakehouse internal/adapters/runtime/kubernetes SHARED_CORE|go test -tags integration -count=1 -run 'TestLakehouse' -timeout 2400s ./cmd/platformctl/
chaos|internal/application/engine internal/application/plan internal/adapters/runtime/docker|go test -tags integration -count=1 -run 'TestChaos' -timeout 2400s ./cmd/platformctl/
backup|internal/adapters/providers/dbjob internal/adapters/providers/postgres internal/adapters/providers/mysql internal/adapters/providers/s3 internal/application/engine/backup.go internal/domain/backup cmd/platformctl/backup.go|go test -tags integration -count=1 -run 'TestBackupRestore' -timeout 1800s ./cmd/platformctl/
prometheus|internal/adapters/providers/prometheus internal/adapters/providers/redpanda internal/adapters/providers/s3 SHARED_CORE|go test -tags integration -count=1 -run 'TestPrometheusMonitoringStackEndToEnd' -timeout 1200s ./cmd/platformctl/
monitoring|internal/adapters/providers/postgres internal/adapters/providers/mysql internal/adapters/providers/grafana internal/adapters/providers/prometheus internal/adapters/providers/providerkit cmd/platformctl/testdata/monitoring-completion-scenario SHARED_CORE|go test -tags integration -count=1 -run 'TestMonitoringStackCompletionEndToEnd' -timeout 1800s ./cmd/platformctl/
ingress|internal/adapters/providers/ingress internal/adapters/providers/nessie internal/adapters/runtime/kubernetes deploy/kubernetes/rbac SHARED_CORE|go test -tags integration -count=1 -run 'TestIngress' -timeout 1200s ./cmd/platformctl/
state-s3|internal/adapters/state internal/ports/state|go test -tags integration -count=1 -timeout 1200s ./internal/adapters/state/... ./cmd/platformctl/ -run 'TestSharedState'
blueprints|internal/application/blueprint SHARED_CORE|go test -tags integration -count=1 -run 'TestBlueprint' -timeout 1800s ./cmd/platformctl/
gc-state-ops|cmd/platformctl/gc.go cmd/platformctl/state.go internal/ports/state|go test -tags integration -count=1 -run 'TestGC|TestState' -timeout 1200s ./cmd/platformctl/
object-store-posture|internal/adapters/providers/s3 internal/domain/dataset internal/domain/provider internal/domain/connection cmd/platformctl/testdata/s3-external-scenario cmd/platformctl/testdata/minio-ha-scenario cmd/platformctl/s3_c4_d7_integration_test.go SHARED_CORE|go test -tags integration -count=1 -run 'TestS3ExternalDatasetEndToEnd|TestS3DistributedMinIONodeKill' -timeout 2400s ./cmd/platformctl/
trino|internal/adapters/providers/trino internal/adapters/providers/nessie internal/adapters/providers/providerkit internal/adapters/providers/s3 internal/adapters/providers/s3sink internal/adapters/kafkaconnect internal/domain/graph internal/ports/reconciler cmd/platformctl/testdata/trino-scenario SHARED_CORE|go test -tags integration -count=1 -run 'TestTrinoComputeEngineEndToEnd' -timeout 1800s ./cmd/platformctl/
jdbcsink|internal/adapters/providers/jdbcsink internal/adapters/providers/debezium internal/adapters/providers/postgres internal/adapters/kafkaconnect internal/adapters/providers/providerkit cmd/platformctl/testdata/jdbcsink-image cmd/platformctl/testdata/avro-connect-image cmd/platformctl/testdata/jdbcsink-scenario SHARED_CORE|go test -tags integration -count=1 -run 'TestJDBCSinkEndToEnd|TestJDBCSinkValidateCapabilityErrorExact' -timeout 1800s ./cmd/platformctl/
s3source|internal/adapters/providers/s3source internal/adapters/providers/s3sink internal/adapters/providers/s3 internal/adapters/kafkaconnect internal/adapters/providers/providerkit cmd/platformctl/testdata/s3source-image cmd/platformctl/testdata/s3sink-image cmd/platformctl/testdata/s3source-scenario SHARED_CORE|go test -tags integration -count=1 -run 'TestS3SourceIngestEndToEnd|TestS3SourceValidateCapabilityErrorExact' -timeout 1800s ./cmd/platformctl/
wireguard|internal/adapters/providers/wireguard internal/adapters/providers/debezium internal/adapters/providers/postgres internal/ports/runtime internal/domain/connection cmd/platformctl/testdata/wireguard-scenario SHARED_CORE|go test -tags integration -count=1 -run 'TestWireGuardTunnelEndToEnd' -timeout 2400s ./cmd/platformctl/
compose|internal/application/compose internal/cliutil internal/application/blueprint cmd/platformctl/add.go cmd/platformctl/wire.go cmd/platformctl/expose.go cmd/platformctl/compose_shared.go SHARED_CORE|go test -tags integration -count=1 -run 'TestComposeOwnerScenario' -timeout 1200s ./cmd/platformctl/
external-import|internal/adapters/providers/postgres internal/adapters/providers/redpanda internal/adapters/providers/debezium internal/application/compatibility cmd/platformctl/testdata/import-scenario cmd/platformctl/testdata/external-scenario SHARED_CORE|go test -tags integration -count=1 -run 'TestImportEndToEnd|TestExternalSourceEndToEnd' -timeout 1200s ./cmd/platformctl/
external-db-tls|internal/domain/connection internal/adapters/providers/providerkit internal/adapters/providers/debezium internal/adapters/providers/jdbcsink internal/adapters/providers/postgres internal/adapters/providers/mysql internal/adapters/kafkaconnect cmd/platformctl/testdata/external-db-tls-scenario SHARED_CORE|go test -tags integration -count=1 -run 'TestExternalDatabaseTLSEndToEnd' -timeout 1200s ./cmd/platformctl/
EOF
}

scope_hash() {
  # Content hash of a scope: tracked file blobs + uncommitted modifications.
  # Same content => same hash, regardless of branch/commit history.
  local scope=$1
  { git ls-files -s -- $scope 2>/dev/null; git diff HEAD -- $scope 2>/dev/null; } | sha256sum | cut -d' ' -f1
}

changed() { git diff --name-only "$BASE"...HEAD 2>/dev/null; git diff --name-only HEAD 2>/dev/null; }
CHANGED=$(changed | sort -u)

selected=0; ran=0; skipped=0; failed=0
while IFS='|' read -r id scope cmd; do
  [[ -z "$id" ]] && continue
  scope=${scope//SHARED_CORE/$SHARED_CORE}
  hit=0
  if [[ $FULL -eq 1 ]]; then hit=1; else
    for path in $scope; do
      if grep -q "^${path}" <<<"$CHANGED"; then hit=1; break; fi
    done
  fi
  [[ $hit -eq 0 ]] && continue
  selected=$((selected+1))
  h=$(scope_hash "$scope")
  entry="$LEDGER/${id}-${h}"
  if [[ -f "$entry" && $FORCE -eq 0 ]]; then
    echo "SKIP  $id — already green at this content-state ($(cat "$entry"))"
    skipped=$((skipped+1)); continue
  fi
  if [[ $PRINT -eq 1 ]]; then echo "WOULD RUN  $id: $cmd"; continue; fi
  echo "RUN   $id: $cmd"
  # One suite at a time on the shared daemon: contention causes flaky
  # timeouts whose retries cost more than the serialization saves.
  if flock "$LOCK" bash -c "$cmd"; then
    echo "pass $(date -u +%Y-%m-%dT%H:%M:%SZ) $(git rev-parse --short HEAD)" > "$entry"
    ran=$((ran+1))
  else
    echo "FAIL  $id" >&2; failed=$((failed+1))
  fi
done < <(suites)

echo "impact: $selected selected, $ran ran, $skipped deduped, $failed failed (base: $BASE)"
[[ $failed -eq 0 ]]
