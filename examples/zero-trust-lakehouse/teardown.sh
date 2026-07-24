#!/usr/bin/env bash
# teardown.sh — remove EVERYTHING this example created, and nothing else.
#
# Safety: this script only ever names the exact objects this example owns.
# It never pattern-matches Docker state — a shared host may hold your own
# unrelated volumes/networks, which this script must never touch.
#
# NOTE: as of 2026-07-24 `platformctl destroy` against the plane-folder
# layout below is blocked (see README.md's "Known blocker" section —
# platformctl does not read subdirectories yet). Step 1 is written for
# when that lands; step 2 (the unmanaged external fixtures) works today.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORMCTL="${1:-${PLATFORMCTL:-platformctl}}"
STATE="${ZTL_STATE:-${HERE}/ztl.state.json}"
GATES="HighAvailability=true,TrinoProvider=true"

# 1. Destroy every platformctl-managed resource (needs the same env the
#    apply used, so secrets resolve; a missing state file is fine).
if [ -f "$STATE" ]; then
  echo "+ platformctl destroy (managed resources)"
  "$PLATFORMCTL" destroy "$HERE" --state-file "$STATE" --auto-approve \
    --feature-gates "$GATES" || true
fi

# 2. The external dark DB + its isolated network are NOT platformctl-managed
#    (the orders Source is external: true) — remove them by exact name.
echo "+ removing the external dark orders DB + its isolated network (by name)"
docker rm -f -v "${ZTL_ORDERS_DB:-ztl-orders-db}" >/dev/null 2>&1 || true
docker network rm "${ZTL_ORDERS_VPC:-ztl-orders-vpc}" >/dev/null 2>&1 || true

echo "+ done. Nothing outside this example was touched."
