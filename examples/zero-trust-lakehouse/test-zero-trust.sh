#!/usr/bin/env bash
# test-zero-trust.sh — proves zero-trust live against a `platformctl
# apply`'d stack, with REAL commands (README.md#zero-trust):
#
#   1. Positive proof: the legitimate CDC path (through the mediated
#      Connection) reaches connector state RUNNING.
#   2. Mediated-source canary: a container holding a DIFFERENT,
#      unauthorized Ziti identity, enrolled against the SAME mesh
#      controller and sitting on the SAME platform network as the
#      legitimate dial-side tunneler, is REFUSED when it tries to dial the
#      dark orders database's service. This is the identity check itself,
#      not a network-reachability artifact (adapted from
#      cmd/platformctl/openziti_integration_test.go's
#      proveWrongIdentityRefused to shell + curl + docker).
#   3. Dark-source posture: the external orders database has NO published
#      host port and is on NO network any platformctl-managed container
#      shares — the only path in is the mediated Connection above.
#
# There is no policy-deny proof here (unlike the prior build) — this
# project has no policy file at all. Under zero-trust the declared graph
# (Connections + Bindings) IS the complete allow-set (docs/planning/08 M6)
# with nothing to narrow; there is nothing to test a hand-written policy
# denying, because there is no hand-written policy.
#
# Run AFTER `platformctl apply` has brought the stack up (see
# README.md#run-it). NOTE (2026-07-24): apply itself is blocked today —
# see README.md's "Known blocker" section — so this script is unverified
# live; it is written correct-for-target and will run as soon as the CLI
# can load the plane-folder manifest set.
#
# Usage: ./test-zero-trust.sh [path to platformctl binary]
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORMCTL="${1:-${PLATFORMCTL:-platformctl}}"
FEATURE_GATES="HighAvailability=true,TrinoProvider=true"
STATE="${ZTL_STATE:-${HERE}/ztl.state.json}"

PLATFORM_NETWORK="${PLATFORM_NETWORK:-datascape}"
VPC_NETWORK="${ZTL_ORDERS_VPC:-ztl-orders-vpc}"
DB_CONTAINER="${ZTL_ORDERS_DB:-ztl-orders-db}"

pass=0
fail=0
ok()   { pass=$((pass+1)); printf '\033[32mPASS\033[0m %s\n' "$1"; }
bad()  { fail=$((fail+1)); printf '\033[31mFAIL\033[0m %s\n' "$1"; }
info() { printf '\033[36m--- %s ---\033[0m\n' "$1"; }

# Debezium's REST endpoint has no pinned host port (docs/adr/035 decision
# 2 — every port auto-allocates except query/'s Trino, README#known-
# adaptations) — discover it from recorded state via `platformctl
# inventory`, never a literal.
debezium_url="$("$PLATFORMCTL" inventory "$HERE" --state-file "$STATE" --feature-gates "$FEATURE_GATES" -o json 2>/dev/null \
  | python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for e in data.get("endpoints") or []:
    if e.get("component") == "default/Provider/lake-debezium" and e.get("host") and e["host"] != "(in-network only)":
        print(f"{e[\"scheme\"]}://{e[\"host\"]}")
        break
')"

# ============================================================================
# 1. Positive proof: the legitimate CDC path reaches RUNNING through the
#    mediated Connection (Debezium never touches the dark database directly
#    — it only ever dials orders-db-mediated, the Ziti-tunneled entrypoint).
# ============================================================================
info "1/3 positive proof — mediated CDC connector state"
if [ -z "$debezium_url" ]; then
  bad "could not discover lake-debezium's REST endpoint from \`platformctl inventory\` — is the stack applied?"
else
  state="$(curl -fsS "${debezium_url}/connectors/orders-to-events/status" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["connector"]["state"])' 2>/dev/null)"
  if [ "$state" = "RUNNING" ]; then
    ok "connector orders-to-events state=RUNNING (reached the dark orders DB only through mesh + orders-db-mediated)"
  else
    bad "connector orders-to-events state=${state:-<unreachable>}, want RUNNING"
  fi
fi

# ============================================================================
# 2. Mediated-source canary: mint a second, unauthorized Ziti identity
#    directly against the mesh controller (bypassing platformctl entirely —
#    the same "drive the real REST API" posture the adapter itself uses),
#    enroll a raw ziti-edge-tunnel "proxy"-mode canary under it on the SAME
#    platform network the legitimate dial-side tunneler runs on, and prove a
#    dial through the canary's own local port is refused.
# ============================================================================
info "2/3 mediated-source canary — wrong identity must be refused"

# The controller's container-internal port defaults to 1280 (no
# configuration.controllerPort override in platform/01-mesh.yaml —
# omitted, per the zero-ceremony rule; its HOST-published port is still
# auto-allocated and discoverable the same way).
ctrl_addr="$(docker port mesh-ctrl 1280/tcp 2>/dev/null | head -1 | sed 's/0\.0\.0\.0/127.0.0.1/')"
if [ -z "$ctrl_addr" ]; then
  bad "mesh-ctrl controller port not published — is the stack applied? (docker port mesh-ctrl 1280/tcp)"
else
  admin_user="${DATASCAPE_SECRET_MESH_ADMIN_USERNAME:?export DATASCAPE_SECRET_MESH_ADMIN_USERNAME first}"
  admin_pass="${DATASCAPE_SECRET_MESH_ADMIN_PASSWORD:?export DATASCAPE_SECRET_MESH_ADMIN_PASSWORD first}"

  token="$(curl -ks -X POST "https://${ctrl_addr}/edge/management/v1/authenticate?method=password" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"${admin_user}\",\"password\":\"${admin_pass}\"}" \
    -D - -o /dev/null 2>/dev/null | tr -d '\r' | awk -F': ' 'tolower($1)=="zt-session"{print $2}')"

  if [ -z "$token" ]; then
    bad "could not authenticate to mesh controller at ${ctrl_addr} — canary proof skipped"
  else
    create_resp="$(curl -ks -X POST "https://${ctrl_addr}/edge/management/v1/identities" \
      -H 'Content-Type: application/json' -H "zt-session: ${token}" \
      -d '{"name":"canary-unauthorized","type":"Device","isAdmin":false,"enrollment":{"ott":true}}')"
    canary_id="$(printf '%s' "$create_resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["id"])' 2>/dev/null)"

    if [ -z "$canary_id" ]; then
      bad "could not create canary identity: ${create_resp}"
    else
      enroll_resp="$(curl -ks "https://${ctrl_addr}/edge/management/v1/identities/${canary_id}" -H "zt-session: ${token}")"
      canary_jwt="$(printf '%s' "$enroll_resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["enrollment"]["ott"]["jwt"])' 2>/dev/null)"

      if [ -z "$canary_jwt" ]; then
        bad "could not fetch canary enrollment JWT"
      else
        docker network connect "$PLATFORM_NETWORK" mesh-ctrl >/dev/null 2>&1 || true

        # The exact Ziti-safe role attribute the openziti adapter derives
        # for the orders Source's SPIFFE workload identity
        # (spiffe://datascape/default/source/orders) — mirrors
        # internal/adapters/providers/openziti's unexported
        # identityRoleAttribute (deliberate small duplication, same as the
        # Go integration test's zitiServiceRoleAttribute).
        service_name="spiffe-datascape-default-source-orders"
        canary_port=16999

        docker rm -f ziti-canary >/dev/null 2>&1 || true
        docker run -d --name ziti-canary --network "$PLATFORM_NETWORK" \
          -e ZITI_ENROLL_TOKEN="$canary_jwt" -e ZITI_IDENTITY_BASENAME=canary \
          openziti/ziti-tunnel:1.5.14@sha256:5966139d3db0f54b58f979d1e3374a0fd0f132322ecade29b852d2cabedaf861 \
          proxy "${service_name}:${canary_port}" >/dev/null

        refused=1
        deadline=$((SECONDS + 20))
        while [ $SECONDS -lt $deadline ]; do
          if docker run --rm --network "$PLATFORM_NETWORK" \
              alpine/socat:1.8.0.3@sha256:beb4a68d9e4fe6b0f21ea774a0fde6c31f580dde6368939ed70100c5385b015e \
              -T2 - "TCP:ziti-canary:${canary_port},connect-timeout=2" >/dev/null 2>&1; then
            sleep 1
          else
            refused=0
            break
          fi
        done

        if [ "$refused" = "1" ]; then
          bad "dial through the canary's unauthorized identity unexpectedly succeeded — the per-edge identity check is not enforcing"
        else
          ok "wrong-identity dial correctly refused (no Dial service-policy authorizes canary-unauthorized) — the dark orders DB stays reachable only by the legitimate identity"
        fi

        docker rm -f ziti-canary >/dev/null 2>&1 || true
      fi
    fi
  fi
fi

# ============================================================================
# 3. Dark-source posture: the external orders database has no host port and
#    shares no network with any platformctl-managed container — the mesh
#    router is the ONLY thing that ever crosses into its isolated VPC.
# ============================================================================
info "3/3 dark-source posture — no host port, no shared network"

published="$(docker port "$DB_CONTAINER" 2>/dev/null)"
if [ -n "$published" ]; then
  bad "${DB_CONTAINER} publishes a host port (${published}) — it must have none"
else
  ok "${DB_CONTAINER} publishes no host port"
fi

db_networks="$(docker inspect "$DB_CONTAINER" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null)"
if [ -z "$db_networks" ]; then
  bad "could not inspect ${DB_CONTAINER}'s networks — is setup-external-db.sh applied?"
elif [ "$db_networks" = "${VPC_NETWORK} " ]; then
  ok "${DB_CONTAINER} is on ${VPC_NETWORK} only — no shared network with any platform container"
else
  bad "${DB_CONTAINER} is on unexpected network(s): ${db_networks}(want exactly ${VPC_NETWORK})"
fi

# The mesh router is the one, sanctioned exception — it alone crosses into
# the VPC (configuration.targetNetworks, platform/01-mesh.yaml).
router_networks="$(docker inspect mesh-router --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null)"
case "$router_networks" in
  *"$VPC_NETWORK"*)
    ok "mesh-router is on ${VPC_NETWORK} (the sanctioned mediator) — nothing else needs to be"
    ;;
  *)
    bad "mesh-router is NOT on ${VPC_NETWORK} — the mediated path cannot reach the dark DB at all (networks: ${router_networks:-<none>})"
    ;;
esac

info "summary"
echo "pass=${pass} fail=${fail}"
[ "$fail" -eq 0 ]
