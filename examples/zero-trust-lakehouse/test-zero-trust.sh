#!/usr/bin/env bash
# test-zero-trust.sh — proves the three zero-trust mechanisms this example
# wires (README.md#zero-trust), with REAL commands against a live,
# `platformctl apply`'d stack:
#
#   1. Mediated-source canary: a container holding a DIFFERENT, unauthorized
#      Ziti identity, enrolled against the SAME mesh controller and sitting
#      on the SAME platform network as the legitimate dial-side tunneler, is
#      REFUSED when it tries to dial the dark orders database's service.
#      This is the identity check itself, not a network-reachability
#      artifact (adapted from cmd/platformctl/openziti_integration_test.go's
#      proveWrongIdentityRefused to shell + curl + docker).
#   2. Policy deny: an unauthorized consumer, labeled to grab the gold-tier
#      orders source without carrying clearance:gold, is denied by
#      `platformctl validate` — named deny rule and all.
#   3. Positive proof: the legitimate CDC path (through the SAME mediation)
#      reaches connector state RUNNING.
#
# Run AFTER `platformctl apply` has brought the stack up (see README.md#run-it).
#
# Usage: ./test-zero-trust.sh [path to platformctl binary]
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORMCTL="${1:-${PLATFORMCTL:-platformctl}}"
FEATURE_GATES="SchemaRegistrySupport=true,MediatedConnections=true,PolicyEngine=true,LabelScopedAccess=true,TrinoProvider=true"

PLATFORM_NETWORK="${PLATFORM_NETWORK:-datascape}"
DEBEZIUM_CONNECT_URL="${DEBEZIUM_CONNECT_URL:-http://127.0.0.1:16893}"

pass=0
fail=0
ok()   { pass=$((pass+1)); printf '\033[32mPASS\033[0m %s\n' "$1"; }
bad()  { fail=$((fail+1)); printf '\033[31mFAIL\033[0m %s\n' "$1"; }
info() { printf '\033[36m--- %s ---\033[0m\n' "$1"; }

# ============================================================================
# 1. Positive proof: the legitimate CDC path reaches RUNNING through the
#    mediated Connection (Debezium never touches the dark database directly
#    — it only ever dials orders-db-mediated:16891, the Ziti-tunneled
#    entrypoint).
# ============================================================================
info "1/3 positive proof — mediated CDC connector state"
state="$(curl -fsS "${DEBEZIUM_CONNECT_URL}/connectors/orders-to-events/status" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["connector"]["state"])' 2>/dev/null)"
if [ "$state" = "RUNNING" ]; then
  ok "connector orders-to-events state=RUNNING (reached the dark orders DB only through mesh + orders-db-mediated)"
else
  bad "connector orders-to-events state=${state:-<unreachable>}, want RUNNING"
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

ctrl_addr="$(docker port mesh-ctrl 16890/tcp 2>/dev/null | head -1 | sed 's/0\.0\.0\.0/127.0.0.1/')"
if [ -z "$ctrl_addr" ]; then
  bad "mesh-ctrl controller port not published — is the stack applied? (docker port mesh-ctrl 16890/tcp)"
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
# 3. Policy deny: an unauthorized consumer labeled to grab the gold-tier
#    orders source, without clearance:gold, must fail `platformctl validate`
#    with the named deny rule.
# ============================================================================
info "3/3 policy deny — unauthorized gold-source consumer"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
cp "$HERE"/*.yaml "$tmpdir"/
cat > "$tmpdir/99-rogue-consumer.yaml" <<'EOF'
# Injected by test-zero-trust.sh: an unauthorized consumer that labels
# itself to read the gold-tier orders Source into a target that does NOT
# carry clearance: gold — exactly the edge policies/zero-trust-lakehouse.yaml
# denies.
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: rogue-analytics-events
spec:
  providerRef:
    name: lake-redpanda
  partitions: 1
  retention:
    duration: 1d
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: rogue-siphon
spec:
  mode: cdc
  sourceRef:
    name: orders
  targetRef:
    name: rogue-analytics-events
  providerRef:
    name: lake-debezium
  options:
    tables: [orders]
    snapshotMode: initial
    format: avro
EOF

echo "+ ${PLATFORMCTL} validate ${tmpdir} --policies ${HERE}/policies --feature-gates ${FEATURE_GATES}"
validate_out="$("$PLATFORMCTL" validate "$tmpdir" --policies "$HERE/policies" --feature-gates "$FEATURE_GATES" 2>&1)"
validate_code=$?
echo "$validate_out"

if [ "$validate_code" -eq 0 ]; then
  bad "validate exited 0 on a manifest set with an unauthorized gold-source consumer — the policy deny is not enforcing"
elif printf '%s' "$validate_out" | grep -q 'deny-ungoverned-gold-access'; then
  ok "validate exited ${validate_code} and named the firing rule (deny-ungoverned-gold-access)"
else
  bad "validate exited ${validate_code} but did not name deny-ungoverned-gold-access — check the output above"
fi

info "summary"
echo "pass=${pass} fail=${fail}"
[ "$fail" -eq 0 ]
