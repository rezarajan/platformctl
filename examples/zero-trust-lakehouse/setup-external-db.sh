#!/usr/bin/env bash
# setup-external-db.sh — stands up the "external" orders database this
# example treats as a PRE-EXISTING, dark production system.
#
# In the real world this is YOUR database — platformctl never creates or
# destroys it (the orders Source is declared `external: true`). For a
# self-contained demo we stand up a stand-in here: a Postgres on an
# ISOLATED Docker network (ztl-orders-vpc) that nothing in the platform
# manifest set ever joins. The ONLY path to it is the openziti-mediated
# Connection (01-mesh.yaml), whose router alone crosses into this network
# via configuration.targetNetworks.
#
# The credential here MUST match the `orders-db-replication` SecretReference
# your environment provides (see .env.example): the mediated Connection
# hands these creds to Debezium. Postgres's POSTGRES_USER is a superuser
# with REPLICATION + CREATE PUBLICATION, which is exactly what CDC needs —
# the same single-superuser shape the proven openziti mediation scenario
# uses (do NOT split this into a bare replication role; a role without
# CREATE PUBLICATION fails Debezium connector registration).
set -euo pipefail

: "${DATASCAPE_SECRET_ORDERS_DB_REPLICATION_USERNAME:?export it (see .env.example) — the mediated Connection DB user}"
: "${DATASCAPE_SECRET_ORDERS_DB_REPLICATION_PASSWORD:?export it (see .env.example)}"

VPC_NETWORK="${ZTL_ORDERS_VPC:-ztl-orders-vpc}"
DB_CONTAINER="${ZTL_ORDERS_DB:-ztl-orders-db}"
PG_IMAGE="postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"

echo "+ ensuring isolated VPC network ${VPC_NETWORK}"
docker network inspect "$VPC_NETWORK" >/dev/null 2>&1 || docker network create --label io.datascape.managed-by=platformctl "$VPC_NETWORK" >/dev/null

echo "+ starting dark orders database ${DB_CONTAINER} (isolated on ${VPC_NETWORK} only)"
docker rm -f "$DB_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$DB_CONTAINER" \
  --network "$VPC_NETWORK" \
  -e POSTGRES_USER="$DATASCAPE_SECRET_ORDERS_DB_REPLICATION_USERNAME" \
  -e POSTGRES_PASSWORD="$DATASCAPE_SECRET_ORDERS_DB_REPLICATION_PASSWORD" \
  -e POSTGRES_DB=ordersdb \
  "$PG_IMAGE" \
  postgres -c wal_level=logical >/dev/null

echo "+ waiting for ${DB_CONTAINER} to accept connections"
for i in $(seq 1 30); do
  if docker exec "$DB_CONTAINER" pg_isready -U "$DATASCAPE_SECRET_ORDERS_DB_REPLICATION_USERNAME" -d ordersdb >/dev/null 2>&1; then
    echo "+ dark orders DB ready — seeding a demo table"
    docker exec "$DB_CONTAINER" psql -U "$DATASCAPE_SECRET_ORDERS_DB_REPLICATION_USERNAME" -d ordersdb -v ON_ERROR_STOP=1 -c "CREATE TABLE IF NOT EXISTS orders (id serial PRIMARY KEY, sku text NOT NULL, qty int NOT NULL, placed_at timestamptz DEFAULT now()); INSERT INTO orders (sku, qty) VALUES ('WIDGET-1', 3), ('GIZMO-9', 1);"
    echo "+ done. The database is reachable ONLY from ${VPC_NETWORK} — no host port, no shared network."
    exit 0
  fi
  sleep 2
done
echo "! ${DB_CONTAINER} did not become ready in time" >&2
exit 1
