#!/usr/bin/env bash
# Initialize the docker-compose Mongo container as a replica set.
# Mongo Change Streams (used by pkg/source/snapshot/mongo for HandoffToken
# capture) require a replica set; this is a one-time init on first start.
# Idempotent — running against an already-initialized replset is a no-op.
set -euo pipefail

CONTAINER="${MONGO_CONTAINER:-murmur-mongo}"

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "[init-mongo-replset] container ${CONTAINER} is not running; skipping"
  exit 0
fi

# Probe: if rs.status() succeeds the replset is already initialized.
if docker exec "${CONTAINER}" mongosh --quiet --eval 'rs.status().ok' 2>/dev/null | grep -q '^1$'; then
  echo "[init-mongo-replset] ${CONTAINER} already initialized"
  exit 0
fi

echo "[init-mongo-replset] initializing replset on ${CONTAINER}"
docker exec "${CONTAINER}" mongosh --quiet --eval \
  "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"
