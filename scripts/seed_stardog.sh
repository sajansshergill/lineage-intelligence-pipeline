#!/usr/bin/env bash
set -euo pipefail

STARDOG_URL="${STARDOG_URL:-http://localhost:5820}"
STARDOG_DATABASE="${STARDOG_DATABASE:-lineage}"
STARDOG_USERNAME="${STARDOG_USERNAME:-admin}"
STARDOG_PASSWORD="${STARDOG_PASSWORD:-admin}"

echo "Checking Stardog at ${STARDOG_URL}..."
curl -fsS -u "${STARDOG_USERNAME}:${STARDOG_PASSWORD}" "${STARDOG_URL}/admin/alive" >/dev/null

echo "Creating database '${STARDOG_DATABASE}' if needed..."
status="$(
  curl -sS -o /tmp/stardog_create_db.out -w "%{http_code}" \
    -u "${STARDOG_USERNAME}:${STARDOG_PASSWORD}" \
    -H "Content-Type: application/json" \
    -d "{\"dbname\":\"${STARDOG_DATABASE}\"}" \
    "${STARDOG_URL}/admin/databases"
)"

if [[ "$status" == "200" || "$status" == "201" || "$status" == "409" ]]; then
  echo "Stardog database ready: ${STARDOG_DATABASE}"
else
  echo "Failed to create Stardog database. HTTP ${status}" >&2
  cat /tmp/stardog_create_db.out >&2
  exit 1
fi
