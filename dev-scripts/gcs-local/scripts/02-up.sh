#!/usr/bin/env bash
# Start Kafka + Kafka Connect and wait until Connect REST is healthy.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

info "[02-up] bringing up kafka + connect ..."
docker compose -f "${COMPOSE_FILE}" up -d

info "[02-up] waiting for Kafka (${KAFKA_CONTAINER}) ..."
wait_container_healthy "${KAFKA_CONTAINER}" 20 \
  || die "Kafka did not become healthy. Logs: docker logs ${KAFKA_CONTAINER}"

info "[02-up] waiting for Connect (${CONNECT_CONTAINER}) ..."
wait_container_healthy "${CONNECT_CONTAINER}" 30 \
  || die "Connect did not become healthy. Logs: docker logs ${CONNECT_CONTAINER}"

info "[02-up] waiting for Connect REST API at ${CONNECT_URL} ..."
wait_http "${CONNECT_URL}/" 30 \
  || die "Connect REST API did not become available."

info "[02-up] stack is up."
curl -s "${CONNECT_URL}/" | (command -v jq &>/dev/null && jq . || cat)
echo ""
