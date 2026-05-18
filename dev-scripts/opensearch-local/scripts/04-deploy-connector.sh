#!/usr/bin/env bash
# Deploy the opensearch-dr connector to the local Kafka Connect worker.
# Reads the config from ../connector-config.json and PUTs it to the REST API.
# Polls the connector status until it reports RUNNING (or fails).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../connector-config.json"
CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="opensearch-dr"

# ── Wait for Connect REST API ─────────────────────────────────────────────────
echo "==> [04-deploy-connector] waiting for Kafka Connect REST API at ${CONNECT_URL} ..."
for i in {1..30}; do
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/" 2>/dev/null || true)
  if [[ "${HTTP}" == "200" ]]; then
    echo "    Connect is up."
    break
  fi
  echo "    attempt ${i}/30 — HTTP ${HTTP}, sleeping 5 s ..."
  sleep 5
done

HTTP=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/" 2>/dev/null || true)
if [[ "${HTTP}" != "200" ]]; then
  echo "ERROR: Connect REST API did not become available."
  exit 1
fi

# ── Validate config file exists ───────────────────────────────────────────────
if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "ERROR: connector config not found at ${CONFIG_FILE}"
  exit 1
fi

# ── Create or update the connector ───────────────────────────────────────────
echo "==> [04-deploy-connector] deploying connector '${CONNECTOR_NAME}' ..."
HTTP=$(curl -s -o /tmp/connect-response.json -w "%{http_code}" \
  -X PUT \
  -H "Content-Type: application/json" \
  -d "@${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" 2>/dev/null)

echo "    response HTTP ${HTTP}:"
cat /tmp/connect-response.json | (command -v jq &>/dev/null && jq . || cat)
echo ""

if [[ "${HTTP}" != "200" && "${HTTP}" != "201" ]]; then
  echo "ERROR: connector deployment failed (HTTP ${HTTP})."
  exit 1
fi

# ── Poll for RUNNING (both connector AND tasks) ───────────────────────────────
# A connector can report RUNNING while every task is FAILED. We must check both.
echo "==> [04-deploy-connector] waiting for connector and tasks to reach RUNNING ..."
for i in {1..30}; do
  STATUS_JSON=$(curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)

  if command -v jq &>/dev/null; then
    CONN_STATE=$(echo "${STATUS_JSON}" | jq -r '.connector.state // "unknown"')
    TASK_COUNT=$(echo "${STATUS_JSON}" | jq -r '.tasks // [] | length')
    FAILED_TASKS=$(echo "${STATUS_JSON}" | jq -r '[.tasks // [] | .[] | select(.state == "FAILED")] | length')
    RUNNING_TASKS=$(echo "${STATUS_JSON}" | jq -r '[.tasks // [] | .[] | select(.state == "RUNNING")] | length')
  else
    CONN_STATE=$(echo "${STATUS_JSON}" | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
    TASK_COUNT=0
    FAILED_TASKS=0
    RUNNING_TASKS=0
  fi

  echo "    attempt ${i}/30 — connector=${CONN_STATE:-unknown} tasks=${RUNNING_TASKS}/${TASK_COUNT} running, ${FAILED_TASKS} failed"

  # Any task failure → bail out immediately with full diagnostics
  if [[ "${FAILED_TASKS}" -gt 0 ]]; then
    echo ""
    echo "ERROR: ${FAILED_TASKS} task(s) are in FAILED state."
    echo "──────────────────────────────────────────────────"
    echo "${STATUS_JSON}" | (command -v jq &>/dev/null && jq '.tasks[] | select(.state == "FAILED") | {id: .id, state: .state, worker_id: .worker_id, trace: .trace}' || cat)
    echo "──────────────────────────────────────────────────"
    echo ""
    echo "Connect worker logs (last 40 lines):"
    echo "──────────────────────────────────────────────────"
    docker logs connect 2>&1 | tail -40
    echo "──────────────────────────────────────────────────"
    exit 1
  fi

  if [[ "${CONN_STATE}" == "FAILED" ]]; then
    echo "ERROR: connector itself entered FAILED state."
    echo "${STATUS_JSON}" | (command -v jq &>/dev/null && jq . || cat)
    exit 1
  fi

  # All-good condition: connector RUNNING and at least one task RUNNING
  if [[ "${CONN_STATE}" == "RUNNING" && "${TASK_COUNT}" -gt 0 && "${RUNNING_TASKS}" -eq "${TASK_COUNT}" ]]; then
    echo "==> [04-deploy-connector] connector and all ${TASK_COUNT} task(s) are RUNNING."
    echo ""
    echo "${STATUS_JSON}" | (command -v jq &>/dev/null && jq . || cat)
    exit 0
  fi

  sleep 5
done

echo "ERROR: connector did not reach a healthy state within the timeout."
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | (command -v jq &>/dev/null && jq . || cat)
echo ""
echo "Connect worker logs (last 40 lines):"
docker logs connect 2>&1 | tail -40
exit 1
