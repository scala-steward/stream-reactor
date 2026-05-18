#!/usr/bin/env bash
# Verify that documents have landed in OpenSearch after the connector processed
# the records produced by 05-produce-data.sh.
#
# Checks:
#   - Connector state is RUNNING
#   - All connector tasks are RUNNING (not just the top-level connector)
#   - dr-index-a has >= 3 documents (from elastic-dr-topic)
#   - dr-index-b has >= 3 documents (from elastic-dr-topic2)

set -euo pipefail

CONNECT_URL="http://localhost:8083"
OPENSEARCH_URL="https://localhost:9200"
OS_USER="admin"
OS_PASS="bY7!qX3@hV9#kM2"
CONNECTOR_NAME="opensearch-dr"
EXPECTED_DOCS=3

FAIL=0

pass() { echo "  [PASS] $*"; }
fail() { echo "  [FAIL] $*"; FAIL=1; }

jq_or_grep() {
  local expr="$1"; shift
  local input="$1"
  if command -v jq &>/dev/null; then
    echo "${input}" | jq -r "${expr}" 2>/dev/null || true
  else
    echo "${input}" | grep -o "${expr}" | head -1 || true
  fi
}

# ── Connector + task status ────────────────────────────────────────────────────
echo "==> [06-verify] checking connector and task status ..."
STATUS_JSON=$(curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)

CONN_STATE=$(jq_or_grep '.connector.state' "${STATUS_JSON}")

if [[ "${CONN_STATE}" == "RUNNING" ]]; then
  pass "connector '${CONNECTOR_NAME}' is RUNNING"
else
  fail "connector '${CONNECTOR_NAME}' state is '${CONN_STATE:-unknown}' (expected RUNNING)"
fi

# Check every task individually — connector can show RUNNING while tasks are FAILED
if command -v jq &>/dev/null; then
  TASK_COUNT=$(echo "${STATUS_JSON}" | jq '.tasks | length')
  FAILED_TASKS=$(echo "${STATUS_JSON}" | jq '[.tasks[] | select(.state != "RUNNING")] | length')
  if [[ "${TASK_COUNT}" -eq 0 ]]; then
    fail "connector has no tasks (deployment may not have started)"
  elif [[ "${FAILED_TASKS}" -gt 0 ]]; then
    fail "${FAILED_TASKS}/${TASK_COUNT} task(s) are not RUNNING:"
    echo "${STATUS_JSON}" | jq '.tasks[] | select(.state != "RUNNING") | {id: .id, state: .state, trace: .trace}'
  else
    pass "all ${TASK_COUNT} task(s) are RUNNING"
  fi
else
  echo "    (jq not available — skipping detailed task check)"
fi

# ── Dump Connect logs on task failure ─────────────────────────────────────────
if [[ "${FAIL}" -eq 1 ]]; then
  echo ""
  echo "==> [06-verify] Connect worker logs (last 40 lines):"
  echo "──────────────────────────────────────────────────"
  docker logs connect 2>&1 | tail -40
  echo "──────────────────────────────────────────────────"
fi

# ── Wait for connector to flush ───────────────────────────────────────────────
echo ""
echo "==> [06-verify] waiting 15 s for connector to flush to OpenSearch ..."
sleep 15

# ── Document count checks ─────────────────────────────────────────────────────
check_index() {
  local index="$1"
  local expected="$2"

  local response
  response=$(curl -sk -u "${OS_USER}:${OS_PASS}" "${OPENSEARCH_URL}/${index}/_count" 2>/dev/null || true)

  local count
  if command -v jq &>/dev/null; then
    count=$(echo "${response}" | jq -r 'if .count then (.count | tostring) else "0" end' 2>/dev/null || echo "0")
  else
    count=$(echo "${response}" | grep -o '"count":[0-9]*' | cut -d: -f2 || echo "0")
  fi

  # Guard against "null" or empty — treat both as 0
  if [[ -z "${count}" || "${count}" == "null" ]]; then
    count=0
  fi

  echo "==> [06-verify] index '${index}': count=${count} (expected >= ${expected})"
  echo "    raw response: ${response}"

  if [[ "${count}" -ge "${expected}" ]]; then
    pass "index '${index}' has ${count} documents (>= ${expected})"
  else
    fail "index '${index}' has ${count} documents (expected >= ${expected})"
  fi
}

echo ""
echo "==> [06-verify] checking document counts in OpenSearch ..."
check_index "dr-index-a" "${EXPECTED_DOCS}"
check_index "dr-index-b" "${EXPECTED_DOCS}"

# ── Sample document dump ──────────────────────────────────────────────────────
echo ""
echo "==> [06-verify] sample documents from dr-index-a:"
curl -sk -u "${OS_USER}:${OS_PASS}" \
  "${OPENSEARCH_URL}/dr-index-a/_search?pretty=true&size=5" 2>/dev/null | \
  (command -v jq &>/dev/null && jq '.hits.hits[]._source' || cat) || true

echo ""
echo "==> [06-verify] sample documents from dr-index-b:"
curl -sk -u "${OS_USER}:${OS_PASS}" \
  "${OPENSEARCH_URL}/dr-index-b/_search?pretty=true&size=5" 2>/dev/null | \
  (command -v jq &>/dev/null && jq '.hits.hits[]._source' || cat) || true

# ── Topic contents spot-check ─────────────────────────────────────────────────
echo ""
echo "==> [06-verify] spot-checking topic contents (should show index_name header) ..."
for TOPIC in elastic-dr-topic elastic-dr-topic2; do
  echo "    --- ${TOPIC} ---"
  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:19092 \
    --topic "${TOPIC}" \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 5000 \
    --property print.headers=true \
    --property print.key=true 2>/dev/null || echo "    (no messages or timeout)"
done

# ── Result ────────────────────────────────────────────────────────────────────
echo ""
if [[ "${FAIL}" -eq 0 ]]; then
  echo "==> [06-verify] ALL CHECKS PASSED"
  exit 0
else
  echo ""
  echo "==> [06-verify] SOME CHECKS FAILED — see [FAIL] lines above"
  echo "    Useful diagnostics:"
  echo "      docker logs connect 2>&1 | tail -50"
  echo "      curl -sk -u admin:<pass> https://localhost:9200/_cat/indices"
  echo "      curl http://localhost:8083/connectors/${CONNECTOR_NAME}/status | jq ."
  exit 1
fi
