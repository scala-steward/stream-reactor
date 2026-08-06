#!/usr/bin/env bash
# Render and deploy gcs-bad + gcs-good connectors to the local Connect worker.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

load_env

deploy_one() {
  local name="$1"
  local tmpl="$2"
  local rendered="${COMPOSE_DIR}/${name}.rendered.json"

  render_template "${tmpl}" "${rendered}"
  info "[04-deploy] deploying connector '${name}' ..."
  echo "    config preview:"
  if command -v jq &>/dev/null; then
    jq '{
      "connector.class": ."connector.class",
      topics,
      "transforms.loadTs.format.to.pattern": ."transforms.loadTs.format.to.pattern",
      "transforms.loadTs.rolling.window.type": ."transforms.loadTs.rolling.window.type",
      "transforms.loadTs.rolling.window.size": ."transforms.loadTs.rolling.window.size",
      "connect.gcpstorage.kcql": ."connect.gcpstorage.kcql"
    }' "${rendered}" || cat "${rendered}"
  else
    cat "${rendered}"
  fi
  echo ""

  local http
  http=$(curl -s -o /tmp/connect-response-"${name}".json -w "%{http_code}" \
    -X PUT \
    -H "Content-Type: application/json" \
    -d @"${rendered}" \
    "${CONNECT_URL}/connectors/${name}/config" 2>/dev/null)

  echo "    response HTTP ${http}:"
  cat /tmp/connect-response-"${name}".json | (command -v jq &>/dev/null && jq . || cat)
  echo ""

  if [[ "${http}" != "200" && "${http}" != "201" ]]; then
    die "connector '${name}' deployment failed (HTTP ${http})."
  fi

  info "[04-deploy] waiting for '${name}' connector + tasks to reach RUNNING ..."
  local i status_json conn_state task_count failed_tasks running_tasks
  for i in {1..36}; do
    status_json=$(curl -s "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null || true)

    if command -v jq &>/dev/null; then
      conn_state=$(echo "${status_json}" | jq -r '.connector.state // "unknown"')
      task_count=$(echo "${status_json}" | jq -r '.tasks // [] | length')
      failed_tasks=$(echo "${status_json}" | jq -r '[.tasks // [] | .[] | select(.state == "FAILED")] | length')
      running_tasks=$(echo "${status_json}" | jq -r '[.tasks // [] | .[] | select(.state == "RUNNING")] | length')
    else
      conn_state=$(echo "${status_json}" | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "unknown")
      task_count=0
      failed_tasks=0
      running_tasks=0
    fi

    echo "    attempt ${i}/36 — connector=${conn_state} tasks=${running_tasks}/${task_count} running, ${failed_tasks} failed"

    if [[ "${failed_tasks}" -gt 0 ]] || [[ "${conn_state}" == "FAILED" ]]; then
      echo ""
      echo "ERROR: connector '${name}' failed."
      echo "${status_json}" | (command -v jq &>/dev/null && jq . || cat)
      echo ""
      echo "Connect worker logs (last 50 lines):"
      docker logs "${CONNECT_CONTAINER}" 2>&1 | tail -50
      exit 1
    fi

    if [[ "${conn_state}" == "RUNNING" && "${task_count}" -gt 0 && "${running_tasks}" -eq "${task_count}" ]]; then
      info "[04-deploy] '${name}' is RUNNING."
      return 0
    fi
    sleep 5
  done

  die "connector '${name}' did not become healthy within the timeout."
}

info "[04-deploy] waiting for Connect REST API ..."
wait_http "${CONNECT_URL}/" 30 || die "Connect REST API not available."

# Confirm plugins are visible (helpful diagnostic if SMT class is missing)
info "[04-deploy] checking plugin path for GCS sink + TimestampConverter ..."
PLUGINS=$(curl -s "${CONNECT_URL}/connector-plugins" 2>/dev/null || true)
if command -v jq &>/dev/null; then
  echo "${PLUGINS}" | jq -r '.[].class' | grep -E 'GCPStorageSinkConnector|gcp.storage' || \
    info "[04-deploy] WARNING: GCPStorageSinkConnector not listed yet (may still load)."
else
  echo "${PLUGINS}" | head -c 400; echo ""
fi

deploy_one "gcs-bad"     "${COMPOSE_DIR}/connector-bad.json.tmpl"
deploy_one "gcs-good"    "${COMPOSE_DIR}/connector-good.json.tmpl"
deploy_one "gcs-rolling" "${COMPOSE_DIR}/connector-rolling.json.tmpl"

info "[04-deploy] all connectors deployed."
