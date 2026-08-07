#!/usr/bin/env bash
# Tear down the local GCS repro stack.
#
# Usage:
#   ./99-teardown.sh              # stop compose, remove staged jars/creds
#   CLEAN_GCS=1 ./99-teardown.sh  # also delete gs://${GCS_BUCKET}/${GCS_PREFIX}/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

info "[99-teardown] stopping Docker Compose stack ..."
docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true

info "[99-teardown] removing staged connector / SMT jars ..."
find "${CONNECTORS_DIR}" -name "*.jar" -delete 2>/dev/null || true

info "[99-teardown] removing staged credentials and rendered configs ..."
rm -f "${SECRETS_DIR}/gcp-credentials.json" 2>/dev/null || true
rm -f "${COMPOSE_DIR}"/*.rendered.json 2>/dev/null || true

if [[ "${CLEAN_GCS:-0}" == "1" ]]; then
  if [[ -f "${ENV_FILE}" ]]; then
    load_env
    info "[99-teardown] deleting gs://${GCS_BUCKET}/${GCS_PREFIX}/ ..."
    gcs_rm_prefix "gs://${GCS_BUCKET}/${GCS_PREFIX}"
    # Exactly-once index/lock files live at the BUCKET ROOT (.indexes/<connector>),
    # outside the data prefix. Leaving them makes a re-run seek past already-committed
    # offsets and write nothing. Remove the two connectors' index trees too.
    info "[99-teardown] deleting exactly-once index dirs (.indexes/gcs-bad, gcs-good, gcs-rolling) ..."
    gcs_rm_prefix "gs://${GCS_BUCKET}/.indexes/gcs-bad"
    gcs_rm_prefix "gs://${GCS_BUCKET}/.indexes/gcs-good"
    gcs_rm_prefix "gs://${GCS_BUCKET}/.indexes/gcs-rolling"
  else
    info "[99-teardown] CLEAN_GCS=1 but no .env — skipping GCS cleanup."
  fi
fi

info "[99-teardown] done."
