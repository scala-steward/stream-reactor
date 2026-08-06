#!/usr/bin/env bash
# Full end-to-end GCS sink time-bucket partition reproduction.
#
# Usage:
#   ./00-run.sh              # normal run (skips jar rebuild if present)
#   ./00-run.sh --clean      # tear down first, then rebuild everything
#   REBUILD=1 ./00-run.sh    # force connector + SMT rebuild
#
# Order:
#   1. (optional) teardown
#   2. Build jars + stage credentials
#   3. Start Kafka + Connect
#   4. Create topic
#   5. Deploy gcs-bad + gcs-good
#   6. Produce timestamped records
#   7. Verify GCS directory counts (the before/after proof)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

CLEAN=0
for arg in "$@"; do
  case "${arg}" in
    --clean) CLEAN=1 ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *) die "Unknown argument: ${arg}" ;;
  esac
done

if [[ "${CLEAN}" -eq 1 ]]; then
  step "CLEAN — tearing down existing stack"
  CLEAN_GCS="${CLEAN_GCS:-0}" bash "${SCRIPT_DIR}/99-teardown.sh" || true
fi

step "1/6  Build connector + SMT jars, stage credentials"
bash "${SCRIPT_DIR}/01-build.sh"

step "2/6  Start Kafka + Kafka Connect"
bash "${SCRIPT_DIR}/02-up.sh"

step "3/6  Create Kafka topic"
bash "${SCRIPT_DIR}/03-create-topic.sh"

step "4/6  Deploy gcs-bad + gcs-good + gcs-rolling connectors"
bash "${SCRIPT_DIR}/04-deploy.sh"

step "5/6  Produce sample records spanning >15 minutes"
bash "${SCRIPT_DIR}/05-produce.sh"

step "6/6  Verify GCS partition directories (before/after proof)"
bash "${SCRIPT_DIR}/06-verify.sh"

# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"
load_env

echo ""
echo "══════════════════════════════════════════════════════"
echo "  GCS local repro complete!"
echo ""
echo "  Kafka (host):        localhost:29092"
echo "  Kafka Connect REST:  ${CONNECT_URL}"
echo "  Bad  listing:        gcloud storage ls gs://${GCS_BUCKET}/${GCS_PREFIX}/bad/"
echo "  Good listing:        gcloud storage ls gs://${GCS_BUCKET}/${GCS_PREFIX}/good/"
echo "  Rolling listing:     gcloud storage ls gs://${GCS_BUCKET}/${GCS_PREFIX}/rolling/ --recursive"
echo "  Connector status:    ${CONNECT_URL}/connectors/gcs-bad/status"
echo "                       ${CONNECT_URL}/connectors/gcs-good/status"
echo "                       ${CONNECT_URL}/connectors/gcs-rolling/status"
echo ""
echo "  Teardown:            ./scripts/99-teardown.sh"
echo "  Teardown + wipe GCS: CLEAN_GCS=1 ./scripts/99-teardown.sh"
echo "══════════════════════════════════════════════════════"
