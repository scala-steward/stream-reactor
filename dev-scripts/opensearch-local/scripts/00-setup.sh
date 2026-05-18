#!/usr/bin/env bash
# Full end-to-end setup for the OpenSearch local dev environment.
#
# Usage:
#   ./00-setup.sh              # normal run (skips connector build if jar exists)
#   ./00-setup.sh --clean      # tear down first, then rebuild everything from scratch
#   REBUILD=1 ./00-setup.sh   # force connector rebuild even if jar exists
#
# What it does (in order):
#   1. (optional) tear down existing stack if --clean
#   2. Build the connector fat-jar (01-build-connector.sh)
#   3. Start Docker Compose (kafka, opensearch) — Connect waits for both
#   4. Extract the OpenSearch CA and generate a JKS truststore (02-make-truststore.sh)
#   5. Bring Connect up now that the truststore exists
#   6. Create Kafka topics (03-create-topics.sh)
#   7. Deploy the connector (04-deploy-connector.sh)
#   8. Produce sample records (05-produce-data.sh)
#   9. Verify documents in OpenSearch (06-verify.sh)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="${SCRIPT_DIR}/.."
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"

step() { echo ""; echo "════════════════════════════════════════════════════"; echo "  STEP: $*"; echo "════════════════════════════════════════════════════"; }
info() { echo "  >> $*"; }

# ── Arg parsing ───────────────────────────────────────────────────────────────
CLEAN=0
for arg in "$@"; do
  case "${arg}" in
    --clean) CLEAN=1 ;;
    *) echo "Unknown argument: ${arg}"; exit 1 ;;
  esac
done

# ── Clean / teardown ──────────────────────────────────────────────────────────
if [[ "${CLEAN}" -eq 1 ]]; then
  step "CLEAN — tearing down existing stack"
  bash "${SCRIPT_DIR}/99-teardown.sh" || true
fi

# ── Step 1: build connector jar ───────────────────────────────────────────────
step "1/7  Build connector fat-jar"
bash "${SCRIPT_DIR}/01-build-connector.sh"

# ── Step 2: start Kafka + OpenSearch (NOT Connect yet — truststore missing) ───
step "2/7  Start Kafka + OpenSearch"
info "Bringing up kafka and opensearch containers ..."
docker compose -f "${COMPOSE_FILE}" up -d kafka opensearch

info "Waiting for opensearch healthcheck to pass (up to ~8 min — demo security init is slow) ..."
for i in {1..48}; do
  STATUS=$(docker inspect -f '{{.State.Health.Status}}' opensearch 2>/dev/null || echo "starting")
  echo "    opensearch health: ${STATUS} (attempt ${i}/48)"
  if [[ "${STATUS}" == "healthy" ]]; then
    break
  fi
  sleep 10
done
STATUS=$(docker inspect -f '{{.State.Health.Status}}' opensearch 2>/dev/null || echo "unknown")
if [[ "${STATUS}" != "healthy" ]]; then
  echo "ERROR: OpenSearch container did not become healthy. Last 50 log lines:"
  echo "──────────────────────────────────────────────────"
  docker logs opensearch 2>&1 | tail -50
  echo "──────────────────────────────────────────────────"
  exit 1
fi

# ── Step 3: extract CA and create truststore ──────────────────────────────────
step "3/7  Generate JKS truststore from OpenSearch demo CA"
bash "${SCRIPT_DIR}/02-make-truststore.sh"

# ── Step 4: start Kafka Connect (truststore now available on host mount) ───────
step "4/7  Start Kafka Connect"
docker compose -f "${COMPOSE_FILE}" up -d connect

info "Waiting for Connect to pass its healthcheck ..."
for i in {1..30}; do
  STATUS=$(docker inspect -f '{{.State.Health.Status}}' connect 2>/dev/null || echo "starting")
  echo "    connect health: ${STATUS} (attempt ${i}/30)"
  if [[ "${STATUS}" == "healthy" ]]; then
    break
  fi
  sleep 10
done
STATUS=$(docker inspect -f '{{.State.Health.Status}}' connect 2>/dev/null || echo "unknown")
if [[ "${STATUS}" != "healthy" ]]; then
  echo "ERROR: Kafka Connect container did not become healthy. Check logs:"
  echo "  docker logs connect"
  exit 1
fi

# ── Step 5: create topics ─────────────────────────────────────────────────────
step "5/7  Create Kafka topics"
bash "${SCRIPT_DIR}/03-create-topics.sh"

# ── Step 6: deploy connector ──────────────────────────────────────────────────
step "6/7  Deploy opensearch-dr connector"
bash "${SCRIPT_DIR}/04-deploy-connector.sh"

# ── Step 7: produce data ──────────────────────────────────────────────────────
step "7/7  Produce sample records"
bash "${SCRIPT_DIR}/05-produce-data.sh"

# ── Verify ────────────────────────────────────────────────────────────────────
step "VERIFY"
bash "${SCRIPT_DIR}/06-verify.sh"

echo ""
echo "══════════════════════════════════════════════════════"
echo "  Setup complete!"
echo ""
echo "  Kafka Connect REST:  http://localhost:8083"
echo "  OpenSearch REST:     https://localhost:9200  (admin / bY7!qX3@hV9#kM2)"
echo "  Connector status:    http://localhost:8083/connectors/opensearch-dr/status"
echo ""
echo "  Teardown:            ./dev-scripts/opensearch-local/scripts/99-teardown.sh"
echo "══════════════════════════════════════════════════════"
