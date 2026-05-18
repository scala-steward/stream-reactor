#!/usr/bin/env bash
# Tear down the local OpenSearch dev environment.
# Removes:
#   - Docker Compose stack (containers + volumes)
#   - Generated connector jar (connectors/*.jar)
#   - Generated secrets (secrets/*.jks, secrets/*.pem)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="${SCRIPT_DIR}/.."
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"
CONNECTORS_DIR="${COMPOSE_DIR}/connectors"
SECRETS_DIR="${COMPOSE_DIR}/secrets"

echo "==> [99-teardown] stopping Docker Compose stack ..."
docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true

echo "==> [99-teardown] removing generated connector jars ..."
find "${CONNECTORS_DIR}" -name "*.jar" -delete 2>/dev/null || true

echo "==> [99-teardown] removing generated secrets (*.jks, *.pem, *.der) ..."
find "${SECRETS_DIR}" -name "*.jks" -delete 2>/dev/null || true
find "${SECRETS_DIR}" -name "*.pem" -delete 2>/dev/null || true
find "${SECRETS_DIR}" -name "*.der" -delete 2>/dev/null || true

echo "==> [99-teardown] done."
