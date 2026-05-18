#!/usr/bin/env bash
# Extract the OpenSearch demo root CA from the running container and import it
# into a JKS truststore at ./secrets/opensearch-truststore.jks.
#
# This is the local equivalent of the `secret.ref://es-cacert` truststore
# referenced in the production connector config.
#
# Usage:
#   ./02-make-truststore.sh
#   TRUSTSTORE_PASS=mypassword ./02-make-truststore.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_DIR="${SCRIPT_DIR}/../secrets"
CA_PEM="${SECRETS_DIR}/opensearch-root-ca.pem"
TRUSTSTORE="${SECRETS_DIR}/opensearch-truststore.jks"
TRUSTSTORE_PASS="${TRUSTSTORE_PASS:-changeit}"
CONTAINER_NAME="opensearch"

echo "==> [02-make-truststore] extracting root CA from container '${CONTAINER_NAME}' ..."

# Wait for the container to be running (it may still be starting)
for i in {1..20}; do
  STATUS=$(docker inspect -f '{{.State.Status}}' "${CONTAINER_NAME}" 2>/dev/null || echo "missing")
  if [[ "${STATUS}" == "running" ]]; then
    break
  fi
  echo "    waiting for container (attempt ${i}/20, status=${STATUS}) ..."
  sleep 5
done

if [[ "$(docker inspect -f '{{.State.Status}}' "${CONTAINER_NAME}" 2>/dev/null)" != "running" ]]; then
  echo "ERROR: container '${CONTAINER_NAME}' is not running."
  echo "  Start it first: docker compose -f <path>/docker-compose.yml up -d opensearch"
  exit 1
fi

mkdir -p "${SECRETS_DIR}"

# OpenSearch demo security ships root-ca.pem at this path inside the image
docker cp "${CONTAINER_NAME}:/usr/share/opensearch/config/root-ca.pem" "${CA_PEM}"
echo "==> [02-make-truststore] CA cert saved to ${CA_PEM}"

# Remove old truststore so keytool doesn't prompt about an existing alias
rm -f "${TRUSTSTORE}"

keytool \
  -importcert \
  -noprompt \
  -alias opensearch-root-ca \
  -file "${CA_PEM}" \
  -keystore "${TRUSTSTORE}" \
  -storepass "${TRUSTSTORE_PASS}" \
  -storetype JKS

echo "==> [02-make-truststore] truststore written: ${TRUSTSTORE}"
echo "    password: ${TRUSTSTORE_PASS}"
echo "    (mounted into Connect container at /etc/secrets/opensearch-truststore.jks)"
