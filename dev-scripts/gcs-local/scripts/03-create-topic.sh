#!/usr/bin/env bash
# Create the Kafka topic used by both gcs-bad and gcs-good connectors.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

load_env

KAFKA_TOPICS_SH="/opt/kafka/bin/kafka-topics.sh"

info "[03-create-topic] waiting for Kafka broker ..."
for i in {1..20}; do
  if docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_SH}" \
      --bootstrap-server "${BOOTSTRAP_INTERNAL}" --list &>/dev/null; then
    break
  fi
  echo "    attempt ${i}/20 — not ready yet, sleeping 5 s ..."
  sleep 5
done

info "[03-create-topic] creating topic: ${TOPIC}"
docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_SH}" \
  --bootstrap-server "${BOOTSTRAP_INTERNAL}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions 1 \
  --replication-factor 1

info "[03-create-topic] topics:"
docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_SH}" \
  --bootstrap-server "${BOOTSTRAP_INTERNAL}" --list
