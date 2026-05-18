#!/usr/bin/env bash
# Create the two Kafka topics consumed by the opensearch connector.
# Idempotent: uses --if-not-exists.

set -euo pipefail

TOPICS=("elastic-dr-topic" "elastic-dr-topic2")
BOOTSTRAP="kafka:19092"
KAFKA_CONTAINER="kafka"
KAFKA_TOPICS_SH="/opt/kafka/bin/kafka-topics.sh"

echo "==> [03-create-topics] waiting for Kafka broker to be ready ..."
for i in {1..20}; do
  if docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_SH}" \
      --bootstrap-server "${BOOTSTRAP}" --list &>/dev/null; then
    break
  fi
  echo "    attempt ${i}/20 — not ready yet, sleeping 5 s ..."
  sleep 5
done

for TOPIC in "${TOPICS[@]}"; do
  echo "==> [03-create-topics] creating topic: ${TOPIC}"
  docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_SH}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions 1 \
    --replication-factor 1
done

echo "==> [03-create-topics] listing topics:"
docker exec "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_SH}" \
  --bootstrap-server "${BOOTSTRAP}" --list
