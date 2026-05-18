#!/usr/bin/env bash
# Produce sample records to elastic-dr-topic and elastic-dr-topic2.
#
# Each record carries an `index_name` message header so the connector's
# `UPSERT INTO _header.index_name` KCQL routes it to the correct OpenSearch index.
#
# Record format (kafka-console-producer with parse.headers + parse.key):
#   headerKey:headerValue<TAB>recordKey<TAB>recordValue
#
# Tab characters are emitted via printf '\t' — never as literal embedded chars —
# to avoid silent corruption when files are saved by editors or tooling.
#
# elastic-dr-topic  → OpenSearch index: dr-index-a
# elastic-dr-topic2 → OpenSearch index: dr-index-b

set -euo pipefail

KAFKA_CONTAINER="kafka"
BOOTSTRAP="kafka:19092"
PRODUCER="/opt/kafka/bin/kafka-console-producer.sh"

run_producer() {
  local topic="$1"
  # $'\t' is expanded by bash on the host to a literal tab before docker exec
  # receives the argument — so the tab reaches the JVM property unambiguously.
  local TAB
  TAB=$'\t'
  docker exec -i "${KAFKA_CONTAINER}" "${PRODUCER}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic "${topic}" \
    --property parse.headers=true \
    --property parse.key=true \
    --property "key.separator=${TAB}" \
    --property "headers.delimiter=${TAB}" \
    --property headers.separator=',' \
    --property headers.key.separator=':'
}

# ── Topic 1: elastic-dr-topic → dr-index-a ────────────────────────────────────
echo "==> [05-produce-data] producing 3 records to 'elastic-dr-topic' (→ dr-index-a) ..."
# printf uses \t for tab, \n for newline — unambiguous regardless of file encoding
printf 'index_name:dr-index-a\tid1\t{"id":1,"name":"alice","score":95}\nindex_name:dr-index-a\tid2\t{"id":2,"name":"bob","score":82}\nindex_name:dr-index-a\tid3\t{"id":3,"name":"carol","score":77}\n' \
  | run_producer "elastic-dr-topic"

# ── Topic 2: elastic-dr-topic2 → dr-index-b ───────────────────────────────────
echo "==> [05-produce-data] producing 3 records to 'elastic-dr-topic2' (→ dr-index-b) ..."
printf 'index_name:dr-index-b\tid4\t{"id":4,"name":"dave","score":91}\nindex_name:dr-index-b\tid5\t{"id":5,"name":"eve","score":68}\nindex_name:dr-index-b\tid6\t{"id":6,"name":"frank","score":55}\n' \
  | run_producer "elastic-dr-topic2"

echo "==> [05-produce-data] all records produced."
echo "    Connector will route them to OpenSearch indices: dr-index-a, dr-index-b"
