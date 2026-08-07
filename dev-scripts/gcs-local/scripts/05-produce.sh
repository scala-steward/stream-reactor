#!/usr/bin/env bash
# Produce JSON records whose load_ts values span >15 minutes so the bad vs good
# partition layouts diverge clearly.
#
# Each record carries the same instant in two forms:
#   load_ts        ISO-8601 string — used by gcs-bad / gcs-good (TimestampConverter).
#                  Must NOT be all-digit: TimestampConverter infers all-digit
#                  strings as Unix epoch millis and ignores format.from.pattern.
#   load_ts_digits yyyyMMddHHmmssSSS all-digit string — used by gcs-rolling
#                  (InsertRollingFieldTimestampHeaders), which honours
#                  format.from.pattern for strings and parses it correctly.
#
#   09:01, 09:07, 09:14, 09:16, 09:22, 09:31
#
# gcs-bad     (HHmm, no window):      6 minute dirs
# gcs-good    (15-min floor, UTC):    09:00, 09:15, 09:30 → 3 dirs
# gcs-rolling (15-min floor, LA tz):  load_minute in {00,15,30} → 3 leaf dirs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

load_env

PRODUCER="/opt/kafka/bin/kafka-console-producer.sh"

info "[05-produce] producing 6 records to '${TOPIC}' ..."

# Value-only JSON (StringConverter key unused). Records are independent of
# wall-clock — the SMTs read the timestamp from the value fields.
printf '%s\n' \
  '{"id":1,"load_ts":"2024-01-15T09:01:00.000","load_ts_digits":"20240115090100000","payload":"t-09:01"}' \
  '{"id":2,"load_ts":"2024-01-15T09:07:00.000","load_ts_digits":"20240115090700000","payload":"t-09:07"}' \
  '{"id":3,"load_ts":"2024-01-15T09:14:00.000","load_ts_digits":"20240115091400000","payload":"t-09:14"}' \
  '{"id":4,"load_ts":"2024-01-15T09:16:00.000","load_ts_digits":"20240115091600000","payload":"t-09:16"}' \
  '{"id":5,"load_ts":"2024-01-15T09:22:00.000","load_ts_digits":"20240115092200000","payload":"t-09:22"}' \
  '{"id":6,"load_ts":"2024-01-15T09:31:00.000","load_ts_digits":"20240115093100000","payload":"t-09:31"}' \
  | docker exec -i "${KAFKA_CONTAINER}" "${PRODUCER}" \
      --bootstrap-server "${BOOTSTRAP_INTERNAL}" \
      --topic "${TOPIC}"

info "[05-produce] done. Waiting briefly for flush.count=1 writers to commit ..."
sleep 10
