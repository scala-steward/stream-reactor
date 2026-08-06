#!/usr/bin/env bash
# List GCS directories under bad/ and good/ and prove the rolling-window fix.
#
# Expected for the 6 produced records:
#   bad/  → ~6 per-minute directories
#   good/ → 2–3 fifteen-minute bucket directories

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

load_env

FAIL=0
pass() { echo "  [PASS] $*"; }
fail() { echo "  [FAIL] $*"; FAIL=1; }

check_connector() {
  local name="$1"
  local status_json
  status_json=$(curl -sf "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null || true)
  local state
  if command -v jq &>/dev/null; then
    state=$(echo "${status_json}" | jq -r '.connector.state // "unknown"')
    local failed
    failed=$(echo "${status_json}" | jq -r '[.tasks // [] | .[] | select(.state == "FAILED")] | length')
    if [[ "${failed}" -gt 0 ]]; then
      fail "connector '${name}' has ${failed} FAILED task(s)"
      echo "${status_json}" | jq '.tasks[] | select(.state == "FAILED") | {id, state, trace}'
      return
    fi
  else
    state=$(echo "${status_json}" | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "unknown")
  fi
  if [[ "${state}" == "RUNNING" ]]; then
    pass "connector '${name}' is RUNNING"
  else
    fail "connector '${name}' state is '${state}' (expected RUNNING)"
  fi
}

# Partition directories are immediate children under the prefix. With
# partition.include.keys=false the SMT header value is the directory name.
list_partition_dirs() {
  local prefix="$1"
  # List one level of "directories" (prefixes ending in /)
  local listing
  listing=$(gcs_ls "gs://${GCS_BUCKET}/${prefix}/")
  # Keep only immediate child prefixes (end with /), strip the common prefix
  echo "${listing}" | awk -v p="gs://${GCS_BUCKET}/${prefix}/" '
    index($0, p) == 1 {
      rest = substr($0, length(p) + 1)
      # take only first path segment; keep if it looks like a dir (has trailing / in original or contains /)
      n = split(rest, parts, "/")
      if (parts[1] != "") print parts[1]
    }' | sort -u
}

count_lines() {
  local input="$1"
  if [[ -z "${input}" ]]; then
    echo 0
  else
    echo "${input}" | grep -c . || echo 0
  fi
}

# gcs-rolling partitions Hive-style on load_date/load_hour/load_minute, so the
# distinct 15-minute buckets are the unique load_minute=NN segments (recursive).
list_rolling_minute_buckets() {
  local prefix="$1"
  gcs_ls_recursive "gs://${GCS_BUCKET}/${prefix}/" \
    | grep -oE 'load_minute=[0-9]+' | sort -u
}

info "[06-verify] checking connector status ..."
check_connector "gcs-bad"
check_connector "gcs-good"
check_connector "gcs-rolling"

# Wait until counts meet the later assertion floors (not just "any object"),
# otherwise a slow Connect/GCS can exit early and fail the bad >= 5 check.
info "[06-verify] waiting for expected partition counts in GCS (up to ~120 s) ..."
BAD_PREFIX="${GCS_PREFIX}/bad"
GOOD_PREFIX="${GCS_PREFIX}/good"
ROLLING_PREFIX="${GCS_PREFIX}/rolling"
for i in {1..24}; do
  BAD_LIST=$(list_partition_dirs "${BAD_PREFIX}")
  GOOD_LIST=$(list_partition_dirs "${GOOD_PREFIX}")
  ROLLING_LIST=$(list_rolling_minute_buckets "${ROLLING_PREFIX}")
  BAD_COUNT=$(count_lines "${BAD_LIST}")
  GOOD_COUNT=$(count_lines "${GOOD_LIST}")
  ROLLING_COUNT=$(count_lines "${ROLLING_LIST}")
  echo "    attempt ${i}/24 — bad dirs=${BAD_COUNT}, good dirs=${GOOD_COUNT}, rolling buckets=${ROLLING_COUNT}"
  if [[ "${BAD_COUNT}" -ge 5 && "${GOOD_COUNT}" -ge 2 && "${ROLLING_COUNT}" -ge 2 ]]; then
    break
  fi
  sleep 5
done

echo ""
info "[06-verify] BAD prefix  gs://${GCS_BUCKET}/${BAD_PREFIX}/  (per-minute HHmm, no rolling window)"
if [[ -z "${BAD_LIST}" ]]; then
  echo "    (empty)"
else
  echo "${BAD_LIST}" | sed 's/^/    /'
fi
echo "    count: ${BAD_COUNT}"

echo ""
info "[06-verify] GOOD prefix gs://${GCS_BUCKET}/${GOOD_PREFIX}/  (TimestampConverter, 15-minute rolling window, single key)"
if [[ -z "${GOOD_LIST}" ]]; then
  echo "    (empty)"
else
  echo "${GOOD_LIST}" | sed 's/^/    /'
fi
echo "    count: ${GOOD_COUNT}"

echo ""
info "[06-verify] ROLLING prefix gs://${GCS_BUCKET}/${ROLLING_PREFIX}/  (InsertRollingFieldTimestampHeaders, all-digit yyyyMMddHHmmssSSS, LA tz, Hive-style)"
if [[ -z "${ROLLING_LIST}" ]]; then
  echo "    (empty)"
else
  echo "${ROLLING_LIST}" | sed 's/^/    /'
fi
echo "    distinct 15-min buckets (load_minute=): ${ROLLING_COUNT}"

echo ""
info "[06-verify] asserting before/after proof ..."

# Bad: expect one dir per distinct minute ≈ 6 for our fixture
if [[ "${BAD_COUNT}" -ge 5 ]]; then
  pass "bad has ${BAD_COUNT} partition dirs (>= 5 expected for per-minute keys)"
else
  fail "bad has ${BAD_COUNT} partition dirs (expected >= 5 for HHmm without rolling window)"
fi

# Good: expect 2–3 fifteen-minute buckets for the fixture
if [[ "${GOOD_COUNT}" -ge 2 && "${GOOD_COUNT}" -le 4 ]]; then
  pass "good has ${GOOD_COUNT} partition dirs (expected 2–3 for 15-minute buckets)"
else
  fail "good has ${GOOD_COUNT} partition dirs (expected 2–3 for 15-minute rolling window)"
fi

if [[ "${GOOD_COUNT}" -gt 0 && "${BAD_COUNT}" -gt 0 && "${GOOD_COUNT}" -lt "${BAD_COUNT}" ]]; then
  pass "good (${GOOD_COUNT}) < bad (${BAD_COUNT}) — rolling window reduced partition proliferation"
else
  fail "expected good dir count (${GOOD_COUNT}) to be strictly less than bad (${BAD_COUNT})"
fi

# Rolling: all-digit yyyyMMddHHmmssSSS via InsertRollingFieldTimestampHeaders,
# expect the same 3 fifteen-minute buckets — proving the all-digit format works
# where TimestampConverter would misread it as a Unix epoch.
if [[ "${ROLLING_COUNT}" -ge 2 && "${ROLLING_COUNT}" -le 4 ]]; then
  pass "rolling has ${ROLLING_COUNT} 15-min buckets (expected 2–3; all-digit input parsed correctly)"
else
  fail "rolling has ${ROLLING_COUNT} 15-min buckets (expected 2–3; check format.from.pattern / task logs)"
fi

if [[ "${ROLLING_COUNT}" -gt 0 && "${ROLLING_COUNT}" -lt "${BAD_COUNT}" ]]; then
  pass "rolling (${ROLLING_COUNT}) < bad (${BAD_COUNT}) — rolling window works for the all-digit format too"
else
  fail "expected rolling bucket count (${ROLLING_COUNT}) to be strictly less than bad (${BAD_COUNT})"
fi

echo ""
if [[ "${FAIL}" -eq 0 ]]; then
  echo "==> [06-verify] ALL CHECKS PASSED"
  echo ""
  echo "  Proof summary:"
  echo "    BAD     (no rolling window):                 ${BAD_COUNT} dirs     → one directory per minute"
  echo "    GOOD    (TimestampConverter, 15-min window): ${GOOD_COUNT} dirs     → one directory per bucket"
  echo "    ROLLING (RollingHeaders, all-digit, LA tz):  ${ROLLING_COUNT} buckets → one directory per bucket"
  echo ""
  exit 0
else
  echo "==> [06-verify] SOME CHECKS FAILED"
  echo "    Useful diagnostics:"
  echo "      docker logs ${CONNECT_CONTAINER} 2>&1 | tail -80"
  echo "      curl -s ${CONNECT_URL}/connectors/gcs-bad/status | jq ."
  echo "      curl -s ${CONNECT_URL}/connectors/gcs-good/status | jq ."
  echo "      curl -s ${CONNECT_URL}/connectors/gcs-rolling/status | jq ."
  echo "      gcloud storage ls gs://${GCS_BUCKET}/${GCS_PREFIX}/ --recursive | head"
  exit 1
fi
