#!/usr/bin/env bash
# Shared helpers for gcs-local scripts. Sourced by other scripts — not run directly.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"
REPO_ROOT="$(cd "${COMPOSE_DIR}/../.." && pwd)"
CONNECTORS_DIR="${COMPOSE_DIR}/connectors"
SECRETS_DIR="${COMPOSE_DIR}/secrets"
ENV_FILE="${COMPOSE_DIR}/.env"

KAFKA_CONTAINER="${KAFKA_CONTAINER:-gcs-local-kafka}"
CONNECT_CONTAINER="${CONNECT_CONTAINER:-gcs-local-connect}"
CONNECT_URL="${CONNECT_URL:-http://localhost:28083}"
BOOTSTRAP_INTERNAL="kafka:19092"

info() { echo "==> $*"; }
step() { echo ""; echo "════════════════════════════════════════════════════"; echo "  $*"; echo "════════════════════════════════════════════════════"; }
die()  { echo "ERROR: $*" >&2; exit 1; }

load_env() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    die "Missing ${ENV_FILE}. Copy .env.example and fill in GCS_BUCKET / GCS_PREFIX / GCS_CREDENTIALS_FILE."
  fi
  # shellcheck disable=SC1090
  set -a
  # shellcheck disable=SC1091
  source "${ENV_FILE}"
  set +a

  : "${GCS_BUCKET:?GCS_BUCKET must be set in .env}"
  : "${GCS_PREFIX:?GCS_PREFIX must be set in .env}"
  : "${GCS_CREDENTIALS_FILE:?GCS_CREDENTIALS_FILE must be set in .env}"
  : "${TOPIC:=load-events}"
  export GCS_BUCKET GCS_PREFIX GCS_CREDENTIALS_FILE TOPIC
}

# Render ${VAR} placeholders in a template using current environment.
# Prefer envsubst; fall back to python3.
render_template() {
  local tmpl="$1"
  local out="$2"
  if command -v envsubst &>/dev/null; then
    envsubst < "${tmpl}" > "${out}"
  elif command -v python3 &>/dev/null; then
    python3 - "${tmpl}" "${out}" <<'PY'
import os, sys, re
tmpl, out = sys.argv[1], sys.argv[2]
text = open(tmpl).read()
def repl(m):
    return os.environ.get(m.group(1), m.group(0))
open(out, "w").write(re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", repl, text))
PY
  else
    die "Need envsubst (gettext) or python3 to render connector templates."
  fi
}

detect_java17() {
  if [[ -n "${JAVA_HOME:-}" ]]; then
    local ver
    ver=$("${JAVA_HOME}/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
    if [[ "${ver}" == "17" ]]; then
      echo "${JAVA_HOME}"
      return 0
    fi
  fi
  if command -v /usr/libexec/java_home &>/dev/null; then
    local jh
    jh=$(/usr/libexec/java_home -v 17 2>/dev/null || true)
    if [[ -n "${jh}" ]]; then
      echo "${jh}"
      return 0
    fi
  fi
  local sdk_dir="${HOME}/.sdkman/candidates/java"
  if [[ -d "${sdk_dir}" ]]; then
    local candidate
    candidate=$(find "${sdk_dir}" -maxdepth 1 -name "17*" -type d | sort | tail -1)
    if [[ -n "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  fi
  for dir in /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk /usr/lib/jvm/temurin-17; do
    if [[ -d "${dir}" ]]; then
      echo "${dir}"
      return 0
    fi
  done
  return 1
}

wait_http() {
  local url="$1"
  local attempts="${2:-30}"
  local i http
  for i in $(seq 1 "${attempts}"); do
    http=$(curl -s -o /dev/null -w "%{http_code}" "${url}" 2>/dev/null || true)
    if [[ "${http}" == "200" ]]; then
      return 0
    fi
    echo "    attempt ${i}/${attempts} — HTTP ${http:-000}, sleeping 5 s ..."
    sleep 5
  done
  return 1
}

wait_container_healthy() {
  local name="$1"
  local attempts="${2:-30}"
  local i status
  for i in $(seq 1 "${attempts}"); do
    status=$(docker inspect -f '{{.State.Health.Status}}' "${name}" 2>/dev/null || echo "starting")
    echo "    ${name} health: ${status} (attempt ${i}/${attempts})"
    if [[ "${status}" == "healthy" ]]; then
      return 0
    fi
    sleep 10
  done
  return 1
}

# Prefer the connector SA key for gcloud/gsutil so verify does not depend on
# an interactive `gcloud auth login` session (which often expires).
gcs_auth_env() {
  if [[ -n "${GCS_CREDENTIALS_FILE:-}" && -f "${GCS_CREDENTIALS_FILE}" ]]; then
    export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="${GCS_CREDENTIALS_FILE}"
    export GOOGLE_APPLICATION_CREDENTIALS="${GCS_CREDENTIALS_FILE}"
  fi
}

gcs_ls() {
  local path="$1"
  gcs_auth_env
  if command -v gcloud &>/dev/null; then
    gcloud storage ls "${path}" 2>/dev/null || true
  elif command -v gsutil &>/dev/null; then
    gsutil ls "${path}" 2>/dev/null || true
  else
    die "Need gcloud or gsutil to list GCS objects."
  fi
}

gcs_ls_recursive() {
  local path="$1"
  gcs_auth_env
  if command -v gcloud &>/dev/null; then
    gcloud storage ls "${path}" --recursive 2>/dev/null || true
  elif command -v gsutil &>/dev/null; then
    gsutil ls -r "${path}" 2>/dev/null || true
  else
    die "Need gcloud or gsutil to list GCS objects."
  fi
}

gcs_rm_prefix() {
  local path="$1"
  gcs_auth_env
  if command -v gcloud &>/dev/null; then
    gcloud storage rm -r "${path}" 2>/dev/null || true
  elif command -v gsutil &>/dev/null; then
    gsutil -m rm -r "${path}" 2>/dev/null || true
  else
    die "Need gcloud or gsutil to delete GCS objects."
  fi
}
