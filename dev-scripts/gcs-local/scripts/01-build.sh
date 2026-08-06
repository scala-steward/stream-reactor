#!/usr/bin/env bash
# Build the GCS sink assembly + Lenses SMT jars and stage them under ./connectors/.
# Also stages the GCP service-account JSON into ./secrets/.
#
# Usage:
#   ./01-build.sh              # skip jars if already present
#   REBUILD=1 ./01-build.sh    # always rebuild / re-stage

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

load_env

GCP_PLUGIN_DIR="${CONNECTORS_DIR}/gcp-storage"
SMT_PLUGIN_DIR="${CONNECTORS_DIR}/kafka-connect-smt"
STAGED_CREDS="${SECRETS_DIR}/gcp-credentials.json"

# SMT is always the published GitHub Release jar — never built from source.
SMT_VERSION="${SMT_VERSION:-1.5.0}"
SMT_RELEASE_URL="${SMT_RELEASE_URL:-https://github.com/lensesio/kafka-connect-smt/releases/download/v${SMT_VERSION}/kafka-connect-smt-${SMT_VERSION}.jar}"

info "[01-build] repo root: ${REPO_ROOT}"

# ── JDK 17 (needed for gcp-storage assembly) ──────────────────────────────────
JAVA17_HOME=$(detect_java17 || true)
if [[ -z "${JAVA17_HOME}" ]]; then
  die "JDK 17 not found. Install temurin@17 or set JAVA_HOME."
fi
export JAVA_HOME="${JAVA17_HOME}"
export PATH="${JAVA_HOME}/bin:${PATH}"
info "[01-build] using Java: $("${JAVA_HOME}/bin/java" -version 2>&1 | head -1)"

# ── Stage credentials ─────────────────────────────────────────────────────────
if [[ ! -f "${GCS_CREDENTIALS_FILE}" ]]; then
  die "GCS_CREDENTIALS_FILE does not exist: ${GCS_CREDENTIALS_FILE}"
fi
mkdir -p "${SECRETS_DIR}"
cp -f "${GCS_CREDENTIALS_FILE}" "${STAGED_CREDS}"
chmod 600 "${STAGED_CREDS}"
info "[01-build] staged credentials → ${STAGED_CREDS}"

# ── GCS sink assembly ─────────────────────────────────────────────────────────
mkdir -p "${GCP_PLUGIN_DIR}"
EXISTING_GCP=$(ls "${GCP_PLUGIN_DIR}"/*.jar 2>/dev/null | head -1 || true)
if [[ "${REBUILD:-0}" != "1" && -n "${EXISTING_GCP}" ]]; then
  info "[01-build] gcp-storage jar already present: ${EXISTING_GCP}"
  info "          Set REBUILD=1 to force a rebuild."
else
  info "[01-build] running sbt 'project gcp-storage' assembly ..."
  cd "${REPO_ROOT}"
  sbt "project gcp-storage" "set assembly / test := {}" assembly
  JAR=$(ls "${REPO_ROOT}/kafka-connect-gcp-storage/target/libs/"*.jar 2>/dev/null | head -1 || true)
  [[ -n "${JAR}" ]] || die "No assembly jar found under kafka-connect-gcp-storage/target/libs/"
  rm -f "${GCP_PLUGIN_DIR}"/*.jar
  cp -v "${JAR}" "${GCP_PLUGIN_DIR}/"
  info "[01-build] gcp-storage jar ready: ${GCP_PLUGIN_DIR}/$(basename "${JAR}")"
fi

# ── SMT jar (published release only) ──────────────────────────────────────────
mkdir -p "${SMT_PLUGIN_DIR}"
EXISTING_SMT=$(ls "${SMT_PLUGIN_DIR}"/*.jar 2>/dev/null | head -1 || true)
if [[ "${REBUILD:-0}" != "1" && -n "${EXISTING_SMT}" ]]; then
  info "[01-build] SMT jar already present: ${EXISTING_SMT}"
else
  DEST="${SMT_PLUGIN_DIR}/kafka-connect-smt-${SMT_VERSION}.jar"
  info "[01-build] downloading SMT v${SMT_VERSION} from GitHub Releases ..."
  info "          ${SMT_RELEASE_URL}"
  rm -f "${SMT_PLUGIN_DIR}"/*.jar
  if ! command -v curl &>/dev/null; then
    die "curl is required to download the SMT release jar."
  fi
  if ! curl -fL --retry 3 -o "${DEST}" "${SMT_RELEASE_URL}"; then
    rm -f "${DEST}" 2>/dev/null || true
    die "Failed to download kafka-connect-smt jar from:
  ${SMT_RELEASE_URL}
See: https://github.com/lensesio/kafka-connect-smt/releases/tag/v${SMT_VERSION}
Override with SMT_VERSION=... or SMT_RELEASE_URL=..."
  fi
  if [[ ! -s "${DEST}" ]] || [[ "$(wc -c < "${DEST}")" -lt 1000 ]]; then
    rm -f "${DEST}"
    die "Downloaded SMT jar looks empty/corrupt: ${DEST}"
  fi
  info "[01-build] SMT jar ready: ${DEST}"
fi

info "[01-build] done."
ls -la "${GCP_PLUGIN_DIR}" "${SMT_PLUGIN_DIR}" "${STAGED_CREDS}"
