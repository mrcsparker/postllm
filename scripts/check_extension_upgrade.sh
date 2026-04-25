#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <pg-major> [from-version]" >&2
  exit 1
fi

PG_MAJOR="$1"
FROM_VERSION="${2:-0.0.0}"
PG_FEATURE="pg${PG_MAJOR}"
CURRENT_VERSION="$(awk -F'"' '/^version = / { print $2; exit }' "${ROOT_DIR}/Cargo.toml")"
PG_CONFIG="$(cargo pgrx info pg-config "${PG_FEATURE}")"
PG_BINDIR="$(dirname "${PG_CONFIG}")"
PSQL="${PG_BINDIR}/psql"
SHAREDIR="$("${PG_CONFIG}" --sharedir)"
EXTENSION_DIR="${SHAREDIR}/extension"
CURRENT_SQL="${EXTENSION_DIR}/postllm--${CURRENT_VERSION}.sql"
FROM_SQL="${EXTENSION_DIR}/postllm--${FROM_VERSION}.sql"
UPDATE_SQL="${EXTENSION_DIR}/postllm--${FROM_VERSION}--${CURRENT_VERSION}.sql"
FIXTURE_FROM_SQL="${ROOT_DIR}/tests/upgrade/postllm--${FROM_VERSION}.sql"
FIXTURE_UPDATE_SQL="${ROOT_DIR}/sql/postllm--${FROM_VERSION}--${CURRENT_VERSION}.sql"
PORT="${POSTLLM_UPGRADE_TEST_PORT:-288${PG_MAJOR}}"
TMP_DIR="$(mktemp -d)"
STARTED_POSTGRES=0

backup_path() {
  local path="$1"
  local name

  name="$(basename "${path}")"
  if [[ -e "${path}" ]]; then
    cp "${path}" "${TMP_DIR}/${name}.bak"
  fi
}

restore_path() {
  local path="$1"
  local name

  name="$(basename "${path}")"
  if [[ -e "${TMP_DIR}/${name}.bak" ]]; then
    cp "${TMP_DIR}/${name}.bak" "${path}"
  else
    rm -f "${path}"
  fi
}

cleanup() {
  local exit_code=$?

  restore_path "${FROM_SQL}"
  restore_path "${UPDATE_SQL}"
  rm -rf "${TMP_DIR}"

  if [[ "${STARTED_POSTGRES}" == "1" ]]; then
    cargo pgrx stop "${PG_FEATURE}" >/dev/null 2>&1 || true
  fi

  return "${exit_code}"
}

trap cleanup EXIT

backup_path "${FROM_SQL}"
backup_path "${UPDATE_SQL}"

cargo pgrx install \
  --pg-config "${PG_CONFIG}" \
  --no-default-features \
  --features "${PG_FEATURE}"

if [[ ! -f "${CURRENT_SQL}" ]]; then
  echo "expected current extension SQL at ${CURRENT_SQL}" >&2
  exit 1
fi

if [[ -f "${FIXTURE_FROM_SQL}" ]]; then
  cp "${FIXTURE_FROM_SQL}" "${FROM_SQL}"
elif [[ "${FROM_VERSION}" == "0.0.0" ]]; then
  printf '%s\n' "-- Synthetic empty pre-release baseline for upgrade coverage." > "${FROM_SQL}"
else
  echo "missing upgrade fixture: ${FIXTURE_FROM_SQL}" >&2
  exit 1
fi

if [[ -f "${FIXTURE_UPDATE_SQL}" ]]; then
  cp "${FIXTURE_UPDATE_SQL}" "${UPDATE_SQL}"
elif [[ "${FROM_VERSION}" == "0.0.0" ]]; then
  cp "${CURRENT_SQL}" "${UPDATE_SQL}"
else
  echo "missing upgrade script: ${FIXTURE_UPDATE_SQL}" >&2
  exit 1
fi

if cargo pgrx status "${PG_FEATURE}" | grep -q "stopped"; then
  cargo pgrx start "${PG_FEATURE}"
  STARTED_POSTGRES=1
fi

"${PSQL}" -h localhost -p "${PORT}" -d postgres -v ON_ERROR_STOP=1 \
  -v from_version="${FROM_VERSION}" \
  -v current_version="${CURRENT_VERSION}" <<'SQL'
DROP EXTENSION IF EXISTS postllm CASCADE;
DROP SCHEMA IF EXISTS postllm CASCADE;

CREATE EXTENSION postllm VERSION :'from_version';
ALTER EXTENSION postllm UPDATE TO :'current_version';

CREATE TEMP TABLE postllm_upgrade_expected(version text NOT NULL);
INSERT INTO postllm_upgrade_expected(version) VALUES (:'current_version');

DO $$
DECLARE
  installed_version text;
  target_version text;
  function_count integer;
BEGIN
  SELECT version
  INTO target_version
  FROM postllm_upgrade_expected;

  SELECT extversion
  INTO installed_version
  FROM pg_extension
  WHERE extname = 'postllm';

  IF installed_version IS DISTINCT FROM target_version THEN
    RAISE EXCEPTION 'expected postllm %, got %',
      target_version,
      installed_version;
  END IF;

  SELECT count(*)
  INTO function_count
  FROM pg_proc proc
  JOIN pg_namespace namespace ON namespace.oid = proc.pronamespace
  WHERE namespace.nspname = 'postllm';

  IF function_count < 100 THEN
    RAISE EXCEPTION 'expected postllm function catalog to be populated, got % functions', function_count;
  END IF;

  IF postllm.user('upgrade check')->>'role' IS DISTINCT FROM 'user' THEN
    RAISE EXCEPTION 'postllm.user returned an unexpected payload after upgrade';
  END IF;

  IF postllm.settings()->>'runtime' IS NULL THEN
    RAISE EXCEPTION 'postllm.settings returned an unexpected payload after upgrade';
  END IF;
END;
$$;

DROP EXTENSION postllm CASCADE;
DROP SCHEMA IF EXISTS postllm CASCADE;
SQL

echo "postllm upgrade check passed: ${FROM_VERSION} -> ${CURRENT_VERSION} on ${PG_FEATURE}"
