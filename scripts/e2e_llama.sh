#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export POSTLLM_PG_PORT="${POSTLLM_PG_PORT:-5541}"
COMPOSE_ARGS=(
  -f "${ROOT_DIR}/compose.yaml"
  -f "${ROOT_DIR}/compose.llama-e2e.yaml"
  -p postllm-llama-e2e
)

cleanup() {
  local exit_code=$?

  if [[ "${POSTLLM_E2E_KEEP:-0}" == "1" ]]; then
    echo "Keeping Docker services running because POSTLLM_E2E_KEEP=1"
    return "${exit_code}"
  fi

  docker compose "${COMPOSE_ARGS[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  return "${exit_code}"
}

trap cleanup EXIT

wait_for_postgres() {
  local timeout_seconds=$1
  local deadline=$((SECONDS + timeout_seconds))

  until docker compose "${COMPOSE_ARGS[@]}" exec -T postgres \
    pg_isready --username postgres --dbname postllm >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for PostgreSQL readiness" >&2
      docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
      return 1
    fi

    sleep 2
  done
}

psql_query() {
  local sql=$1

  docker compose "${COMPOSE_ARGS[@]}" exec -T postgres \
    psql \
      --username postgres \
      --dbname postllm \
      --tuples-only \
      --no-align \
      --command "${sql}"
}

wait_for_sql() {
  local timeout_seconds=$1
  local deadline=$((SECONDS + timeout_seconds))

  until psql_query "SELECT 1;" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for SQL query readiness" >&2
      docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
      return 1
    fi

    sleep 2
  done
}

wait_for_runtime_ready() {
  local timeout_seconds=$1
  local deadline=$((SECONDS + timeout_seconds))

  until [[ "$(psql_query "SELECT postllm.runtime_ready();" | tr -d '\r' | tr -d '[:space:]')" == "t" ]]; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for postllm runtime readiness" >&2
      psql_query "SELECT postllm.runtime_discover()::text;" >&2 || true
      docker compose "${COMPOSE_ARGS[@]}" logs --no-color llama postgres >&2 || true
      return 1
    fi

    sleep 2
  done
}

echo "Starting llama-server and PostgreSQL..."
docker compose "${COMPOSE_ARGS[@]}" up -d --build

echo "Waiting for PostgreSQL readiness..."
wait_for_postgres 300
wait_for_sql 120

echo "Waiting for postllm runtime readiness..."
wait_for_runtime_ready 900

settings_json="$(psql_query "SELECT postllm.settings()::text;" | tr -d '\r')"
runtime_discovery_json="$(psql_query "SELECT postllm.runtime_discover()::text;" | tr -d '\r')"
response="$(psql_query "SELECT trim(postllm.complete(prompt => 'Reply with the single word ok.', system_prompt => 'You are a literal test harness. Reply with only ok.', temperature => 0.0, max_tokens => 8));" | tr -d '\r')"
response_normalized="${response//$'\n'/ }"
response_lower="$(printf '%s' "${response_normalized}" | tr '[:upper:]' '[:lower:]')"

echo "Resolved settings: ${settings_json}"
echo "Runtime discovery: ${runtime_discovery_json}"
echo "Model response: ${response_normalized}"

if [[ -z "${response_lower// /}" ]]; then
  echo "The llama-server smoke test returned an empty response" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color llama postgres >&2 || true
  exit 1
fi

if [[ "${response_lower}" != *"ok"* ]]; then
  echo "Expected the smoke response to contain 'ok', got: ${response_normalized}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color llama postgres >&2 || true
  exit 1
fi

echo "llama-server end-to-end smoke test passed."
