#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export POSTLLM_PG_PORT="${POSTLLM_PG_PORT:-5544}"
export POSTLLM_TIMEOUT_MS="${POSTLLM_TIMEOUT_MS:-120000}"
COMPOSE_ARGS=(
  -f "${ROOT_DIR}/compose.yaml"
  -f "${ROOT_DIR}/compose.llama-e2e.yaml"
  -p postllm-demo
)

cleanup() {
  local exit_code=$?

  if [[ "${POSTLLM_DEMO_KEEP:-0}" == "1" ]]; then
    echo "Keeping demo services running because POSTLLM_DEMO_KEEP=1"
    echo "Connect with: psql postgresql://postgres:postgres@127.0.0.1:${POSTLLM_PG_PORT}/postllm"
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

run_demo_sql() {
  docker compose "${COMPOSE_ARGS[@]}" exec -T postgres \
    psql \
      --username postgres \
      --dbname postllm \
      --set ON_ERROR_STOP=1 \
      --file - < "${ROOT_DIR}/demo/support_triage.sql"
}

echo "Starting postllm demo stack..."
docker compose "${COMPOSE_ARGS[@]}" up -d --build

echo "Waiting for PostgreSQL..."
wait_for_postgres 300
wait_for_sql 120

echo "Waiting for llama.cpp runtime..."
wait_for_runtime_ready 900

echo "Running support-triage sample app..."
run_demo_sql
