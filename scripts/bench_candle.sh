#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export POSTLLM_PG_PORT="${POSTLLM_PG_PORT:-5542}"
COMPOSE_ARGS=(
  -f "${ROOT_DIR}/compose.yaml"
  -f "${ROOT_DIR}/compose.candle-e2e.yaml"
  -p postllm-candle-bench
)

cleanup() {
  local exit_code=$?

  if [[ "${POSTLLM_BENCH_KEEP:-0}" == "1" ]]; then
    echo "Keeping Docker services running because POSTLLM_BENCH_KEEP=1"
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
      docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
      return 1
    fi

    sleep 2
  done
}

echo "Starting candle benchmark services..."
docker compose "${COMPOSE_ARGS[@]}" up -d --build

wait_for_postgres 300
wait_for_sql 120
wait_for_runtime_ready 300

export POSTLLM_BENCH_DSN="postgresql://postgres:postgres@127.0.0.1:${POSTLLM_PG_PORT}/postllm"
export POSTLLM_BENCH_COMPOSE_FILES="${ROOT_DIR}/compose.yaml:${ROOT_DIR}/compose.candle-e2e.yaml"
export POSTLLM_BENCH_COMPOSE_PROJECT="postllm-candle-bench"
export POSTLLM_BENCH_DOCKER_SERVICE="postgres"

SUITE_PATH="${POSTLLM_BENCH_SUITE:-${ROOT_DIR}/benchmarks/runtime_matrix.json}"
OUTPUT_DIR="${POSTLLM_BENCH_OUTPUT_DIR:-${ROOT_DIR}/target/benchmarks/candle}"

python3 "${ROOT_DIR}/scripts/benchmark_suite.py" \
  --suite "${SUITE_PATH}" \
  --output-dir "${OUTPUT_DIR}" \
  --scenario candle-generation-small \
  --scenario candle-embedding-small
