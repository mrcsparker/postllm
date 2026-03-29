#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export POSTLLM_PG_PORT="${POSTLLM_PG_PORT:-5542}"
COMPOSE_ARGS=(
  -f "${ROOT_DIR}/compose.yaml"
  -f "${ROOT_DIR}/compose.candle-e2e.yaml"
  -p postllm-candle-e2e
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

echo "Starting PostgreSQL for Candle smoke test..."
docker compose "${COMPOSE_ARGS[@]}" up -d --build

echo "Waiting for PostgreSQL readiness..."
wait_for_postgres 300
wait_for_sql 120

settings_json="$(psql_query "SELECT postllm.settings()::text;" | tr -d '\r')"
embedding_dimension="$(psql_query "SELECT array_length(postllm.embed('Candle smoke test'), 1);" | tr -d '\r' | tr -d '[:space:]')"
embedding_norm="$(psql_query "SELECT sqrt(sum(value * value)) FROM unnest(postllm.embed('Candle smoke test')) AS value;" | tr -d '\r' | tr -d '[:space:]')"
batch_count="$(psql_query "SELECT jsonb_array_length(postllm.embed_many(ARRAY['alpha', 'beta']));" | tr -d '\r' | tr -d '[:space:]')"
embedding_inspect_json="$(psql_query "SELECT postllm.model_inspect(lane => 'embedding')::text;" | tr -d '\r')"
embed_document_count="$(psql_query "SELECT count(*) FROM postllm.embed_document('guide-1', 'Alpha sentence. Beta sentence.', '{\"source\":\"manual\"}'::jsonb, chunk_chars => 18, overlap_chars => 4);" | tr -d '\r' | tr -d '[:space:]')"
generation_model="$(psql_query "SELECT postllm.settings()->>'model';" | tr -d '\r' | tr -d '[:space:]')"
generation_install_json="$(psql_query "SELECT postllm.model_install(lane => 'generation')::text;" | tr -d '\r')"
generation_prewarm_json="$(psql_query "SELECT postllm.model_prewarm(lane => 'generation')::text;" | tr -d '\r')"
offline_settings_json="$(psql_query "SELECT postllm.configure(candle_offline => true)::text;" | tr -d '\r')"
rerank_json="$(psql_query "SELECT jsonb_agg(to_jsonb(ranked) ORDER BY rank)::text FROM postllm.rerank('How does PostgreSQL remove dead tuples?', ARRAY['Autovacuum removes dead tuples and helps control table bloat.', 'Bananas are yellow and grow in bunches.'], top_n => 1) AS ranked;" | tr -d '\r')"
hybrid_json="$(psql_query "SELECT jsonb_agg(to_jsonb(ranked) ORDER BY rank)::text FROM postllm.hybrid_rank('How does PostgreSQL remove dead tuples?', ARRAY['Autovacuum removes dead tuples and helps control table bloat.', 'Bananas are yellow and grow in bunches.'], top_n => 1) AS ranked;" | tr -d '\r')"
chat_response_json="$(psql_query "SELECT postllm.chat(ARRAY[postllm.system('You are a literal test harness. Reply with only 4.'), postllm.user('What is 2 + 2?')], temperature => 0.0, max_tokens => 8)::text;" | tr -d '\r')"
complete_response="$(psql_query "SELECT trim(postllm.complete(prompt => '2 + 2 =', system_prompt => 'You are a literal test harness. Reply with only 4.', temperature => 0.0, max_tokens => 8));" | tr -d '\r')"
complete_response_normalized="${complete_response//$'\n'/ }"
complete_response_lower="$(printf '%s' "${complete_response_normalized}" | tr '[:upper:]' '[:lower:]')"

psql_query "DROP TABLE IF EXISTS public.doc_chunks_ingest;
CREATE TABLE public.doc_chunks_ingest (
  chunk_id text PRIMARY KEY,
  doc_id text NOT NULL,
  chunk_no integer NOT NULL,
  content text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  embedding real[] NOT NULL
);" >/dev/null

ingest_summary_json="$(psql_query "SELECT postllm.ingest_document('public.doc_chunks_ingest', 'guide-1', 'Alpha sentence. Beta sentence.', '{\"source\":\"manual\"}'::jsonb, chunk_chars => 18, overlap_chars => 4)::text;" | tr -d '\r')"
ingest_repeat_json="$(psql_query "SELECT postllm.ingest_document('public.doc_chunks_ingest', 'guide-1', 'Alpha sentence. Beta sentence.', '{\"source\":\"manual\"}'::jsonb, chunk_chars => 18, overlap_chars => 4)::text;" | tr -d '\r')"
ingest_row_count="$(psql_query "SELECT count(*) FROM public.doc_chunks_ingest WHERE doc_id = 'guide-1';" | tr -d '\r' | tr -d '[:space:]')"

chat_fields="$(
  CHAT_RESPONSE_JSON="${chat_response_json}" python3 - <<'PY'
import json
import os

response = json.loads(os.environ["CHAT_RESPONSE_JSON"])
metadata = response.get("_postllm", {})
usage = metadata.get("usage", {})
content = response["choices"][0]["message"]["content"].replace("\n", " ")

print(metadata.get("runtime", ""))
print(metadata.get("provider", ""))
print(metadata.get("model", ""))
print(usage.get("prompt_tokens", 0))
print(usage.get("completion_tokens", 0))
print(content)
PY
)"
chat_field_lines=()
while IFS= read -r line; do
  chat_field_lines+=("${line}")
done <<<"${chat_fields}"
chat_metadata_runtime="${chat_field_lines[0]:-}"
chat_metadata_provider="${chat_field_lines[1]:-}"
chat_metadata_model="${chat_field_lines[2]:-}"
chat_prompt_tokens="${chat_field_lines[3]:-0}"
chat_completion_tokens="${chat_field_lines[4]:-0}"
chat_response_normalized="${chat_field_lines[5]:-}"
chat_response_lower="$(printf '%s' "${chat_response_normalized}" | tr '[:upper:]' '[:lower:]')"

ingest_fields="$(
  INGEST_SUMMARY_JSON="${ingest_summary_json}" INGEST_REPEAT_JSON="${ingest_repeat_json}" python3 - <<'PY'
import json
import os

initial = json.loads(os.environ["INGEST_SUMMARY_JSON"])
repeat = json.loads(os.environ["INGEST_REPEAT_JSON"])

print(initial.get("chunk_count", 0))
print(initial.get("written", 0))
print(initial.get("unchanged", 0))
print(initial.get("deleted", 0))
print(repeat.get("written", 0))
print(repeat.get("unchanged", 0))
print(repeat.get("deleted", 0))
PY
)"
ingest_field_lines=()
while IFS= read -r line; do
  ingest_field_lines+=("${line}")
done <<<"${ingest_fields}"
ingest_chunk_count="${ingest_field_lines[0]:-0}"
ingest_written="${ingest_field_lines[1]:-0}"
ingest_unchanged="${ingest_field_lines[2]:-0}"
ingest_deleted="${ingest_field_lines[3]:-0}"
ingest_repeat_written="${ingest_field_lines[4]:-0}"
ingest_repeat_unchanged="${ingest_field_lines[5]:-0}"
ingest_repeat_deleted="${ingest_field_lines[6]:-0}"

embedding_inspect_fields="$(
  EMBEDDING_INSPECT_JSON="${embedding_inspect_json}" python3 - <<'PY'
import json
import os

inspection = json.loads(os.environ["EMBEDDING_INSPECT_JSON"])
metadata = inspection.get("metadata", {})
integrity = inspection.get("integrity", {})

print(inspection.get("lane", ""))
print(inspection.get("disk_cached", False))
print(metadata.get("dimension", 0))
print(integrity.get("ok", False))
print(integrity.get("status", ""))
print(integrity.get("verified_files", 0))
PY
)"
embedding_inspect_lines=()
while IFS= read -r line; do
  embedding_inspect_lines+=("${line}")
done <<<"${embedding_inspect_fields}"
embedding_lane="${embedding_inspect_lines[0]:-}"
embedding_disk_cached="${embedding_inspect_lines[1]:-False}"
embedding_reported_dimension="${embedding_inspect_lines[2]:-0}"
embedding_integrity_ok="${embedding_inspect_lines[3]:-False}"
embedding_integrity_status="${embedding_inspect_lines[4]:-}"
embedding_integrity_verified="${embedding_inspect_lines[5]:-0}"

generation_lifecycle_fields="$(
  GENERATION_INSTALL_JSON="${generation_install_json}" GENERATION_PREWARM_JSON="${generation_prewarm_json}" python3 - <<'PY'
import json
import os

installed = json.loads(os.environ["GENERATION_INSTALL_JSON"])
prewarmed = json.loads(os.environ["GENERATION_PREWARM_JSON"])
installed_integrity = installed.get("integrity", {})
prewarmed_integrity = prewarmed.get("integrity", {})

print(installed.get("action", ""))
print(installed.get("lane", ""))
print(installed.get("disk_cached", False))
print(installed.get("cached_file_count", 0))
print(len(installed.get("downloaded_files") or []))
print(installed_integrity.get("ok", False))
print(installed_integrity.get("status", ""))
print(installed_integrity.get("verified_files", 0))
print(prewarmed.get("action", ""))
print(prewarmed.get("lane", ""))
print(prewarmed.get("memory_cached", False))
print(prewarmed.get("disk_cached", False))
print(prewarmed_integrity.get("ok", False))
print(prewarmed_integrity.get("status", ""))
PY
)"
generation_lifecycle_lines=()
while IFS= read -r line; do
  generation_lifecycle_lines+=("${line}")
done <<<"${generation_lifecycle_fields}"
generation_install_action="${generation_lifecycle_lines[0]:-}"
generation_install_lane="${generation_lifecycle_lines[1]:-}"
generation_install_disk_cached="${generation_lifecycle_lines[2]:-False}"
generation_install_file_count="${generation_lifecycle_lines[3]:-0}"
generation_install_download_count="${generation_lifecycle_lines[4]:-0}"
generation_install_integrity_ok="${generation_lifecycle_lines[5]:-False}"
generation_install_integrity_status="${generation_lifecycle_lines[6]:-}"
generation_install_integrity_verified="${generation_lifecycle_lines[7]:-0}"
generation_prewarm_action="${generation_lifecycle_lines[8]:-}"
generation_prewarm_lane="${generation_lifecycle_lines[9]:-}"
generation_prewarm_memory_cached="${generation_lifecycle_lines[10]:-False}"
generation_prewarm_disk_cached="${generation_lifecycle_lines[11]:-False}"
generation_prewarm_integrity_ok="${generation_lifecycle_lines[12]:-False}"
generation_prewarm_integrity_status="${generation_lifecycle_lines[13]:-}"

offline_settings_fields="$(
  OFFLINE_SETTINGS_JSON="${offline_settings_json}" python3 - <<'PY'
import json
import os

settings = json.loads(os.environ["OFFLINE_SETTINGS_JSON"])

print(settings.get("candle_offline", False))
PY
)"
offline_settings_lines=()
while IFS= read -r line; do
  offline_settings_lines+=("${line}")
done <<<"${offline_settings_fields}"
offline_enabled="${offline_settings_lines[0]:-False}"

rerank_fields="$(
  RERANK_JSON="${rerank_json}" python3 - <<'PY'
import json
import os

rows = json.loads(os.environ["RERANK_JSON"])
first = rows[0] if rows else {}

print(len(rows))
print(first.get("rank", ""))
print(first.get("index", ""))
print(first.get("document", ""))
print(first.get("score", 0))
PY
)"
rerank_field_lines=()
while IFS= read -r line; do
  rerank_field_lines+=("${line}")
done <<<"${rerank_fields}"
rerank_count="${rerank_field_lines[0]:-0}"
rerank_rank="${rerank_field_lines[1]:-}"
rerank_index="${rerank_field_lines[2]:-}"
rerank_document="${rerank_field_lines[3]:-}"
rerank_score="${rerank_field_lines[4]:-0}"

hybrid_fields="$(
  HYBRID_JSON="${hybrid_json}" python3 - <<'PY'
import json
import os

rows = json.loads(os.environ["HYBRID_JSON"])
first = rows[0] if rows else {}

print(len(rows))
print(first.get("rank", ""))
print(first.get("index", ""))
print(first.get("document", ""))
print(first.get("semantic_rank", ""))
print(first.get("keyword_rank", ""))
print(first.get("score", 0))
PY
)"
hybrid_field_lines=()
while IFS= read -r line; do
  hybrid_field_lines+=("${line}")
done <<<"${hybrid_fields}"
hybrid_count="${hybrid_field_lines[0]:-0}"
hybrid_rank="${hybrid_field_lines[1]:-}"
hybrid_index="${hybrid_field_lines[2]:-}"
hybrid_document="${hybrid_field_lines[3]:-}"
hybrid_semantic_rank="${hybrid_field_lines[4]:-}"
hybrid_keyword_rank="${hybrid_field_lines[5]:-}"
hybrid_score="${hybrid_field_lines[6]:-0}"

echo "Resolved settings: ${settings_json}"
echo "Embedding dimension: ${embedding_dimension}"
echo "Embedding norm: ${embedding_norm}"
echo "Embedding inspection: ${embedding_inspect_json}"
echo "Batch result count: ${batch_count}"
echo "Embedded document row count: ${embed_document_count}"
echo "Generation model: ${generation_model}"
echo "Generation install: ${generation_install_json}"
echo "Generation prewarm: ${generation_prewarm_json}"
echo "Offline settings: ${offline_settings_json}"
echo "Rerank rows: ${rerank_json}"
echo "Hybrid rows: ${hybrid_json}"
echo "Chat metadata runtime/provider/model: ${chat_metadata_runtime}/${chat_metadata_provider}/${chat_metadata_model}"
echo "Chat usage prompt/completion tokens: ${chat_prompt_tokens}/${chat_completion_tokens}"
echo "Chat response: ${chat_response_normalized}"
echo "Complete response: ${complete_response_normalized}"
echo "Ingest summary: ${ingest_summary_json}"
echo "Repeat ingest summary: ${ingest_repeat_json}"
echo "Persisted ingest rows: ${ingest_row_count}"

if [[ -z "${embedding_dimension}" || "${embedding_dimension}" == "0" ]]; then
  echo "Expected a non-empty embedding vector, got dimension=${embedding_dimension:-<empty>}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${batch_count}" != "2" ]]; then
  echo "Expected two batch embeddings, got ${batch_count}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${embedding_lane}" != "embedding" || "${embedding_disk_cached}" != "True" || "${embedding_reported_dimension}" != "${embedding_dimension}" || "${embedding_integrity_ok}" != "True" || "${embedding_integrity_status}" != "verified" ]]; then
  echo "Expected embedding lifecycle inspection to report lane=embedding, disk_cached=True, dimension=${embedding_dimension}, and verified integrity, got lane=${embedding_lane} disk_cached=${embedding_disk_cached} dimension=${embedding_reported_dimension} integrity_ok=${embedding_integrity_ok} integrity_status=${embedding_integrity_status}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${embedding_integrity_verified}" == "0" ]]; then
  echo "Expected embedding inspection to verify at least one cached file, got verified_files=${embedding_integrity_verified}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${embed_document_count}" != "2" ]]; then
  echo "Expected two embedded document rows, got ${embed_document_count}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${generation_model}" != "Qwen/Qwen2.5-0.5B-Instruct" && "${generation_model}" != "Qwen/Qwen2.5-1.5B-Instruct" ]]; then
  echo "Expected a registered Candle starter generation model, got ${generation_model}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${generation_install_action}" != "install" || "${generation_install_lane}" != "generation" || "${generation_install_disk_cached}" != "True" || "${generation_install_integrity_ok}" != "True" || "${generation_install_integrity_status}" != "verified" ]]; then
  echo "Expected generation install to report action=install, lane=generation, disk_cached=True, and verified integrity, got action=${generation_install_action} lane=${generation_install_lane} disk_cached=${generation_install_disk_cached} integrity_ok=${generation_install_integrity_ok} integrity_status=${generation_install_integrity_status}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${generation_install_file_count}" == "0" || "${generation_install_download_count}" == "0" || "${generation_install_integrity_verified}" == "0" ]]; then
  echo "Expected generation install to report cached files, downloaded files, and verified checksums, got cached_file_count=${generation_install_file_count} downloaded_file_count=${generation_install_download_count} verified_files=${generation_install_integrity_verified}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${generation_prewarm_action}" != "prewarm" || "${generation_prewarm_lane}" != "generation" || "${generation_prewarm_memory_cached}" != "True" || "${generation_prewarm_disk_cached}" != "True" || "${generation_prewarm_integrity_ok}" != "True" || "${generation_prewarm_integrity_status}" != "verified" ]]; then
  echo "Expected generation prewarm to report action=prewarm, lane=generation, memory_cached=True, disk_cached=True, and verified integrity, got action=${generation_prewarm_action} lane=${generation_prewarm_lane} memory_cached=${generation_prewarm_memory_cached} disk_cached=${generation_prewarm_disk_cached} integrity_ok=${generation_prewarm_integrity_ok} integrity_status=${generation_prewarm_integrity_status}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${offline_enabled}" != "True" ]]; then
  echo "Expected Candle offline mode to be enabled after configure(candle_offline => true), got ${offline_enabled}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${rerank_count}" != "1" || "${rerank_rank}" != "1" || "${rerank_index}" != "1" ]]; then
  echo "Expected exactly one top-ranked Candle rerank row pointing at the first document, got count=${rerank_count} rank=${rerank_rank} index=${rerank_index}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${rerank_document}" != "Autovacuum removes dead tuples and helps control table bloat." ]]; then
  echo "Expected Candle rerank to keep the autovacuum document on top, got: ${rerank_document}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${hybrid_count}" != "1" || "${hybrid_rank}" != "1" || "${hybrid_index}" != "1" ]]; then
  echo "Expected exactly one top-ranked Candle hybrid row pointing at the first document, got count=${hybrid_count} rank=${hybrid_rank} index=${hybrid_index}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${hybrid_document}" != "Autovacuum removes dead tuples and helps control table bloat." ]]; then
  echo "Expected Candle hybrid ranking to keep the autovacuum document on top, got: ${hybrid_document}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${hybrid_semantic_rank}" != "1" || "${hybrid_keyword_rank}" != "1" ]]; then
  echo "Expected the top Candle hybrid row to have semantic_rank=1 and keyword_rank=1, got semantic_rank=${hybrid_semantic_rank} keyword_rank=${hybrid_keyword_rank}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${chat_metadata_runtime}" != "candle" || "${chat_metadata_provider}" != "candle" ]]; then
  echo "Expected Candle metadata on the chat response, got runtime=${chat_metadata_runtime} provider=${chat_metadata_provider}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${chat_metadata_model}" != "${generation_model}" ]]; then
  echo "Expected chat metadata model ${generation_model}, got ${chat_metadata_model}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${chat_prompt_tokens}" == "0" || "${chat_completion_tokens}" == "0" ]]; then
  echo "Expected non-zero Candle chat usage metadata, got prompt=${chat_prompt_tokens} completion=${chat_completion_tokens}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ -z "${chat_response_lower// /}" ]]; then
  echo "The Candle chat smoke test returned an empty response" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${chat_response_lower}" != *"4"* && "${chat_response_lower}" != *"four"* ]]; then
  echo "Expected the Candle chat smoke response to contain '4' or 'four', got: ${chat_response_normalized}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ -z "${complete_response_lower// /}" ]]; then
  echo "The Candle complete smoke test returned an empty response" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${complete_response_lower}" != *"4"* && "${complete_response_lower}" != *"four"* ]]; then
  echo "Expected the Candle complete smoke response to contain '4' or 'four', got: ${complete_response_normalized}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${ingest_chunk_count}" != "2" || "${ingest_written}" != "2" || "${ingest_unchanged}" != "0" || "${ingest_deleted}" != "0" ]]; then
  echo "Expected the first ingest to write two rows with no unchanged/deleted rows, got chunk_count=${ingest_chunk_count} written=${ingest_written} unchanged=${ingest_unchanged} deleted=${ingest_deleted}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${ingest_repeat_written}" != "0" || "${ingest_repeat_unchanged}" != "2" || "${ingest_repeat_deleted}" != "0" ]]; then
  echo "Expected the repeat ingest to be idempotent, got written=${ingest_repeat_written} unchanged=${ingest_repeat_unchanged} deleted=${ingest_repeat_deleted}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

if [[ "${ingest_row_count}" != "2" ]]; then
  echo "Expected two persisted ingest rows after the idempotent repeat, got ${ingest_row_count}" >&2
  docker compose "${COMPOSE_ARGS[@]}" logs --no-color postgres >&2 || true
  exit 1
fi

python3 - "$embedding_norm" <<'PY'
import sys

norm = float(sys.argv[1])
if not 0.99 <= norm <= 1.01:
    raise SystemExit(f"expected normalized embedding norm near 1.0, got {norm}")
PY

python3 - "$rerank_score" <<'PY'
import sys

score = float(sys.argv[1])
if score <= 0.0:
    raise SystemExit(f"expected positive Candle rerank score, got {score}")
PY

python3 - "$hybrid_score" <<'PY'
import sys

score = float(sys.argv[1])
if score <= 0.0:
    raise SystemExit(f"expected positive Candle hybrid score, got {score}")
PY

echo "Candle end-to-end smoke test passed."
