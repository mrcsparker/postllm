#!/bin/bash
set -euo pipefail

psql \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" \
  --set postllm_runtime="${POSTLLM_RUNTIME:-openai}" \
  --set postllm_base_url="${POSTLLM_BASE_URL:-http://host.docker.internal:11434/v1/chat/completions}" \
  --set postllm_model="${POSTLLM_MODEL:-llama3.2}" \
  --set postllm_embedding_model="${POSTLLM_EMBEDDING_MODEL:-sentence-transformers/paraphrase-MiniLM-L3-v2}" \
  --set postllm_api_key="${POSTLLM_API_KEY:-}" \
  --set postllm_timeout_ms="${POSTLLM_TIMEOUT_MS:-30000}" \
  --set postllm_candle_cache_dir="${POSTLLM_CANDLE_CACHE_DIR:-}" <<'SQL'
CREATE EXTENSION postllm;
ALTER SYSTEM SET postllm.runtime = :'postllm_runtime';
ALTER SYSTEM SET postllm.base_url = :'postllm_base_url';
ALTER SYSTEM SET postllm.model = :'postllm_model';
ALTER SYSTEM SET postllm.embedding_model = :'postllm_embedding_model';
ALTER SYSTEM SET postllm.api_key = :'postllm_api_key';
ALTER SYSTEM SET postllm.timeout_ms = :'postllm_timeout_ms';
ALTER SYSTEM SET postllm.candle_cache_dir = :'postllm_candle_cache_dir';
SELECT pg_reload_conf();
SQL
