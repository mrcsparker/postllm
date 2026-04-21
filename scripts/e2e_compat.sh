#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run_step() {
  local label=$1
  shift

  echo
  echo "==> ${label}"
  "$@"
}

if [[ "${POSTLLM_COMPAT_SKIP_OLLAMA:-0}" != "1" ]]; then
  run_step "Ollama compatibility" "${ROOT_DIR}/scripts/e2e_ollama.sh"
fi

if [[ "${POSTLLM_COMPAT_SKIP_LLAMA:-0}" != "1" ]]; then
  run_step "llama.cpp compatibility" "${ROOT_DIR}/scripts/e2e_llama.sh"
fi

if [[ "${POSTLLM_COMPAT_SKIP_PG_TESTS:-0}" != "1" ]]; then
  run_step \
    "OpenAI Responses compatibility" \
    cargo pgrx test pg17 sql_chat_structured_should_support_responses_api_base_url -F pg_test
  run_step \
    "Anthropic Messages compatibility" \
    cargo pgrx test pg17 sql_chat_text_should_support_anthropic_messages_api -F pg_test
  run_step \
    "Anthropic tool compatibility" \
    cargo pgrx test pg17 sql_chat_tools_should_support_anthropic_messages_api -F pg_test
  run_step \
    "Anthropic multimodal compatibility" \
    cargo pgrx test pg17 sql_chat_text_should_support_anthropic_multimodal_inputs -F pg_test
fi

echo
echo "Compatibility matrix passed."
