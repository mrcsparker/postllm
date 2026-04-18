# Operations Guide

This page covers extension development, Docker entrypoints, and quality checks.

## Native development

```bash
cargo pgrx run pg17
```

Use `psql`:

```sql
CREATE EXTENSION postllm;
SELECT postllm.settings();
```

## Docker runtime

```bash
docker compose up --build
psql postgresql://postgres:postgres@127.0.0.1:5440/postllm
```

The default container points `postllm.base_url` at an Ollama-compatible host inside the compose network.

```sql
SELECT postllm.runtime_discover();
SELECT postllm.runtime_ready();
```

## Environment and artifact integration

For local embeddings, environment variables in the container image include:

- `POSTLLM_EMBEDDING_MODEL`
- `POSTLLM_CANDLE_CACHE_DIR`

## End-to-end checks

Run the Docker smoke suites from the repository root:

```bash
./scripts/e2e_llama.sh
./scripts/e2e_candle.sh
```

`e2e_llama.sh` validates the hosted OpenAI-compatible lane against the bundled `llama-server` container. A passing run proves that:

- the extension can be built and installed into the PostgreSQL Docker image,
- PostgreSQL starts cleanly with `postllm` enabled,
- `postllm.runtime_discover()` and `postllm.runtime_ready()` both succeed for the hosted runtime, and
- `postllm.complete(...)` returns a non-empty response from the local `llama-server`.

`e2e_candle.sh` validates the local Candle lane. A passing run proves that:

- local embedding and batch embedding calls succeed,
- local model inspection, install, prewarm, and offline configuration work,
- `postllm.rerank(...)` and `postllm.hybrid_rank(...)` keep the relevant document on top,
- `postllm.chat(...)` and `postllm.complete(...)` return usable generation output, and
- `postllm.ingest_document(...)` is idempotent across repeated runs.

Common script controls:

```bash
POSTLLM_PG_PORT=5541 ./scripts/e2e_llama.sh
POSTLLM_PG_PORT=5542 ./scripts/e2e_candle.sh
POSTLLM_E2E_KEEP=1 ./scripts/e2e_candle.sh
```

- `POSTLLM_PG_PORT` overrides the published PostgreSQL port for the Docker stack.
- `POSTLLM_E2E_KEEP=1` keeps the Docker services running after the script exits so you can inspect logs or connect with `psql`.

Success is reported explicitly at the end of each run:

- `llama-server end-to-end smoke test passed.`
- `Candle end-to-end smoke test passed.`

## Quality gates

Recommended local checks:

```bash
cargo fmt
cargo test
env PGRX_HOME=/tmp/postllm-pgrx-home cargo clippy --all-targets --no-default-features --features pg17,pg_test -- -D warnings
env PGRX_HOME=/tmp/postllm-pgrx-home CARGO_TARGET_DIR=/tmp/postllm-target cargo pgrx test pg17
```

Optional local Candle coverage:

```bash
env PGRX_HOME=/tmp/postllm-pgrx-home \
  POSTLLM_PG_TEST_CANDLE_E2E=1 \
  POSTLLM_PG_TEST_CANDLE_MODEL=Qwen/Qwen2.5-0.5B-Instruct \
  cargo pgrx test pg17
```

## Notes for production use

This extension executes network and inference work inside the PostgreSQL backend process.
Treat this as part of your architecture:

- keep latency-sensitive SQL paths explicit.
- gate runtime switches with permissions and allowlists.
- use `postllm.request_audit_log` only when you explicitly need audit visibility, and prefer redacted payload settings for routine production debugging.
- when request logging is enabled, prefer `postllm.request_metrics`, `postllm.request_count_metrics`, `postllm.request_error_metrics`, `postllm.request_latency_metrics`, and `postllm.request_token_usage_metrics` for latency/error/token rollups instead of re-parsing JSON from the raw audit table.
- use `runtime_discover()` and `runtime_ready()` in startup scripts.
