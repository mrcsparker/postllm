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

Dockerized llama-server:

```bash
./scripts/e2e_llama.sh
```

Dockerized Candle:

```bash
./scripts/e2e_candle.sh
```

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
- use `runtime_discover()` and `runtime_ready()` in startup scripts.

