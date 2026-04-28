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
./scripts/e2e_ollama.sh
./scripts/e2e_llama.sh
./scripts/e2e_candle.sh
./scripts/e2e_compat.sh
```

`e2e_ollama.sh` validates the hosted OpenAI-compatible lane against a real Ollama container. A passing run proves that:

- the extension can talk to Ollama through the OpenAI-compatible chat-completions surface,
- `postllm.runtime_discover()` and `postllm.runtime_ready()` both succeed for an Ollama-backed profile, and
- `postllm.complete(...)` returns a non-empty response from a pulled Ollama model.

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

`e2e_compat.sh` is the aggregate compatibility runner. It executes:

- the real Ollama smoke lane,
- the real llama.cpp/`llama-server` smoke lane, and
- targeted `pg_test` compatibility fixtures for OpenAI Responses and Anthropic Messages features.

Common script controls:

```bash
POSTLLM_PG_PORT=5543 ./scripts/e2e_ollama.sh
POSTLLM_PG_PORT=5541 ./scripts/e2e_llama.sh
POSTLLM_PG_PORT=5542 ./scripts/e2e_candle.sh
POSTLLM_E2E_KEEP=1 ./scripts/e2e_candle.sh
POSTLLM_OLLAMA_MODEL=llama3.2:1b-text-q4_K_M ./scripts/e2e_ollama.sh
POSTLLM_COMPAT_SKIP_OLLAMA=1 ./scripts/e2e_compat.sh
```

- `POSTLLM_PG_PORT` overrides the published PostgreSQL port for the Docker stack.
- `POSTLLM_OLLAMA_MODEL` chooses which Ollama model is pulled for the real compatibility lane.
- `POSTLLM_E2E_KEEP=1` keeps the Docker services running after the script exits so you can inspect logs or connect with `psql`.
- `POSTLLM_COMPAT_SKIP_OLLAMA`, `POSTLLM_COMPAT_SKIP_LLAMA`, and `POSTLLM_COMPAT_SKIP_PG_TESTS` let you narrow the aggregate matrix to one slice when debugging.

Success is reported explicitly at the end of each run:

- `Ollama end-to-end smoke test passed.`
- `llama-server end-to-end smoke test passed.`
- `Candle end-to-end smoke test passed.`
- `Compatibility matrix passed.`

## Quality gates

Recommended local checks:

```bash
cargo fmt --all --check
env PGRX_HOME=/tmp/postllm-pgrx-home cargo clippy --all-targets --no-default-features --features pg17,pg_test -- -D warnings
env PGRX_HOME=/tmp/postllm-pgrx-home cargo test --lib --locked --no-default-features --features pg17
env PGRX_HOME=/tmp/postllm-pgrx-home CARGO_TARGET_DIR=/tmp/postllm-target cargo pgrx test pg17
```

GitHub Actions runs the corresponding repository gate in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml). The workflow is split deliberately:

- `quality` on Ubuntu with PostgreSQL 17 runs `cargo fmt --check`, strict Clippy, `cargo test --lib`, a focused `pg_test` smoke slice, and extension upgrade coverage.
- `postgres-compile-matrix` builds the crate across PostgreSQL 13 through 18 on Ubuntu so every advertised feature flag stays live.
- `postgres-smoke-matrix` runs focused `pg_test` smoke checks on PostgreSQL 16 and 18, with PostgreSQL 17 already covered by `quality`.
- `os-build-matrix` runs cross-platform build checks on macOS and Windows against PostgreSQL 17 so non-Linux regressions are caught before release.

Contributor review and milestone QA expectations live in [contributor-style-guide.md](./contributor-style-guide.md) and [professionalization-qa.md](./professionalization-qa.md). The pull request template requires those checks before milestone work merges.

The upgrade check is available locally:

```bash
./scripts/check_extension_upgrade.sh 17
```

By default it validates the initial release path by creating a synthetic empty `0.0.0` extension and upgrading into the current packaged SQL. Future release-to-release checks can add `tests/upgrade/postllm--<old-version>.sql` fixtures and matching `sql/postllm--<old-version>--<current-version>.sql` migration scripts, then call the same script with the old version.

## Release automation

Release automation lives in [`.github/workflows/release.yml`](../.github/workflows/release.yml) and [`.github/workflows/release-drafter.yml`](../.github/workflows/release-drafter.yml).

- Pushing a `v*.*.*` tag packages extension artifacts for PostgreSQL 13 through 18 on Linux and uploads them to the GitHub Release.
- The same release workflow builds and pushes the PostgreSQL 17 runtime image to `ghcr.io/<owner>/postllm` for both `linux/amd64` and `linux/arm64`.
- GitHub Release notes are generated automatically when the tagged release is published.
- `release-drafter.yml` keeps a draft changelog current on `main` so upcoming release notes stay visible before a tag is cut.

The packaged extension helper is also available for local dry runs:

```bash
./scripts/package_release_artifact.sh 17
```

Optional local Candle coverage:

```bash
env PGRX_HOME=/tmp/postllm-pgrx-home \
  POSTLLM_PG_TEST_CANDLE_E2E=1 \
  POSTLLM_PG_TEST_CANDLE_MODEL=Qwen/Qwen2.5-0.5B-Instruct \
  cargo pgrx test pg17
```

## Benchmarks

Run the benchmark wrappers from the repository root:

```bash
bash scripts/bench_llama.sh
bash scripts/bench_candle.sh
```

Useful controls:

```bash
POSTLLM_BENCH_KEEP=1 bash scripts/bench_llama.sh
POSTLLM_BENCH_SUITE=benchmarks/model_size_ladder.json bash scripts/bench_candle.sh
POSTLLM_BENCH_OUTPUT_DIR=target/benchmarks/custom bash scripts/bench_candle.sh
```

- `POSTLLM_BENCH_KEEP=1` leaves the Docker services running after the benchmark finishes.
- `POSTLLM_BENCH_SUITE` switches from the small runtime matrix to another suite file such as the model-size ladder.
- `POSTLLM_BENCH_OUTPUT_DIR` chooses where the JSON and Markdown reports are written.

The standalone harness also works against any already-running PostgreSQL instance with `postllm` installed:

```bash
POSTLLM_BENCH_DSN=postgresql://postgres:postgres@127.0.0.1:5440/postllm \
python3 scripts/benchmark_suite.py --suite benchmarks/runtime_matrix.json
```

## Notes for production use

This extension executes network and inference work inside the PostgreSQL backend process.
Treat this as part of your architecture:

- keep latency-sensitive SQL paths explicit.
- gate runtime switches with permissions and allowlists.
- use `postllm.request_max_concurrency`, `postllm.request_token_budget`, `postllm.request_runtime_budget_ms`, and `postllm.request_spend_budget_microusd` when you need hard operator ceilings on concurrency, output size, wall-clock time, or estimated generated-output spend.
- use `postllm.request_audit_log` only when you explicitly need audit visibility, and prefer redacted payload settings for routine production debugging.
- when request logging is enabled, prefer `postllm.request_metrics`, `postllm.request_count_metrics`, `postllm.request_error_metrics`, `postllm.request_latency_metrics`, and `postllm.request_token_usage_metrics` for latency/error/token rollups instead of re-parsing JSON from the raw audit table.
- use `postllm.job_submit(...)`, `postllm.job_poll(...)`, `postllm.job_result(...)`, and `postllm.job_cancel(...)` when you need durable submit/poll/cancel semantics for one request without wiring a separate application queue first.
- `LISTEN postllm_async_jobs` when you want push-style async job lifecycle events instead of polling; payloads stay compact and include the event name, job id, status, kind, timestamps, and terminal error/result flags without exposing full request or result bodies.
- use `postllm.conversation_create(...)`, `postllm.conversation_append(...)`, `postllm.conversation_history(...)`, and `postllm.conversation_reply(...)` when you want durable multi-turn transcripts owned by the current role instead of rebuilding chat history in the application on every call.
- use `postllm.prompt_set(...)`, `postllm.prompt_render(...)`, and `postllm.prompt_message(...)` when prompt assets should live in the database with version history instead of inside application source or ad hoc SQL strings.
- use `postllm.eval_dataset_set(...)`, `postllm.eval_case_set(...)`, `postllm.eval_score(...)`, and `postllm.eval_case_score(...)` when prompt or model regressions should be stored as role-owned fixtures in the database instead of scattered through application test code or ad hoc notebook checks.
- use `runtime_discover()` and `runtime_ready()` in startup scripts.

## When it fits

Inference inside PostgreSQL is a good fit when:

- the model call is a deliberate part of a SQL workflow such as ingestion, reranking, tagging, summarization, or operator tooling.
- each statement performs a small, bounded amount of model work and the caller can tolerate the request living on the backend connection.
- you want one policy surface for permissions, network controls, audit logging, and request guardrails.
- the database is the natural coordination point and shipping data out to another service would add more complexity than it removes.

## When it does not

Push model work out of the backend process when:

- the request can run for a long time, fan out over many rows, or compete with core OLTP traffic.
- you need queueing, retries, admission control, or cancellation semantics that belong in a worker tier rather than one SQL statement.
- the workload is bursty enough that concurrent inference could starve normal database work even with `request_max_concurrency`, `candle_max_concurrency`, and request budgets in place.
- the application already has an async orchestration layer and PostgreSQL does not need to own the model call itself.

## Practical guidance

- Prefer synchronous in-database inference for small, explicit, human-scale calls.
- Prefer batch jobs or external workers for large backfills, long document pipelines, or user-facing hot paths with tight latency SLOs.
- Start with conservative budgets and raise them intentionally: `request_max_concurrency`, `request_runtime_budget_ms`, `request_token_budget`, `request_spend_budget_microusd`, and `candle_max_concurrency` are the first levers to reach for.
