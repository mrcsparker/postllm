# Benchmark Guide

`postllm` now ships repeatable benchmark suites for latency, throughput, and backend RSS growth across runtimes and model sizes.

## What is included

- `benchmarks/runtime_matrix.json` compares the small-model happy path across hosted completion, local Candle generation, and local Candle embeddings.
- `benchmarks/model_size_ladder.json` provides a small-vs-medium model ladder so you can compare how latency, throughput, and backend RSS move as model size grows.
- `scripts/benchmark_suite.py` runs one suite, records per-scenario latency and throughput, measures backend RSS before and after warmup, and writes JSON plus Markdown reports to `target/benchmarks/`.
- `scripts/bench_llama.sh` and `scripts/bench_candle.sh` are Docker-backed wrappers that reuse the repository’s existing smoke-test runtime setups.

## Quick start

Hosted small-model benchmark:

```bash
bash scripts/bench_llama.sh
```

Local Candle runtime matrix benchmark:

```bash
bash scripts/bench_candle.sh
```

Model-size ladder benchmark with overrides:

```bash
POSTLLM_BENCH_SUITE=benchmarks/model_size_ladder.json \
POSTLLM_CANDLE_MEDIUM_GENERATION_MODEL=Qwen/Qwen2.5-1.5B-Instruct \
bash scripts/bench_candle.sh
```

## Standalone usage

The harness can run against any reachable PostgreSQL instance that already has `postllm` configured:

```bash
POSTLLM_BENCH_DSN=postgresql://postgres:postgres@127.0.0.1:5440/postllm \
python3 scripts/benchmark_suite.py \
  --suite benchmarks/runtime_matrix.json
```

Dry-run suite validation:

```bash
python3 scripts/benchmark_suite.py \
  --suite benchmarks/model_size_ladder.json \
  --dry-run
```

## Scenario shape

Each suite entry defines:

- `name`: stable scenario label used in reports.
- `runtime`: logical runtime family such as `openai` or `candle`.
- `model_size`: comparison bucket like `small` or `medium`.
- `kind`: workload family such as `complete` or `embed`.
- `configure`: arguments passed through `postllm.configure(...)`.
- `prewarm_sql`: optional one-time warmup statement before timed work.
- `statement`: SQL measured during the benchmark loop.
- `warmup`, `iterations`, `concurrency`: probe controls.

String values support `${ENV_VAR}` and `${ENV_VAR:-default}` expansion so one suite can describe multiple environments without hardcoding secrets or local aliases.

## Report contents

Each scenario records:

- `min`, `avg`, `p50`, `p95`, and `max` latency in milliseconds.
- `requests_per_second` for the measured workload.
- backend RSS in KB before warmup, after warmup, and after the measured loop.
- warmup and benchmark RSS deltas so you can spot model-cache growth separately from steady-state request overhead.

The harness writes both:

- `target/benchmarks/<suite>-<timestamp>.json`
- `target/benchmarks/<suite>-<timestamp>.md`

Use the JSON for spreadsheet analysis and the Markdown for commit comments, issues, or design notes.
