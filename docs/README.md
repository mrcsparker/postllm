# Documentation Index

This page gives the quickest route to what you need.

## Roles

- [getting-started.md](./getting-started.md) — Install and run first SQL example.
- [runtime.md](./runtime.md) — Runtime and capability differences.
- [configuration.md](./configuration.md) — GUCs, `configure(...)`, profiles, secrets, permissions, allowlists, model aliases.
- [cookbook.md](./cookbook.md) — Copy-paste recipes for local chat, hosted chat, embeddings, RAG, structured outputs, and tools.
- [demo.md](./demo.md) — One-command sample app path from clone to generated output.
- [examples.md](./examples.md) — Copy-paste SQL flows for common use cases.
- [operations.md](./operations.md) — Docker, local dev workflows, and smoke tests.
- [benchmarks.md](./benchmarks.md) — Benchmark suites for latency, throughput, and backend RSS.
- [architecture.md](./architecture.md) — Internal request flow and ownership map.
- [contributor-style-guide.md](./contributor-style-guide.md) — Contributor rules for ownership, naming, complexity, errors, comments, and tests.
- [code-quality-rubric.md](./code-quality-rubric.md) — Release-gate cleanup rubric for naming, comments, module boundaries, and dead code.
- [professionalization-qa.md](./professionalization-qa.md) — Milestone merge checklist for source quality, behavior, docs, and verification.

## Reference and advanced usage

- [reference.md](./reference.md) — Complete SQL function index grouped by family.
- [pgvector-integration.md](./pgvector-integration.md) — Vector integration patterns.
- [candle-roadmap.md](./candle-roadmap.md) — Local runtime roadmap and known constraints.

## Recommended reading order

1. [getting-started](./getting-started.md) → [demo](./demo.md) → [runtime](./runtime.md) → [configuration](./configuration.md)
2. [architecture](./architecture.md) → [reference](./reference.md)
3. [cookbook](./cookbook.md) → [examples](./examples.md) → [operations](./operations.md) → [benchmarks](./benchmarks.md)

## Minimum reading checklist before first production pilot

- `getting-started`: confirm successful `CREATE EXTENSION` and a basic `chat_text(...)` call.
- `demo`: run the bundled support-triage sample app and confirm generated output.
- `configuration`: confirm `configure(...)`, `permissions`, and secret handling path.
- `runtime`: confirm capability checks (`runtime_discover`, `runtime_ready`) with your target runtime.
- `cookbook`: confirm the closest full workflow recipe for your first production use case.
- `reference`: confirm expected functions for your top two workflows (chat + embeddings).
- `operations`: confirm smoke path for startup and containerized checks.

## Read once, then keep in your working set

- Quick users: keep `getting-started`, `demo`, `runtime`, and `cookbook` open.
- Integrators: add `configuration`, `reference`, `operations`, and `benchmarks` when performance sizing matters.
- Contributors: add `architecture`, `contributor-style-guide`, `code-quality-rubric`, `professionalization-qa`, and `reference` for API ownership, cleanup expectations, and milestone merge checks.
