# Documentation Index

This page gives the quickest route to what you need.

## Roles

- [getting-started.md](./getting-started.md) — Install and run first SQL example.
- [runtime.md](./runtime.md) — Runtime and capability differences.
- [configuration.md](./configuration.md) — GUCs, `configure(...)`, profiles, secrets, permissions, allowlists, model aliases.
- [examples.md](./examples.md) — Copy-paste SQL flows for common use cases.
- [operations.md](./operations.md) — Docker, local dev workflows, and smoke tests.
- [architecture.md](./architecture.md) — Internal request flow and ownership map.

## Reference and advanced usage

- [reference.md](./reference.md) — Complete SQL function index grouped by family.
- [pgvector-integration.md](./pgvector-integration.md) — Vector integration patterns.
- [candle-roadmap.md](./candle-roadmap.md) — Local runtime roadmap and known constraints.

## Recommended reading order

1. [getting-started](./getting-started.md) → [runtime](./runtime.md) → [configuration](./configuration.md)
2. [architecture](./architecture.md) → [reference](./reference.md)
3. [examples](./examples.md) → [operations](./operations.md)

## Minimum reading checklist before first production pilot

- `getting-started`: confirm successful `CREATE EXTENSION` and a basic `chat_text(...)` call.
- `configuration`: confirm `configure(...)`, `permissions`, and secret handling path.
- `runtime`: confirm capability checks (`runtime_discover`, `runtime_ready`) with your target runtime.
- `reference`: confirm expected functions for your top two workflows (chat + embeddings).
- `operations`: confirm smoke path for startup and containerized checks.

## Read once, then keep in your working set

- Quick users: keep `getting-started`, `runtime`, and `examples` open.
- Integrators: add `configuration`, `reference`, and `operations`.
- Contributors: add `architecture` and `reference` for API ownership.
