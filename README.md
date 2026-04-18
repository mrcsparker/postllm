# postllm

`postllm` is a `PostgreSQL` extension that makes LLM workflows native to SQL.

It provides:

- chat and completion APIs for hosted and local inference,
- embedding/chunking/retrieval helpers for vector workflows,
- runtime and governance configuration (`configure`, profiles, secrets, permissions, allowlists),
- request-response helpers so SQL consumers can stay in SQL.

## Start in 60 seconds

1. Install and run a local extension:

```bash
cargo install cargo-pgrx --version 0.18.0 --locked
cargo pgrx init --pg17 download
cargo pgrx run pg17
```

2. Load the extension and run one call:

```sql
CREATE EXTENSION postllm;

SELECT postllm.configure(
  runtime => 'openai',
  model => 'gpt-4o-mini',
  base_url => 'http://127.0.0.1:11434/v1/chat/completions'
);

SELECT postllm.chat_text(ARRAY[
  postllm.system('You are concise.'),
  postllm.user('Explain MVCC in one sentence.')
]);
```

## Where to read first

Choose one path based on your role:

- **Trying it out quickly** → [getting-started](./docs/getting-started.md)
- **Configuring security/governance** → [configuration](./docs/configuration.md)
- **Choosing a runtime model** → [runtime](./docs/runtime.md)
- **Debugging and deployment** → [operations](./docs/operations.md)
- **Understanding the architecture** → [architecture](./docs/architecture.md)
- **Finding every function** → [reference](./docs/reference.md)
- **Role-based navigation list** → [docs/README.md](./docs/README.md)

## API families at a glance

- Session/configuration: `settings`, `capabilities`, `configure`, `runtime_discover`, `runtime_ready`
- Profiles/governance: `profile*`, `secret*`, `permission*`, `model_alias*`
- Message builders: `message`, `user`, `assistant`, parts/templates, tool helpers
- Generation: `chat*`, `complete*`, streaming, structured outputs, tools, and helpers (`usage`, `finish_reason`, etc.)
- Retrieval/embeddings: `chunk_*`, `embed*`, `ingest_document`, `rerank`, `hybrid_rank`, `rag*`

For full signatures, use the grouped list in [docs/reference.md](./docs/reference.md).

## Runtime model

`postllm` exposes two runtime lanes:

- `openai` for `OpenAI`-compatible HTTP APIs (`OpenAI`, Ollama, llama-server style).
- `candle` for local inference paths (embeddings, reranking, and starter generation).

The SQL API shape is shared across runtimes; capability checks determine what arguments are valid in each lane.

## Internal structure

Current code organization:

- `src/lib.rs` — SQL exports and extension SQL schema.
- `src/api/` — API namespace modules (`config`, `messages`, `inference`, `retrieval`, `ops`).
- `src/api/config.rs`/`messages.rs`/`inference.rs`/`retrieval.rs`/`ops.rs` — SQL-facing API implementations that keep `lib.rs` compact.
- Internal domain modules (`backend`, `client`, `guc`, `catalog`, `permissions`, `http_policy`, `operator_policy`) for request validation and execution.

## Notes for production use

Inference runs inside `PostgreSQL` backends. Before running in shared environments, make intent checks explicit:

- `runtime_discover()` and `runtime_ready()` for environment sanity.
- `permission_*` for role-aware controls.
- `http_allowed_hosts` and `http_allowed_providers` for outbound HTTP policy.
- `model_alias*` and profiles for deterministic runtime configuration.

## Validation

The repository includes two Docker end-to-end smoke suites:

- `./scripts/e2e_llama.sh` for the hosted OpenAI-compatible lane via `llama-server`
- `./scripts/e2e_candle.sh` for the local Candle lane

They are documented in [docs/operations.md](./docs/operations.md#end-to-end-checks).

Deeper topic docs:

- [Local Candle roadmap](./docs/candle-roadmap.md)
- [pgvector integration](./docs/pgvector-integration.md)
