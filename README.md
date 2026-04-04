# postllm

`postllm` is a `PostgreSQL` extension written with `pgrx` that makes LLM workflows feel native to SQL.

Use it as a single contract for:
- chat and completion flows,
- embeddings and retrieval pipelines,
- operational governance around profiles, secrets, and permissions,
- and local runtime vs hosted runtime execution.

## In one minute

```bash
cargo install --locked cargo-pgrx
cargo pgrx init --pg17 download
cargo pgrx run pg17
```

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

## Why the documentation is organized this way

`postllm` has a broad surface area, so we split docs by reader intent instead of forcing everyone through one long file.

- [Getting started](./docs/getting-started.md) for first successful call.
- [Configuration and governance](./docs/configuration.md) for settings, profiles, secrets, permissions, and allowlists.
- [Runtime behavior](./docs/runtime.md) for `openai` vs `candle`.
- [Function reference](./docs/reference.md) for the full function contract.
- [Examples](./docs/examples.md) for copy/paste SQL flows.
- [Operations](./docs/operations.md) for local dev, Docker, and test commands.
- [Documentation index](./docs/README.md) for role-based navigation.

## API families at a glance

- Session and capabilities: `settings`, `capabilities`, `configure`, `runtime_discover`, `runtime_ready`
- Profiles and governance: `profile*`, `secret*`, `permission*`, `model_alias*`
- Chat and completion: `chat*`, `complete*`, `chat_stream`, `complete_stream`, structured output, tools
- Retrieval and embeddings: `chunk_*`, `embed*`, `ingest_document`, `rerank`, `hybrid_rank`, `rag*`
- Message and helper builders: message templates, content parts, `usage`, `finish_reason`, `extract_text`

If you need the complete list of functions and signatures, use [docs/reference.md](./docs/reference.md).

## Runtime model

`postllm` supports two runtime lanes:
- `openai`: OpenAI-compatible HTTP endpoints (OpenAI, Ollama, llama-server style).
- `candle`: in-process local runtime for embeddings, reranking, and starter text generation.

The same SQL patterns apply to both, while capabilities determine what each lane accepts.

## Notes

Requests run inside PostgreSQL backends. Keep this production tradeoff explicit and gate inference paths with:
- `runtime_discover()` and `runtime_ready()` for environment checks,
- `permission_*` for role-aware control,
- `http_allowed_hosts` and `http_allowed_providers` for outbound policy.

For deeper guidance:
- [Local Candle roadmap](./docs/candle-roadmap.md)
- [pgvector integration](./docs/pgvector-integration.md)
