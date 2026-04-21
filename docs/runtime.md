# Runtime Model Guide

`postllm` supports two runtime lanes with shared SQL entrypoints and different capabilities.

## Runtime matrix

- `openai` runtime is the hosted HTTP lane.
  - Pros: OpenAI-compatible generation, embeddings, reranking, structured outputs, tools, and streaming, plus a native Anthropic Messages adapter for generation, streaming, tool use, and URL-based image inputs.
  - Constraints: network policy applies (`http_allowed_hosts`, `http_allowed_providers`), request latency depends on upstream.
- `candle` runtime is local in-process Candle inference.
  - Pros: local embeddings, reranking, and starter generation models.
  - Constraints: multimodal/tooling/streaming are not yet supported for local generation.

## OpenAI-compatible runtime

Configuration uses:

- `postllm.base_url`
- `postllm.api_key` or `postllm.api_key_secret`
- `postllm.model`
- optional `postllm.max_retries`, `postllm.retry_backoff_ms`, and `postllm.timeout_ms`

Example:

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.openai.com/v1/chat/completions',
    model => 'gpt-4o-mini',
    api_key_secret => 'openai-prod'
);

SELECT postllm.chat_structured(
    ARRAY[
        postllm.system('Return JSON matching the provided schema.'),
        postllm.user('Return name, age, and country as JSON.')
    ],
    postllm.json_schema(
        'person',
        '{
            "type":"object",
            "properties":{"name":{"type":"string"},"age":{"type":"integer"},"country":{"type":"string"}},
            "required":["name","age","country"]
        }'::jsonb
    )
);
```

Hosted tool-calling and streaming are available on runtimes that support those features.

`postllm.base_url` may point at any of:

- a Chat Completions-style endpoint such as `https://api.openai.com/v1/chat/completions`
- a Responses-style endpoint such as `https://api.openai.com/v1/responses`
- an Anthropic Messages endpoint such as `https://api.anthropic.com/v1/messages`

The SQL API stays the same either way. `postllm` translates requests to the provider endpoint shape and normalizes the response back into the existing SQL-facing format.

Hosted embedding calls use the same runtime profile. `postllm.embed(...)` and `postllm.embed_many(...)` derive a sibling `/v1/embeddings` endpoint from `postllm.base_url`, so one hosted profile can serve generation and embeddings together.

Current Anthropic adapter scope:

- Supported: `chat*`, `complete*`, streaming text generation, tool calling, and URL-based image inputs for vision-capable Claude models.
- Not yet supported: embeddings, reranking, and structured outputs.

Example Anthropic configuration:

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.anthropic.com/v1/messages',
    model => 'claude-3-5-sonnet-latest',
    api_key_secret => 'anthropic-prod'
);

SELECT postllm.chat_text(ARRAY[
    postllm.system('You are concise.'),
    postllm.user('Explain MVCC in one sentence.')
]);
```

## Candle runtime

Use local models through the same SQL API:

- `postllm.chat(...)` and `postllm.complete(...)` for starter generation models.
- `postllm.embed(...)` and `postllm.rerank(...)` for local retrieval workflows.
- `postllm.model_install`, `model_prewarm`, `model_inspect`, `model_evict` for artifact management.

Configure Candle controls:

- `postllm.candle_device`: `auto`, `cpu`, `cuda`, `metal`.
- `postllm.candle_offline`: fail fast on cache misses.
- `postllm.candle_cache_dir`: local cache root.
- `postllm.candle_max_input_tokens` and `postllm.candle_max_concurrency`: caps.

Example local generation:

```sql
SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct');
SELECT postllm.chat_text(ARRAY[
    postllm.system('You are concise.'),
    postllm.user('Explain MVCC in one sentence.')
]);
```

Local model lifecycle operations are per-backend and cache-aware:

```sql
SELECT postllm.model_inspect();
SELECT postllm.model_install(lane => 'generation');
SELECT postllm.model_prewarm(lane => 'generation');
SELECT postllm.model_evict(lane => 'generation', scope => 'memory');
```

## Discovery and readiness

- `runtime_discover()` is intentionally non-throwing for probe workflows.
- `runtime_ready()` is the boolean form for shells/scripts and deployment checks.

Open a discovery sample:

```sql
SELECT postllm.runtime_discover();
SELECT postllm.runtime_ready();
```

On `openai`, discovery confirms endpoint reachability and checks configured model listing when possible.
On `candle`, discovery reports model inspection state, cache status, device status, and offline-readiness.

## Capability-aware usage

Call the shared helpers to inspect available behavior:

- `postllm.capabilities()`
- `postllm.settings()->>'capabilities'`

Prefer:

- `postllm.chat_text` for the happy path.
- `postllm.chat` when full `_postllm` metadata is needed.

`postllm.capabilities()` now also returns `model_features`, a best-effort model/profile view of:

- `vision`
- `json_mode`
- `reasoning`
- `tool_use`

These flags are inferred from the configured provider and model name. Treat them as a planning signal for profile selection and guardrails, not as a replacement for provider-side validation.
