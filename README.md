# postllm

`postllm` is a `PostgreSQL` extension written with `pgrx` that makes LLM calls feel like a `PostgreSQL` subsystem instead of an external bolt-on.

The extension is built around a native schema-scoped API:

- `postllm.settings() -> jsonb`
- `postllm.capabilities() -> jsonb`
- `postllm.configure(...) -> jsonb`
- `postllm.profiles() -> jsonb`
- `postllm.profile(name text) -> jsonb`
- `postllm.profile_set(name text, ...) -> jsonb`
- `postllm.profile_apply(name text) -> jsonb`
- `postllm.profile_delete(name text) -> jsonb`
- `postllm.model_aliases() -> jsonb`
- `postllm.model_alias(alias text, lane text) -> jsonb`
- `postllm.model_alias_set(alias text, lane text, model text, description text default null) -> jsonb`
- `postllm.model_alias_delete(alias text, lane text) -> jsonb`
- `postllm.message(role text, content text) -> jsonb`
- `postllm.system(content text) -> jsonb`
- `postllm.user(content text) -> jsonb`
- `postllm.assistant(content text) -> jsonb`
- `postllm.render_template(template text, variables jsonb default null) -> text`
- `postllm.message_template(role text, template text, variables jsonb default null) -> jsonb`
- `postllm.system_template(template text, variables jsonb default null) -> jsonb`
- `postllm.user_template(template text, variables jsonb default null) -> jsonb`
- `postllm.assistant_template(template text, variables jsonb default null) -> jsonb`
- `postllm.text_part(text text) -> jsonb`
- `postllm.image_url_part(url text, detail text default null) -> jsonb`
- `postllm.message_parts(role text, parts jsonb[]) -> jsonb`
- `postllm.system_parts(parts jsonb[]) -> jsonb`
- `postllm.user_parts(parts jsonb[]) -> jsonb`
- `postllm.assistant_parts(parts jsonb[]) -> jsonb`
- `postllm.function_tool(name text, parameters jsonb, description text default null) -> jsonb`
- `postllm.tool_choice_auto() -> jsonb`
- `postllm.tool_choice_none() -> jsonb`
- `postllm.tool_choice_required() -> jsonb`
- `postllm.tool_choice_function(name text) -> jsonb`
- `postllm.tool_call(id text, name text, arguments jsonb) -> jsonb`
- `postllm.assistant_tool_calls(tool_calls jsonb[], content text default null) -> jsonb`
- `postllm.tool_result(tool_call_id text, content text) -> jsonb`
- `postllm.json_schema(name text, schema jsonb, strict bool default true) -> jsonb`
- `postllm.messages_agg(message jsonb) -> jsonb[]`
- `postllm.chat(messages jsonb[], ...) -> jsonb`
- `postllm.chat_text(messages jsonb[], ...) -> text`
- `postllm.chat_stream(messages jsonb[], ...) -> table(index int, delta text, event jsonb)`
- `postllm.chat_structured(messages jsonb[], response_format jsonb, ...) -> jsonb`
- `postllm.chat_tools(messages jsonb[], tools jsonb[], tool_choice jsonb default null, ...) -> jsonb`
- `postllm.usage(response jsonb) -> jsonb`
- `postllm.choice(response jsonb, index int) -> jsonb`
- `postllm.finish_reason(response jsonb) -> text`
- `postllm.extract_text(response jsonb) -> text`
- `postllm.chunk_text(input text, chunk_chars int default 1000, overlap_chars int default 200) -> text[]`
- `postllm.chunk_document(input text, metadata jsonb default null, chunk_chars int default 1000, overlap_chars int default 200) -> table(index int, chunk text, metadata jsonb)`
- `postllm.embed_document(doc_id text, input text, metadata jsonb default null, chunk_chars int default 1000, overlap_chars int default 200, model text default null, normalize bool default true) -> table(chunk_id text, doc_id text, chunk_no int, content text, metadata jsonb, embedding real[])`
- `postllm.embed(input text, ...) -> real[]`
- `postllm.embed_many(inputs text[], ...) -> jsonb`
- `postllm.embedding_model_info(model text default null) -> jsonb`
- `postllm.model_install(model text default null, lane text default null) -> jsonb`
- `postllm.model_prewarm(model text default null, lane text default null) -> jsonb`
- `postllm.model_inspect(model text default null, lane text default null) -> jsonb`
- `postllm.model_evict(model text default null, lane text default null, scope text default 'all') -> jsonb`
- `postllm.ingest_document(target_table text, doc_id text, input text, metadata jsonb default null, chunk_chars int default 1000, overlap_chars int default 200, model text default null, normalize bool default true, delete_missing bool default true) -> jsonb`
- `postllm.rerank(query text, documents text[], top_n int default null, model text default null) -> table(rank int, index int, document text, score double precision)`
- `postllm.keyword_rank(query text, documents text[], top_n int default null, text_search_config text default null, normalization int default 32) -> table(rank int, index int, document text, score double precision)`
- `postllm.rrf_score(semantic_rank int default null, keyword_rank int default null, semantic_weight double precision default 1.0, keyword_weight double precision default 1.0, rrf_k int default 60) -> double precision`
- `postllm.hybrid_rank(query text, documents text[], top_n int default null, model text default null, text_search_config text default null, semantic_weight double precision default 1.0, keyword_weight double precision default 1.0, rrf_k int default 60, normalization int default 32) -> table(rank int, index int, document text, score double precision, semantic_rank int, keyword_rank int, semantic_score double precision, keyword_score double precision)`
- `postllm.rag(query text, documents text[], system_prompt text default null, model text default null, retrieval text default null, retrieval_model text default null, top_n int default 5, temperature double precision default 0.2, max_tokens int default null, text_search_config text default null, semantic_weight double precision default 1.0, keyword_weight double precision default 1.0, rrf_k int default 60, normalization int default 32) -> jsonb`
- `postllm.rag_text(query text, documents text[], system_prompt text default null, model text default null, retrieval text default null, retrieval_model text default null, top_n int default 5, temperature double precision default 0.2, max_tokens int default null, text_search_config text default null, semantic_weight double precision default 1.0, keyword_weight double precision default 1.0, rrf_k int default 60, normalization int default 32) -> text`
- `postllm.complete(prompt text, ...) -> text`
- `postllm.complete_stream(prompt text, ...) -> table(index int, delta text, event jsonb)`
- `postllm.complete_structured(prompt text, response_format jsonb, ...) -> jsonb`
- `postllm.complete_tools(prompt text, tools jsonb[], system_prompt text default null, tool_choice jsonb default null, ...) -> jsonb`
- `postllm.complete_many(prompts text[], ...) -> text[]`
- `postllm.complete_many_rows(prompts text[], ...) -> table(index int, prompt text, completion text)`

It defaults to an Ollama-style local endpoint, but the transport is OpenAI-compatible, so the same SQL can target local or hosted providers.

The runtime setting currently supports:

- `openai`: OpenAI-compatible HTTP endpoints such as `OpenAI`, `Ollama`, and `llama-server`
- `candle`: the local Candle runtime lane; starter-model `postllm.chat(...)` and `postllm.complete(...)` are now available locally

Local Candle embeddings are implemented today through `postllm.embed(...)` and `postllm.embed_many(...)`, regardless of `postllm.runtime`. That embedding lane now supports SentenceTransformer-style local pipelines across `bert`, `distilbert`, and `xlm-roberta` encoder families, including `cls`, `max`, `mean`, and `mean_sqrt_len` pooling plus optional Dense projection heads when the model repo declares them. `postllm.rerank(...)` is also live today: on the Candle runtime it uses the active local embedding model, while on the `openai` runtime it expects `postllm.base_url` to point at a hosted rerank endpoint. `postllm.keyword_rank(...)`, `postllm.rrf_score(...)`, `postllm.hybrid_rank(...)`, `postllm.rag(...)`, and `postllm.rag_text(...)` are also live today for lexical, hybrid, and retrieval-plus-generation workflows inside SQL. Local Candle chat and complete generation are now available for the starter Qwen models listed below.

## Prerequisites

Install the `cargo-pgrx` toolchain and initialize a `PostgreSQL` version:

```bash
cargo install --locked cargo-pgrx
cargo pgrx init --pg17 download
```

Build with `--features candle-cuda` or `--features candle-metal` when you want optional local GPU execution for the Candle runtime. Without those crate features, `postllm` falls back to CPU execution only.

On macOS with Homebrew ICU, `PostgreSQL` 17 may need:

```bash
export PKG_CONFIG_PATH="$(brew --prefix icu4c@78)/lib/pkgconfig"
```

## Native Development

```bash
cargo pgrx run pg17
```

Then load the extension in `psql`:

```sql
CREATE EXTENSION postllm;
SELECT postllm.settings();
```

## Configuration

The extension exposes `PostgreSQL` settings that work naturally with `SET`, `ALTER SYSTEM`, and connection-level defaults:

- `postllm.runtime`
- `postllm.base_url`
- `postllm.model`
- `postllm.embedding_model`
- `postllm.api_key`
- `postllm.timeout_ms`
- `postllm.max_retries`
- `postllm.retry_backoff_ms`
- `postllm.candle_cache_dir`
- `postllm.candle_offline`
- `postllm.candle_device`
- `postllm.candle_max_input_tokens`
- `postllm.candle_max_concurrency`

You can also configure the current session from SQL:

```sql
SELECT postllm.configure(
    base_url => 'http://127.0.0.1:11434/v1/chat/completions',
    model => 'llama3.2',
    embedding_model => 'sentence-transformers/paraphrase-MiniLM-L3-v2',
    timeout_ms => 10000,
    max_retries => 2,
    retry_backoff_ms => 250,
    runtime => 'openai',
    candle_offline => false,
    candle_device => 'auto',
    candle_max_input_tokens => 0,
    candle_max_concurrency => 0
);
```

For reusable non-secret environments, `postllm.profile_set(...)` stores named configuration profiles in the database and `postllm.profile_apply(...)` reapplies them to the current session. Profile application resets managed non-secret settings back to extension defaults before applying the stored profile, so switching from a local Candle setup to a hosted setup does not leak stale runtime settings. Profiles intentionally do not store `postllm.api_key`.

For reusable model shorthands, `postllm.model_alias_set(...)` stores lane-aware generation and embedding aliases. Once defined, aliases resolve automatically through `postllm.capabilities()`, `postllm.chat(...)`, `postllm.complete(...)`, `postllm.embed(...)`, `postllm.embedding_model_info(...)`, rerank helpers, and local model lifecycle commands.

```sql
SELECT postllm.profile_set(
    name => 'hosted-staging',
    runtime => 'openai',
    base_url => 'http://127.0.0.1:4000/v1/chat/completions',
    model => 'staging-chat'
);

SELECT postllm.model_alias_set(
    alias => 'starter',
    lane => 'generation',
    model => 'Qwen/Qwen2.5-0.5B-Instruct'
);

SELECT postllm.profile_apply('hosted-staging');
SELECT postllm.configure(runtime => 'candle', model => 'starter');
```

Inspect the active runtime capability snapshot from SQL:

```sql
SELECT postllm.capabilities();
```

`postllm.settings()` now includes the same capability metadata under the `capabilities` key so runtime support is visible alongside the current configuration.

Generation responses now preserve the provider payload and add a normalized `_postllm` metadata block with `runtime`, `provider`, `base_url`, `model`, `finish_reason`, and `usage`.

For common SQL chat usage, prefer `postllm.chat_text(...)`; keep `postllm.chat(...)` when you need the full response JSON or `_postllm` metadata. Use `postllm.usage(...)`, `postllm.choice(...)`, and `postllm.finish_reason(...)` to inspect responses without manual JSON indexing.

The SQL API now also exposes first-class content-part and message-part helpers for multimodal OpenAI-compatible requests. Candle generation remains text-only today, so image-bearing messages are rejected up front on the local runtime.

For row-backed conversation history, use `postllm.messages_agg(...)` so you can build `jsonb[]` chats with `ORDER BY` directly from SQL rowsets instead of hand-writing `ARRAY[...]` expressions.

Prompt templates use `{{name}}` placeholders resolved from top-level keys in a JSON object. Whitespace inside the braces is ignored, string values are inserted raw, and non-string values render as compact JSON.

For prompt-first batch generation, use `postllm.complete_many(...)` when you want an array result and `postllm.complete_many_rows(...)` when you want one SQL row per completion.

`postllm.rerank(...)` returns ordered rows with a 1-based `rank`, a 1-based original `index`, the `document` text, and the provider or local relevance `score`. On `runtime = 'candle'`, reranking embeds the query and candidate texts locally with the active embedding model. On `runtime = 'openai'`, reranking forwards `model`, `query`, `documents`, and optional `top_n` to `postllm.base_url`, so point `base_url` at a rerank-compatible endpoint such as `/v1/rerank`.

For lexical retrieval over candidate arrays, `postllm.keyword_rank(...)` uses `PostgreSQL` full-text search and scores documents by query-term overlap. For hybrid retrieval, `postllm.hybrid_rank(...)` fuses semantic and keyword ranks with reciprocal rank fusion, while `postllm.rrf_score(...)` is available when you want to fuse those ranks yourself in custom SQL.

`postllm.rag(...)` and `postllm.rag_text(...)` are the batteries-included retrieval-plus-generation helpers. They retrieve context from a document array, build a grounded prompt, and run generation in one SQL call. `retrieval => 'hybrid'` is the default when you omit it. `postllm.rag(...)` returns the final answer plus the selected context rows, rendered prompt, and raw response metadata; `postllm.rag_text(...)` returns only the answer text.

User-facing errors now aim to be corrective. Argument-validation failures name the bad argument, runtime/model capability failures name the active runtime and model, and both include a likely fix where `postllm` can infer one.

Hosted HTTP requests now retry transient failures by default on the `openai` runtime. The retry knobs are `postllm.max_retries` and `postllm.retry_backoff_ms`; the default policy retries transport/read failures plus upstream `408`, `409`, `425`, `429`, `500`, `502`, `503`, and `504`, with exponential backoff starting at `250ms`.

`postllm.timeout_ms` now bounds both hosted HTTP requests and local Candle inference work. `PostgreSQL` query cancellation is also checked during hosted HTTP waits and local Candle inference loops, so cancelled statements can break out of in-flight requests and local generation work instead of waiting for the full timeout or token budget.

For local Candle device selection, `postllm.candle_device` accepts `auto`, `cpu`, `cuda`, or `metal`. `auto` prefers an available accelerated device for the current build and falls back to CPU otherwise. Explicit `cuda` or `metal` selection fails fast if that accelerator is unavailable or the extension was not built with the matching `candle-cuda` or `candle-metal` feature.

For local Candle safety controls, `postllm.candle_max_input_tokens` caps tokenized local inputs when set above `0`, and `postllm.candle_max_concurrency` caps the number of concurrent local Candle requests across backends when set above `0`. Leave either at `0` to disable that specific cap.

Structured outputs are now available on the `openai` runtime through `postllm.json_schema(...)`, `postllm.chat_structured(...)`, and `postllm.complete_structured(...)`. Those functions send an OpenAI-style `response_format` contract and return parsed `jsonb`. The local Candle runtime still rejects structured-output requests for now.

Tool-calling requests are now available on the `openai` runtime through `postllm.chat_tools(...)` and `postllm.complete_tools(...)`. Use `postllm.function_tool(...)` and the `tool_choice_*()` helpers to build the request, then feed returned tool calls back through `postllm.assistant_tool_calls(...)` and `postllm.tool_result(...)`. The local Candle runtime still rejects tool-calling requests for now.

Streaming is now available on the `openai` runtime through `postllm.chat_stream(...)` and `postllm.complete_stream(...)`. Those functions return one SQL row per SSE chunk with a normalized `delta` column plus the raw provider event JSON. The local Candle runtime still rejects streaming requests for now.

For the Candle generation lane, the current starter model registry recognizes:

- `Qwen/Qwen2.5-0.5B-Instruct`
- `Qwen/Qwen2.5-1.5B-Instruct`

Those models are now treated as the explicit local-generation starter set and back both `postllm.chat(...)` and `postllm.complete(...)` through the in-process Candle runtime.

### Local Ollama Example

```sql
SELECT postllm.complete(
    prompt => 'Explain MVCC in one sentence.',
    system_prompt => 'You are concise.'
);
```

### Structured Chat Example

```sql
SELECT postllm.chat_text(ARRAY[
    postllm.system('You are a PostgreSQL expert.'),
    postllm.user('Explain VACUUM and autovacuum.')
]);
```

### Prompt Template Example

```sql
SELECT postllm.complete(
    prompt => postllm.render_template(
        'Explain {{topic}} in {{word_count}} words.',
        '{"topic":"MVCC","word_count":20}'::jsonb
    ),
    system_prompt => postllm.render_template(
        'You are writing for a {{audience}} audience.',
        '{"audience":"beginner"}'::jsonb
    )
);

SELECT postllm.user_template(
    'Summarize {{topic}} for a {{audience}} audience.',
    '{"topic":"VACUUM","audience":"beginner"}'::jsonb
);
```

### Structured Output Example

```sql
SELECT postllm.chat_structured(
    ARRAY[
        postllm.system('Extract a person record.'),
        postllm.user('Ada Lovelace was 36 and lived in London.')
    ],
    postllm.json_schema(
        'person',
        '{
            "type":"object",
            "properties":{
                "name":{"type":"string"},
                "age":{"type":"integer"},
                "city":{"type":"string"}
            },
            "required":["name","age","city"],
            "additionalProperties":false
        }'::jsonb
    ),
    temperature => 0.0
);
```

### Tool Calling Example

```sql
WITH first_pass AS (
    SELECT postllm.chat_tools(
        ARRAY[
            postllm.system('Use tools when needed.'),
            postllm.user('What is the weather in Austin?')
        ],
        ARRAY[
            postllm.function_tool(
                'lookup_weather',
                '{
                    "type":"object",
                    "properties":{"city":{"type":"string"}},
                    "required":["city"],
                    "additionalProperties":false
                }'::jsonb,
                description => 'Look up the current weather by city.'
            )
        ],
        tool_choice => postllm.tool_choice_auto(),
        model => 'gpt-4o-mini'
    ) AS response
),
tool_request AS (
    SELECT
        response,
        response->'choices'->0->'message'->'tool_calls'->0 AS tool_call
    FROM first_pass
)
SELECT postllm.chat_text(ARRAY[
    postllm.system('Use tools when needed.'),
    postllm.user('What is the weather in Austin?'),
    postllm.assistant_tool_calls(ARRAY[tool_call]),
    postllm.tool_result(tool_call->>'id', '{"temperature_f":72,"condition":"sunny"}')
])
FROM tool_request;
```

### Streaming Example

```sql
SELECT string_agg(coalesce(delta, ''), '' ORDER BY index) AS answer
FROM postllm.complete_stream(
    prompt => 'Count from 1 to 3.',
    system_prompt => 'Reply with digits and commas only.',
    model => 'gpt-4o-mini'
);

SELECT index, delta, event
FROM postllm.chat_stream(ARRAY[
    postllm.system('Be brief.'),
    postllm.user('Say hello.')
], model => 'gpt-4o-mini');
```

### Batch Generation Example

```sql
SELECT postllm.complete_many(
    ARRAY[
        'Explain MVCC in one sentence.',
        'Explain VACUUM in one sentence.'
    ],
    system_prompt => 'You are concise.'
);

SELECT *
FROM postllm.complete_many_rows(
    ARRAY[
        'Explain MVCC in one sentence.',
        'Explain VACUUM in one sentence.'
    ],
    system_prompt => 'You are concise.'
);
```

### Conversation Rowset Example

```sql
SELECT postllm.chat_text(
    postllm.messages_agg(
        postllm.message(role, content)
        ORDER BY created_at
    )
)
FROM chat_history
WHERE conversation_id = 42;
```

### Multimodal Chat Example

```sql
SELECT postllm.chat_text(ARRAY[
    postllm.user_parts(ARRAY[
        postllm.text_part('Describe this image in one sentence.'),
        postllm.image_url_part('https://example.com/cat.png', detail => 'low')
    ])
], model => 'gpt-4o-mini');
```

### Response Inspection Example

```sql
WITH response AS (
    SELECT postllm.chat(ARRAY[
        postllm.system('You are concise.'),
        postllm.user('Explain MVCC in one sentence.')
    ]) AS value
)
SELECT
    postllm.extract_text(value) AS answer,
    postllm.finish_reason(value) AS finish_reason,
    postllm.usage(value) AS usage,
    postllm.choice(value, 0) AS first_choice
FROM response;
```

### Tool Helper Example

Tool-definition builders and tool-call history helpers are available today. Full request execution flow still lands in the next milestone.

```sql
SELECT postllm.function_tool(
    'lookup_weather',
    '{
        "type":"object",
        "properties":{"city":{"type":"string"}},
        "required":["city"],
        "additionalProperties":false
    }'::jsonb,
    description => 'Look up the current weather.'
);

SELECT postllm.tool_choice_auto();

SELECT postllm.tool_choice_function('lookup_weather');

SELECT postllm.assistant_tool_calls(ARRAY[
    postllm.tool_call(
        'call_123',
        'lookup_weather',
        '{"city":"Austin"}'::jsonb
    )
]);

SELECT postllm.tool_result('call_123', '{"temperature":72}');
```

### Local Candle Chat Example

```sql
SELECT postllm.configure(
    runtime => 'candle',
    model => 'Qwen/Qwen2.5-0.5B-Instruct'
);

SELECT postllm.chat_text(ARRAY[
    postllm.system('You are concise.'),
    postllm.user('Explain MVCC in one sentence.')
], max_tokens => 96);
```

### Local Candle Complete Example

```sql
SELECT postllm.configure(
    runtime => 'candle',
    model => 'Qwen/Qwen2.5-0.5B-Instruct'
);

SELECT postllm.complete(
    prompt => 'Explain MVCC in one sentence.',
    system_prompt => 'You are concise.',
    max_tokens => 96
);
```

### Local Candle Embeddings Example

```sql
SELECT postllm.embed('Explain VACUUM in one sentence.');

SELECT postllm.embed_many(ARRAY[
    'What is MVCC?',
    'What does autovacuum do?'
]);

SELECT postllm.embedding_model_info();

SELECT postllm.embedding_model_info('BAAI/bge-small-en-v1.5');
```

By default, embeddings use `sentence-transformers/paraphrase-MiniLM-L3-v2`. Override it with `postllm.embedding_model` or the `model => ...` argument on `postllm.embed(...)`.

Use `postllm.embedding_model_info(...)` to inspect the active or requested embedding model. It exposes the final output dimension after any `SentenceTransformer` pooling or `Dense` projection, the max tokenized sequence length, pooling behavior, optional projection metadata, and normalization defaults that the local Candle embedding lane applies.

For local Candle model operations, use `postllm.model_install(...)`, `postllm.model_prewarm(...)`, `postllm.model_inspect(...)`, and `postllm.model_evict(...)`. When you omit `lane`, `postllm` picks `generation` only if the current or requested model is one of the supported local generation starters; otherwise it falls back to the active embedding model. `model_prewarm(...)` and `model_evict(..., scope => 'memory')` operate on the current `PostgreSQL` backend process only, while disk install/evict targets the shared artifact cache under `postllm.candle_cache_dir` or the default Hugging Face cache.

Set `postllm.candle_offline = on` or call `postllm.configure(candle_offline => true)` when you want Candle to use only already-cached local artifacts. In offline mode, missing files fail fast with a cache-miss error instead of falling back to Hugging Face downloads.

`postllm.model_inspect(...)` now includes an `integrity` summary plus per-file integrity metadata. It also reports the requested and resolved Candle device for the current build and host, so you can tell whether `auto` chose CPU, CUDA, or Metal. When a cached artifact is backed by a checksum-named Hugging Face blob, Candle verifies the cached bytes against that checksum and reports `verified`, `partial`, `unchecked`, or `mismatch` status. `postllm.model_install(...)` also refuses to leave a mismatched repo cache behind: if integrity validation fails after install, the local repo cache is evicted and the install call errors.

Known local metadata fast-paths currently include:

- `sentence-transformers/paraphrase-MiniLM-L3-v2`
- `sentence-transformers/all-MiniLM-L6-v2`
- `intfloat/e5-small-v2`
- `BAAI/bge-small-en-v1.5`
- `sentence-transformers/distiluse-base-multilingual-cased-v2`

### Local Model Lifecycle Example

```sql
SELECT postllm.model_inspect();

SELECT postllm.model_install(lane => 'generation');

SELECT postllm.model_prewarm(lane => 'generation');

SELECT postllm.configure(candle_device => 'auto', candle_offline => true);

SELECT postllm.model_evict(lane => 'generation', scope => 'memory');
```

Use `lane => 'embedding'` to target the local embedding model explicitly, `lane => 'generation'` to target the local starter-generation model explicitly, or omit `lane` to let `postllm` choose. Turn on `candle_offline` after `model_install(...)` when you want cached-only local execution. Set `candle_device => 'cpu'` when you need deterministic CPU-only behavior even on a GPU-capable build.

### Chunking Example

For retrieval workflows, `postllm` now exposes text chunking helpers with character-count defaults and overlap:

```sql
SELECT postllm.chunk_text(
    'Autovacuum removes dead tuples. VACUUM can reclaim space. MVCC depends on tuple visibility.',
    chunk_chars => 48,
    overlap_chars => 12
);

SELECT *
FROM postllm.chunk_document(
    'Autovacuum removes dead tuples. VACUUM can reclaim space. MVCC depends on tuple visibility.',
    '{"doc_id":"vacuum-guide","source":"manual"}'::jsonb,
    chunk_chars => 48,
    overlap_chars => 12
);
```

`chunk_chars` is a soft target near clean boundaries: `postllm` prefers sentence, line, and whitespace splits, and it will let the final chunk run slightly long instead of creating a tiny trailing fragment.

`postllm.chunk_document(...)` propagates your metadata and adds per-chunk provenance under `_postllm_chunk`, including the 1-based chunk index and source character offsets.

### Document Embedding Example

For canonical retrieval rows, `postllm.embed_document(...)` gives you deterministic `chunk_id` values plus embeddings in one step:

```sql
SELECT *
FROM postllm.embed_document(
    'vacuum-guide',
    'Autovacuum removes dead tuples. VACUUM can reclaim space. MVCC depends on tuple visibility.',
    '{"source":"manual"}'::jsonb,
    chunk_chars => 48,
    overlap_chars => 12
);
```

If your table follows the canonical ingestion contract, `postllm.ingest_document(...)` can upsert and prune stale chunks directly:

```sql
CREATE TABLE doc_chunks_ingest (
    chunk_id text PRIMARY KEY,
    doc_id text NOT NULL,
    chunk_no integer NOT NULL,
    content text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    embedding real[] NOT NULL
);

SELECT postllm.ingest_document(
    'public.doc_chunks_ingest',
    'vacuum-guide',
    'Autovacuum removes dead tuples. VACUUM can reclaim space. MVCC depends on tuple visibility.',
    '{"source":"manual"}'::jsonb
);
```

`postllm.embed_document(...)` is the better fit when you want to cast into `vector(n)` yourself or write a custom `INSERT ... ON CONFLICT` statement. `postllm.ingest_document(...)` is the batteries-included path for tables that store embeddings as `real[]`.

### Reranking Example

```sql
SELECT *
FROM postllm.rerank(
    'How does PostgreSQL control table bloat?',
    ARRAY[
        'Bananas are yellow and grow in bunches.',
        'Autovacuum removes dead tuples and helps control table bloat.'
    ],
    top_n => 1
);
```

That same SQL works locally on the Candle runtime or against a hosted rerank endpoint on the `openai` runtime.

### Hybrid Retrieval Example

```sql
SELECT *
FROM postllm.hybrid_rank(
    'How does PostgreSQL control table bloat?',
    ARRAY[
        'Bananas are yellow and grow in bunches.',
        'Autovacuum removes dead tuples and helps control table bloat.',
        'VACUUM can be run manually to reclaim space.'
    ],
    top_n => 2
);
```

`postllm.hybrid_rank(...)` returns the fused `score` plus `semantic_rank`, `keyword_rank`, `semantic_score`, and `keyword_score` so you can inspect why a row won. If you already have separate vector and lexical rowsets, `postllm.rrf_score(...)` gives you the same reciprocal-rank-fusion primitive for custom joins.

### Batteries-Included RAG Example

```sql
SELECT postllm.rag_text(
    query => 'How does PostgreSQL control table bloat?',
    documents => ARRAY[
        'Bananas are yellow and grow in bunches.',
        'Autovacuum removes dead tuples and helps control table bloat.',
        'VACUUM can be run manually to reclaim space.'
    ],
    retrieval => 'hybrid',
    top_n => 2,
    system_prompt => 'Answer from the retrieved PostgreSQL context and say when it is insufficient.'
);

SELECT postllm.rag(
    query => 'How does PostgreSQL control table bloat?',
    documents => ARRAY[
        'Bananas are yellow and grow in bunches.',
        'Autovacuum removes dead tuples and helps control table bloat.',
        'VACUUM can be run manually to reclaim space.'
    ],
    top_n => 2
);
```

Use `retrieval_model => ...` when you want a different rerank model than the generation model. `postllm.rag(...)` is the better fit when you want to inspect which rows were selected, the generated prompt, or the normalized `_postllm` response metadata.

### pgvector Workflow

`postllm` fits directly into `pgvector`: `postllm.embed(...)` returns `real[]`, and `pgvector` supports casting `real[]` to `vector(n)`.

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE doc_chunks (
    id bigserial PRIMARY KEY,
    content text NOT NULL,
    embedding vector(384) NOT NULL
);

INSERT INTO doc_chunks (content, embedding)
VALUES (
    'Autovacuum removes dead tuples and helps control table bloat.',
    postllm.embed(
        'Autovacuum removes dead tuples and helps control table bloat.'
    )::vector(384)
);

CREATE INDEX doc_chunks_embedding_ip_hnsw
ON doc_chunks
USING hnsw (embedding vector_ip_ops);
```

For the default normalized embedding path, `vector_ip_ops` is a practical default. See [docs/pgvector-integration.md](docs/pgvector-integration.md) for the full ingest, filtered retrieval, reranking, hybrid retrieval, one-call RAG, and answer-generation workflow.

### OpenAI-Compatible Example

```sql
SET postllm.base_url = 'https://api.openai.com/v1/chat/completions';
SET postllm.api_key = 'sk-...';
SET postllm.model = 'gpt-4o-mini';

SELECT postllm.complete(
    prompt => 'Write a haiku about PostgreSQL extensions.',
    system_prompt => 'You are concise.',
    temperature => 0.4,
    max_tokens => 120
);
```

## Docker Runtime

The repo includes a local `PostgreSQL` image that installs the extension into `postgres:17` and creates it automatically on first boot:

```bash
docker compose up --build
psql postgresql://postgres:postgres@127.0.0.1:5440/postllm
```

The container will point `postllm.base_url` at `http://host.docker.internal:11434/v1/chat/completions` by default, which works well with a host-running Ollama instance.

For local embeddings, the same image also accepts:

- `POSTLLM_EMBEDDING_MODEL`
- `POSTLLM_CANDLE_CACHE_DIR`

## Dockerized llama-server E2E

The repo also includes a dedicated end-to-end smoke test that runs a tiny `llama-server` model in Docker and drives `postllm` against it through the full HTTP path:

```bash
./scripts/e2e_llama.sh
```

That flow uses:

- `ghcr.io/ggml-org/llama.cpp:server`
- `Qwen/Qwen2.5-0.5B-Instruct-GGUF:Q2_K`
- the compose override in `compose.llama-e2e.yaml`

The script waits for the model to load, checks `postllm.settings()`, and then runs a small `postllm.complete(...)` smoke query against the live server.

## Dockerized Candle E2E

The repo also includes a Candle smoke test that runs the extension inside Docker, downloads local model assets, and exercises in-process embeddings plus starter-model generation from SQL:

```bash
./scripts/e2e_candle.sh
```

That flow uses:

- `Qwen/Qwen2.5-0.5B-Instruct`
- `sentence-transformers/paraphrase-MiniLM-L3-v2`
- the compose override in `compose.candle-e2e.yaml`

The script verifies that `postllm.embed(...)` returns a non-empty normalized vector, `postllm.embed_many(...)` returns two batch results, `postllm.chat(...)` returns Candle `_postllm` metadata, and `postllm.complete(...)` produces a non-empty local answer.

## Quality Gates

Run formatting:

```bash
cargo fmt
```

Run the Rust test suite:

```bash
cargo test
```

Run the strict lint suite:

```bash
env PGRX_HOME=/tmp/postllm-pgrx-home cargo clippy --all-targets --no-default-features --features pg17,pg_test -- -D warnings
```

Run the extension test suite inside `PostgreSQL`:

```bash
env PGRX_HOME=/tmp/postllm-pgrx-home CARGO_TARGET_DIR=/tmp/postllm-target cargo pgrx test pg17
```

Opt into the live Candle generation `pg_test` coverage:

```bash
env PGRX_HOME=/tmp/postllm-pgrx-home \
  POSTLLM_PG_TEST_CANDLE_E2E=1 \
  POSTLLM_PG_TEST_CANDLE_MODEL=Qwen/Qwen2.5-0.5B-Instruct \
  cargo pgrx test pg17
```

Run the Dockerized llama-server smoke test:

```bash
./scripts/e2e_llama.sh
```

Run the Dockerized Candle smoke test:

```bash
./scripts/e2e_candle.sh
```

## Candle

`postllm` now uses Candle for local sentence embeddings plus starter-model local chat and complete generation. The remaining production gaps are broader model support, GPU support where it is stable enough, and deeper production-governance features.

The detailed roadmap is in `docs/candle-roadmap.md`.

## Notes

Requests are synchronous and run inside the `PostgreSQL` backend process. This extension is intended to feel native, but it still executes network I/O, so keep it out of latency-sensitive query paths unless that tradeoff is deliberate.
