# Candle Roadmap

`postllm` now has two runtime lanes in its settings model:

- `openai`: the existing OpenAI-compatible HTTP path used for Ollama, llama-server, and hosted providers
- `candle`: the in-process local runtime lane. Candle embeddings ship today, and starter-model local chat and `complete` generation are now live.

The first shipped Candle capabilities are implemented today:

- `postllm.embed(input text, ...) -> real[]`
- `postllm.embed_many(inputs text[], ...) -> jsonb`
- `postllm.rerank(query text, documents text[], ...) -> table(...)` through local embedding similarity
- `postllm.keyword_rank(query text, documents text[], ...) -> table(...)` through `PostgreSQL` full-text overlap scoring
- `postllm.rrf_score(...) -> double precision` for custom reciprocal-rank fusion
- `postllm.hybrid_rank(query text, documents text[], ...) -> table(...)` by fusing local or hosted semantic rerank with keyword ranking
- `postllm.rag(query text, documents text[], ...) -> jsonb` and `postllm.rag_text(query text, documents text[], ...) -> text` for one-call retrieval-plus-generation over candidate arrays
- `postllm.chat(messages jsonb[], ...) -> jsonb` for the starter Qwen instruct models
- `postllm.complete(prompt text, ...) -> text` for the same starter Qwen instruct models

Those functions load local Hugging Face model assets in-process, cache them per backend process, and run inference without leaving PostgreSQL.

The local generation foundation is now in place. The next step is making the SQL surface more ergonomic on top of the shared runtime and response model.

Hosted tool-calling request flow is now available on the `openai` runtime through `postllm.chat_tools(...)` and `postllm.complete_tools(...)`. Candle still rejects tool-calling requests until a local tool-execution story exists.

Hosted streaming is now available on the `openai` runtime through `postllm.chat_stream(...)` and `postllm.complete_stream(...)`. Candle still rejects streaming requests until a local streaming token loop exists.

Hosted HTTP retries are now configurable on the `openai` runtime through `postllm.max_retries` and `postllm.retry_backoff_ms`, with transient-failure classification for transport/read failures and retryable upstream statuses.

`postllm.timeout_ms` now bounds both hosted HTTP requests and local Candle inference. `PostgreSQL` query cancellation is also polled during hosted HTTP waits and local Candle inference loops so cancelled statements can stop generation work earlier.

Local Candle runtime controls now include `postllm.candle_max_input_tokens` for tokenized input-size caps and `postllm.candle_max_concurrency` for cross-backend local inference throttling.

Optional local GPU selection is now available through `postllm.candle_device`, with `auto`, `cpu`, `cuda`, and `metal` modes. CUDA and Metal require building `postllm` with the matching `candle-cuda` or `candle-metal` crate feature, while `auto` falls back to CPU when no accelerator is available.

Named configuration profiles, encrypted provider-secret references, and lane-aware model aliases are now available through `postllm.profile_set(...)`, `postllm.profile_apply(...)`, `postllm.secret_set(...)`, and `postllm.model_alias_set(...)`, so switching between local and hosted setups no longer requires hand-editing a full bundle of session settings each time.

Role-aware governance controls are now available through `postllm.permission_set(...)`, `postllm.permissions()`, and the shared runtime/model resolution path, so operators can allowlist runtimes, generation models, embedding models, and privileged setting changes per PostgreSQL role.

Reranking is now available through `postllm.rerank(...)`. On the Candle runtime it reuses the active local embedding model and scores candidates by local similarity. On the `openai` runtime it forwards a hosted rerank request to `postllm.base_url` and normalizes the returned ranked rows.

## Why Candle

Candle is a Rust-native ML stack built for lightweight inference. It is a good fit for `postllm` because it keeps the local runtime in Rust instead of forcing a Python sidecar or another HTTP hop.

## Target Capabilities

Phase 1: local embeddings

Status: complete for the first local SentenceTransformer embedding path.

- SQL API for embedding single strings and arrays now exists.
- `postllm.embedding_model_info(...)` now exposes embedding dimension, max sequence length, pooling, optional projection metadata, and normalization behavior for the active or requested local embedding model.
- `postllm.chunk_text(...)` and `postllm.chunk_document(...)` now cover pre-embedding chunk preparation with overlap and carried metadata.
- `postllm.embed_document(...)` and `postllm.ingest_document(...)` now cover canonical embedding-table rows, deterministic chunk IDs, and direct upsert/prune workflows.
- `postllm.rerank(...)` now covers candidate reranking locally on Candle and through hosted rerank endpoints on the `openai` runtime.
- `postllm.keyword_rank(...)`, `postllm.rrf_score(...)`, and `postllm.hybrid_rank(...)` now cover keyword-only and hybrid retrieval directly in SQL.
- `postllm.rag(...)` and `postllm.rag_text(...)` now cover the batteries-included candidate-array RAG path, including retrieved context selection, prompt construction, and generation in one SQL call.
- `pgvector` integration docs now cover schema design, `real[]` to `vector(n)` casting, HNSW indexing, filtered retrieval, and a copy-paste retrieval-plus-generation workflow.
- The local embedding lane now supports SentenceTransformer-style `bert`, `distilbert`, and `xlm-roberta` encoder families.
- Pooling now follows SentenceTransformer module metadata for `cls`, `max`, `mean`, and `mean_sqrt_len`, with optional Dense projection heads when the model repo declares them.
- The current known-model metadata set includes `sentence-transformers/paraphrase-MiniLM-L3-v2`, `sentence-transformers/all-MiniLM-L6-v2`, `intfloat/e5-small-v2`, `BAAI/bge-small-en-v1.5`, and `sentence-transformers/distiluse-base-multilingual-cased-v2`.
- Single embeddings return `real[]`; batch embeddings return `jsonb`.

Phase 2: local text generation

- Status: starter-model chat and `complete` are implemented.
- The initial starter registry now recognizes `Qwen/Qwen2.5-0.5B-Instruct` and `Qwen/Qwen2.5-1.5B-Instruct`.
- `postllm.chat(...)` and `postllm.complete(...)` now render ChatML prompts, download single-file or sharded safetensors safely, and run local token sampling with Qwen stop-token handling.
- Responses now preserve provider payloads and carry normalized `_postllm` metadata for runtime, provider, model, finish reason, and usage so `extract_text` and future SQL helpers can target one stable shape.
- Docker smoke coverage now exercises local embeddings plus starter-model chat and `complete` generation against real downloaded model assets, and opt-in `pg_test` coverage can drive the same path from the SQL test suite.

Phase 3: runtime capabilities

- Expose runtime capability metadata in `postllm.settings()`.
- Differentiate models that support chat, embeddings, reranking, or multimodal inference.
- Reject unsupported function/runtime combinations with explicit errors rather than trying to coerce them.
- Make validation and configuration errors name the bad argument or active runtime/model and include a likely fix when one is obvious.

Phase 4: artifact management

- Status: local lifecycle helpers, offline mode, checksum-aware cache validation, and optional device-aware GPU selection are now live.
- `postllm.model_install(...)`, `postllm.model_prewarm(...)`, `postllm.model_inspect(...)`, and `postllm.model_evict(...)` now manage local Candle embedding and starter-generation models from SQL.
- The lifecycle helpers now report disk-cache state, current-backend memory-cache state, cached file inventories, lane-aware metadata, integrity summaries, and resolved device metadata for both embeddings and generation.
- `postllm.candle_offline` now forces Candle to use already-cached artifacts only, so local embedding, rerank, lifecycle, and starter-generation calls fail fast on cache misses instead of downloading from Hugging Face.
- Cached files now verify against checksum-named Hugging Face blobs when possible, and `postllm.model_install(...)` evicts the repo cache if integrity validation fails.
- `postllm.candle_device` now supports `auto`, `cpu`, `cuda`, and `metal`, with device-aware memory caching so CPU and accelerated runtimes do not collide inside one backend process.
- `postllm.profile_set(...)`, `postllm.profile_apply(...)`, and `postllm.model_alias_set(...)` now provide a higher-level operator workflow for switching between local Candle and hosted setups without rewriting full session configuration by hand.
- Use a configurable local cache directory for weights and tokenizers.
- Decide whether model loading is per-backend process, per-session, or shared global state.

## Recommended API Shape

Keep the shared concepts stable across runtimes:

- `postllm.runtime`
- `postllm.model`
- `postllm.configure(...)`
- `postllm.chat(...)`
- `postllm.chat_text(...)`
- `postllm.chat_stream(...)`
- `postllm.chat_structured(...)`
- `postllm.chat_tools(...)`
- `postllm.render_template(...)`
- `postllm.message_template(...)`
- `postllm.text_part(...)`
- `postllm.image_url_part(...)`
- `postllm.message_parts(...)`
- `postllm.function_tool(...)`
- `postllm.tool_choice_auto(...)`
- `postllm.tool_choice_none(...)`
- `postllm.tool_choice_required(...)`
- `postllm.tool_choice_function(...)`
- `postllm.tool_call(...)`
- `postllm.tool_result(...)`
- `postllm.json_schema(...)`
- `postllm.messages_agg(...)`
- `postllm.usage(...)`
- `postllm.choice(...)`
- `postllm.finish_reason(...)`
- `postllm.complete(...)`
- `postllm.complete_stream(...)`
- `postllm.complete_structured(...)`
- `postllm.complete_tools(...)`
- `postllm.complete_many(...)`
- `postllm.complete_many_rows(...)`

Add new local-first functions instead of overloading chat for everything:

- `postllm.embed(input text) -> real[]`
- `postllm.embed_many(inputs text[]) -> jsonb`
- `postllm.chunk_text(input text, chunk_chars int default 1000, overlap_chars int default 200) -> text[]`
- `postllm.chunk_document(input text, metadata jsonb default null, chunk_chars int default 1000, overlap_chars int default 200) -> table(index int, chunk text, metadata jsonb)`
- `postllm.embed_document(doc_id text, input text, metadata jsonb default null, chunk_chars int default 1000, overlap_chars int default 200, model text default null, normalize bool default true) -> table(chunk_id text, doc_id text, chunk_no int, content text, metadata jsonb, embedding real[])`
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
- `postllm.capabilities() -> jsonb`

## Implementation Notes

- Treat the current OpenAI-compatible HTTP client as one backend behind a dispatcher, not the center of the design.
- Normalize local runtime outputs into the existing response shape whenever possible.
- Keep model-specific code isolated so adding a second Candle model does not force changes through the SQL layer.
- The current implementation supports SentenceTransformer-style `bert`, `distilbert`, and `xlm-roberta` local embeddings plus local reranking, `PostgreSQL` full-text overlap scoring plus reciprocal-rank-fusion hybrid retrieval, one-call candidate-array RAG helpers, Qwen2.5 instruct models for local chat generation, OpenAI-style `response_format` JSON-schema structured outputs on the hosted runtime, OpenAI-compatible tool-calling request payloads on the hosted runtime, hosted rerank endpoints surfaced as ordered SQL rows, and hosted SSE streaming surfaced as SQL rows.
