# SQL Reference

This page is grouped by domain so you can scan what you need quickly.

## Settings and capability surface

- `postllm.settings() -> jsonb`
- `postllm.capabilities() -> jsonb`
- `postllm.runtime_discover() -> jsonb`
- `postllm.runtime_ready() -> bool`
- `postllm.configure(...) -> jsonb`

## Profiles

- `postllm.profiles() -> jsonb`
- `postllm.profile(name text) -> jsonb`
- `postllm.profile_set(name text, ...) -> jsonb`
- `postllm.profile_apply(name text) -> jsonb`
- `postllm.profile_delete(name text) -> jsonb`

## Secrets

- `postllm.secrets() -> jsonb`
- `postllm.secret(name text) -> jsonb`
- `postllm.secret_set(name text, value text, description text default null) -> jsonb`
- `postllm.secret_delete(name text) -> jsonb`

## Permissions

- `postllm.permissions() -> jsonb`
- `postllm.permission(role_name text, object_type text, target text) -> jsonb`
- `postllm.permission_set(role_name text, object_type text, target text, description text default null) -> jsonb`
- `postllm.permission_delete(role_name text, object_type text, target text) -> jsonb`

## Model aliases

- `postllm.model_aliases() -> jsonb`
- `postllm.model_alias(alias text, lane text) -> jsonb`
- `postllm.model_alias_set(alias text, lane text, model text, description text default null) -> jsonb`
- `postllm.model_alias_delete(alias text, lane text) -> jsonb`

## Message construction

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
- `postllm.messages_agg(message jsonb) -> jsonb[]`

## Structured and tool helpers

- `postllm.function_tool(name text, parameters jsonb, description text default null) -> jsonb`
- `postllm.tool_choice_auto() -> jsonb`
- `postllm.tool_choice_none() -> jsonb`
- `postllm.tool_choice_required() -> jsonb`
- `postllm.tool_choice_function(name text) -> jsonb`
- `postllm.tool_call(id text, name text, arguments jsonb) -> jsonb`
- `postllm.assistant_tool_calls(tool_calls jsonb[], content text default null) -> jsonb`
- `postllm.tool_result(tool_call_id text, content text) -> jsonb`
- `postllm.json_schema(name text, schema jsonb, strict bool default true) -> jsonb`

## Chat and completion APIs

- `postllm.chat(messages jsonb[], ...) -> jsonb`
- `postllm.chat_text(messages jsonb[], ...) -> text`
- `postllm.chat_stream(messages jsonb[], ...) -> table(index int, delta text, event jsonb)`
- `postllm.chat_structured(messages jsonb[], response_format jsonb, ...) -> jsonb`
- `postllm.chat_tools(messages jsonb[], tools jsonb[], tool_choice jsonb default null, ...) -> jsonb`
- `postllm.complete(prompt text, ...) -> text`
- `postllm.complete_stream(prompt text, ...) -> table(index int, delta text, event jsonb)`
- `postllm.complete_structured(prompt text, response_format jsonb, ...) -> jsonb`
- `postllm.complete_tools(prompt text, tools jsonb[], system_prompt text default null, tool_choice jsonb default null, ...) -> jsonb`
- `postllm.complete_many(prompts text[], ...) -> text[]`
- `postllm.complete_many_rows(prompts text[], ...) -> table(index int, prompt text, completion text)`

## Response helpers

- `postllm.usage(response jsonb) -> jsonb`
- `postllm.choice(response jsonb, index int) -> jsonb`
- `postllm.finish_reason(response jsonb) -> text`
- `postllm.extract_text(response jsonb) -> text`

## Chunking and embedding

- `postllm.chunk_text(input text, chunk_chars int default 1000, overlap_chars int default 200) -> text[]`
- `postllm.chunk_document(input text, metadata jsonb default null, chunk_chars int default 1000, overlap_chars int default 200) -> table(index int, chunk text, metadata jsonb)`
- `postllm.embed_document(doc_id text, input text, metadata jsonb default null, chunk_chars int default 1000, overlap_chars int default 200, model text default null, normalize bool default true) -> table(chunk_id text, doc_id text, chunk_no int, content text, metadata jsonb, embedding real[])`
- `postllm.embed(input text, ...) -> real[]`
- `postllm.embed_many(inputs text[], ...) -> jsonb`
- `postllm.embedding_model_info(model text default null) -> jsonb`
- `postllm.ingest_document(target_table text, doc_id text, input text, metadata jsonb default null, chunk_chars int default 1000, overlap_chars int default 200, model text default null, normalize bool default true, delete_missing bool default true) -> jsonb`

## Model lifecycle

- `postllm.model_install(model text default null, lane text default null) -> jsonb`
- `postllm.model_prewarm(model text default null, lane text default null) -> jsonb`
- `postllm.model_inspect(model text default null, lane text default null) -> jsonb`
- `postllm.model_evict(model text default null, lane text default null, scope text default 'all') -> jsonb`

## Retrieval and generation orchestration

- `postllm.rerank(query text, documents text[], top_n int default null, model text default null) -> table(rank int, index int, document text, score double precision)`
- `postllm.keyword_rank(query text, documents text[], top_n int default null, text_search_config text default null, normalization int default 32) -> table(rank int, index int, document text, score double precision)`
- `postllm.rrf_score(semantic_rank int default null, keyword_rank int default null, semantic_weight double precision default 1.0, keyword_weight double precision default 1.0, rrf_k int default 60) -> double precision`
- `postllm.hybrid_rank(query text, documents text[], top_n int default null, model text default null, text_search_config text default null, semantic_weight double precision default 1.0, keyword_weight double precision default 1.0, rrf_k int default 60, normalization int default 32) -> table(rank int, index int, document text, score double precision, semantic_rank int, keyword_rank int, semantic_score double precision, keyword_score double precision)`
- `postllm.rag(query text, documents text[], system_prompt text default null, model text default null, retrieval text default null, retrieval_model text default null, top_n int default 5, temperature double precision default 0.2, max_tokens int default null, text_search_config text default null, semantic_weight double precision default 1.0, keyword_weight double precision default 1.0, rrf_k int default 60, normalization int default 32) -> jsonb`
- `postllm.rag_text(query text, documents text[], system_prompt text default null, model text default null, retrieval text default null, retrieval_model text default null, top_n int default 5, temperature double precision default 0.2, max_tokens int default null, text_search_config text default null, semantic_weight double precision default 1.0, keyword_weight double precision default 1.0, rrf_k int default 60, normalization int default 32) -> text`

