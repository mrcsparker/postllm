# Examples

This page contains SQL examples grouped by outcome.

## Quick inference and prompts

### Local Ollama-compatible example

```sql
SELECT postllm.complete(
    prompt => 'Explain MVCC in one sentence.',
    system_prompt => 'You are concise.'
);
```

### Structured chat

```sql
SELECT postllm.chat_text(ARRAY[
    postllm.system('You are a PostgreSQL expert.'),
    postllm.user('Explain VACUUM and autovacuum.')
]);
```

### Prompt templates

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

## Structured output and tools

### Structured output

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

### Tool calling

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

### Tool helper functions

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

### Streaming

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

### Batch generation

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

### Conversation rowset

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

### Multimodal chat

```sql
SELECT postllm.chat_text(ARRAY[
    postllm.user_parts(ARRAY[
        postllm.text_part('Describe this image in one sentence.'),
        postllm.image_url_part('https://example.com/cat.png', detail => 'low')
    ])
], model => 'gpt-4o-mini');
```

### Response inspection

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

## Local Candle examples

### Local chat and complete

```sql
SELECT postllm.configure(
    runtime => 'candle',
    model => 'Qwen/Qwen2.5-0.5B-Instruct'
);

SELECT postllm.chat_text(ARRAY[
    postllm.system('You are concise.'),
    postllm.user('Explain MVCC in one sentence.')
], max_tokens => 96);

SELECT postllm.complete(
    prompt => 'Explain MVCC in one sentence.',
    system_prompt => 'You are concise.',
    max_tokens => 96
);
```

### Hosted embeddings

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.openai.com/v1/chat/completions',
    embedding_model => 'text-embedding-3-small',
    api_key_secret => 'openai-prod'
);

SELECT postllm.embed('Explain VACUUM in one sentence.');

SELECT postllm.embed_many(ARRAY[
    'What is MVCC?',
    'What does autovacuum do?'
]);
```

`postllm` derives the sibling `/v1/embeddings` endpoint from `base_url`, so the same hosted profile can handle chat and embeddings.

### Anthropic chat

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.anthropic.com/v1/messages',
    model => 'claude-3-5-sonnet-latest',
    api_key_secret => 'anthropic-prod'
);

SELECT postllm.chat_text(ARRAY[
    postllm.system('You are concise.'),
    postllm.user('Explain VACUUM in one sentence.')
]);
```

### Local embeddings

```sql
SELECT postllm.embed('Explain VACUUM in one sentence.');

SELECT postllm.embed_many(ARRAY[
    'What is MVCC?',
    'What does autovacuum do?'
]);

SELECT postllm.embedding_model_info();
SELECT postllm.embedding_model_info('BAAI/bge-small-en-v1.5');
```

```sql
SELECT *
FROM postllm.model_inspect();
SELECT postllm.model_install(lane => 'generation');
SELECT postllm.model_prewarm(lane => 'generation');
SELECT postllm.configure(candle_device => 'auto', candle_offline => true);
SELECT postllm.model_evict(lane => 'generation', scope => 'memory');
```

### Chunking and documents

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

SELECT *
FROM postllm.embed_document(
    'vacuum-guide',
    'Autovacuum removes dead tuples. VACUUM can reclaim space. MVCC depends on tuple visibility.',
    '{"source":"manual"}'::jsonb,
    chunk_chars => 48,
    overlap_chars => 12
);
```

### Ingest rows

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

## Retrieval and reranking

### Rerank

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

### Keyword and hybrid retrieval

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

SELECT *
FROM postllm.keyword_rank(
    'How does PostgreSQL control table bloat?',
    ARRAY['Bananas are yellow and grow in bunches.', 'Autovacuum removes dead tuples and helps control table bloat.'],
    top_n => 2
);
```

### Batteries-included RAG

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

### pgvector pipeline

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

For a complete `pgvector` workflow, see [docs/pgvector-integration.md](./pgvector-integration.md).

## OpenAI-compatible quick check

```sql
SET postllm.base_url = 'https://api.openai.com/v1/responses';
SET postllm.api_key = 'sk-...';
SET postllm.model = 'gpt-4o-mini';

SELECT postllm.complete(
    prompt => 'Write a haiku about PostgreSQL extensions.',
    system_prompt => 'You are concise.',
    temperature => 0.4,
    max_tokens => 120
);
```
