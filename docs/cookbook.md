# Cookbook

This cookbook contains copy-paste SQL recipes for the workflows most users try first.

Assumptions:

- `CREATE EXTENSION postllm;` has already run in the current database.
- Hosted examples use a secret named `openai-prod` or `anthropic-prod`.
- Local examples use the Candle runtime and may download model artifacts unless `postllm.candle_offline` is enabled.

## Local Chat

Use this when the model should run in the PostgreSQL backend through Candle.

```sql
SELECT postllm.configure(
    runtime => 'candle',
    model => 'Qwen/Qwen2.5-0.5B-Instruct',
    timeout_ms => 60000
);

SELECT postllm.model_install(lane => 'generation');
SELECT postllm.model_prewarm(lane => 'generation');

SELECT postllm.chat_text(
    ARRAY[
        postllm.system('You are concise and practical.'),
        postllm.user('Explain MVCC in one sentence.')
    ],
    max_tokens => 96,
    temperature => 0.0
);
```

Use `postllm.complete(...)` when the input is a single prompt:

```sql
SELECT postllm.complete(
    prompt => 'Explain autovacuum in one sentence.',
    system_prompt => 'You are concise and practical.',
    max_tokens => 96,
    temperature => 0.0
);
```

## Hosted Chat

Use this for OpenAI-compatible HTTP endpoints, including OpenAI Chat Completions.

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.openai.com/v1/chat/completions',
    model => 'gpt-4o-mini',
    api_key_secret => 'openai-prod',
    timeout_ms => 30000
);

SELECT postllm.chat_text(ARRAY[
    postllm.system('You are concise and practical.'),
    postllm.user('Explain VACUUM and autovacuum.')
]);
```

The same SQL shape works against the OpenAI Responses-style endpoint:

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.openai.com/v1/responses',
    model => 'gpt-4o-mini',
    api_key_secret => 'openai-prod'
);

SELECT postllm.complete(
    prompt => 'Write a two-line release note for a PostgreSQL extension.',
    system_prompt => 'Keep it concrete.',
    max_tokens => 80
);
```

Anthropic Messages is also available through the hosted runtime adapter:

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.anthropic.com/v1/messages',
    model => 'claude-3-5-sonnet-latest',
    api_key_secret => 'anthropic-prod'
);

SELECT postllm.chat_text(ARRAY[
    postllm.system('You are concise and practical.'),
    postllm.user('Explain table bloat in one sentence.')
]);
```

## Embeddings

Use local Candle embeddings when data should stay on the database host.

```sql
SELECT postllm.configure(
    runtime => 'candle',
    embedding_model => 'sentence-transformers/paraphrase-MiniLM-L3-v2'
);

SELECT postllm.embedding_model_info();

SELECT postllm.embed(
    'Autovacuum removes dead tuples and helps control table bloat.'
);

SELECT postllm.embed_many(ARRAY[
    'MVCC stores multiple row versions.',
    'VACUUM removes dead tuples.',
    'Indexes speed up selective lookups.'
]);
```

Use hosted embeddings when the configured provider exposes an OpenAI-compatible embeddings endpoint:

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.openai.com/v1/chat/completions',
    model => 'gpt-4o-mini',
    embedding_model => 'text-embedding-3-small',
    api_key_secret => 'openai-prod'
);

SELECT postllm.embed('Explain PostgreSQL checkpoints in one sentence.');
```

## RAG

Use the one-call helper when you already have candidate documents in SQL.

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.openai.com/v1/chat/completions',
    model => 'gpt-4o-mini',
    api_key_secret => 'openai-prod'
);

SELECT postllm.rag_text(
    query => 'How does PostgreSQL control table bloat?',
    documents => ARRAY[
        'Bananas are yellow and grow in bunches.',
        'Autovacuum removes dead tuples and helps control table bloat.',
        'VACUUM can be run manually to reclaim space.',
        'Indexes help queries find matching rows faster.'
    ],
    retrieval => 'hybrid',
    top_n => 2,
    system_prompt => 'Answer only from the retrieved PostgreSQL context. Say when the context is insufficient.',
    max_tokens => 160,
    temperature => 0.0
);
```

Use the observable form when you want selected context and raw response metadata:

```sql
SELECT postllm.rag(
    query => 'How does PostgreSQL control table bloat?',
    documents => ARRAY[
        'Autovacuum removes dead tuples and helps control table bloat.',
        'VACUUM can be run manually to reclaim space.'
    ],
    retrieval => 'hybrid',
    top_n => 2,
    max_tokens => 160
);
```

## Structured Outputs

Use structured outputs when the caller needs validated JSON instead of plain text.

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.openai.com/v1/chat/completions',
    model => 'gpt-4o-mini',
    api_key_secret => 'openai-prod'
);

SELECT postllm.chat_structured(
    ARRAY[
        postllm.system('Extract a database maintenance task.'),
        postllm.user('Run VACUUM on public.orders tonight and report reclaimed space.')
    ],
    postllm.json_schema(
        'maintenance_task',
        '{
            "type":"object",
            "properties":{
                "action":{"type":"string"},
                "schema":{"type":"string"},
                "table":{"type":"string"},
                "schedule":{"type":"string"},
                "report_metric":{"type":"string"}
            },
            "required":["action","schema","table","schedule","report_metric"],
            "additionalProperties":false
        }'::jsonb
    ),
    temperature => 0.0,
    max_tokens => 180
);
```

The completion variant works for single-prompt extraction:

```sql
SELECT postllm.complete_structured(
    prompt => 'Extract a task from: Reindex public.accounts this weekend.',
    response_format => postllm.json_schema(
        'task',
        '{
            "type":"object",
            "properties":{
                "action":{"type":"string"},
                "target":{"type":"string"},
                "when":{"type":"string"}
            },
            "required":["action","target","when"],
            "additionalProperties":false
        }'::jsonb
    ),
    temperature => 0.0,
    max_tokens => 120
);
```

## Tools

Use tool calling when the model should ask SQL or application code to perform an action before the final answer.

```sql
SELECT postllm.configure(
    runtime => 'openai',
    base_url => 'https://api.openai.com/v1/chat/completions',
    model => 'gpt-4o-mini',
    api_key_secret => 'openai-prod'
);

WITH first_pass AS (
    SELECT postllm.chat_tools(
        ARRAY[
            postllm.system('Use tools when current database facts are needed.'),
            postllm.user('How many active orders are waiting for fulfillment?')
        ],
        ARRAY[
            postllm.function_tool(
                'count_active_orders',
                '{
                    "type":"object",
                    "properties":{
                        "status":{"type":"string"}
                    },
                    "required":["status"],
                    "additionalProperties":false
                }'::jsonb,
                description => 'Return a count of active orders by status.'
            )
        ],
        tool_choice => postllm.tool_choice_auto(),
        temperature => 0.0,
        max_tokens => 160
    ) AS response
),
tool_request AS (
    SELECT
        response,
        response->'choices'->0->'message'->'tool_calls'->0 AS tool_call
    FROM first_pass
),
tool_execution AS (
    SELECT
        tool_call,
        jsonb_build_object(
            'status', tool_call->'function'->'arguments'->>'status',
            'count', 42
        ) AS tool_result
    FROM tool_request
)
SELECT postllm.chat_text(ARRAY[
    postllm.system('Use tools when current database facts are needed.'),
    postllm.user('How many active orders are waiting for fulfillment?'),
    postllm.assistant_tool_calls(ARRAY[tool_call]),
    postllm.tool_result(tool_call->>'id', tool_result)
])
FROM tool_execution;
```

For a fixed tool call, force a named function:

```sql
SELECT postllm.tool_choice_function('count_active_orders');
```
