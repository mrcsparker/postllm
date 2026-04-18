# Configuration and Governance

`postllm` has two classes of configuration:

- PostgreSQL settings (via `SET`, `ALTER SYSTEM`, role defaults, and session overrides).
- SQL helpers for reusable profiles, secret management, model aliases, and role permissions.

## Core GUCs

- `postllm.runtime`
- `postllm.base_url`
- `postllm.model`
- `postllm.embedding_model`
- `postllm.api_key`
- `postllm.api_key_secret`
- `postllm.timeout_ms`
- `postllm.max_retries`
- `postllm.retry_backoff_ms`
- `postllm.request_token_budget`
- `postllm.request_runtime_budget_ms`
- `postllm.request_spend_budget_microusd`
- `postllm.output_token_price_microusd_per_1k`
- `postllm.http_allowed_hosts`
- `postllm.http_allowed_providers`
- `postllm.candle_cache_dir`
- `postllm.candle_offline`
- `postllm.candle_device`
- `postllm.candle_max_input_tokens`
- `postllm.candle_max_concurrency`
- `postllm.request_logging`
- `postllm.request_log_redact_inputs`
- `postllm.request_log_redact_outputs`

Use `postllm.configure(...)` for session intent and `SET LOCAL`/`ALTER SYSTEM` for environment-level policy.

```sql
SELECT postllm.configure(
    base_url => 'http://127.0.0.1:11434/v1/chat/completions',
    model => 'llama3.2',
    embedding_model => 'sentence-transformers/paraphrase-MiniLM-L3-v2',
    timeout_ms => 10000,
    max_retries => 2,
    retry_backoff_ms => 250,
    request_token_budget => 512,
    request_runtime_budget_ms => 5000,
    request_spend_budget_microusd => 2500,
    output_token_price_microusd_per_1k => 5000,
    runtime => 'openai',
    candle_offline => false,
    candle_device => 'auto'
);
```

## Request guardrails

Operators can add coarse per-request limits without changing application SQL:

- `postllm.request_token_budget`
- `postllm.request_runtime_budget_ms`
- `postllm.request_spend_budget_microusd`
- `postllm.output_token_price_microusd_per_1k`

Behavior:

- `request_token_budget` caps generation-style output tokens. If callers omit `max_tokens`, `postllm` injects the guardrail value automatically.
- `request_runtime_budget_ms` clamps the effective timeout for hosted HTTP requests and local Candle work even when `postllm.timeout_ms` is higher.
- `request_spend_budget_microusd` derives an output-token ceiling from `output_token_price_microusd_per_1k`.

The spend guardrail is intentionally conservative: it estimates generated output spend for chat, complete, stream, and RAG requests. It does not try to guess hosted prompt-token pricing ahead of time, and local Candle work is still governed primarily by `timeout_ms`, `candle_max_input_tokens`, and `candle_max_concurrency`.

Example:

```sql
ALTER SYSTEM SET postllm.request_token_budget = 256;
ALTER SYSTEM SET postllm.request_runtime_budget_ms = 4000;
ALTER SYSTEM SET postllm.request_spend_budget_microusd = 1200;
ALTER SYSTEM SET postllm.output_token_price_microusd_per_1k = 300000;
SELECT pg_reload_conf();
```

## Profiles

Profiles store session configuration in one row and make switching between environments deterministic.

- Use `profile_set(...)` to persist settings.
- Use `profile_apply(name)` to apply them.
- `postllm.profile_apply(...)` resets managed settings to defaults before applying the profile.

Example:

```sql
SELECT postllm.secret_set(
    name => 'openai-prod',
    value => 'sk-live-redacted',
    description => 'Primary hosted provider key'
);

SELECT postllm.profile_set(
    name => 'hosted-prod',
    runtime => 'openai',
    base_url => 'https://api.openai.com/v1/chat/completions',
    model => 'gpt-4.1-mini',
    api_key_secret => 'openai-prod'
);

SELECT postllm.profile_apply('hosted-prod');
```

## Secrets

`postllm.secret_set(...)` stores provider credentials in `postllm.provider_secrets`. Secret values are encrypted, and secret-management functions expose metadata only.

`api_key` is still supported as a direct override, but production should prefer:

1. `POSTLLM_SECRET_KEY` environment variable at operator level.
2. `postllm.secret_set(...)` with role-scoped credential references.
3. `postllm.configure(api_key_secret => 'name')`.

## Permissions

Use `postllm.permission_set(...)` to control which roles can change:

- runtime
- `generation_model`
- `embedding_model`
- privileged `setting` targets

Rules are category-wide once a category has entries, so granting `*` in a category enables all targets inside that category.

```sql
SELECT postllm.permission_set(
    role_name => 'app_runtime_openai',
    object_type => 'runtime',
    target => 'openai'
);

SELECT postllm.permission_set(
    role_name => 'app_runtime_openai',
    object_type => 'setting',
    target => 'base_url'
);
```

## Network allowlists

Operators can constrain hosted outbound calls with:

- `postllm.http_allowed_hosts`
- `postllm.http_allowed_providers`

Examples:

```sql
ALTER SYSTEM SET postllm.http_allowed_hosts = 'api.openai.com,host.docker.internal:11434';
ALTER SYSTEM SET postllm.http_allowed_providers = 'openai,ollama';
```

`postllm.http_allowed_hosts` accepts `host`, `host:port`, `*.suffix`, and empty (unrestricted).  
`postllm.http_allowed_providers` accepts `openai`, `ollama`, `openai-compatible`, or `*`.

## Request audit logging

Operators can persist an audit trail for request execution with:

- `postllm.request_logging`
- `postllm.request_log_redact_inputs`
- `postllm.request_log_redact_outputs`

When logging is enabled, `postllm` writes one row per chat, completion, streaming, embedding, or rerank request into `postllm.request_audit_log`.

Recommended safe default:

```sql
SET LOCAL postllm.request_logging = on;
SET LOCAL postllm.request_log_redact_inputs = on;
SET LOCAL postllm.request_log_redact_outputs = on;
```

If an operator explicitly needs full prompt and response capture for a short-lived debugging session:

```sql
SET LOCAL postllm.request_logging = on;
SET LOCAL postllm.request_log_redact_inputs = off;
SET LOCAL postllm.request_log_redact_outputs = off;
```

Audit rows include role, backend PID, runtime, model, duration, request payload, response payload, and any error text. The audit table is intended for operator review rather than routine application queries.

## Model aliases

Aliases are lane-aware and resolve automatically during generation, embeddings, discovery, and lifecycle operations.

```sql
SELECT postllm.model_alias_set(
    alias => 'starter',
    lane => 'generation',
    model => 'Qwen/Qwen2.5-0.5B-Instruct'
);

SELECT postllm.model_alias_set(
    alias => 'embed_fast',
    lane => 'embedding',
    model => 'sentence-transformers/paraphrase-MiniLM-L3-v2'
);
```

## Runtime visibility

Use:

- `postllm.capabilities()`
- `postllm.settings()`
- `postllm.runtime_discover()`
- `postllm.runtime_ready()`

to verify what is active and what is currently supported in that runtime/model combination.
