# Architecture Overview

This page gives a one-screen mental model of request flow and where policies are enforced.

```mermaid
flowchart TD
    A["SQL entrypoint (e.g. postllm.chat/postllm.complete)"] --> B["build RequestOptions"]
    B --> C["guc::resolve + alias + permission checks"]
    C --> D["Policy layer"]
    D --> E["Runtime dispatch"]
    E --> F["openai: HTTP client + retry + stream parsing"]
    E --> G["candle: local backend + model lifecycle hooks"]
    F --> H["post-process response"]
    G --> H
    H --> I["extract JSON + usage/choices helpers"]
    I --> J["return SQL-friendly payload"]
    D --> K["http_policy::enforce_settings for hosted endpoints"]
    D --> L["operator checks + privilege guards"]
```

## Current boundaries

- `src/lib.rs` registers SQL functions and keeps extension SQL metadata.
- `src/backend.rs` centralizes request types, capability metadata, and settings model.
- `src/guc.rs` resolves and validates runtime/configuration state.
- `src/permissions.rs` and `src/operator_policy.rs` hold governance rules.
- `src/client.rs` and `src/candle.rs` implement backend transport and runtime-specific execution.
- `src/http_policy.rs`, `src/secrets.rs`, `src/catalog.rs` handle security and metadata helpers.

## Design notes for maintainers

Keep the same shape when adding features:

1. Add/extend a request option type first.
2. Resolve and validate configuration once.
3. Apply policy checks in one location before runtime execution.
4. Keep SQL wrappers thin and delegate cross-cutting behavior to internal helpers.
