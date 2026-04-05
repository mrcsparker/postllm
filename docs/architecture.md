# Architecture Overview

This page gives a one-screen mental model of request flow and ownership boundaries.

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

    subgraph "SQL boundary"
        S["src/lib.rs SQL exports"]
        N["api::config"]
        O["api::messages"]
        P["api::inference"]
        Q["api::retrieval"]
        R["api::ops"]
    end

    S --> N
    S --> O
    S --> P
    S --> Q
    S --> R
```

## Current boundaries

- `src/lib.rs` registers SQL functions and keeps extension SQL metadata.
- `src/api/mod.rs` owns API namespacing (`api::config`, `api::messages`, `api::inference`, `api::retrieval`, `api::ops`).
- `src/api/config.rs` implements `api::config`.
- `src/api/messages.rs` implements `api::messages`.
- `src/api/inference.rs` implements `api::inference`.
- `src/api/retrieval.rs` implements `api::retrieval`.
- `src/api/ops.rs` implements `api::ops`.
- `src/backend.rs` centralizes request types, capability metadata, and settings model.
- `src/guc.rs` resolves and validates runtime/configuration state.
- `src/permissions.rs` and `src/operator_policy.rs` hold governance rules.
- `src/client.rs` and `src/candle.rs` implement HTTP/local runtime transport and execution.
- `src/http_policy.rs`, `src/secrets.rs`, `src/catalog.rs` handle security and metadata helpers.

## Design notes for maintainers

Keep this structure when adding features:

1. Add or extend the request option type first.
2. Resolve and validate settings once, in one place.
3. Apply policy checks before runtime execution.
4. Keep SQL entrypoints thin and delegate behavior to internal helpers.
5. Add a tiny API entrypoint function for each new SQL helper within the relevant `api::*` module.
