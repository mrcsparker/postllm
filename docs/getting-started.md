# Getting Started

This page is the quickest path to a working `postllm` setup.

## Prerequisites

Install PostgreSQL tooling support:

```bash
cargo install cargo-pgrx --version 0.18.0 --locked
cargo pgrx init --pg17 download
```

Build with local GPU support only when you need it:

```bash
cargo build --features candle-cuda
# or
cargo build --features candle-metal
```

Without those features, the Candle runtime uses CPU only.

On macOS with Homebrew ICU:

```bash
export PKG_CONFIG_PATH="$(brew --prefix icu4c@78)/lib/pkgconfig"
```

## Native development flow

Start PostgreSQL with extension loading:

```bash
cargo pgrx run pg17
```

Load the extension and inspect current runtime state:

```sql
CREATE EXTENSION postllm;
SELECT postllm.settings();
SELECT postllm.capabilities();
```

## First successful call

```sql
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

## Choose your workflow

For hosted inference, keep `runtime => 'openai'` and set a compatible `base_url`.

For local inference, keep `runtime => 'candle'` and choose a supported local model:

```sql
SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct');
SELECT postllm.chat_text(ARRAY[
    postllm.system('You are concise.'),
    postllm.user('Explain MVCC in one sentence.')
]);
```

## Read this next

- [Configuration and governance](./configuration.md)
- [Runtime behavior](./runtime.md)
- [Reference index](./reference.md)
- [Examples](./examples.md)
