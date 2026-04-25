# Demo

The fastest full demo path is the bundled support-triage sample app.

It starts PostgreSQL with `postllm`, starts a small `llama.cpp` server, seeds a support-ticket schema, retrieves matching knowledge-base rows in SQL, and drafts one grounded response with `postllm.chat_text(...)`.

## Run It

From a fresh clone:

```bash
./scripts/demo_quickstart.sh
```

The first run builds the PostgreSQL extension image and downloads the small demo model configured by `compose.llama-e2e.yaml`. Later runs reuse Docker layers and model cache.

The script defaults `POSTLLM_TIMEOUT_MS` to `120000` so slower local model startup does not fail the first request.

Expected final line:

```text
postllm demo completed successfully.
```

## Keep The Database Running

Use this when you want to inspect the sample schema after the demo finishes:

```bash
POSTLLM_DEMO_KEEP=1 ./scripts/demo_quickstart.sh
psql postgresql://postgres:postgres@127.0.0.1:5544/postllm
```

Useful queries:

```sql
TABLE postllm_demo.tickets;
TABLE postllm_demo.kb_articles;
TABLE postllm_demo.ticket_responses;
SELECT postllm.settings();
SELECT postllm.runtime_discover();
```

Shut the stack down when finished:

```bash
docker compose -f compose.yaml -f compose.llama-e2e.yaml -p postllm-demo down -v --remove-orphans
```

## What It Demonstrates

- A real `postllm` extension installed inside PostgreSQL.
- Hosted-runtime compatibility through a local `llama.cpp` OpenAI-compatible endpoint.
- A SQL-native sample app with tables, seed data, and generated output.
- SQL retrieval followed by grounded generation through `postllm.chat_text(...)`.
- A concrete success condition that fails the script if no draft response is produced.
