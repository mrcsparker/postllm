# postllm Backlog And Milestone Plan

This file is the execution plan for turning `postllm` into a serious PostgreSQL-native LLM extension.

The ordering is intentional:

- Lower-numbered issues should land before higher-numbered ones unless there is a clear dependency reason not to.
- Each milestone has explicit exit criteria.
- The first priority is closing the product gap between local embeddings and local generation.

## Current Baseline

- `postllm.chat(...)` and `postllm.complete(...)` work through OpenAI-compatible HTTP endpoints.
- `postllm.embed(...)` and `postllm.embed_many(...)` work locally through Candle.
- Session configuration is handled through `postllm.configure(...)` and GUCs.
- The repo already has unit tests, `pgrx` SQL tests, and Docker smoke tests.

## Milestone 1: Local Generation Foundation

Goal: make the Candle lane real for generation, not just embeddings.

Exit criteria:

- `postllm.runtime = 'candle'` supports both `postllm.chat(...)` and `postllm.complete(...)`.
- `postllm.capabilities()` exists and reports what the current runtime/model can do.
- Runtime/function/model mismatch errors are explicit and consistent.
- Local generation is covered by SQL tests and a Docker smoke test.

Issues:

- [x] `PL-001` Add a runtime capability model in Rust that describes support for chat, complete, embeddings, tools, structured outputs, streaming, and multimodal inputs.
- [x] `PL-002` Add `postllm.capabilities() -> jsonb` and include capability metadata in `postllm.settings()`.
- [x] `PL-003` Centralize runtime/function/model compatibility checks so unsupported combinations fail before work starts.
- [x] `PL-004` Add a Candle generation runtime abstraction with explicit support for a small starter model set.
- [x] `PL-005` Implement Candle-backed local generation for `postllm.chat(...)`.
- [x] `PL-006` Implement Candle-backed local generation for `postllm.complete(...)`.
- [x] `PL-007` Normalize response metadata across runtimes, including finish reason, usage, model name, and provider/runtime identity.
- [x] `PL-008` Add SQL tests and a Dockerized Candle generation smoke test.

## Milestone 2: SQL API Ergonomics

Goal: make the SQL surface pleasant enough that normal usage does not require manual JSON plumbing.

Exit criteria:

- Common text-generation usage no longer requires `extract_text(...)`.
- Response inspection helpers exist for usage and choices.
- Prompt and message construction feels native in SQL.
- Errors point to the fix instead of just the failure.

Issues:

- [x] `PL-009` Add `postllm.chat_text(...) -> text` as the primary happy-path wrapper over `chat + extract_text`.
- [x] `PL-010` Add `postllm.usage(response jsonb) -> jsonb`, `postllm.choice(response jsonb, index int) -> jsonb`, and `postllm.finish_reason(response jsonb) -> text`.
- [x] `PL-011` Add richer message helpers for content parts, tool calls, tool results, and multimodal messages.
- [x] `PL-012` Add message aggregation helpers so callers can build `jsonb[]` conversations from rowsets without manual array assembly.
- [x] `PL-013` Add prompt-template helpers with named variables and simple rendering rules.
- [x] `PL-014` Add batch generation APIs for arrays and set-oriented workloads.
- [x] `PL-015` Improve all user-facing errors so they name the bad argument, runtime, model, and likely fix.

## Milestone 3: Structured Outputs, Tools, And Streaming

Goal: make `postllm` useful for real application workflows, not just plain text completions.

Exit criteria:

- Structured output generation works with validation.
- Tool definitions and tool-call handling are exposed in SQL.
- Streaming exists for supported runtimes.
- Query cancellation interrupts long-running generation work.

Issues:

- [x] `PL-016` Add structured output support using JSON schema or an equivalent typed-output contract.
- [x] `PL-017` Add SQL helpers for tool definitions, tool call payloads, and tool result messages.
- [x] `PL-018` Implement tool-calling request/response flow for runtimes that support it.
- [x] `PL-019` Add a streaming API for generation, ideally as a set-returning SQL function that emits deltas or events.
- [x] `PL-020` Propagate PostgreSQL query cancellation into both HTTP requests and local Candle generation.
- [x] `PL-021` Add configurable retry behavior and transient-failure classification for HTTP-backed runtimes.

## Milestone 4: Retrieval And Embedding Workflow

Goal: make `postllm` excellent for the most common database-native AI workflow: embed, retrieve, rerank, generate.

Exit criteria:

- A user can go from raw text to indexed chunks to retrieval to answer generation with first-class helpers.
- `pgvector` integration is documented and easy.
- Embedding and reranking metadata is discoverable.

Issues:

- [x] `PL-022` Expose embedding model metadata such as dimension, normalization behavior, and max sequence length.
- [x] `PL-023` Add first-class `pgvector` integration docs and helper examples.
- [x] `PL-024` Add chunking helpers with sane defaults for chunk size, overlap, and metadata propagation.
- [x] `PL-025` Add ingestion helpers for embedding tables, including deterministic chunk IDs and upsert behavior.
- [x] `PL-026` Add reranking support, local where feasible and HTTP-backed where necessary.
- [x] `PL-027` Add hybrid retrieval primitives that combine vector similarity with keyword search.
- [x] `PL-028` Add a batteries-included RAG helper that can retrieve context, build a prompt, and run generation in one SQL call.
- [x] `PL-029` Expand local embedding model support beyond the initial sentence-transformer.

## Milestone 5: Runtime And Model Operations

Goal: make local and hosted runtime management operationally sane.

Exit criteria:

- Local models can be installed, prewarmed, inspected, and reused predictably.
- Runtime state and cache health are visible.
- Resource controls exist for local inference.

Issues:

- [x] `PL-030` Add local model lifecycle commands or functions for install, prewarm, inspect, and evict.
- [x] `PL-031` Add offline mode for already-cached Candle models.
- [x] `PL-032` Add cache integrity checks and checksum-aware artifact handling.
- [x] `PL-033` Add memory, concurrency, and timeout controls for local inference.
- [x] `PL-034` Add optional GPU support for Candle where platform support is stable enough.
- [x] `PL-035` Add named config profiles and model aliases for switching between local, staging, and hosted setups.
- [x] `PL-036` Add runtime discovery helpers so Docker and local environments can report readiness cleanly.

## Milestone 6: Production Safety And Governance

Goal: make the extension safe to run in serious environments.

Exit criteria:

- Secrets are not forced into ad hoc SQL strings.
- Operators can control who may call which runtimes and where outbound traffic can go.
- Usage, latency, and failures are observable.

Issues:

- [x] `PL-037` Add a proper secret-management story for provider credentials.
- [x] `PL-038` Add role-aware permission controls for runtimes, models, and privileged settings.
- [x] `PL-039` Add network allowlists and provider safelists for HTTP runtimes.
- [ ] `PL-040` Add request logging and audit trails with opt-in prompt/response redaction.
- [ ] `PL-041` Add metrics views for latency, errors, token usage, and request counts.
- [ ] `PL-042` Add quotas and guardrails for token budget, runtime budget, and spend.
- [ ] `PL-043` Add backpressure controls so concurrent model work cannot overwhelm the database.
- [ ] `PL-044` Document operational guidance for when inference inside PostgreSQL is appropriate and when it is not.

## Milestone 7: Async Workflows And Higher-Level Primitives

Goal: move beyond synchronous request/response and make `postllm` useful for durable workflows.

Exit criteria:

- Long-running work can be submitted, polled, canceled, and observed.
- Conversations and prompt assets can be stored and managed as data.
- Evaluation and benchmarking are part of the repo, not afterthoughts.

Issues:

- [ ] `PL-045` Add an async job model for submit, poll, fetch result, and cancel.
- [ ] `PL-046` Add `NOTIFY` or event-hook support for async completions and streaming progress.
- [ ] `PL-047` Add conversation/session primitives for multi-turn workflows.
- [ ] `PL-048` Add durable prompt registries with versioning and metadata.
- [ ] `PL-049` Add evaluation datasets and scoring helpers for prompt and model regression testing.
- [ ] `PL-050` Add benchmark suites for latency, throughput, and memory across runtimes and model sizes.

## Milestone 8: Provider Coverage, Packaging, And Adoption

Goal: make `postllm` broadly usable, easy to install, and easy to trust.

Exit criteria:

- The extension supports more than one serious hosted path cleanly.
- CI and release automation cover supported versions.
- The project has a strong demo path, cookbook, and upgrade story.

Issues:

- [ ] `PL-051` Add an OpenAI Responses-style adapter in addition to chat-completions compatibility.
- [ ] `PL-052` Add HTTP-backed embeddings so hosted providers fit the same mental model as local Candle embeddings.
- [ ] `PL-053` Add native adapters for important providers that are not truly OpenAI-compatible.
- [ ] `PL-054` Add multimodal inputs and model feature flags for vision, JSON mode, reasoning, and tool use.
- [ ] `PL-055` Add compatibility tests across Ollama, llama.cpp, OpenAI, and at least one non-OpenAI hosted provider.
- [ ] `PL-056` Add a real CI matrix across supported PostgreSQL versions and major operating systems.
- [ ] `PL-057` Add release automation for extension artifacts, Docker images, and changelogs.
- [ ] `PL-058` Add migration-safe extension upgrade coverage.
- [ ] `PL-059` Add a cookbook with copy-paste examples for local chat, hosted chat, embeddings, RAG, structured outputs, and tools.
- [ ] `PL-060` Add a polished demo path and sample app that gets a new user from clone to success in under ten minutes.

## Release Gate: Code Quality And Maintainability

Goal: make the codebase look deliberate, readable, and professionally maintainable before public release.

This is a release gate, not a nice-to-have:

- Do not ship publicly until this section is cleared.
- The focus is reducing unnecessary complexity, duplication, and long hard-to-follow functions in core runtime, policy, and SQL-surface code.

Exit criteria:

- Core request/configuration/policy code has clear ownership boundaries and smaller units.
- New contributors can follow the runtime/configuration path without reading giant functions end to end.
- Error handling and policy enforcement are centralized instead of repeated in multiple layers.
- Tests validate behavior without relying on sprawling, repetitive setup code.
- Public-facing source reads like production code written on purpose, not accumulated patches.

Issues:

- [x] `PL-061` Refactor the session-settings and runtime-resolution path into smaller focused units with a clear data flow from GUCs to validated request settings.
- [x] `PL-062` Split hosted HTTP endpoint policy, provider inference, and discovery logic into a cohesive policy module so URL parsing and enforcement are not scattered across `guc`, `client`, and `backend`.
- [x] `PL-063` Reduce repetitive `Settings` construction and test fixture boilerplate across Rust unit tests and `pg_test` coverage with shared builders/helpers.
- [x] `PL-064` Break up long SQL entrypoint and helper functions in `src/lib.rs` so each function does one thing and cross-cutting concerns are pushed into narrower modules.
- [x] `PL-065` Standardize operator-policy code around one obvious pattern for secrets, permissions, network policy, quotas, and future governance controls.
- [ ] `PL-066` Add a maintainability pass focused on naming, comments, module boundaries, and deletion of dead or redundant code introduced during feature expansion.

## Recommended Ship Order

If this work needs to be broken into release trains, ship it in this order:

1. Milestone 1: Candle generation foundation.
2. Milestone 2: SQL ergonomics.
3. Milestone 3: structured outputs, tools, and streaming.
4. Milestone 4: retrieval and embedding workflow.
5. Milestone 6: production safety and governance.
6. Release Gate: code quality and maintainability.
7. Milestone 5: runtime and model operations.
8. Milestone 7: async workflows and higher-level primitives.
9. Milestone 8: provider coverage, packaging, and adoption.

Milestones 5 and 6 are intentionally separate. Runtime operations make local inference usable; production safety makes the extension deployable.

## What "Amazing" Looks Like

- A user can install `postllm` and succeed with local generation, hosted generation, embeddings, structured outputs, and RAG without reading the source code.
- The SQL API is small, coherent, and discoverable.
- Local and hosted runtimes share the same mental model and response shape.
- Operators can control secrets, quotas, logging, policies, and failure modes.
- The docs, tests, and demos make the project feel deliberate instead of experimental.
