# pgvector Integration

`postllm` composes directly with `pgvector`.

The basic pattern is:

1. Chunk raw text with `postllm.chunk_document(...)`
2. Generate embeddings with `postllm.embed(...)`
3. Cast the returned `real[]` to `vector(n)`
4. Store and index that `vector(n)` with `pgvector`
5. Retrieve the nearest chunks
6. Optionally rerank or hybrid-rank the candidates with `postllm.rerank(...)` or `postllm.hybrid_rank(...)`
7. Feed the final context back into `postllm.complete(...)` or `postllm.chat_text(...)`, or use `postllm.rag(...)` / `postllm.rag_text(...)` for a one-call path over candidate arrays

## 1. Enable Extensions And Check Dimensions

Enable both extensions in the target database:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS postllm;
```

Inspect the active embedding model before creating `vector(n)` columns:

```sql
SELECT postllm.embedding_model_info();
```

For the default `sentence-transformers/paraphrase-MiniLM-L3-v2` model today, the important values are:

- `dimension = 384`
- `max_sequence_length = 512`
- `pooling = 'mean'`
- `normalization.default = 'l2'`

If you switch embedding models, re-check `postllm.embedding_model_info(...)` and update every `vector(n)` declaration or cast to match the new dimension.

`dimension` is the final vector width that `postllm.embed(...)` returns after any SentenceTransformer pooling or optional Dense projection. That means models like `BAAI/bge-small-en-v1.5` can change `pooling`, and models like `sentence-transformers/distiluse-base-multilingual-cased-v2` can change both the encoder family and the final output dimension.

## 2. Create A Simple Retrieval Table

For a single embedding model, keep the schema simple:

```sql
CREATE TABLE doc_chunks (
    chunk_id text PRIMARY KEY,
    doc_id text NOT NULL,
    chunk_no integer NOT NULL,
    content text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    embedding vector(384) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (doc_id, chunk_no)
);
```

This is the best default if you only use one embedding model in a database or schema. `chunk_id` is the deterministic conflict key that lets repeated ingests upsert cleanly.

## 3. Chunk, Insert, Or Backfill Embeddings

`postllm.embed_document(...)` is the easiest way to turn raw source text into canonical chunk rows. It already applies chunking, carries metadata, computes a deterministic `chunk_id`, and returns `real[]` embeddings that you can cast into `vector(384)`:

```sql
INSERT INTO doc_chunks (chunk_id, doc_id, chunk_no, content, metadata, embedding)
SELECT
    row.chunk_id,
    row.doc_id,
    row.chunk_no,
    row.content,
    row.metadata,
    row.embedding::vector(384)
FROM postllm.embed_document(
    'vacuum-guide',
    'Autovacuum removes dead tuples and helps control table bloat. VACUUM can reclaim space and update visibility metadata.',
    '{"source":"manual"}'::jsonb,
    chunk_chars => 80,
    overlap_chars => 12
) AS row
ON CONFLICT (chunk_id) DO UPDATE
SET chunk_no = EXCLUDED.chunk_no,
    content = EXCLUDED.content,
    metadata = EXCLUDED.metadata,
    embedding = EXCLUDED.embedding;
```

Each returned `metadata` object keeps your original fields and adds `_postllm_chunk` with the 1-based chunk index plus source character offsets. That makes it easy to trace retrieval hits back to the source text without inventing a second metadata convention.

If you store embeddings as `real[]` instead of `vector(n)`, `postllm.ingest_document(...)` can run the whole upsert-and-prune cycle for you as long as the table exposes the canonical columns `chunk_id`, `doc_id`, `chunk_no`, `content`, `metadata`, and `embedding`.

For backfills from an existing table of raw documents:

```sql
INSERT INTO doc_chunks (chunk_id, doc_id, chunk_no, content, metadata, embedding)
SELECT
    row.chunk_id,
    row.doc_id,
    row.chunk_no,
    row.content,
    row.metadata,
    row.embedding::vector(384)
FROM staging_docs AS d
CROSS JOIN LATERAL postllm.embed_document(
    d.doc_id,
    d.body,
    COALESCE(d.metadata, '{}'::jsonb),
    chunk_chars => 800,
    overlap_chars => 120
) AS row
ON CONFLICT (chunk_id) DO UPDATE
SET chunk_no = EXCLUDED.chunk_no,
    content = EXCLUDED.content,
    metadata = EXCLUDED.metadata,
    embedding = EXCLUDED.embedding;
```

## 4. Exact Nearest-Neighbor Search

Because `postllm.embed(...)` normalizes vectors by default, inner product is a strong default for retrieval.

```sql
WITH query AS (
    SELECT postllm.embed(
        'How does autovacuum prevent table bloat?'
    )::vector(384) AS embedding
)
SELECT
    c.doc_id,
    c.chunk_no,
    c.content,
    -(c.embedding <#> q.embedding) AS similarity
FROM doc_chunks AS c
CROSS JOIN query AS q
ORDER BY c.embedding <#> q.embedding
LIMIT 5;
```

If you prefer cosine distance explicitly, switch both the query and index operator class to cosine:

```sql
ORDER BY c.embedding <=> q.embedding
```

For small tables, exact search with no ANN index is often good enough.

## 5. Add An HNSW Index

For larger tables, add an approximate nearest-neighbor index:

```sql
CREATE INDEX doc_chunks_embedding_ip_hnsw
ON doc_chunks
USING hnsw (embedding vector_ip_ops);
```

That index matches the inner-product query shown above. If you use cosine distance instead, use:

```sql
CREATE INDEX doc_chunks_embedding_cosine_hnsw
ON doc_chunks
USING hnsw (embedding vector_cosine_ops);
```

Practical guidance:

- Build the index after large initial loads when possible.
- Use `vector_ip_ops` when you keep `normalize => true`.
- Use `vector_cosine_ops` when you want cosine distance explicitly.
- Use `ivfflat` if it fits your operational constraints better, but HNSW is the simplest strong default.

## 6. Filtered Retrieval

If you filter and use HNSW, increase the candidate list when needed:

```sql
BEGIN;
SET LOCAL hnsw.ef_search = 100;

WITH query AS (
    SELECT postllm.embed(
        'What does VACUUM do?'
    )::vector(384) AS embedding
)
SELECT
    c.doc_id,
    c.chunk_no,
    c.content
FROM doc_chunks AS c
CROSS JOIN query AS q
WHERE c.metadata @> '{"source":"manual"}'::jsonb
ORDER BY c.embedding <#> q.embedding
LIMIT 5;

COMMIT;
```

For highly selective filters, partial indexes or partitioning can be a better fit than one global ANN index.

## 7. Rerank Retrieved Candidates

Vector similarity is a strong first-pass retriever, but it is often useful to rerank the top candidates before generation:

```sql
WITH query AS (
    SELECT postllm.embed(
        'How does autovacuum prevent table bloat?'
    )::vector(384) AS embedding
),
candidates AS (
    SELECT c.content
    FROM doc_chunks AS c
    CROSS JOIN query AS q
    ORDER BY c.embedding <#> q.embedding
    LIMIT 10
)
SELECT *
FROM postllm.rerank(
    'How does autovacuum prevent table bloat?',
    ARRAY(SELECT content FROM candidates),
    top_n => 5
);
```

`postllm.rerank(...)` returns a 1-based `rank`, a 1-based original `index` into the supplied `documents` array, the `document` text itself, and a relevance `score`. On the Candle runtime it uses the active local embedding model. On the `openai` runtime, point `postllm.base_url` at a rerank-compatible hosted endpoint.

## 8. Add Hybrid Retrieval

If you want semantic retrieval plus lexical confirmation in one SQL step, `postllm.hybrid_rank(...)` fuses semantic rerank results with `PostgreSQL` full-text keyword ranks through reciprocal rank fusion:

```sql
WITH query AS (
    SELECT postllm.embed(
        'How does autovacuum prevent table bloat?'
    )::vector(384) AS embedding
),
candidates AS (
    SELECT c.content
    FROM doc_chunks AS c
    CROSS JOIN query AS q
    ORDER BY c.embedding <#> q.embedding
    LIMIT 10
)
SELECT *
FROM postllm.hybrid_rank(
    'How does autovacuum prevent table bloat?',
    ARRAY(SELECT content FROM candidates),
    top_n => 5
);
```

`postllm.keyword_rank(...)` is the lexical half of that workflow when you want keyword-only scoring over a candidate array, and `postllm.rrf_score(...)` is the low-level primitive when you already have separate vector and keyword rowsets to fuse manually.

## 9. Generate An Answer From Retrieved Chunks

This is the simplest retrieval-plus-generation loop entirely in SQL:

```sql
WITH query AS (
    SELECT postllm.embed(
        'How does autovacuum prevent table bloat?'
    )::vector(384) AS embedding
),
candidates AS (
    SELECT
        row_number() OVER (ORDER BY c.embedding <#> q.embedding) AS rank,
        c.content
    FROM doc_chunks AS c
    CROSS JOIN query AS q
    ORDER BY c.embedding <#> q.embedding
    LIMIT 10
),
retrieved AS (
    SELECT *
    FROM postllm.hybrid_rank(
        'How does autovacuum prevent table bloat?',
        ARRAY(SELECT content FROM candidates ORDER BY rank),
        top_n => 5
    )
),
context AS (
    SELECT string_agg(
        format('[%s] %s', rank, document),
        E'\n\n'
        ORDER BY rank
    ) AS joined_context
    FROM retrieved
)
SELECT postllm.complete(
    prompt => format(
        'Answer the question using only the context below.\n\nQuestion: %s\n\nContext:\n%s',
        'How does autovacuum prevent table bloat?',
        context.joined_context
    ),
    system_prompt => 'You answer from retrieved PostgreSQL context and say when the context is insufficient.'
)
FROM context;
```

## 10. Use The One-Call RAG Helper

If you already have a candidate array, `postllm.rag(...)` and `postllm.rag_text(...)` can retrieve context, build the grounded prompt, and run generation in one SQL call:

```sql
WITH query AS (
    SELECT postllm.embed(
        'How does autovacuum prevent table bloat?'
    )::vector(384) AS embedding
),
candidates AS (
    SELECT c.content
    FROM doc_chunks AS c
    CROSS JOIN query AS q
    ORDER BY c.embedding <#> q.embedding
    LIMIT 10
)
SELECT postllm.rag_text(
    query => 'How does autovacuum prevent table bloat?',
    documents => ARRAY(SELECT content FROM candidates),
    retrieval => 'hybrid',
    top_n => 5,
    system_prompt => 'Answer from the retrieved PostgreSQL context and say when the context is insufficient.'
);
```

Use `retrieval => 'semantic'` for rerank-only retrieval, `retrieval => 'keyword'` for lexical-only retrieval, or omit it to default to hybrid retrieval. `postllm.rag(...)` returns the selected context rows, rendered prompt, final answer, and raw response JSON when you want observability instead of text only.

## 11. Multi-Model Storage Pattern

If you need multiple embedding models in one table, store the raw array plus `model_id`, and index an expression per model:

```sql
CREATE TABLE embeddings (
    id bigserial PRIMARY KEY,
    model_id text NOT NULL,
    content text NOT NULL,
    embedding real[] NOT NULL
);

ALTER TABLE embeddings
ADD CHECK (vector_dims(embedding::vector) = 384);

CREATE INDEX embeddings_minilm_ip_hnsw
ON embeddings
USING hnsw ((embedding::vector(384)) vector_ip_ops)
WHERE model_id = 'sentence-transformers/paraphrase-MiniLM-L3-v2';
```

That pattern is useful if you want to preserve the original array representation, migrate models gradually, or maintain more than one embedding space.

## Recommended Default

If you want the shortest path that works well:

1. Keep `normalize => true`
2. Store embeddings in `vector(384)`
3. Use `vector_ip_ops`
4. Query with `<#>`
5. Add an HNSW index once the table is large enough to need ANN search

That matches the current default local embedding model and minimizes guesswork.
