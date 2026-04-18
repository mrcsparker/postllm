#![allow(
    clippy::needless_pass_by_value,
    clippy::redundant_pub_crate,
    reason = "pgrx materializes SQL-facing values as owned Rust types and these wrappers are crate-visible by design"
)]

use pgrx::JsonB;
use pgrx::iter::TableIterator;
use serde_json::json;

// SQL-facing retrieval and embedding entrypoints.
//
// Wrappers are intentionally small so request shapes stay close to the SQL
// contracts and map directly to implementation behavior in internal domain
// modules.

pub(crate) fn embed(
    input: &str,
    model: pgrx::default!(Option<&str>, "NULL"),
    normalize: bool,
) -> Vec<f32> {
    crate::finish_vector_result(crate::embed_impl(input, model, normalize))
}

pub(crate) fn embed_many(
    inputs: Vec<String>,
    model: pgrx::default!(Option<&str>, "NULL"),
    normalize: bool,
) -> JsonB {
    crate::finish_json_result(
        crate::embed_many_impl(&inputs, model, normalize).map(|vectors| json!(vectors)),
    )
}

pub(crate) fn embedding_model_info(model: pgrx::default!(Option<&str>, "NULL")) -> JsonB {
    crate::finish_json_result(crate::embedding_model_info_impl(model))
}

pub(crate) fn embed_document(
    doc_id: &str,
    input: &str,
    metadata: pgrx::default!(Option<JsonB>, "NULL"),
    chunk_chars: i32,
    overlap_chars: i32,
    model: pgrx::default!(Option<&str>, "NULL"),
    normalize: bool,
) -> TableIterator<
    'static,
    (
        pgrx::name!(chunk_id, String),
        pgrx::name!(doc_id, String),
        pgrx::name!(chunk_no, i32),
        pgrx::name!(content, String),
        pgrx::name!(metadata, JsonB),
        pgrx::name!(embedding, Vec<f32>),
    ),
> {
    crate::finish_embedding_document_rows_result(crate::embed_document_impl(
        crate::DocumentEmbeddingRequest {
            doc_id,
            input,
            metadata: metadata.as_ref().map(|metadata| &metadata.0),
            chunk_chars,
            overlap_chars,
            model,
            normalize,
        },
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the SQL surface intentionally keeps ingestion configuration flat instead of forcing callers through a JSON argument"
)]
pub(crate) fn ingest_document(
    target_table: &str,
    doc_id: &str,
    input: &str,
    metadata: pgrx::default!(Option<JsonB>, "NULL"),
    chunk_chars: i32,
    overlap_chars: i32,
    model: pgrx::default!(Option<&str>, "NULL"),
    normalize: bool,
    delete_missing: bool,
) -> JsonB {
    crate::finish_json_result(crate::ingest_document_impl(
        target_table,
        crate::DocumentEmbeddingRequest {
            doc_id,
            input,
            metadata: metadata.as_ref().map(|metadata| &metadata.0),
            chunk_chars,
            overlap_chars,
            model,
            normalize,
        },
        delete_missing,
    ))
}

pub(crate) fn chunk_text(input: &str, chunk_chars: i32, overlap_chars: i32) -> Vec<String> {
    crate::finish_text_array_result(crate::chunk_text_impl(input, chunk_chars, overlap_chars))
}

pub(crate) fn chunk_document(
    input: &str,
    metadata: pgrx::default!(Option<JsonB>, "NULL"),
    chunk_chars: i32,
    overlap_chars: i32,
) -> TableIterator<
    'static,
    (
        pgrx::name!(index, i32),
        pgrx::name!(chunk, String),
        pgrx::name!(metadata, JsonB),
    ),
> {
    crate::finish_chunk_rows_result(crate::chunk_document_impl(
        input,
        metadata.as_ref().map(|metadata| &metadata.0),
        chunk_chars,
        overlap_chars,
    ))
}

#[allow(
    clippy::type_complexity,
    reason = "pgrx SQL generation requires the exported TableIterator shape inline"
)]
pub(crate) fn keyword_rank(
    query: &str,
    documents: Vec<String>,
    top_n: pgrx::default!(Option<i32>, "NULL"),
    text_search_config: pgrx::default!(Option<&str>, "NULL"),
    normalization: i32,
) -> TableIterator<
    'static,
    (
        pgrx::name!(rank, i32),
        pgrx::name!(index, i32),
        pgrx::name!(document, String),
        pgrx::name!(score, f64),
    ),
> {
    crate::finish_rank_rows_result(crate::keyword_rank_impl(
        query,
        &documents,
        top_n,
        text_search_config,
        normalization,
    ))
}

pub(crate) fn rrf_score(
    semantic_rank: pgrx::default!(Option<i32>, "NULL"),
    keyword_rank: pgrx::default!(Option<i32>, "NULL"),
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
) -> f64 {
    crate::finish_float_result(crate::rrf_score_impl(
        semantic_rank,
        keyword_rank,
        semantic_weight,
        keyword_weight,
        rrf_k,
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the SQL surface keeps hybrid retrieval controls flat instead of forcing a JSON wrapper"
)]
#[allow(
    clippy::type_complexity,
    reason = "pgrx SQL generation requires the exported TableIterator shape inline"
)]
pub(crate) fn hybrid_rank(
    query: &str,
    documents: Vec<String>,
    top_n: pgrx::default!(Option<i32>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    text_search_config: pgrx::default!(Option<&str>, "NULL"),
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
    normalization: i32,
) -> TableIterator<
    'static,
    (
        pgrx::name!(rank, i32),
        pgrx::name!(index, i32),
        pgrx::name!(document, String),
        pgrx::name!(score, f64),
        pgrx::name!(semantic_rank, Option<i32>),
        pgrx::name!(keyword_rank, Option<i32>),
        pgrx::name!(semantic_score, Option<f64>),
        pgrx::name!(keyword_score, Option<f64>),
    ),
> {
    crate::finish_hybrid_rank_rows_result(crate::hybrid_rank_impl(
        query,
        &documents,
        top_n,
        model,
        text_search_config,
        semantic_weight,
        keyword_weight,
        rrf_k,
        normalization,
    ))
}

#[allow(
    clippy::type_complexity,
    reason = "pgrx SQL generation requires the exported TableIterator shape inline"
)]
pub(crate) fn rerank(
    query: &str,
    documents: Vec<String>,
    top_n: pgrx::default!(Option<i32>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        pgrx::name!(rank, i32),
        pgrx::name!(index, i32),
        pgrx::name!(document, String),
        pgrx::name!(score, f64),
    ),
> {
    crate::finish_rank_rows_result(crate::rerank_impl(query, &documents, top_n, model))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the SQL surface keeps the batteries-included RAG helper flat instead of forcing a JSON wrapper"
)]
pub(crate) fn rag(
    query: &str,
    documents: Vec<String>,
    system_prompt: pgrx::default!(Option<&str>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    retrieval: pgrx::default!(Option<&str>, "NULL"),
    retrieval_model: pgrx::default!(Option<&str>, "NULL"),
    top_n: i32,
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
    text_search_config: pgrx::default!(Option<&str>, "NULL"),
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
    normalization: i32,
) -> JsonB {
    crate::finish_json_result(crate::rag_impl(
        query,
        &documents,
        system_prompt,
        model,
        retrieval,
        retrieval_model,
        top_n,
        temperature,
        max_tokens,
        text_search_config,
        semantic_weight,
        keyword_weight,
        rrf_k,
        normalization,
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the SQL surface keeps the batteries-included RAG helper flat instead of forcing a JSON wrapper"
)]
pub(crate) fn rag_text(
    query: &str,
    documents: Vec<String>,
    system_prompt: pgrx::default!(Option<&str>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    retrieval: pgrx::default!(Option<&str>, "NULL"),
    retrieval_model: pgrx::default!(Option<&str>, "NULL"),
    top_n: i32,
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
    text_search_config: pgrx::default!(Option<&str>, "NULL"),
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
    normalization: i32,
) -> String {
    crate::finish_text_result(crate::rag_text_impl(
        query,
        &documents,
        system_prompt,
        model,
        retrieval,
        retrieval_model,
        top_n,
        temperature,
        max_tokens,
        text_search_config,
        semantic_weight,
        keyword_weight,
        rrf_k,
        normalization,
    ))
}
