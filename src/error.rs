#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use pgrx::spi;
use reqwest::StatusCode;

/// Shared error type for `postllm`.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// The extension was configured incorrectly.
    #[error("postllm is not configured correctly: {0}")]
    Config(String),

    /// The caller supplied an invalid argument.
    #[error("postllm received an invalid argument: {0}")]
    InvalidInput(String),

    /// An HTTP request failed before the provider returned a response.
    #[error(
        "failed to call the configured LLM endpoint: {0}; fix: verify postllm.base_url, network access, and postllm.timeout_ms"
    )]
    Http(#[from] reqwest::Error),

    /// Reading the HTTP response body failed before the provider payload could be consumed.
    #[error(
        "failed to read the LLM response body: {0}; fix: verify postllm.base_url, network access, and postllm.timeout_ms"
    )]
    HttpRead(String),

    /// Accessing a local model artifact failed.
    #[error("failed to access local model files: {0}")]
    Io(#[from] std::io::Error),

    /// The provider returned JSON that could not be parsed.
    #[error(
        "failed to parse the LLM response as JSON: {0}; fix: verify postllm.base_url points to an OpenAI-compatible JSON endpoint"
    )]
    Json(#[from] serde_json::Error),

    /// A background worker or internal coordination path failed unexpectedly.
    #[error("postllm internal error: {0}")]
    Internal(String),

    /// The current `PostgreSQL` query was cancelled.
    #[error("query was cancelled by PostgreSQL")]
    Interrupted,

    /// Downloading or resolving local model assets failed.
    #[error(
        "failed to prepare local Candle model assets: {0}; fix: verify postllm.model, network access, and postllm.candle_cache_dir"
    )]
    ModelAssets(String),

    /// Local Candle inference or tensor handling failed.
    #[error(
        "failed to run the local Candle model: {0}; fix: verify the selected local model is supported, the request fits the model limits, and the backend has enough memory"
    )]
    Candle(String),

    /// A `PostgreSQL` SPI command failed.
    #[error("failed to update PostgreSQL session settings: {0}")]
    Spi(#[from] spi::Error),

    /// The selected runtime exists conceptually but has not been implemented yet.
    #[error("postllm backend is not available: {0}")]
    Unsupported(String),

    /// The provider returned a non-success status code.
    #[error(
        "LLM endpoint returned HTTP {status}: {body}; fix: verify postllm.base_url, postllm.api_key, and provider model availability"
    )]
    Upstream { status: StatusCode, body: String },

    /// The provider response did not include a usable text payload.
    #[error(
        "LLM response was missing choices[0].message.content; fix: verify postllm.base_url points to a chat-completions-compatible endpoint or inspect the raw provider response"
    )]
    MalformedResponse,

    /// The provider returned a malformed reranking payload.
    #[error(
        "LLM rerank response was malformed: {0}; fix: verify postllm.base_url points to a rerank-compatible endpoint and inspect the raw provider response"
    )]
    MalformedRerankResponse(String),

    /// The provider returned a malformed server-sent event stream.
    #[error(
        "LLM streaming response was malformed: {0}; fix: verify postllm.base_url points to a chat-completions-compatible SSE endpoint or use non-streaming postllm.chat/postllm.complete"
    )]
    MalformedStream(String),

    /// A structured-output request returned content that could not be decoded as valid JSON.
    #[error(
        "failed to decode structured output: {0}; fix: use a provider/model that supports response_format json_schema, try temperature => 0.0, or increase max_tokens"
    )]
    StructuredOutput(String),
}

impl Error {
    /// Builds a consistent invalid-argument error with an actionable fix.
    pub(crate) fn invalid_argument(
        argument: &str,
        problem: impl Into<String>,
        fix: impl Into<String>,
    ) -> Self {
        Self::InvalidInput(format!(
            "argument '{argument}' {}; fix: {}",
            problem.into(),
            fix.into()
        ))
    }

    /// Builds a consistent invalid configuration error with an actionable fix.
    pub(crate) fn invalid_setting(
        name: &str,
        problem: impl Into<String>,
        fix: impl Into<String>,
    ) -> Self {
        Self::Config(format!("{name} {}; fix: {}", problem.into(), fix.into()))
    }

    /// Builds a missing-setting configuration error with an actionable fix.
    pub(crate) fn missing_setting(name: &str, fix: impl Into<String>) -> Self {
        Self::Config(format!("{name} is not set; fix: {}", fix.into()))
    }
}

/// Shared result type for `postllm`.
pub(crate) type Result<T> = core::result::Result<T, Error>;
