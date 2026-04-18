#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::enum_parser;
use crate::error::{Error, Result};
use serde_json::{Map, Value, json};

const POSTLLM_METADATA_KEY: &str = "_postllm";

/// Supported LLM runtime families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Runtime {
    /// OpenAI-compatible HTTP chat completions, including Ollama and llama-server.
    OpenAi,
    /// In-process local execution powered by Candle.
    Candle,
}

impl Runtime {
    pub(crate) const OPENAI: &'static str = "openai";
    pub(crate) const CANDLE: &'static str = "candle";
    const VARIANTS: [(&'static str, Self); 2] =
        [(Self::OPENAI, Self::OpenAi), (Self::CANDLE, Self::Candle)];

    /// Returns the canonical configuration string for this runtime.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => Self::OPENAI,
            Self::Candle => Self::CANDLE,
        }
    }

    /// Parses a user-supplied runtime string.
    pub(crate) fn parse(value: &str) -> Result<Self> {
        enum_parser::parse_case_insensitive_with_default_error("runtime", value, &Self::VARIANTS)
    }
}

/// Preferred execution device for local Candle requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CandleDevice {
    /// Prefer an available local accelerator and fall back to CPU when none are usable.
    Auto,
    /// Force CPU execution.
    Cpu,
    /// Force CUDA execution on device 0.
    Cuda,
    /// Force Metal execution on device 0.
    Metal,
}

impl CandleDevice {
    pub(crate) const AUTO: &'static str = "auto";
    pub(crate) const CPU: &'static str = "cpu";
    pub(crate) const CUDA: &'static str = "cuda";
    pub(crate) const METAL: &'static str = "metal";
    pub(crate) const ACCEPTED_VALUES: &'static str = "'auto', 'cpu', 'cuda', or 'metal'";
    const VARIANTS: [(&'static str, Self); 4] = [
        (Self::AUTO, Self::Auto),
        (Self::CPU, Self::Cpu),
        (Self::CUDA, Self::Cuda),
        (Self::METAL, Self::Metal),
    ];

    /// Returns the canonical configuration string for this device preference.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => Self::AUTO,
            Self::Cpu => Self::CPU,
            Self::Cuda => Self::CUDA,
            Self::Metal => Self::METAL,
        }
    }

    /// Parses a user-supplied Candle device preference.
    #[must_use]
    pub(crate) fn parse(value: &str) -> Option<Self> {
        enum_parser::parse_case_insensitive_optional(value, &Self::VARIANTS)
    }
}

/// Resolved runtime settings for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Settings {
    /// Selected runtime family.
    pub(crate) runtime: Runtime,
    /// Default model or local model identifier.
    pub(crate) model: String,
    /// Optional OpenAI-compatible endpoint.
    pub(crate) base_url: Option<String>,
    /// Optional HTTP bearer token.
    pub(crate) api_key: Option<String>,
    /// Request timeout in milliseconds.
    pub(crate) timeout_ms: u64,
    /// Maximum number of transient HTTP retries for hosted runtimes.
    pub(crate) max_retries: u32,
    /// Base backoff between transient HTTP retries in milliseconds.
    pub(crate) retry_backoff_ms: u64,
    /// Optional operator-enforced cap on concurrent model requests across `PostgreSQL` backends.
    pub(crate) request_max_concurrency: u32,
    /// Optional host or host:port allowlist for hosted HTTP runtimes.
    pub(crate) http_allowed_hosts: Vec<String>,
    /// Optional provider safelist for hosted HTTP runtimes.
    pub(crate) http_allowed_providers: Vec<String>,
    /// Optional cache directory for Candle-managed model assets.
    pub(crate) candle_cache_dir: Option<String>,
    /// Whether Candle should refuse all network fetches and use cached artifacts only.
    pub(crate) candle_offline: bool,
    /// Preferred execution device for local Candle requests.
    pub(crate) candle_device: CandleDevice,
    /// Optional operator-enforced cap on local Candle tokenized input size per request value.
    pub(crate) candle_max_input_tokens: u32,
    /// Optional operator-enforced cap on concurrent local Candle requests across backends.
    pub(crate) candle_max_concurrency: u32,
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::{CandleDevice, Runtime, Settings};

    #[derive(Debug, Clone)]
    pub(crate) struct SettingsBuilder {
        settings: Settings,
    }

    impl Default for SettingsBuilder {
        fn default() -> Self {
            Self {
                settings: Settings {
                    runtime: Runtime::OpenAi,
                    model: "llama3.2".to_owned(),
                    base_url: Some("http://127.0.0.1:11434/v1/chat/completions".to_owned()),
                    api_key: None,
                    timeout_ms: 30_000,
                    max_retries: 2,
                    retry_backoff_ms: 250,
                    request_max_concurrency: 0,
                    http_allowed_hosts: Vec::new(),
                    http_allowed_providers: Vec::new(),
                    candle_cache_dir: None,
                    candle_offline: false,
                    candle_device: CandleDevice::Auto,
                    candle_max_input_tokens: 0,
                    candle_max_concurrency: 0,
                },
            }
        }
    }

    impl SettingsBuilder {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        pub(crate) fn runtime(mut self, runtime: Runtime) -> Self {
            self.settings.runtime = runtime;
            self
        }

        pub(crate) fn model(mut self, model: &str) -> Self {
            self.settings.model = model.to_owned();
            self
        }

        pub(crate) fn base_url(mut self, base_url: &str) -> Self {
            self.settings.base_url = Some(base_url.to_owned());
            self
        }

        pub(crate) fn no_base_url(mut self) -> Self {
            self.settings.base_url = None;
            self
        }

        pub(crate) fn api_key(mut self, api_key: Option<&str>) -> Self {
            self.settings.api_key = api_key.map(str::to_owned);
            self
        }

        pub(crate) fn retries(mut self, max_retries: u32, retry_backoff_ms: u64) -> Self {
            self.settings.max_retries = max_retries;
            self.settings.retry_backoff_ms = retry_backoff_ms;
            self
        }

        pub(crate) fn build(self) -> Settings {
            self.settings
        }
    }
}

/// Optional controls applied to a chat request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RequestOptions {
    /// Sampling temperature.
    pub(crate) temperature: f64,
    /// Optional maximum token budget for the completion.
    pub(crate) max_tokens: Option<i32>,
}

/// A single reranked document with its original index and relevance score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RerankResult {
    /// The zero-based index of the input document.
    pub(crate) index: usize,
    /// The relevance score assigned by the runtime.
    pub(crate) score: f64,
}

/// The coarse-grained features `postllm` may expose across runtimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Feature {
    /// Multi-message chat completion requests.
    Chat,
    /// Single-prompt text completion requests.
    Complete,
    /// Embedding generation.
    Embeddings,
    /// Document reranking.
    Reranking,
    /// Tool or function calling.
    Tools,
    /// Structured output generation.
    StructuredOutputs,
    /// Incremental streaming responses.
    Streaming,
    /// Image or multimodal message inputs.
    MultimodalInputs,
}

impl Feature {
    const ALL: [Self; 8] = [
        Self::Chat,
        Self::Complete,
        Self::Embeddings,
        Self::Reranking,
        Self::Tools,
        Self::StructuredOutputs,
        Self::Streaming,
        Self::MultimodalInputs,
    ];

    const fn key(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Complete => "complete",
            Self::Embeddings => "embeddings",
            Self::Reranking => "reranking",
            Self::Tools => "tools",
            Self::StructuredOutputs => "structured_outputs",
            Self::Streaming => "streaming",
            Self::MultimodalInputs => "multimodal_inputs",
        }
    }

    const fn sql_target(self) -> &'static str {
        match self {
            Self::Chat => "postllm.chat",
            Self::Complete => "postllm.complete",
            Self::Embeddings => "postllm.embed/postllm.embed_many",
            Self::Reranking => "postllm.rerank",
            Self::Tools => "postllm.chat_tools/postllm.complete_tools",
            Self::StructuredOutputs => "postllm.chat_structured/postllm.complete_structured",
            Self::Streaming => "postllm.chat_stream/postllm.complete_stream",
            Self::MultimodalInputs => "multimodal inputs",
        }
    }
}

/// The current feature availability implied by the active settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapabilitySnapshot {
    runtime_setting: Option<String>,
    runtime: Option<Runtime>,
    runtime_error: Option<String>,
    model: Option<String>,
    embedding_model: Option<String>,
}

impl CapabilitySnapshot {
    /// Builds a capability snapshot from validated request settings.
    #[must_use]
    pub(crate) fn from_settings(settings: &Settings, embedding_model: Option<&str>) -> Self {
        Self {
            runtime_setting: Some(settings.runtime.as_str().to_owned()),
            runtime: Some(settings.runtime),
            runtime_error: None,
            model: Some(settings.model.clone()),
            embedding_model: embedding_model.map(str::to_owned),
        }
    }

    /// Builds a best-effort capability snapshot from raw GUC values.
    #[must_use]
    pub(crate) fn from_raw(
        runtime_setting: Option<String>,
        model: Option<String>,
        embedding_model: Option<String>,
    ) -> Self {
        let (runtime, runtime_error) = parse_runtime_setting(runtime_setting.as_deref());

        Self {
            runtime_setting,
            runtime,
            runtime_error,
            model,
            embedding_model,
        }
    }

    /// Returns the current capability snapshot as JSON.
    #[must_use]
    pub(crate) fn snapshot(&self) -> Value {
        let features = Feature::ALL
            .into_iter()
            .map(|feature| (feature.key().to_owned(), self.feature(feature).snapshot()))
            .collect::<Map<String, Value>>();

        json!({
            "runtime": self.runtime_setting,
            "model": self.model,
            "embedding_model": self.embedding_model,
            "features": features,
        })
    }

    /// Requires that a feature be available under the current settings.
    pub(crate) fn require(&self, feature: Feature) -> Result<()> {
        let support = self.feature(feature);
        if support.available {
            Ok(())
        } else {
            Err(Error::Unsupported(support.error_message(feature)))
        }
    }

    fn feature(&self, feature: Feature) -> FeatureSupport {
        match feature {
            Feature::Chat | Feature::Complete => self.generation_support(feature),
            Feature::Embeddings => self.embedding_support(),
            Feature::Reranking => self.reranking_support(),
            Feature::Tools => self.tool_support(),
            Feature::StructuredOutputs => self.structured_output_support(),
            Feature::Streaming => self.streaming_support(),
            Feature::MultimodalInputs => self.multimodal_input_support(),
        }
    }

    fn generation_support(&self, feature: Feature) -> FeatureSupport {
        match (self.runtime, self.model.as_deref()) {
            (Some(Runtime::OpenAi), Some(model)) => {
                FeatureSupport::available(self.runtime_setting.clone(), Some(model.to_owned()))
            }
            (Some(Runtime::Candle), Some(model)) => {
                let availability = crate::candle::generation_availability(model, feature);

                if availability.available {
                    FeatureSupport::available(self.runtime_setting.clone(), Some(model.to_owned()))
                } else {
                    FeatureSupport::unsupported(
                        self.runtime_setting.clone(),
                        Some(model.to_owned()),
                        availability.reason.unwrap_or_else(|| {
                            "the local Candle generation runtime is not available".to_owned()
                        }),
                    )
                    .with_supported_models(availability.supported_models)
                }
            }
            (Some(_), None) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                None,
                "postllm.model is not set".to_owned(),
            ),
            (None, _) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                self.model.clone(),
                self.runtime_error
                    .clone()
                    .unwrap_or_else(|| "postllm.runtime is not set".to_owned()),
            ),
        }
    }

    fn embedding_support(&self) -> FeatureSupport {
        match (self.runtime, self.embedding_model.as_deref()) {
            (Some(runtime), Some(model)) => {
                FeatureSupport::available(Some(runtime.as_str().to_owned()), Some(model.to_owned()))
            }
            (Some(runtime), None) => FeatureSupport::unsupported(
                Some(runtime.as_str().to_owned()),
                None,
                "postllm.embedding_model is not set".to_owned(),
            ),
            (None, _) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                self.embedding_model.clone(),
                self.runtime_error
                    .clone()
                    .unwrap_or_else(|| "postllm.runtime is not set".to_owned()),
            ),
        }
    }

    fn reranking_support(&self) -> FeatureSupport {
        match self.runtime {
            Some(Runtime::OpenAi) => self.model.as_deref().map_or_else(
                || {
                    FeatureSupport::unsupported(
                        self.runtime_setting.clone(),
                        None,
                        "postllm.model is not set".to_owned(),
                    )
                },
                |model| {
                    FeatureSupport::available(self.runtime_setting.clone(), Some(model.to_owned()))
                },
            ),
            Some(Runtime::Candle) => self.embedding_model.as_deref().map_or_else(
                || {
                    FeatureSupport::unsupported(
                        Some(Runtime::CANDLE.to_owned()),
                        None,
                        "postllm.embedding_model is not set".to_owned(),
                    )
                },
                |model| {
                    FeatureSupport::available(
                        Some(Runtime::CANDLE.to_owned()),
                        Some(model.to_owned()),
                    )
                },
            ),
            None => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                self.model.clone(),
                self.runtime_error
                    .clone()
                    .unwrap_or_else(|| "postllm.runtime is not set".to_owned()),
            ),
        }
    }

    fn multimodal_input_support(&self) -> FeatureSupport {
        match (self.runtime, self.model.as_deref()) {
            (Some(Runtime::OpenAi), Some(model)) => {
                FeatureSupport::available(self.runtime_setting.clone(), Some(model.to_owned()))
            }
            (Some(Runtime::Candle), Some(model)) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                Some(model.to_owned()),
                "multimodal inputs are not implemented by the local Candle runtime".to_owned(),
            ),
            (Some(_), None) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                None,
                "postllm.model is not set".to_owned(),
            ),
            (None, _) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                self.model.clone(),
                self.runtime_error
                    .clone()
                    .unwrap_or_else(|| "postllm.runtime is not set".to_owned()),
            ),
        }
    }

    fn structured_output_support(&self) -> FeatureSupport {
        match (self.runtime, self.model.as_deref()) {
            (Some(Runtime::OpenAi), Some(model)) => {
                FeatureSupport::available(self.runtime_setting.clone(), Some(model.to_owned()))
            }
            (Some(Runtime::Candle), Some(model)) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                Some(model.to_owned()),
                "structured outputs are not implemented by the local Candle runtime".to_owned(),
            ),
            (Some(_), None) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                None,
                "postllm.model is not set".to_owned(),
            ),
            (None, _) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                self.model.clone(),
                self.runtime_error
                    .clone()
                    .unwrap_or_else(|| "postllm.runtime is not set".to_owned()),
            ),
        }
    }

    fn tool_support(&self) -> FeatureSupport {
        match (self.runtime, self.model.as_deref()) {
            (Some(Runtime::OpenAi), Some(model)) => {
                FeatureSupport::available(self.runtime_setting.clone(), Some(model.to_owned()))
            }
            (Some(Runtime::Candle), Some(model)) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                Some(model.to_owned()),
                "tool-calling requests are not implemented by the local Candle runtime".to_owned(),
            ),
            (Some(_), None) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                None,
                "postllm.model is not set".to_owned(),
            ),
            (None, _) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                self.model.clone(),
                self.runtime_error
                    .clone()
                    .unwrap_or_else(|| "postllm.runtime is not set".to_owned()),
            ),
        }
    }

    fn streaming_support(&self) -> FeatureSupport {
        match (self.runtime, self.model.as_deref()) {
            (Some(Runtime::OpenAi), Some(model)) => {
                FeatureSupport::available(self.runtime_setting.clone(), Some(model.to_owned()))
            }
            (Some(Runtime::Candle), Some(model)) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                Some(model.to_owned()),
                "streaming is not implemented by the local Candle runtime".to_owned(),
            ),
            (Some(_), None) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                None,
                "postllm.model is not set".to_owned(),
            ),
            (None, _) => FeatureSupport::unsupported(
                self.runtime_setting.clone(),
                self.model.clone(),
                self.runtime_error
                    .clone()
                    .unwrap_or_else(|| "postllm.runtime is not set".to_owned()),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FeatureSupport {
    available: bool,
    runtime: Option<String>,
    model: Option<String>,
    reason: Option<String>,
    supported_models: Option<Vec<String>>,
}

impl FeatureSupport {
    const fn available(runtime: Option<String>, model: Option<String>) -> Self {
        Self {
            available: true,
            runtime,
            model,
            reason: None,
            supported_models: None,
        }
    }

    const fn unsupported(runtime: Option<String>, model: Option<String>, reason: String) -> Self {
        Self {
            available: false,
            runtime,
            model,
            reason: Some(reason),
            supported_models: None,
        }
    }

    fn with_supported_models(mut self, supported_models: Vec<String>) -> Self {
        if !supported_models.is_empty() {
            self.supported_models = Some(supported_models);
        }

        self
    }

    fn snapshot(&self) -> Value {
        let mut object = Map::from_iter([
            ("available".to_owned(), json!(self.available)),
            ("runtime".to_owned(), json!(self.runtime)),
            ("model".to_owned(), json!(self.model)),
        ]);

        if let Some(reason) = self.reason.as_deref() {
            object.insert("reason".to_owned(), json!(reason));
        }

        if let Some(supported_models) = self.supported_models.as_ref() {
            object.insert("supported_models".to_owned(), json!(supported_models));
        }

        Value::Object(object)
    }

    fn error_message(&self, feature: Feature) -> String {
        let runtime = self.runtime.as_deref().unwrap_or("<unset>");
        let model = self.model.as_deref().unwrap_or("<unset>");
        let mut message = format!(
            "{} is not available for runtime '{runtime}' and model '{model}'",
            feature.sql_target(),
        );

        if let Some(reason) = self.reason.as_deref() {
            message.push_str(": ");
            message.push_str(reason);
        }

        if let Some(supported_models) = self.supported_models.as_ref() {
            message.push_str("; supported models: ");
            message.push_str(&supported_models.join(", "));
        }

        if let Some(fix) = self.suggested_fix(feature) {
            message.push_str("; fix: ");
            message.push_str(&fix);
        }

        message
    }

    fn suggested_fix(&self, feature: Feature) -> Option<String> {
        let reason = self.reason.as_deref();

        if self.runtime.is_none()
            || reason == Some("postllm.runtime is not set")
            || reason.is_some_and(|reason| reason.starts_with("postllm.runtime must be "))
        {
            return Some("SET postllm.runtime = 'openai' or 'candle'".to_owned());
        }

        if reason == Some("postllm.model is not set") {
            return Some(format!(
                "SET postllm.model = 'llama3.2' or pass model => '...' to {}",
                feature.sql_target()
            ));
        }

        if reason == Some("postllm.embedding_model is not set") {
            return Some(match feature {
                Feature::Reranking => match self.runtime.as_deref() {
                    Some(Runtime::OPENAI) => {
                        "SET postllm.embedding_model = 'text-embedding-3-small' or pass model => '...' to postllm.rerank"
                            .to_owned()
                    }
                    Some(Runtime::CANDLE) | None => {
                        "SET postllm.embedding_model = 'sentence-transformers/paraphrase-MiniLM-L3-v2' or pass model => '...' to postllm.rerank"
                            .to_owned()
                    }
                    Some(_) => unreachable!("feature support stores normalized runtime names"),
                },
                Feature::Chat
                | Feature::Complete
                | Feature::Embeddings
                | Feature::Tools
                | Feature::StructuredOutputs
                | Feature::Streaming
                | Feature::MultimodalInputs => match self.runtime.as_deref() {
                    Some(Runtime::OPENAI) => {
                        "SET postllm.embedding_model = 'text-embedding-3-small' or pass model => '...' to postllm.embed/postllm.embed_many"
                            .to_owned()
                    }
                    Some(Runtime::CANDLE) | None => {
                        "SET postllm.embedding_model = 'sentence-transformers/paraphrase-MiniLM-L3-v2' or pass model => '...' to postllm.embed/postllm.embed_many"
                            .to_owned()
                    }
                    Some(_) => unreachable!("feature support stores normalized runtime names"),
                },
            });
        }

        match (self.runtime.as_deref(), feature) {
            (Some(Runtime::CANDLE), Feature::Chat | Feature::Complete) => {
                self.supported_models.as_ref().map(|supported_models| {
                    format!(
                        "set postllm.model to one of: {} or switch postllm.runtime to 'openai' for hosted generation",
                        supported_models.join(", ")
                    )
                })
            }
            (Some(Runtime::CANDLE), Feature::MultimodalInputs) => Some(
                "switch postllm.runtime to 'openai' for image inputs or send text-only messages to Candle"
                    .to_owned(),
            ),
            (Some(Runtime::CANDLE), Feature::StructuredOutputs) => Some(
                "switch postllm.runtime to 'openai' for schema-constrained generation".to_owned(),
            ),
            (Some(Runtime::CANDLE), Feature::Tools) => Some(
                "switch postllm.runtime to 'openai' and use postllm.chat_tools/postllm.complete_tools for hosted tool calling"
                    .to_owned(),
            ),
            (Some(Runtime::CANDLE), Feature::Streaming) => Some(
                "switch postllm.runtime to 'openai' and use postllm.chat_stream/postllm.complete_stream for hosted streaming"
                    .to_owned(),
            ),
            (_, Feature::StructuredOutputs) => {
                Some("use postllm.chat_structured/postllm.complete_structured on the openai runtime".to_owned())
            }
            (_, Feature::Tools) => Some(
                "use postllm.chat_tools/postllm.complete_tools with postllm.function_tool(...) on the openai runtime"
                    .to_owned(),
            ),
            (_, Feature::Streaming) => Some(
                "use postllm.chat_stream/postllm.complete_stream on the openai runtime".to_owned(),
            ),
            _ => None,
        }
    }
}

fn parse_runtime_setting(runtime_setting: Option<&str>) -> (Option<Runtime>, Option<String>) {
    runtime_setting.map_or_else(
        || (None, Some("postllm.runtime is not set".to_owned())),
        |value| {
            Runtime::parse(value).map_or_else(
                |_| {
                    (
                        None,
                        Some(format!(
                            "postllm.runtime must be '{}' or '{}', got '{}'",
                            Runtime::OPENAI,
                            Runtime::CANDLE,
                            value.trim().to_ascii_lowercase(),
                        )),
                    )
                },
                |runtime| (Some(runtime), None),
            )
        },
    )
}

/// Executes a chat request against the selected runtime.
pub(crate) fn chat_response(
    settings: &Settings,
    messages: &[Value],
    options: RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
) -> Result<Value> {
    let response = match settings.runtime {
        Runtime::OpenAi => crate::client::chat_response(
            settings,
            messages,
            options,
            response_format,
            tools,
            tool_choice,
        ),
        Runtime::Candle => crate::candle::chat_response(
            settings,
            messages,
            options,
            response_format,
            tools,
            tool_choice,
        ),
    }?;

    Ok(normalize_response_metadata(response, settings))
}

/// Executes a streaming chat request against the selected runtime.
pub(crate) fn chat_stream_response(
    settings: &Settings,
    messages: &[Value],
    options: RequestOptions,
) -> Result<Vec<Value>> {
    let events = match settings.runtime {
        Runtime::OpenAi => crate::client::chat_stream_response(settings, messages, options),
        Runtime::Candle => crate::candle::chat_stream_response(settings, messages, options),
    }?;

    Ok(events
        .into_iter()
        .map(|event| normalize_stream_event(event, settings))
        .collect())
}

/// Executes a rerank request against the selected runtime.
pub(crate) fn rerank_response(
    settings: &Settings,
    query: &str,
    documents: &[String],
    top_n: Option<usize>,
) -> Result<Vec<RerankResult>> {
    match settings.runtime {
        Runtime::OpenAi => crate::client::rerank_response(settings, query, documents, top_n),
        Runtime::Candle => crate::candle::rerank(settings, query, documents, top_n),
    }
}

/// Executes an embedding request against the selected runtime.
pub(crate) fn embed_response(
    settings: &Settings,
    inputs: &[String],
    normalize: bool,
) -> Result<Vec<Vec<f32>>> {
    match settings.runtime {
        Runtime::OpenAi => crate::client::embed_response(settings, inputs, normalize),
        Runtime::Candle => crate::candle::embed(settings, inputs, normalize),
    }
}

/// Extracts the first textual completion from a runtime response object.
pub(crate) fn extract_text(response: &Value) -> Result<String> {
    crate::client::extract_text(response)
}

/// Returns a normalized usage object for a runtime response.
#[must_use]
pub(crate) fn usage(response: &Value) -> Value {
    postllm_metadata(response)
        .and_then(|metadata| metadata.get("usage"))
        .filter(|usage| usage.is_object())
        .cloned()
        .unwrap_or_else(|| normalized_usage(response))
}

/// Returns the first choice object for a runtime response.
pub(crate) fn choice(response: &Value, index: usize) -> Result<Value> {
    let Some(choices) = response.get("choices").and_then(Value::as_array) else {
        return Err(Error::MalformedResponse);
    };

    choices.get(index).cloned().ok_or_else(|| {
        Error::invalid_argument(
            "index",
            format!(
                "with value {index} is out of range for {} available choices",
                choices.len()
            ),
            format!(
                "pass a value between 0 and {}",
                choices.len().saturating_sub(1)
            ),
        )
    })
}

/// Returns the normalized finish reason for a runtime response when one is available.
#[must_use]
pub(crate) fn finish_reason(response: &Value) -> Option<String> {
    postllm_metadata(response)
        .and_then(|metadata| metadata.get("finish_reason"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            normalized_finish_reason(response)
                .as_str()
                .map(str::to_owned)
        })
}

/// Returns the normalized first-choice text delta for a stream event when one is available.
#[must_use]
pub(crate) fn stream_text_delta(event: &Value) -> Option<String> {
    postllm_metadata(event)
        .and_then(|metadata| metadata.get("content_delta"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| raw_stream_text_delta(event))
}

fn normalize_response_metadata(mut response: Value, settings: &Settings) -> Value {
    let metadata = response_metadata(&response, settings);

    if let Some(object) = response.as_object_mut() {
        object.insert(POSTLLM_METADATA_KEY.to_owned(), metadata);
    }

    response
}

fn normalize_stream_event(mut event: Value, settings: &Settings) -> Value {
    let metadata = json!({
        "runtime": settings.runtime.as_str(),
        "provider": crate::http_policy::provider_identity(settings),
        "base_url": settings.base_url,
        "model": normalized_model(&event, settings),
        "content_delta": raw_stream_text_delta(&event),
        "finish_reason": stream_finish_reason(&event),
        "usage": normalized_usage(&event),
    });

    if let Some(object) = event.as_object_mut() {
        object.insert(POSTLLM_METADATA_KEY.to_owned(), metadata);
    }

    event
}

fn response_metadata(response: &Value, settings: &Settings) -> Value {
    json!({
        "runtime": settings.runtime.as_str(),
        "provider": crate::http_policy::provider_identity(settings),
        "base_url": settings.base_url,
        "model": normalized_model(response, settings),
        "finish_reason": finish_reason(response),
        "usage": usage(response),
    })
}

fn postllm_metadata(response: &Value) -> Option<&Value> {
    response
        .get(POSTLLM_METADATA_KEY)
        .filter(|metadata| metadata.is_object())
}

fn normalized_model(response: &Value, settings: &Settings) -> String {
    response
        .get("model")
        .and_then(Value::as_str)
        .map_or_else(|| settings.model.clone(), str::to_owned)
}

fn normalized_finish_reason(response: &Value) -> Value {
    response
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("finish_reason"))
        .filter(|value| value.is_string())
        .cloned()
        .unwrap_or(Value::Null)
}

fn stream_finish_reason(event: &Value) -> Value {
    event
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("finish_reason"))
        .filter(|value| value.is_string())
        .cloned()
        .unwrap_or(Value::Null)
}

fn normalized_usage(response: &Value) -> Value {
    let mut prompt_tokens = usage_token_count(response, "prompt_tokens");
    let mut completion_tokens = usage_token_count(response, "completion_tokens");
    let mut total_tokens = usage_token_count(response, "total_tokens");

    if total_tokens.is_none() {
        total_tokens = prompt_tokens
            .zip(completion_tokens)
            .and_then(|(prompt, completion)| prompt.checked_add(completion));
    }

    if prompt_tokens.is_none() {
        prompt_tokens = total_tokens
            .zip(completion_tokens)
            .and_then(|(total, completion)| total.checked_sub(completion));
    }

    if completion_tokens.is_none() {
        completion_tokens = total_tokens
            .zip(prompt_tokens)
            .and_then(|(total, prompt)| total.checked_sub(prompt));
    }

    json!({
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
    })
}

fn usage_token_count(response: &Value, key: &str) -> Option<u64> {
    response
        .get("usage")
        .and_then(|usage| usage.get(key))
        .and_then(Value::as_u64)
}

fn raw_stream_text_delta(event: &Value) -> Option<String> {
    let choice = event
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())?;

    choice
        .get("delta")
        .and_then(|delta| delta.get("content"))
        .and_then(content_text)
        .or_else(|| {
            choice
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

fn content_text(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => Some(text.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<String>();

            (!text.is_empty()).then_some(text)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => None,
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "capability snapshots are easiest to verify by direct JSON indexing in tests"
)]
mod tests {
    use super::{
        CandleDevice, CapabilitySnapshot, Feature, RequestOptions, Runtime, Settings, choice,
        finish_reason, normalize_response_metadata, normalize_stream_event, stream_text_delta,
        usage,
    };
    use crate::backend::test_support::SettingsBuilder;
    use serde_json::json;

    fn settings(runtime: Runtime) -> Settings {
        SettingsBuilder::new().runtime(runtime).build()
    }

    #[test]
    fn capability_snapshot_should_report_openai_chat_and_local_embeddings() {
        let snapshot = CapabilitySnapshot::from_settings(
            &settings(Runtime::OpenAi),
            Some("sentence-transformers/paraphrase-MiniLM-L3-v2"),
        )
        .snapshot();

        assert_eq!(snapshot["features"]["chat"]["available"], true);
        assert_eq!(snapshot["features"]["chat"]["runtime"], "openai");
        assert_eq!(snapshot["features"]["tools"]["available"], true);
        assert_eq!(snapshot["features"]["tools"]["runtime"], "openai");
        assert_eq!(
            snapshot["features"]["structured_outputs"]["available"],
            true
        );
        assert_eq!(
            snapshot["features"]["structured_outputs"]["runtime"],
            "openai"
        );
        assert_eq!(snapshot["features"]["streaming"]["available"], true);
        assert_eq!(snapshot["features"]["streaming"]["runtime"], "openai");
        assert_eq!(snapshot["features"]["embeddings"]["available"], true);
        assert_eq!(snapshot["features"]["embeddings"]["runtime"], "candle");
        assert_eq!(snapshot["features"]["reranking"]["available"], true);
        assert_eq!(snapshot["features"]["reranking"]["runtime"], "openai");
        assert_eq!(snapshot["features"]["reranking"]["model"], "llama3.2");
        assert_eq!(snapshot["features"]["multimodal_inputs"]["available"], true);
    }

    #[test]
    fn capability_snapshot_should_reject_unknown_candle_generation_models() {
        let snapshot = CapabilitySnapshot::from_settings(
            &settings(Runtime::Candle),
            Some("sentence-transformers/paraphrase-MiniLM-L3-v2"),
        );

        let error = snapshot
            .require(Feature::Chat)
            .expect_err("candle generation should be gated");

        assert_eq!(
            error.to_string(),
            "postllm backend is not available: postllm.chat is not available for runtime 'candle' and model 'llama3.2': model 'llama3.2' is not in the local Candle generation starter set; supported starter models are Qwen/Qwen2.5-0.5B-Instruct, Qwen/Qwen2.5-1.5B-Instruct; supported models: Qwen/Qwen2.5-0.5B-Instruct, Qwen/Qwen2.5-1.5B-Instruct; fix: set postllm.model to one of: Qwen/Qwen2.5-0.5B-Instruct, Qwen/Qwen2.5-1.5B-Instruct or switch postllm.runtime to 'openai' for hosted generation"
        );
    }

    #[test]
    fn capability_snapshot_should_report_registered_candle_starter_models() {
        let snapshot = CapabilitySnapshot::from_settings(
            &SettingsBuilder::new()
                .runtime(Runtime::Candle)
                .model("Qwen/Qwen2.5-0.5B-Instruct")
                .no_base_url()
                .build(),
            Some("sentence-transformers/paraphrase-MiniLM-L3-v2"),
        )
        .snapshot();

        assert_eq!(snapshot["features"]["chat"]["available"], true);
        assert_eq!(snapshot["features"]["chat"]["runtime"], "candle");
        assert_eq!(
            snapshot["features"]["chat"]["model"],
            "Qwen/Qwen2.5-0.5B-Instruct"
        );
        assert_eq!(snapshot["features"]["chat"].get("reason"), None);
        assert_eq!(snapshot["features"]["complete"]["available"], true);
        assert_eq!(snapshot["features"]["complete"]["runtime"], "candle");
        assert_eq!(
            snapshot["features"]["complete"]["model"],
            "Qwen/Qwen2.5-0.5B-Instruct"
        );
        assert_eq!(snapshot["features"]["complete"].get("reason"), None);
        assert_eq!(snapshot["features"]["reranking"]["available"], true);
        assert_eq!(snapshot["features"]["reranking"]["runtime"], "candle");
        assert_eq!(
            snapshot["features"]["reranking"]["model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
        assert_eq!(snapshot["features"]["reranking"].get("reason"), None);
    }

    #[test]
    fn normalize_response_metadata_should_add_postllm_fields_for_openai_runtime() {
        let response = normalize_response_metadata(
            json!({
                "id": "chatcmpl-123",
                "object": "chat.completion",
                "model": "provider-model",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "hello"},
                    "finish_reason": "stop",
                }],
                "usage": {
                    "prompt_tokens": 12,
                    "completion_tokens": 4,
                },
            }),
            &settings(Runtime::OpenAi),
        );

        assert_eq!(
            response["_postllm"],
            json!({
                "runtime": "openai",
                "provider": "ollama",
                "base_url": "http://127.0.0.1:11434/v1/chat/completions",
                "model": "provider-model",
                "finish_reason": "stop",
                "usage": {
                    "prompt_tokens": 12,
                    "completion_tokens": 4,
                    "total_tokens": 16,
                },
            })
        );
        assert_eq!(response["choices"][0]["message"]["content"], "hello");
    }

    #[test]
    fn normalize_response_metadata_should_add_postllm_fields_for_candle_runtime() {
        let response = normalize_response_metadata(
            json!({
                "id": "chatcmpl-candle-1",
                "object": "chat.completion",
                "model": "Qwen/Qwen2.5-0.5B-Instruct",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "hi"},
                    "finish_reason": "length",
                }],
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 7,
                    "total_tokens": 16,
                },
            }),
            &SettingsBuilder::new()
                .runtime(Runtime::Candle)
                .model("Qwen/Qwen2.5-0.5B-Instruct")
                .no_base_url()
                .build(),
        );

        assert_eq!(
            response["_postllm"],
            json!({
                "runtime": "candle",
                "provider": "candle",
                "base_url": null,
                "model": "Qwen/Qwen2.5-0.5B-Instruct",
                "finish_reason": "length",
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 7,
                    "total_tokens": 16,
                },
            })
        );
    }

    #[test]
    fn normalize_stream_event_should_add_postllm_fields_for_openai_runtime() {
        let event = normalize_stream_event(
            json!({
                "id": "chatcmpl-123",
                "object": "chat.completion.chunk",
                "model": "provider-model",
                "choices": [{
                    "index": 0,
                    "delta": {"content": "hel"},
                    "finish_reason": null,
                }],
            }),
            &settings(Runtime::OpenAi),
        );

        assert_eq!(
            event["_postllm"],
            json!({
                "runtime": "openai",
                "provider": "ollama",
                "base_url": "http://127.0.0.1:11434/v1/chat/completions",
                "model": "provider-model",
                "content_delta": "hel",
                "finish_reason": null,
                "usage": {
                    "prompt_tokens": null,
                    "completion_tokens": null,
                    "total_tokens": null,
                },
            })
        );
    }

    #[test]
    fn stream_text_delta_should_prefer_normalized_postllm_metadata() {
        let delta = stream_text_delta(&json!({
            "_postllm": {
                "content_delta": "hello"
            },
            "choices": [{
                "delta": {"content": "ignored"}
            }]
        }))
        .expect("normalized metadata should expose the text delta");

        assert_eq!(delta, "hello");
    }

    #[test]
    fn usage_should_prefer_normalized_postllm_metadata() {
        let response = json!({
            "choices": [{
                "message": {"content": "hello"},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 4,
                "completion_tokens": 2,
                "total_tokens": 6
            },
            "_postllm": {
                "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 3,
                    "total_tokens": 12
                }
            }
        });

        assert_eq!(
            usage(&response),
            json!({
                "prompt_tokens": 9,
                "completion_tokens": 3,
                "total_tokens": 12
            })
        );
    }

    #[test]
    fn usage_should_derive_total_tokens_from_raw_provider_usage() {
        let response = json!({
            "choices": [{
                "message": {"content": "hello"},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 4,
                "completion_tokens": 2
            }
        });

        assert_eq!(
            usage(&response),
            json!({
                "prompt_tokens": 4,
                "completion_tokens": 2,
                "total_tokens": 6
            })
        );
    }

    #[test]
    fn finish_reason_should_prefer_normalized_postllm_metadata() {
        let response = json!({
            "choices": [{
                "message": {"content": "hello"},
                "finish_reason": "length"
            }],
            "_postllm": {
                "finish_reason": "stop"
            }
        });

        assert_eq!(finish_reason(&response), Some("stop".to_owned()));
    }

    #[test]
    fn choice_should_return_the_requested_choice() {
        let response = json!({
            "choices": [
                {"index": 0, "message": {"content": "first"}},
                {"index": 1, "message": {"content": "second"}}
            ]
        });

        assert_eq!(
            choice(&response, 1).expect("the second choice should exist"),
            json!({"index": 1, "message": {"content": "second"}})
        );
    }

    #[test]
    fn choice_should_reject_out_of_range_indexes() {
        let response = json!({
            "choices": [
                {"index": 0, "message": {"content": "first"}}
            ]
        });

        let error = choice(&response, 1).expect_err("out-of-range choice should fail");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'index' with value 1 is out of range for 1 available choices; fix: pass a value between 0 and 0"
        );
    }

    #[test]
    fn capability_snapshot_should_surface_invalid_runtime_settings() {
        let snapshot = CapabilitySnapshot::from_raw(
            Some("bogus".to_owned()),
            Some("llama3.2".to_owned()),
            Some("sentence-transformers/paraphrase-MiniLM-L3-v2".to_owned()),
        )
        .snapshot();

        assert_eq!(snapshot["features"]["chat"]["available"], false);
        assert_eq!(
            snapshot["features"]["chat"]["reason"],
            "postllm.runtime must be 'openai' or 'candle', got 'bogus'"
        );
    }

    #[test]
    fn request_options_should_remain_copyable() {
        let options = RequestOptions {
            temperature: 0.2,
            max_tokens: Some(32),
        };

        let copied = options;

        assert!((copied.temperature - 0.2).abs() < f64::EPSILON);
        assert_eq!(copied.max_tokens, Some(32));
    }

    #[test]
    fn runtime_parse_should_accept_supported_values() {
        assert_eq!(
            Runtime::parse("openai").expect("openai should parse"),
            Runtime::OpenAi
        );
        assert_eq!(
            Runtime::parse("OpenAI").expect("case-insensitive parsing should work"),
            Runtime::OpenAi
        );
        assert_eq!(
            Runtime::parse("CANDLE").expect("case-insensitive parsing should work"),
            Runtime::Candle
        );
    }

    #[test]
    fn runtime_parse_should_reject_unknown_values() {
        let error =
            Runtime::parse("invalid").expect_err("unknown runtime values should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'runtime' must be one of 'openai' or 'candle', got 'invalid'; fix: pass runtime => 'openai' or 'candle'"
        );
    }

    #[test]
    fn candle_device_parse_should_accept_supported_values() {
        assert_eq!(CandleDevice::parse("cpu"), Some(CandleDevice::Cpu));
        assert_eq!(CandleDevice::parse(" Cpu "), Some(CandleDevice::Cpu));
        assert_eq!(CandleDevice::parse("TeSp"), None);
        assert_eq!(CandleDevice::parse("cuda"), Some(CandleDevice::Cuda));
    }
}
