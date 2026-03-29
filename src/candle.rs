#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::backend::{CandleDevice, Feature, RequestOptions, RerankResult, Settings};
use crate::error::{Error, Result};
use candle_core::safetensors::BufferedSafetensors;
use candle_core::{DType, Device, Error as CandleCoreError, Shape, Tensor};
use candle_nn::var_builder::SimpleBackend;
use candle_nn::{Init, Linear, Module, VarBuilder, linear_b};
use candle_transformers::generation::LogitsProcessor;
use candle_transformers::models::bert::{BertModel, Config as BertConfig, DTYPE as BERT_DTYPE};
use candle_transformers::models::distilbert::{Config as DistilBertConfig, DistilBertModel};
use candle_transformers::models::qwen2::{Config as Qwen2Config, ModelForCausalLM as Qwen2Model};
use candle_transformers::models::xlm_roberta::{Config as XLMRobertaConfig, XLMRobertaModel};
use fs2::FileExt;
use hf_hub::api::sync::{Api, ApiBuilder, ApiRepo};
use hf_hub::{Cache, Repo};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokenizers::{Encoding, Tokenizer};

thread_local! {
    static EMBEDDING_MODELS: RefCell<HashMap<ModelCacheKey, Rc<EmbeddingRuntime>>> =
        RefCell::new(HashMap::new());
    static GENERATION_MODELS: RefCell<HashMap<ModelCacheKey, Rc<RefCell<GenerationRuntime>>>> =
        RefCell::new(HashMap::new());
    static LOCAL_RUNTIME_LIMITS: RefCell<Option<LocalRuntimeExecution>> = const { RefCell::new(None) };
}

const DEFAULT_MAX_GENERATION_TOKENS: usize = 256;
const LOCAL_CONCURRENCY_POLL_INTERVAL_MS: u64 = 10;
const QWEN_DEFAULT_SYSTEM_PROMPT: &str =
    "You are Qwen, created by Alibaba Cloud.\nYou are a helpful assistant.";
const QWEN_IM_END_TOKEN: &str = "<|im_end|>";
const QWEN_END_OF_TEXT_TOKEN: &str = "<|endoftext|>";
const EMBEDDING_NORMALIZATION_DEFAULT: &str = "l2";
const EMBEDDING_NORMALIZATION_SUPPORTED: [&str; 2] = ["l2", "none"];

const KNOWN_EMBEDDING_MODELS: [EmbeddingModelSpec; 5] = [
    EmbeddingModelSpec {
        model_id: "sentence-transformers/paraphrase-MiniLM-L3-v2",
        architecture: EmbeddingArchitecture::Bert,
        dimension: 384,
        max_sequence_length: 512,
        pooling: "mean",
        projection_in_dimension: None,
        projection_activation: None,
    },
    EmbeddingModelSpec {
        model_id: "sentence-transformers/all-MiniLM-L6-v2",
        architecture: EmbeddingArchitecture::Bert,
        dimension: 384,
        max_sequence_length: 512,
        pooling: "mean",
        projection_in_dimension: None,
        projection_activation: None,
    },
    EmbeddingModelSpec {
        model_id: "intfloat/e5-small-v2",
        architecture: EmbeddingArchitecture::Bert,
        dimension: 384,
        max_sequence_length: 512,
        pooling: "mean",
        projection_in_dimension: None,
        projection_activation: None,
    },
    EmbeddingModelSpec {
        model_id: "BAAI/bge-small-en-v1.5",
        architecture: EmbeddingArchitecture::Bert,
        dimension: 384,
        max_sequence_length: 512,
        pooling: "cls",
        projection_in_dimension: None,
        projection_activation: None,
    },
    EmbeddingModelSpec {
        model_id: "sentence-transformers/distiluse-base-multilingual-cased-v2",
        architecture: EmbeddingArchitecture::DistilBert,
        dimension: 512,
        max_sequence_length: 512,
        pooling: "mean",
        projection_in_dimension: Some(768),
        projection_activation: Some("tanh"),
    },
];

const STARTER_GENERATION_MODELS: [GenerationModelSpec; 2] = [
    GenerationModelSpec {
        model_id: "Qwen/Qwen2.5-0.5B-Instruct",
        family: GenerationFamily::Qwen2_5,
        chat_template: ChatTemplate::ChatMl,
    },
    GenerationModelSpec {
        model_id: "Qwen/Qwen2.5-1.5B-Instruct",
        family: GenerationFamily::Qwen2_5,
        chat_template: ChatTemplate::ChatMl,
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GenerationFamily {
    Qwen2_5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalModelLane {
    Embedding,
    Generation,
}

impl LocalModelLane {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Embedding => "embedding",
            Self::Generation => "generation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalModelEvictionScope {
    Memory,
    Disk,
    All,
}

impl LocalModelEvictionScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Disk => "disk",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmbeddingArchitecture {
    Bert,
    DistilBert,
    XlmRoberta,
}

impl EmbeddingArchitecture {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Bert => "bert",
            Self::DistilBert => "distilbert",
            Self::XlmRoberta => "xlm-roberta",
        }
    }

    fn from_config_json(model_id: &str, config_json: &Value) -> Result<Self> {
        if let Some(model_type) = config_json.get("model_type").and_then(Value::as_str) {
            if let Some(architecture) = Self::from_model_type(model_type) {
                return Ok(architecture);
            }

            return Err(Error::Unsupported(format!(
                "local Candle embeddings for model '{model_id}' do not support model_type '{model_type}'; supported encoder families are bert, distilbert, and xlm-roberta"
            )));
        }

        if let Some(architecture_name) = config_json
            .get("architectures")
            .and_then(Value::as_array)
            .and_then(|architectures| architectures.first())
            .and_then(Value::as_str)
        {
            if let Some(architecture) = Self::from_architecture_name(architecture_name) {
                return Ok(architecture);
            }

            return Err(Error::Unsupported(format!(
                "local Candle embeddings for model '{model_id}' do not support architecture '{architecture_name}'; supported encoder families are BertModel, DistilBertModel, and XLMRobertaModel"
            )));
        }

        Ok(Self::Bert)
    }

    fn from_model_type(model_type: &str) -> Option<Self> {
        match model_type.trim().to_ascii_lowercase().as_str() {
            "bert" => Some(Self::Bert),
            "distilbert" => Some(Self::DistilBert),
            "xlm-roberta" | "xlm_roberta" => Some(Self::XlmRoberta),
            _ => None,
        }
    }

    fn from_architecture_name(architecture_name: &str) -> Option<Self> {
        match architecture_name.trim().to_ascii_lowercase().as_str() {
            "bertmodel" => Some(Self::Bert),
            "distilbertmodel" => Some(Self::DistilBert),
            "xlmrobertamodel" => Some(Self::XlmRoberta),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EmbeddingModelSpec {
    model_id: &'static str,
    architecture: EmbeddingArchitecture,
    dimension: usize,
    max_sequence_length: usize,
    pooling: &'static str,
    projection_in_dimension: Option<usize>,
    projection_activation: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChatTemplate {
    ChatMl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GenerationModelSpec {
    pub(crate) model_id: &'static str,
    family: GenerationFamily,
    chat_template: ChatTemplate,
}

impl GenerationModelSpec {
    fn render_prompt(self, messages: &[Value]) -> Result<String> {
        match self.chat_template {
            ChatTemplate::ChatMl => render_chatml_prompt(messages),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GenerationAvailability {
    pub(crate) available: bool,
    pub(crate) reason: Option<String>,
    pub(crate) supported_models: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EmbeddingModelInfo {
    model_id: String,
    architecture: String,
    dimension: usize,
    max_sequence_length: usize,
    pooling: String,
    projection: Option<EmbeddingProjectionInfo>,
}

impl EmbeddingModelInfo {
    fn from_spec(spec: &EmbeddingModelSpec) -> Self {
        Self {
            model_id: spec.model_id.to_owned(),
            architecture: spec.architecture.as_str().to_owned(),
            dimension: spec.dimension,
            max_sequence_length: spec.max_sequence_length,
            pooling: spec.pooling.to_owned(),
            projection: spec
                .projection_in_dimension
                .map(|in_dimension| EmbeddingProjectionInfo {
                    in_dimension,
                    out_dimension: spec.dimension,
                    activation: spec.projection_activation.unwrap_or("identity").to_owned(),
                }),
        }
    }

    fn from_layout(model_id: &str, layout: &EmbeddingLayout) -> Self {
        Self {
            model_id: model_id.to_owned(),
            architecture: layout.architecture.as_str().to_owned(),
            dimension: layout.output_dimension(),
            max_sequence_length: layout.max_sequence_length,
            pooling: layout.pooling.label(),
            projection: layout
                .projection
                .as_ref()
                .map(EmbeddingProjectionInfo::from_spec),
        }
    }

    fn snapshot(&self) -> Value {
        let mut snapshot = json!({
            "runtime": "candle",
            "model": self.model_id,
            "architecture": self.architecture,
            "dimension": self.dimension,
            "max_sequence_length": self.max_sequence_length,
            "pooling": self.pooling,
            "normalization": {
                "default": EMBEDDING_NORMALIZATION_DEFAULT,
                "supported": EMBEDDING_NORMALIZATION_SUPPORTED,
            }
        });

        if let Some(projection) = &self.projection {
            if let Some(snapshot_object) = snapshot.as_object_mut() {
                snapshot_object.insert(
                    "projection".to_owned(),
                    json!({
                        "in_dimension": projection.in_dimension,
                        "out_dimension": projection.out_dimension,
                        "activation": projection.activation,
                    }),
                );
            }
        }

        snapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmbeddingProjectionInfo {
    in_dimension: usize,
    out_dimension: usize,
    activation: String,
}

impl EmbeddingProjectionInfo {
    fn from_spec(spec: &EmbeddingProjectionSpec) -> Self {
        Self {
            in_dimension: spec.in_features,
            out_dimension: spec.out_features,
            activation: spec.activation.as_str().to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmbeddingLayout {
    architecture: EmbeddingArchitecture,
    hidden_size: usize,
    max_sequence_length: usize,
    pooling: EmbeddingPooling,
    projection: Option<EmbeddingProjectionSpec>,
}

impl EmbeddingLayout {
    fn output_dimension(&self) -> usize {
        self.projection.as_ref().map_or_else(
            || self.pooling.output_dimension(self.hidden_size),
            |projection| projection.out_features,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmbeddingProjectionSpec {
    path: String,
    in_features: usize,
    out_features: usize,
    bias: bool,
    activation: EmbeddingProjectionActivation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmbeddingProjectionActivation {
    Identity,
    Tanh,
}

impl EmbeddingProjectionActivation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Tanh => "tanh",
        }
    }

    fn from_sentence_transformer(value: Option<&str>) -> Result<Self> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            None => Ok(Self::Identity),
            Some(value)
                if value.eq_ignore_ascii_case("identity")
                    || value.ends_with(".Identity")
                    || value.ends_with(".Linear") =>
            {
                Ok(Self::Identity)
            }
            Some(value) if value.eq_ignore_ascii_case("tanh") || value.ends_with(".Tanh") => {
                Ok(Self::Tanh)
            }
            Some(value) => Err(Error::Unsupported(format!(
                "local Candle embeddings do not support sentence-transformer Dense activation '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "sentence-transformer pooling enables several independent output modes"
)]
struct EmbeddingPooling {
    cls_token: bool,
    mean_tokens: bool,
    max_tokens: bool,
    mean_sqrt_len_tokens: bool,
}

impl EmbeddingPooling {
    const fn default_mean() -> Self {
        Self {
            cls_token: false,
            mean_tokens: true,
            max_tokens: false,
            mean_sqrt_len_tokens: false,
        }
    }

    fn from_sentence_transformer_config(config: &SentenceTransformerPoolingConfig) -> Result<Self> {
        if config.pooling_mode_weightedmean_tokens || config.pooling_mode_lasttoken {
            return Err(Error::Unsupported(
                "local Candle embeddings do not yet support weighted-mean or last-token sentence-transformer pooling".to_owned(),
            ));
        }

        let pooling = Self {
            cls_token: config.pooling_mode_cls_token,
            mean_tokens: config.pooling_mode_mean_tokens,
            max_tokens: config.pooling_mode_max_tokens,
            mean_sqrt_len_tokens: config.pooling_mode_mean_sqrt_len_tokens,
        };

        if pooling.enabled_mode_count() == 0 {
            return Err(Error::ModelAssets(
                "sentence-transformer pooling config did not enable any supported pooling modes"
                    .to_owned(),
            ));
        }

        Ok(pooling)
    }

    fn enabled_mode_count(&self) -> usize {
        usize::from(self.cls_token)
            + usize::from(self.mean_tokens)
            + usize::from(self.max_tokens)
            + usize::from(self.mean_sqrt_len_tokens)
    }

    fn output_dimension(&self, hidden_size: usize) -> usize {
        hidden_size.saturating_mul(self.enabled_mode_count())
    }

    fn label(&self) -> String {
        let mut labels = Vec::with_capacity(self.enabled_mode_count());

        if self.cls_token {
            labels.push("cls");
        }
        if self.max_tokens {
            labels.push("max");
        }
        if self.mean_tokens {
            labels.push("mean");
        }
        if self.mean_sqrt_len_tokens {
            labels.push("mean_sqrt_len");
        }

        labels.join("+")
    }

    fn pool(&self, token_embeddings: &[Vec<f32>], attention_mask: &[u32]) -> Result<Vec<f32>> {
        let Some(hidden_size) = token_embeddings.first().map(Vec::len) else {
            return Err(Error::Candle(
                "the local embedding backend returned no token embeddings".to_owned(),
            ));
        };
        let mut pooled = Vec::with_capacity(self.output_dimension(hidden_size));

        if self.cls_token {
            pooled.extend(cls_pool(token_embeddings)?);
        }
        if self.max_tokens {
            pooled.extend(max_pool(token_embeddings, attention_mask)?);
        }
        if self.mean_tokens {
            pooled.extend(mean_pool(token_embeddings, attention_mask)?);
        }
        if self.mean_sqrt_len_tokens {
            pooled.extend(mean_sqrt_len_pool(token_embeddings, attention_mask)?);
        }

        Ok(pooled)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SentenceTransformerModule {
    #[serde(default)]
    path: String,
    #[serde(rename = "type", default)]
    module_type: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "this mirrors sentence-transformer pooling config flags directly"
)]
struct SentenceTransformerPoolingConfig {
    #[serde(default)]
    pooling_mode_cls_token: bool,
    #[serde(default)]
    pooling_mode_mean_tokens: bool,
    #[serde(default)]
    pooling_mode_max_tokens: bool,
    #[serde(default)]
    pooling_mode_mean_sqrt_len_tokens: bool,
    #[serde(default)]
    pooling_mode_weightedmean_tokens: bool,
    #[serde(default)]
    pooling_mode_lasttoken: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct SentenceTransformerDenseConfig {
    in_features: usize,
    out_features: usize,
    #[serde(default)]
    bias: bool,
    #[serde(default)]
    activation_function: Option<String>,
}

impl GenerationAvailability {
    fn available() -> Self {
        Self {
            available: true,
            reason: None,
            supported_models: supported_generation_models(),
        }
    }

    fn unsupported(reason: String) -> Self {
        Self {
            available: false,
            reason: Some(reason),
            supported_models: supported_generation_models(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ModelCacheKey {
    model_id: String,
    cache_dir: Option<String>,
    device_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalModelCacheEntry {
    path: String,
    bytes: u64,
    integrity: LocalModelFileIntegrity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalModelFileIntegrity {
    status: LocalModelFileIntegrityStatus,
    algorithm: Option<&'static str>,
    expected: Option<String>,
    actual: Option<String>,
}

impl LocalModelFileIntegrity {
    const fn unchecked() -> Self {
        Self {
            status: LocalModelFileIntegrityStatus::Unchecked,
            algorithm: None,
            expected: None,
            actual: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalModelFileIntegrityStatus {
    Verified,
    Mismatch,
    Unchecked,
}

impl LocalModelFileIntegrityStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Mismatch => "mismatch",
            Self::Unchecked => "unchecked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct LocalModelIntegritySummary {
    verified: usize,
    mismatched: usize,
    unchecked: usize,
}

impl LocalModelIntegritySummary {
    const fn ok(self) -> bool {
        self.mismatched == 0
    }

    const fn status(self) -> &'static str {
        if self.mismatched > 0 {
            "mismatch"
        } else if self.verified > 0 && self.unchecked == 0 {
            "verified"
        } else if self.verified > 0 {
            "partial"
        } else {
            "unchecked"
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolvedCandleDeviceKind {
    Cpu,
    Cuda,
    Metal,
}

impl ResolvedCandleDeviceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Cuda => "cuda",
            Self::Metal => "metal",
        }
    }

    const fn accelerated(self) -> bool {
        !matches!(self, Self::Cpu)
    }

    const fn ordinal(self) -> Option<usize> {
        if self.accelerated() { Some(0) } else { None }
    }
}

#[derive(Debug, Clone)]
struct ResolvedCandleDevice {
    kind: ResolvedCandleDeviceKind,
    device: Device,
}

impl ResolvedCandleDevice {
    const fn cache_key(&self) -> &'static str {
        self.kind.as_str()
    }
}

#[derive(Debug, Clone)]
struct CandleDeviceProbe {
    requested: CandleDevice,
    resolved: Option<ResolvedCandleDeviceKind>,
    reason: Option<String>,
}

impl CandleDeviceProbe {
    fn cache_key(&self) -> Option<&'static str> {
        self.resolved.map(ResolvedCandleDeviceKind::as_str)
    }

    fn snapshot(&self) -> Value {
        json!({
            "requested": self.requested.as_str(),
            "resolved": self.resolved.map(ResolvedCandleDeviceKind::as_str),
            "available": self.resolved.is_some(),
            "accelerated": self.resolved.is_some_and(ResolvedCandleDeviceKind::accelerated),
            "ordinal": self.resolved.and_then(ResolvedCandleDeviceKind::ordinal),
            "reason": self.reason,
        })
    }
}

fn probe_candle_device(preference: CandleDevice) -> CandleDeviceProbe {
    match preference {
        CandleDevice::Auto => {
            let mut reasons = Vec::new();

            for candidate in auto_accelerator_candidates() {
                match try_accelerator_device(candidate) {
                    Ok(_) => {
                        return CandleDeviceProbe {
                            requested: preference,
                            resolved: Some(candidate),
                            reason: None,
                        };
                    }
                    Err(reason) => reasons.push(format!("{}: {reason}", candidate.as_str())),
                }
            }

            CandleDeviceProbe {
                requested: preference,
                resolved: Some(ResolvedCandleDeviceKind::Cpu),
                reason: (!reasons.is_empty()).then(|| {
                    format!(
                        "no local GPU backend was available; using CPU ({})",
                        reasons.join("; ")
                    )
                }),
            }
        }
        CandleDevice::Cpu => CandleDeviceProbe {
            requested: preference,
            resolved: Some(ResolvedCandleDeviceKind::Cpu),
            reason: None,
        },
        CandleDevice::Cuda => {
            explicit_candle_device_probe(preference, ResolvedCandleDeviceKind::Cuda)
        }
        CandleDevice::Metal => {
            explicit_candle_device_probe(preference, ResolvedCandleDeviceKind::Metal)
        }
    }
}

fn explicit_candle_device_probe(
    requested: CandleDevice,
    kind: ResolvedCandleDeviceKind,
) -> CandleDeviceProbe {
    match try_accelerator_device(kind) {
        Ok(_) => CandleDeviceProbe {
            requested,
            resolved: Some(kind),
            reason: None,
        },
        Err(reason) => CandleDeviceProbe {
            requested,
            resolved: None,
            reason: Some(reason),
        },
    }
}

fn resolve_runtime_device(preference: CandleDevice) -> Result<ResolvedCandleDevice> {
    match preference {
        CandleDevice::Auto => {
            for candidate in auto_accelerator_candidates() {
                if let Ok(device) = try_accelerator_device(candidate) {
                    return Ok(ResolvedCandleDevice {
                        kind: candidate,
                        device,
                    });
                }
            }

            Ok(ResolvedCandleDevice {
                kind: ResolvedCandleDeviceKind::Cpu,
                device: Device::Cpu,
            })
        }
        CandleDevice::Cpu => Ok(ResolvedCandleDevice {
            kind: ResolvedCandleDeviceKind::Cpu,
            device: Device::Cpu,
        }),
        CandleDevice::Cuda => {
            resolve_explicit_runtime_device(CandleDevice::Cuda, ResolvedCandleDeviceKind::Cuda)
        }
        CandleDevice::Metal => {
            resolve_explicit_runtime_device(CandleDevice::Metal, ResolvedCandleDeviceKind::Metal)
        }
    }
}

fn resolve_explicit_runtime_device(
    requested: CandleDevice,
    kind: ResolvedCandleDeviceKind,
) -> Result<ResolvedCandleDevice> {
    try_accelerator_device(kind)
        .map(|device| ResolvedCandleDevice { kind, device })
        .map_err(|reason| {
            Error::Unsupported(format!(
                "local Candle device '{}' is unavailable: {reason}; set postllm.candle_device = 'cpu' or 'auto' to fall back to CPU",
                requested.as_str(),
            ))
        })
}

const fn auto_accelerator_candidates() -> [ResolvedCandleDeviceKind; 2] {
    if cfg!(target_os = "macos") {
        [
            ResolvedCandleDeviceKind::Metal,
            ResolvedCandleDeviceKind::Cuda,
        ]
    } else {
        [
            ResolvedCandleDeviceKind::Cuda,
            ResolvedCandleDeviceKind::Metal,
        ]
    }
}

fn try_accelerator_device(kind: ResolvedCandleDeviceKind) -> core::result::Result<Device, String> {
    match kind {
        ResolvedCandleDeviceKind::Cpu => Ok(Device::Cpu),
        ResolvedCandleDeviceKind::Cuda => {
            Device::new_cuda(0).map_err(|error| candle_device_unavailable_reason(kind, error))
        }
        ResolvedCandleDeviceKind::Metal => {
            Device::new_metal(0).map_err(|error| candle_device_unavailable_reason(kind, error))
        }
    }
}

fn candle_device_unavailable_reason(
    kind: ResolvedCandleDeviceKind,
    error: CandleCoreError,
) -> String {
    match (kind, error) {
        (
            ResolvedCandleDeviceKind::Cuda,
            CandleCoreError::NotCompiledWithCudaSupport,
        ) => "CUDA support is not compiled into this build; rebuild postllm with --features candle-cuda to enable it".to_owned(),
        (
            ResolvedCandleDeviceKind::Metal,
            CandleCoreError::NotCompiledWithMetalSupport,
        ) => "Metal support is not compiled into this build; rebuild postllm with --features candle-metal to enable it".to_owned(),
        (ResolvedCandleDeviceKind::Cuda, error) => {
            format!("failed to open CUDA device 0: {error}")
        }
        (ResolvedCandleDeviceKind::Metal, error) => {
            format!("failed to open Metal device 0: {error}")
        }
        (ResolvedCandleDeviceKind::Cpu, error) => {
            format!("failed to open the CPU device: {error}")
        }
    }
}

struct LocalRuntimeExecution {
    started_at: Instant,
    timeout_ms: u64,
    max_input_tokens: Option<usize>,
    _concurrency_slot: Option<LocalConcurrencySlotGuard>,
}

impl LocalRuntimeExecution {
    fn enter(settings: &Settings, operation: &str) -> Result<Self> {
        let max_input_tokens = usize::try_from(settings.candle_max_input_tokens)
            .ok()
            .filter(|limit| *limit > 0);
        let concurrency_slot = acquire_local_concurrency_slot(
            settings.candle_cache_dir.as_deref(),
            settings.candle_max_concurrency,
            settings.timeout_ms,
            operation,
        )?;

        Ok(Self {
            started_at: Instant::now(),
            timeout_ms: settings.timeout_ms,
            max_input_tokens,
            _concurrency_slot: concurrency_slot,
        })
    }

    fn ensure_within_timeout(&self, stage: &str) -> Result<()> {
        if self.started_at.elapsed() <= Duration::from_millis(self.timeout_ms) {
            return Ok(());
        }

        Err(Error::Candle(format!(
            "local Candle {stage} exceeded postllm.timeout_ms={}ms",
            self.timeout_ms
        )))
    }
}

struct LocalConcurrencySlotGuard {
    file: std::fs::File,
}

impl Drop for LocalConcurrencySlotGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn with_local_runtime_limits<T>(
    settings: &Settings,
    operation: &str,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    if local_runtime_limits_active() {
        return f();
    }

    let execution = LocalRuntimeExecution::enter(settings, operation)?;
    let previous = LOCAL_RUNTIME_LIMITS.with(|limits| limits.replace(Some(execution)));
    let result = f();
    drop(LOCAL_RUNTIME_LIMITS.with(|limits| limits.replace(previous)));

    result
}

fn local_runtime_limits_active() -> bool {
    LOCAL_RUNTIME_LIMITS.with(|limits| limits.borrow().is_some())
}

fn local_runtime_checkpoint(stage: &str) -> Result<()> {
    crate::interrupt::checkpoint();
    LOCAL_RUNTIME_LIMITS.with(|limits| {
        limits
            .borrow()
            .as_ref()
            .map_or(Ok(()), |execution| execution.ensure_within_timeout(stage))
    })
}

fn enforce_local_input_token_limit(argument: &str, token_count: usize) -> Result<()> {
    LOCAL_RUNTIME_LIMITS.with(|limits| {
        let Some(limit) = limits.borrow().as_ref().and_then(|execution| execution.max_input_tokens)
        else {
            return Ok(());
        };

        if token_count <= limit {
            return Ok(());
        }

        Err(Error::invalid_argument(
            argument,
            format!(
                "tokenized length {token_count} exceeds the configured local Candle limit of {limit}"
            ),
            "shorten the input or raise postllm.candle_max_input_tokens",
        ))
    })
}

fn acquire_local_concurrency_slot(
    cache_dir: Option<&str>,
    max_concurrency: u32,
    timeout_ms: u64,
    operation: &str,
) -> Result<Option<LocalConcurrencySlotGuard>> {
    if max_concurrency == 0 {
        return Ok(None);
    }

    let lock_root = local_concurrency_lock_root(cache_dir);
    std::fs::create_dir_all(&lock_root)?;
    let started_at = Instant::now();

    loop {
        local_runtime_checkpoint("local concurrency wait")?;
        if started_at.elapsed() > Duration::from_millis(timeout_ms) {
            return Err(Error::Candle(format!(
                "local Candle {operation} could not start because postllm.candle_max_concurrency={max_concurrency} stayed saturated until postllm.timeout_ms={timeout_ms}ms elapsed"
            )));
        }

        for slot in 0..max_concurrency {
            let lock_path = lock_root.join(format!("slot-{slot}.lock"));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_path)?;

            match file.try_lock_exclusive() {
                Ok(()) => {
                    return Ok(Some(LocalConcurrencySlotGuard { file }));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
        }

        thread::sleep(Duration::from_millis(LOCAL_CONCURRENCY_POLL_INTERVAL_MS));
    }
}

fn local_concurrency_lock_root(cache_dir: Option<&str>) -> PathBuf {
    build_cache(cache_dir).path().join(".postllm-concurrency")
}

#[derive(Debug, Clone)]
struct LocalModelCacheState {
    cache_dir: PathBuf,
    repo_root: PathBuf,
    ref_path: PathBuf,
    revision: String,
    commit_hash: Option<String>,
    snapshot_path: Option<PathBuf>,
    cached_files: Vec<LocalModelCacheEntry>,
    cached_bytes: u64,
    integrity: LocalModelIntegritySummary,
}

impl LocalModelCacheState {
    const fn disk_cached(&self) -> bool {
        self.snapshot_path.is_some()
    }
}

fn local_cache_key(model_id: &str, cache_dir: Option<&str>, device_key: &str) -> ModelCacheKey {
    ModelCacheKey {
        model_id: model_id.to_owned(),
        cache_dir: cache_dir.map(str::to_owned),
        device_key: device_key.to_owned(),
    }
}

enum EmbeddingModel {
    Bert(BertModel),
    DistilBert(DistilBertModel),
    XlmRoberta(XLMRobertaModel),
}

impl EmbeddingModel {
    fn forward(
        &self,
        input_tensor: &Tensor,
        token_type_tensor: &Tensor,
        attention_tensor: &Tensor,
    ) -> Result<Tensor> {
        match self {
            Self::Bert(model) => model
                .forward(input_tensor, token_type_tensor, Some(attention_tensor))
                .map_err(|error| Error::Candle(error.to_string())),
            Self::DistilBert(model) => model
                .forward(input_tensor, attention_tensor)
                .map_err(|error| Error::Candle(error.to_string())),
            Self::XlmRoberta(model) => model
                .forward(
                    input_tensor,
                    attention_tensor,
                    token_type_tensor,
                    None,
                    None,
                    None,
                )
                .map_err(|error| Error::Candle(error.to_string())),
        }
    }
}

struct EmbeddingProjection {
    linear: Linear,
    activation: EmbeddingProjectionActivation,
    device: Device,
}

impl EmbeddingProjection {
    fn load(
        repo: &ApiRepo,
        sibling_files: &HashSet<String>,
        model_id: &str,
        cache_dir: Option<&str>,
        offline: bool,
        spec: &EmbeddingProjectionSpec,
        device: &Device,
    ) -> Result<Self> {
        let weight_paths = model_weight_paths(
            repo,
            sibling_files,
            model_id,
            cache_dir,
            offline,
            &spec.path,
            "the local Candle embedding Dense projection",
        )?;
        let vb = buffered_safetensor_var_builder(&weight_paths, BERT_DTYPE, device)?;
        let linear = linear_b(
            spec.in_features,
            spec.out_features,
            spec.bias,
            vb.pp("linear"),
        )
        .map_err(|error| Error::Candle(error.to_string()))?;

        Ok(Self {
            linear,
            activation: spec.activation,
            device: device.clone(),
        })
    }

    fn project(&self, embedding: &[f32]) -> Result<Vec<f32>> {
        let input = Tensor::from_slice(embedding, (1, embedding.len()), &self.device)
            .map_err(|error| Error::Candle(error.to_string()))?;
        let output = self
            .linear
            .forward(&input)
            .map_err(|error| Error::Candle(error.to_string()))?;
        let output = match self.activation {
            EmbeddingProjectionActivation::Identity => output,
            EmbeddingProjectionActivation::Tanh => output
                .tanh()
                .map_err(|error| Error::Candle(error.to_string()))?,
        };

        output
            .squeeze(0)
            .map_err(|error| Error::Candle(error.to_string()))?
            .to_vec1::<f32>()
            .map_err(|error| Error::Candle(error.to_string()))
    }
}

struct EmbeddingRuntime {
    device: Device,
    tokenizer: Tokenizer,
    model: EmbeddingModel,
    max_position_embeddings: usize,
    pooling: EmbeddingPooling,
    projection: Option<EmbeddingProjection>,
}

impl EmbeddingRuntime {
    fn load(
        model_id: &str,
        cache_dir: Option<&str>,
        offline: bool,
        resolved_device: ResolvedCandleDevice,
    ) -> Result<Self> {
        local_runtime_checkpoint("embedding runtime startup")?;
        let api = build_api(cache_dir)?;
        let repo = api.model(model_id.to_owned());
        let sibling_files = repo_sibling_files(&repo, model_id, cache_dir, offline)?;
        let raw_config = std::fs::read_to_string(download_repo_file(
            &repo,
            model_id,
            cache_dir,
            offline,
            "config.json",
        )?)?;
        let config_json: Value = serde_json::from_str(&raw_config)?;
        let layout = resolve_embedding_layout(
            model_id,
            &repo,
            &sibling_files,
            cache_dir,
            offline,
            &raw_config,
        )?;
        let tokenizer_path =
            download_repo_file(&repo, model_id, cache_dir, offline, "tokenizer.json")?;
        let weight_paths = model_weight_paths(
            &repo,
            &sibling_files,
            model_id,
            cache_dir,
            offline,
            "",
            "the local Candle embedding model",
        )?;
        let tokenizer = Tokenizer::from_file(tokenizer_path)
            .map_err(|error| Error::ModelAssets(error.to_string()))?;
        let device = resolved_device.device;
        let vb = buffered_safetensor_var_builder(&weight_paths, BERT_DTYPE, &device)?;
        let model = load_embedding_model(&vb, &raw_config, &config_json, layout.architecture)?;
        let projection = layout
            .projection
            .as_ref()
            .map(|spec| {
                EmbeddingProjection::load(
                    &repo,
                    &sibling_files,
                    model_id,
                    cache_dir,
                    offline,
                    spec,
                    &device,
                )
            })
            .transpose()?;

        Ok(Self {
            device,
            tokenizer,
            model,
            max_position_embeddings: layout.max_sequence_length,
            pooling: layout.pooling,
            projection,
        })
    }

    fn embed(&self, argument: &str, input: &str, normalize: bool) -> Result<Vec<f32>> {
        local_runtime_checkpoint("embedding request")?;
        let encoding = self
            .tokenizer
            .encode(input, true)
            .map_err(|error| Error::Candle(error.to_string()))?;
        let input_ids = encoding.get_ids();
        enforce_local_input_token_limit(argument, input_ids.len())?;

        if input_ids.len() > self.max_position_embeddings {
            return Err(Error::invalid_argument(
                argument,
                format!(
                    "tokenized length {} exceeds the local Candle model maximum of {}",
                    input_ids.len(),
                    self.max_position_embeddings,
                ),
                "shorten the input or switch to an embedding model with a larger context window",
            ));
        }

        let input_tensor = tensor_from_u32(input_ids, &self.device)?;
        let token_type_ids = token_type_ids(&encoding);
        let token_type_tensor = tensor_from_u32(&token_type_ids, &self.device)?;
        let attention_mask = encoding.get_attention_mask().to_vec();
        let attention_tensor = tensor_from_u32(&attention_mask, &self.device)?;
        let hidden_states =
            self.model
                .forward(&input_tensor, &token_type_tensor, &attention_tensor)?;
        let token_embeddings = hidden_states
            .squeeze(0)
            .map_err(|error| Error::Candle(error.to_string()))?
            .to_vec2::<f32>()
            .map_err(|error| Error::Candle(error.to_string()))?;
        let pooled = self.pooling.pool(&token_embeddings, &attention_mask)?;
        let embedding = match &self.projection {
            Some(projection) => projection.project(&pooled)?,
            None => pooled,
        };

        Ok(if normalize {
            normalize_l2(embedding)
        } else {
            embedding
        })
    }
}

struct GenerationRuntime {
    device: Device,
    tokenizer: Tokenizer,
    model: GenerationModel,
    max_position_embeddings: usize,
    stop_token_ids: Vec<u32>,
    top_p: Option<f64>,
}

impl GenerationRuntime {
    fn load(
        spec: &GenerationModelSpec,
        model_id: &str,
        cache_dir: Option<&str>,
        offline: bool,
        resolved_device: ResolvedCandleDevice,
    ) -> Result<Self> {
        local_runtime_checkpoint("generation runtime startup")?;
        let api = build_api(cache_dir)?;
        let repo = api.model(model_id.to_owned());
        let sibling_files = repo_sibling_files(&repo, model_id, cache_dir, offline)?;
        let config_path = download_repo_file(&repo, model_id, cache_dir, offline, "config.json")?;
        let tokenizer_path =
            download_repo_file(&repo, model_id, cache_dir, offline, "tokenizer.json")?;
        let weight_paths = model_weight_paths(
            &repo,
            &sibling_files,
            model_id,
            cache_dir,
            offline,
            "",
            "the local Candle generation model",
        )?;
        let raw_config = std::fs::read_to_string(config_path)?;
        let config_json: Value = serde_json::from_str(&raw_config)?;
        let tokenizer = Tokenizer::from_file(tokenizer_path)
            .map_err(|error| Error::ModelAssets(error.to_string()))?;
        let generation_config = download_optional_repo_json::<GenerationConfigFile>(
            &repo,
            &sibling_files,
            model_id,
            cache_dir,
            offline,
            "generation_config.json",
        )?;
        let stop_token_ids =
            resolve_generation_stop_tokens(&tokenizer, &config_json, generation_config.as_ref())?;
        let device = resolved_device.device;
        let top_p = generation_config
            .as_ref()
            .and_then(GenerationConfigFile::top_p);
        let dtype = generation_runtime_dtype(candle_dtype_from_config(&config_json)?, &device);
        let vb = buffered_safetensor_var_builder(&weight_paths, dtype, &device)?;

        let (model, max_position_embeddings) = match spec.family {
            GenerationFamily::Qwen2_5 => {
                let config: Qwen2Config = serde_json::from_str(&raw_config)?;
                let model = Qwen2Model::new(&config, vb)
                    .map_err(|error| Error::Candle(error.to_string()))?;
                (
                    GenerationModel::Qwen2_5(model),
                    config.max_position_embeddings,
                )
            }
        };

        Ok(Self {
            device,
            tokenizer,
            model,
            max_position_embeddings,
            stop_token_ids,
            top_p,
        })
    }

    fn chat_response(
        &mut self,
        model_id: &str,
        spec: &GenerationModelSpec,
        messages: &[Value],
        options: RequestOptions,
    ) -> Result<Value> {
        local_runtime_checkpoint("generation request")?;
        self.clear_kv_cache();

        let prompt = spec.render_prompt(messages)?;
        let encoding = self
            .tokenizer
            .encode(prompt, false)
            .map_err(|error| Error::Candle(error.to_string()))?;
        let prompt_ids = encoding.get_ids().to_vec();
        enforce_local_input_token_limit("messages", prompt_ids.len())?;

        if prompt_ids.is_empty() {
            return Err(Error::invalid_argument(
                "messages",
                "produce an empty tokenized prompt for local Candle generation",
                "pass at least one non-empty text message",
            ));
        }

        if prompt_ids.len() >= self.max_position_embeddings {
            return Err(Error::invalid_argument(
                "messages",
                format!(
                    "tokenized prompt length {} exceeds the local Candle model maximum of {}",
                    prompt_ids.len(),
                    self.max_position_embeddings,
                ),
                "shorten the conversation or switch to a model with a larger context window",
            ));
        }

        let max_new_tokens = self.resolve_max_new_tokens(prompt_ids.len(), options.max_tokens)?;
        let (generated_ids, finish_reason) =
            self.generate_tokens(&prompt_ids, options.temperature, max_new_tokens)?;
        local_runtime_checkpoint("generation decode")?;
        let text = self
            .tokenizer
            .decode(&generated_ids, true)
            .map_err(|error| Error::Candle(error.to_string()))?;

        Ok(build_local_chat_completion(
            model_id,
            &text,
            prompt_ids.len(),
            generated_ids.len(),
            finish_reason,
        ))
    }

    fn resolve_max_new_tokens(
        &self,
        prompt_tokens: usize,
        max_tokens: Option<i32>,
    ) -> Result<usize> {
        let available = self.max_position_embeddings.saturating_sub(prompt_tokens);
        if available == 0 {
            return Err(Error::invalid_argument(
                "messages",
                format!(
                    "tokenized prompt length {} leaves no room for generation within the local Candle model maximum of {}",
                    prompt_tokens, self.max_position_embeddings,
                ),
                "shorten the conversation or lower the prompt size",
            ));
        }

        let requested = match max_tokens {
            Some(max_tokens) => usize::try_from(max_tokens).map_err(|_| {
                Error::invalid_argument(
                    "max_tokens",
                    format!("must fit in the local runtime token budget type, got {max_tokens}"),
                    "pass a smaller positive integer for max_tokens",
                )
            })?,
            None => DEFAULT_MAX_GENERATION_TOKENS,
        };

        Ok(requested.min(available))
    }

    fn generate_tokens(
        &mut self,
        prompt_ids: &[u32],
        temperature: f64,
        max_new_tokens: usize,
    ) -> Result<(Vec<u32>, &'static str)> {
        let mut logits_processor =
            LogitsProcessor::new(generation_seed(), Some(temperature), self.top_p);
        let mut generated_ids = Vec::with_capacity(max_new_tokens);
        let mut logits = self.forward_logits(prompt_ids, 0)?;

        for step in 0..max_new_tokens {
            local_runtime_checkpoint("token generation")?;
            let next_token = logits_processor
                .sample(&logits)
                .map_err(|error| Error::Candle(error.to_string()))?;

            if self.stop_token_ids.contains(&next_token) {
                return Ok((generated_ids, "stop"));
            }

            generated_ids.push(next_token);

            if step + 1 == max_new_tokens {
                break;
            }

            logits =
                self.forward_logits(&[next_token], prompt_ids.len() + generated_ids.len() - 1)?;
        }

        Ok((generated_ids, "length"))
    }

    fn forward_logits(&mut self, input_ids: &[u32], seqlen_offset: usize) -> Result<Tensor> {
        let input_tensor = tensor_from_u32(input_ids, &self.device)?;
        let logits = match &mut self.model {
            GenerationModel::Qwen2_5(model) => model
                .forward(&input_tensor, seqlen_offset)
                .map_err(|error| Error::Candle(error.to_string()))?,
        };

        last_token_logits(&logits)
    }

    fn clear_kv_cache(&mut self) {
        match &mut self.model {
            GenerationModel::Qwen2_5(model) => model.clear_kv_cache(),
        }
    }
}

enum GenerationModel {
    Qwen2_5(Qwen2Model),
}

struct BufferedSafetensorBackend {
    shards: Vec<BufferedSafetensors>,
    routing: HashMap<String, usize>,
}

impl BufferedSafetensorBackend {
    fn load(paths: &[PathBuf]) -> Result<Self> {
        let mut shards = Vec::with_capacity(paths.len());
        let mut routing = HashMap::new();

        for path in paths {
            local_runtime_checkpoint("model shard loading")?;
            let shard = BufferedSafetensors::new(std::fs::read(path)?)
                .map_err(|error| Error::Candle(error.to_string()))?;
            let shard_index = shards.len();

            for (tensor_name, _) in shard.tensors() {
                if routing.insert(tensor_name.clone(), shard_index).is_some() {
                    return Err(Error::ModelAssets(format!(
                        "duplicate tensor '{tensor_name}' found across Candle safetensor shards"
                    )));
                }
            }

            shards.push(shard);
        }

        if routing.is_empty() {
            return Err(Error::ModelAssets(
                "the local Candle generation checkpoint did not contain any tensors".to_owned(),
            ));
        }

        Ok(Self { shards, routing })
    }
}

impl SimpleBackend for BufferedSafetensorBackend {
    fn get(
        &self,
        shape: Shape,
        name: &str,
        _: Init,
        dtype: DType,
        device: &Device,
    ) -> candle_core::Result<Tensor> {
        let tensor = self.get_unchecked(name, dtype, device)?;

        if tensor.shape() != &shape {
            Err(CandleCoreError::UnexpectedShape {
                msg: format!("shape mismatch for {name}"),
                expected: shape,
                got: tensor.shape().clone(),
            }
            .bt())?;
        }

        Ok(tensor)
    }

    fn get_unchecked(
        &self,
        name: &str,
        dtype: DType,
        device: &Device,
    ) -> candle_core::Result<Tensor> {
        let Some(shard_index) = self.routing.get(name).copied() else {
            Err(CandleCoreError::CannotFindTensor {
                path: name.to_owned(),
            }
            .bt())?
        };

        let Some(shard) = self.shards.get(shard_index) else {
            Err(CandleCoreError::CannotFindTensor {
                path: name.to_owned(),
            }
            .bt())?
        };

        shard.load(name, device)?.to_dtype(dtype)
    }

    fn contains_tensor(&self, name: &str) -> bool {
        self.routing.contains_key(name)
    }
}

enum PreparedGenerationRuntime {
    Starter(StarterGenerationRuntime),
}

impl PreparedGenerationRuntime {
    fn prepare(settings: &Settings) -> Result<Self> {
        let Some(spec) = generation_model_spec(&settings.model) else {
            return Err(Error::Unsupported(unsupported_generation_model_message(
                &settings.model,
                settings.candle_cache_dir.as_deref(),
            )));
        };

        Ok(Self::Starter(StarterGenerationRuntime {
            spec,
            cache_dir: settings.candle_cache_dir.clone(),
            offline: settings.candle_offline,
            device_preference: settings.candle_device,
        }))
    }

    fn chat_response(self, messages: &[Value], options: RequestOptions) -> Result<Value> {
        match self {
            Self::Starter(runtime) => runtime.chat_response(messages, options),
        }
    }
}

struct StarterGenerationRuntime {
    spec: &'static GenerationModelSpec,
    cache_dir: Option<String>,
    offline: bool,
    device_preference: CandleDevice,
}

impl StarterGenerationRuntime {
    fn chat_response(self, messages: &[Value], options: RequestOptions) -> Result<Value> {
        with_generation_runtime(
            self.spec,
            self.cache_dir.as_deref(),
            self.offline,
            self.device_preference,
            |runtime| runtime.chat_response(self.spec.model_id, self.spec, messages, options),
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
struct GenerationConfigFile {
    #[serde(default)]
    eos_token_id: Option<Value>,
    #[serde(default)]
    top_p: Option<f64>,
}

impl GenerationConfigFile {
    fn top_p(&self) -> Option<f64> {
        self.top_p.filter(|top_p| top_p.is_finite())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SafetensorIndexFile {
    #[serde(default)]
    weight_map: HashMap<String, String>,
}

/// Executes a chat request against the Candle runtime.
pub(crate) fn chat_response(
    settings: &Settings,
    messages: &[Value],
    options: RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
) -> Result<Value> {
    if response_format.is_some() {
        return Err(Error::Unsupported(
            "structured outputs are not implemented by the local Candle runtime".to_owned(),
        ));
    }

    if tools.is_some() || tool_choice.is_some() {
        return Err(Error::Unsupported(
            "tool-calling requests are not implemented by the local Candle runtime".to_owned(),
        ));
    }

    with_local_runtime_limits(settings, "generation request", || {
        PreparedGenerationRuntime::prepare(settings)?.chat_response(messages, options)
    })
}

/// Executes a streaming chat request against the Candle runtime.
pub(crate) fn chat_stream_response(
    _settings: &Settings,
    _messages: &[Value],
    _options: RequestOptions,
) -> Result<Vec<Value>> {
    Err(Error::Unsupported(
        "streaming is not implemented by the local Candle runtime".to_owned(),
    ))
}

#[must_use]
pub(crate) fn generation_availability(model_id: &str, feature: Feature) -> GenerationAvailability {
    let Some(_) = generation_model_spec(model_id) else {
        return GenerationAvailability::unsupported(unsupported_generation_model_message(
            model_id, None,
        ));
    };

    match feature {
        Feature::Chat | Feature::Complete => GenerationAvailability::available(),
        Feature::Embeddings
        | Feature::Reranking
        | Feature::Tools
        | Feature::StructuredOutputs
        | Feature::Streaming
        | Feature::MultimodalInputs => GenerationAvailability::unsupported(format!(
            "feature '{feature:?}' does not use the local Candle generation lane"
        )),
    }
}

fn generation_model_spec(model_id: &str) -> Option<&'static GenerationModelSpec> {
    let normalized = model_id.trim();

    STARTER_GENERATION_MODELS
        .iter()
        .find(|spec| spec.model_id.eq_ignore_ascii_case(normalized))
}

fn supported_generation_models() -> Vec<String> {
    STARTER_GENERATION_MODELS
        .iter()
        .map(|spec| spec.model_id.to_owned())
        .collect()
}

fn unsupported_generation_model_message(model_id: &str, cache_dir: Option<&str>) -> String {
    let mut detail =
        format!("model '{model_id}' is not in the local Candle generation starter set");

    if let Some(cache_dir) = cache_dir {
        let _ = write!(detail, " (candle_cache_dir='{cache_dir}')");
    }

    detail.push_str("; supported starter models are ");
    detail.push_str(&supported_generation_models().join(", "));

    detail
}

/// Computes local embeddings using a Candle-backed sentence-transformer model.
pub(crate) fn embed(
    settings: &Settings,
    inputs: &[String],
    normalize: bool,
) -> Result<Vec<Vec<f32>>> {
    with_local_runtime_limits(settings, "embedding request", || {
        with_embedding_runtime(
            &settings.model,
            settings.candle_cache_dir.as_deref(),
            settings.candle_offline,
            settings.candle_device,
            |runtime| {
                inputs
                    .iter()
                    .map(|input| runtime.embed("input", input, normalize))
                    .collect()
            },
        )
    })
}

/// Reranks documents locally by embedding the query and documents and scoring them by similarity.
pub(crate) fn rerank(
    settings: &Settings,
    query: &str,
    documents: &[String],
    top_n: Option<usize>,
) -> Result<Vec<RerankResult>> {
    with_local_runtime_limits(settings, "rerank request", || {
        with_embedding_runtime(
            &settings.model,
            settings.candle_cache_dir.as_deref(),
            settings.candle_offline,
            settings.candle_device,
            |runtime| {
                let query_embedding = runtime.embed("query", query, true)?;
                let document_embeddings = documents
                    .iter()
                    .enumerate()
                    .map(|(index, document)| {
                        runtime.embed(&format!("documents[{index}]"), document, true)
                    })
                    .collect::<Result<Vec<_>>>()?;

                rank_embeddings(&query_embedding, &document_embeddings, top_n)
            },
        )
    })
}

/// Returns metadata for a Candle-backed embedding model.
pub(crate) fn embedding_model_info(
    model_id: &str,
    cache_dir: Option<&str>,
    offline: bool,
) -> Result<Value> {
    if let Some(info) = known_embedding_model_info(model_id) {
        return Ok(info.snapshot());
    }

    local_runtime_checkpoint("embedding model inspection")?;
    let api = build_api(cache_dir)?;
    let repo = api.model(model_id.to_owned());
    let sibling_files = repo_sibling_files(&repo, model_id, cache_dir, offline)?;
    let raw_config = std::fs::read_to_string(download_repo_file(
        &repo,
        model_id,
        cache_dir,
        offline,
        "config.json",
    )?)?;
    let layout = resolve_embedding_layout(
        model_id,
        &repo,
        &sibling_files,
        cache_dir,
        offline,
        &raw_config,
    )?;

    Ok(EmbeddingModelInfo::from_layout(model_id, &layout).snapshot())
}

pub(crate) fn install_model(
    model_id: &str,
    lane: LocalModelLane,
    cache_dir: Option<&str>,
    offline: bool,
    device_preference: CandleDevice,
) -> Result<Value> {
    let downloaded_files = match lane {
        LocalModelLane::Embedding => install_embedding_assets(model_id, cache_dir, offline)?,
        LocalModelLane::Generation => install_generation_assets(model_id, cache_dir, offline)?,
    };
    let mut inspection = inspect_model(model_id, lane, cache_dir, offline, device_preference)?;
    reject_invalid_cache_integrity(model_id, cache_dir, &inspection)?;

    if let Some(inspection_object) = inspection.as_object_mut() {
        inspection_object.insert("action".to_owned(), json!("install"));
        inspection_object.insert(
            "downloaded_files".to_owned(),
            json!(
                downloaded_files
                    .into_iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
            ),
        );
    }

    Ok(inspection)
}

pub(crate) fn prewarm_model(
    model_id: &str,
    lane: LocalModelLane,
    cache_dir: Option<&str>,
    offline: bool,
    device_preference: CandleDevice,
) -> Result<Value> {
    match lane {
        LocalModelLane::Embedding => {
            with_embedding_runtime(
                model_id,
                cache_dir,
                offline,
                device_preference,
                |_runtime| Ok::<(), Error>(()),
            )?;
        }
        LocalModelLane::Generation => {
            let spec = generation_model_spec(model_id).ok_or_else(|| {
                Error::Unsupported(unsupported_generation_model_message(model_id, cache_dir))
            })?;
            with_generation_runtime(spec, cache_dir, offline, device_preference, |_runtime| {
                Ok::<(), Error>(())
            })?;
        }
    }

    let mut inspection = inspect_model(model_id, lane, cache_dir, offline, device_preference)?;
    if let Some(inspection_object) = inspection.as_object_mut() {
        inspection_object.insert("action".to_owned(), json!("prewarm"));
    }

    Ok(inspection)
}

pub(crate) fn inspect_model(
    model_id: &str,
    lane: LocalModelLane,
    cache_dir: Option<&str>,
    offline: bool,
    device_preference: CandleDevice,
) -> Result<Value> {
    let cache = build_cache(cache_dir);
    let repo = Repo::model(model_id.to_owned());
    let cache_state = inspect_cache_state(&cache, &repo)?;
    let cache_dir_value = cache_state.cache_dir.display().to_string();
    let repo_root_value = cache_state.repo_root.display().to_string();
    let ref_path_value = cache_state.ref_path.display().to_string();
    let snapshot_path_value = cache_state
        .snapshot_path
        .as_ref()
        .map(|path| path.display().to_string());
    let metadata = match lane {
        LocalModelLane::Embedding => cached_embedding_model_info(model_id, &cache, &repo)?,
        LocalModelLane::Generation => Some(generation_model_snapshot(model_id, cache_dir)),
    };
    let integrity = cache_state.integrity;
    let device = probe_candle_device(device_preference);

    Ok(json!({
        "runtime": "candle",
        "model": model_id,
        "lane": lane.as_str(),
        "offline": offline,
        "device": device.snapshot(),
        "cache_dir": cache_dir_value,
        "repo_cache_path": repo_root_value,
        "revision": cache_state.revision,
        "ref_path": ref_path_value,
        "commit_hash": cache_state.commit_hash,
        "snapshot_path": snapshot_path_value,
        "disk_cached": cache_state.disk_cached(),
        "memory_cached": local_model_is_loaded(model_id, lane, cache_dir, device.cache_key()),
        "cached_file_count": cache_state.cached_files.len(),
        "cached_bytes": cache_state.cached_bytes,
        "integrity": {
            "ok": integrity.ok(),
            "status": integrity.status(),
            "verified_files": integrity.verified,
            "mismatched_files": integrity.mismatched,
            "unchecked_files": integrity.unchecked,
        },
        "cached_files": cache_state.cached_files.iter().map(|entry| {
            json!({
                "path": entry.path,
                "bytes": entry.bytes,
                "integrity": {
                    "status": entry.integrity.status.as_str(),
                    "algorithm": entry.integrity.algorithm,
                    "expected": entry.integrity.expected.as_deref(),
                    "actual": entry.integrity.actual.as_deref(),
                },
            })
        }).collect::<Vec<_>>(),
        "metadata": metadata,
    }))
}

fn reject_invalid_cache_integrity(
    model_id: &str,
    cache_dir: Option<&str>,
    inspection: &Value,
) -> Result<()> {
    let mismatched_files = inspection
        .get("integrity")
        .and_then(|value| value.get("mismatched_files"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if mismatched_files == 0 {
        return Ok(());
    }

    let cache = build_cache(cache_dir);
    let repo = Repo::model(model_id.to_owned());
    let repo_root = repo_cache_root(&cache, &repo);
    if repo_root.exists() {
        std::fs::remove_dir_all(&repo_root)?;
    }

    let mut detail = format!(
        "downloaded Candle artifacts for model '{model_id}' failed integrity validation ({mismatched_files} mismatched files)"
    );
    if let Some(cache_dir) = cache_dir {
        let _ = write!(detail, " (candle_cache_dir='{cache_dir}')");
    }
    detail.push_str(
        "; the local repo cache was evicted, rerun postllm.model_install(...) or disable postllm.candle_offline until the cache is repopulated",
    );

    Err(Error::ModelAssets(detail))
}

pub(crate) fn evict_model(
    model_id: &str,
    lane: LocalModelLane,
    scope: LocalModelEvictionScope,
    cache_dir: Option<&str>,
    offline: bool,
    device_preference: CandleDevice,
) -> Result<Value> {
    let device = probe_candle_device(device_preference);
    let memory_evicted = match scope {
        LocalModelEvictionScope::Memory | LocalModelEvictionScope::All => {
            evict_memory_model(model_id, lane, cache_dir, device.cache_key())
        }
        LocalModelEvictionScope::Disk => false,
    };
    let (disk_evicted, removed_bytes, removed_files, repo_root) = match scope {
        LocalModelEvictionScope::Disk | LocalModelEvictionScope::All => {
            let cache = build_cache(cache_dir);
            let repo = Repo::model(model_id.to_owned());
            let repo_root = repo_cache_root(&cache, &repo);
            let cache_state = inspect_cache_state(&cache, &repo)?;
            if repo_root.exists() {
                std::fs::remove_dir_all(&repo_root)?;
                (
                    true,
                    cache_state.cached_bytes,
                    cache_state.cached_files.len(),
                    repo_root,
                )
            } else {
                (false, 0, 0, repo_root)
            }
        }
        LocalModelEvictionScope::Memory => {
            let cache = build_cache(cache_dir);
            let repo = Repo::model(model_id.to_owned());
            (false, 0, 0, repo_cache_root(&cache, &repo))
        }
    };

    Ok(json!({
        "runtime": "candle",
        "action": "evict",
        "model": model_id,
        "lane": lane.as_str(),
        "offline": offline,
        "device": device.snapshot(),
        "scope": scope.as_str(),
        "memory_evicted": memory_evicted,
        "disk_evicted": disk_evicted,
        "removed_bytes": removed_bytes,
        "removed_files": removed_files,
        "repo_cache_path": repo_root.display().to_string(),
        "memory_cached": local_model_is_loaded(model_id, lane, cache_dir, device.cache_key()),
        "disk_cached": repo_root.exists(),
    }))
}

fn rank_embeddings(
    query_embedding: &[f32],
    document_embeddings: &[Vec<f32>],
    top_n: Option<usize>,
) -> Result<Vec<RerankResult>> {
    let mut ranked = document_embeddings
        .iter()
        .enumerate()
        .map(|(index, embedding)| {
            Ok(RerankResult {
                index,
                score: dot_product(query_embedding, embedding)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    ranked.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then(left.index.cmp(&right.index))
    });

    if let Some(top_n) = top_n {
        ranked.truncate(top_n);
    }

    Ok(ranked)
}

fn dot_product(left: &[f32], right: &[f32]) -> Result<f64> {
    if left.len() != right.len() {
        return Err(Error::Candle(format!(
            "rerank embedding dimension mismatch: query has {} values but document has {}",
            left.len(),
            right.len()
        )));
    }

    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| f64::from(*left) * f64::from(*right))
        .sum())
}

fn with_embedding_runtime<T>(
    model_id: &str,
    cache_dir: Option<&str>,
    offline: bool,
    device_preference: CandleDevice,
    f: impl FnOnce(&EmbeddingRuntime) -> Result<T>,
) -> Result<T> {
    let resolved_device = resolve_runtime_device(device_preference)?;
    let key = local_cache_key(model_id, cache_dir, resolved_device.cache_key());
    let runtime = EMBEDDING_MODELS.with(|cache| -> Result<Rc<EmbeddingRuntime>> {
        if let Some(runtime) = cache.borrow().get(&key).cloned() {
            return Ok(runtime);
        }

        let runtime = Rc::new(EmbeddingRuntime::load(
            model_id,
            cache_dir,
            offline,
            resolved_device.clone(),
        )?);
        cache.borrow_mut().insert(key, Rc::clone(&runtime));

        Ok(runtime)
    })?;

    f(runtime.as_ref())
}

fn with_generation_runtime<T>(
    spec: &GenerationModelSpec,
    cache_dir: Option<&str>,
    offline: bool,
    device_preference: CandleDevice,
    f: impl FnOnce(&mut GenerationRuntime) -> Result<T>,
) -> Result<T> {
    let resolved_device = resolve_runtime_device(device_preference)?;
    let key = local_cache_key(spec.model_id, cache_dir, resolved_device.cache_key());
    let runtime = GENERATION_MODELS.with(|cache| -> Result<Rc<RefCell<GenerationRuntime>>> {
        if let Some(runtime) = cache.borrow().get(&key).cloned() {
            return Ok(runtime);
        }

        let runtime = Rc::new(RefCell::new(GenerationRuntime::load(
            spec,
            spec.model_id,
            cache_dir,
            offline,
            resolved_device.clone(),
        )?));
        cache.borrow_mut().insert(key, Rc::clone(&runtime));

        Ok(runtime)
    })?;

    let mut runtime = runtime.borrow_mut();
    f(&mut runtime)
}

fn known_embedding_model_info(model_id: &str) -> Option<EmbeddingModelInfo> {
    let normalized = model_id.trim();

    KNOWN_EMBEDDING_MODELS
        .iter()
        .find(|spec| spec.model_id.eq_ignore_ascii_case(normalized))
        .map(EmbeddingModelInfo::from_spec)
}

fn build_cache(cache_dir: Option<&str>) -> Cache {
    cache_dir.map_or_else(Cache::from_env, |cache_dir| {
        Cache::new(PathBuf::from(cache_dir))
    })
}

fn build_api(cache_dir: Option<&str>) -> Result<Api> {
    let builder = ApiBuilder::from_cache(build_cache(cache_dir))
        .with_progress(false)
        .with_user_agent("postllm", env!("CARGO_PKG_VERSION"));

    builder
        .build()
        .map_err(|error| Error::ModelAssets(error.to_string()))
}

fn offline_cache_miss(model_id: &str, cache_dir: Option<&str>, missing: &str) -> Error {
    let mut detail = format!(
        "offline mode is enabled and model '{model_id}' is missing cached artifact '{missing}'"
    );

    if let Some(cache_dir) = cache_dir {
        let _ = write!(detail, " (candle_cache_dir='{cache_dir}')");
    }

    detail.push_str(
        "; cache the model first with postllm.model_install(...) while online or disable postllm.candle_offline",
    );

    Error::ModelAssets(detail)
}

fn download_repo_file(
    repo: &ApiRepo,
    model_id: &str,
    cache_dir: Option<&str>,
    offline: bool,
    filename: &str,
) -> Result<PathBuf> {
    local_runtime_checkpoint("model artifact access")?;
    if offline {
        return build_cache(cache_dir)
            .repo(Repo::model(model_id.to_owned()))
            .get(filename)
            .ok_or_else(|| offline_cache_miss(model_id, cache_dir, filename));
    }

    repo.get(filename)
        .map_err(|error| Error::ModelAssets(error.to_string()))
}

fn repo_cache_root(cache: &Cache, repo: &Repo) -> PathBuf {
    let mut path = cache.path().clone();
    path.push(repo.folder_name());
    path
}

fn repo_ref_path(cache: &Cache, repo: &Repo) -> PathBuf {
    let mut path = repo_cache_root(cache, repo);
    path.push("refs");
    path.push(repo.revision());
    path
}

fn repo_snapshot_path(cache: &Cache, repo: &Repo, commit_hash: &str) -> PathBuf {
    let mut path = repo_cache_root(cache, repo);
    path.push("snapshots");
    path.push(commit_hash.trim());
    path
}

fn inspect_cache_state(cache: &Cache, repo: &Repo) -> Result<LocalModelCacheState> {
    let cache_dir = cache.path().clone();
    let repo_root = repo_cache_root(cache, repo);
    let ref_path = repo_ref_path(cache, repo);
    let revision = repo.revision().to_owned();
    let commit_hash = std::fs::read_to_string(&ref_path)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let snapshot_path = commit_hash
        .as_deref()
        .map(|commit_hash| repo_snapshot_path(cache, repo, commit_hash))
        .filter(|path| path.exists());
    let cached_files = snapshot_path
        .as_deref()
        .map_or(Ok(Vec::new()), collect_cached_snapshot_files)?;
    let cached_bytes = cached_files.iter().map(|entry| entry.bytes).sum();
    let integrity = summarize_cached_file_integrity(&cached_files);

    Ok(LocalModelCacheState {
        cache_dir,
        repo_root,
        ref_path,
        revision,
        commit_hash,
        snapshot_path,
        cached_files,
        cached_bytes,
        integrity,
    })
}

fn collect_cached_snapshot_files(snapshot_path: &Path) -> Result<Vec<LocalModelCacheEntry>> {
    let mut stack = vec![snapshot_path.to_path_buf()];
    let mut files = Vec::new();

    while let Some(path) = stack.pop() {
        local_runtime_checkpoint("cache inspection")?;
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
                continue;
            }

            let bytes = std::fs::metadata(&entry_path)?.len();
            let relative = entry_path
                .strip_prefix(snapshot_path)
                .map_err(|error| Error::Internal(error.to_string()))?
                .to_string_lossy()
                .into_owned();
            files.push(LocalModelCacheEntry {
                path: relative,
                bytes,
                integrity: inspect_cached_file_integrity(&entry_path)?,
            });
        }
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn inspect_cached_file_integrity(path: &Path) -> Result<LocalModelFileIntegrity> {
    let Some(spec) = expected_checksum_for_cached_file(path)? else {
        return Ok(LocalModelFileIntegrity::unchecked());
    };

    let actual = hash_file(path, spec.algorithm)?;
    let status = if actual == spec.expected {
        LocalModelFileIntegrityStatus::Verified
    } else {
        LocalModelFileIntegrityStatus::Mismatch
    };

    Ok(LocalModelFileIntegrity {
        status,
        algorithm: Some(spec.algorithm.as_str()),
        expected: Some(spec.expected),
        actual: Some(actual),
    })
}

fn summarize_cached_file_integrity(
    cached_files: &[LocalModelCacheEntry],
) -> LocalModelIntegritySummary {
    cached_files.iter().fold(
        LocalModelIntegritySummary::default(),
        |mut summary, entry| {
            match entry.integrity.status {
                LocalModelFileIntegrityStatus::Verified => summary.verified += 1,
                LocalModelFileIntegrityStatus::Mismatch => summary.mismatched += 1,
                LocalModelFileIntegrityStatus::Unchecked => summary.unchecked += 1,
            }
            summary
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChecksumAlgorithm {
    Sha256,
    GitBlobSha1,
}

impl ChecksumAlgorithm {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::GitBlobSha1 => "git_blob_sha1",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChecksumSpec {
    algorithm: ChecksumAlgorithm,
    expected: String,
}

fn expected_checksum_for_cached_file(path: &Path) -> Result<Option<ChecksumSpec>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_symlink() {
        return Ok(None);
    }

    let link_target = std::fs::read_link(path)?;
    let resolved = if link_target.is_absolute() {
        link_target
    } else {
        path.parent()
            .ok_or_else(|| {
                Error::Internal(format!(
                    "cached snapshot entry '{}' is missing a parent directory",
                    path.display()
                ))
            })?
            .join(link_target)
    };
    let Some(blob_name) = resolved.file_name().and_then(|name| name.to_str()) else {
        return Ok(None);
    };

    if is_hex_digest(blob_name, 64) {
        return Ok(Some(ChecksumSpec {
            algorithm: ChecksumAlgorithm::Sha256,
            expected: blob_name.to_owned(),
        }));
    }
    if is_hex_digest(blob_name, 40) {
        return Ok(Some(ChecksumSpec {
            algorithm: ChecksumAlgorithm::GitBlobSha1,
            expected: blob_name.to_owned(),
        }));
    }

    Ok(None)
}

fn is_hex_digest(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
}

fn hash_file(path: &Path, algorithm: ChecksumAlgorithm) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    match algorithm {
        ChecksumAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            stream_file_into_digest(&mut file, &mut hasher)?;
            Ok(encode_lower_hex(&hasher.finalize()))
        }
        ChecksumAlgorithm::GitBlobSha1 => {
            let size = file.metadata()?.len();
            let mut hasher = Sha1::new();
            hasher.update(format!("blob {size}\0").as_bytes());
            stream_file_into_digest(&mut file, &mut hasher)?;
            Ok(encode_lower_hex(&hasher.finalize()))
        }
    }
}

fn stream_file_into_digest<D: sha1::digest::Digest>(
    file: &mut std::fs::File,
    digest: &mut D,
) -> Result<()> {
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();

    loop {
        local_runtime_checkpoint("cache integrity hashing")?;
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let chunk = buffer.get(..read).ok_or_else(|| {
            Error::Internal("failed to read a digest chunk from the local Candle cache".to_owned())
        })?;
        digest.update(chunk);
    }

    Ok(())
}

fn encode_lower_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn install_embedding_assets(
    model_id: &str,
    cache_dir: Option<&str>,
    offline: bool,
) -> Result<Vec<PathBuf>> {
    let api = build_api(cache_dir)?;
    let repo = api.model(model_id.to_owned());
    let sibling_files = repo_sibling_files(&repo, model_id, cache_dir, offline)?;
    let raw_config = std::fs::read_to_string(download_repo_file(
        &repo,
        model_id,
        cache_dir,
        offline,
        "config.json",
    )?)?;
    let _layout = resolve_embedding_layout(
        model_id,
        &repo,
        &sibling_files,
        cache_dir,
        offline,
        &raw_config,
    )?;
    let mut downloaded_files = vec![
        download_repo_file(&repo, model_id, cache_dir, offline, "config.json")?,
        download_repo_file(&repo, model_id, cache_dir, offline, "tokenizer.json")?,
    ];
    downloaded_files.extend(model_weight_paths(
        &repo,
        &sibling_files,
        model_id,
        cache_dir,
        offline,
        "",
        "the local Candle embedding model",
    )?);

    if let Some(modules_path) = sibling_files
        .contains("modules.json")
        .then(|| download_repo_file(&repo, model_id, cache_dir, offline, "modules.json"))
        .transpose()?
    {
        downloaded_files.push(modules_path);
    }

    if let Some(modules) = download_optional_repo_json::<Vec<SentenceTransformerModule>>(
        &repo,
        &sibling_files,
        model_id,
        cache_dir,
        offline,
        "modules.json",
    )? {
        for module in modules {
            if module.module_type.ends_with(".Pooling") {
                let config_path = sentence_transformer_module_path(&module.path, "config.json");
                if sibling_files.contains(&config_path) {
                    downloaded_files.push(download_repo_file(
                        &repo,
                        model_id,
                        cache_dir,
                        offline,
                        &config_path,
                    )?);
                }
            }

            if module.module_type.ends_with(".Dense") {
                let config_path = sentence_transformer_module_path(&module.path, "config.json");
                if sibling_files.contains(&config_path) {
                    downloaded_files.push(download_repo_file(
                        &repo,
                        model_id,
                        cache_dir,
                        offline,
                        &config_path,
                    )?);
                }
                downloaded_files.extend(model_weight_paths(
                    &repo,
                    &sibling_files,
                    model_id,
                    cache_dir,
                    offline,
                    &module.path,
                    "the local Candle embedding Dense projection",
                )?);
            }
        }
    }

    sort_and_dedup_paths(&mut downloaded_files);
    Ok(downloaded_files)
}

fn install_generation_assets(
    model_id: &str,
    cache_dir: Option<&str>,
    offline: bool,
) -> Result<Vec<PathBuf>> {
    let Some(_spec) = generation_model_spec(model_id) else {
        return Err(Error::Unsupported(unsupported_generation_model_message(
            model_id, cache_dir,
        )));
    };

    let api = build_api(cache_dir)?;
    let repo = api.model(model_id.to_owned());
    let sibling_files = repo_sibling_files(&repo, model_id, cache_dir, offline)?;
    let mut downloaded_files = vec![
        download_repo_file(&repo, model_id, cache_dir, offline, "config.json")?,
        download_repo_file(&repo, model_id, cache_dir, offline, "tokenizer.json")?,
    ];
    if sibling_files.contains("generation_config.json") {
        downloaded_files.push(download_repo_file(
            &repo,
            model_id,
            cache_dir,
            offline,
            "generation_config.json",
        )?);
    }
    downloaded_files.extend(model_weight_paths(
        &repo,
        &sibling_files,
        model_id,
        cache_dir,
        offline,
        "",
        "the local Candle generation model",
    )?);
    sort_and_dedup_paths(&mut downloaded_files);

    Ok(downloaded_files)
}

fn cached_embedding_model_info(
    model_id: &str,
    cache: &Cache,
    repo: &Repo,
) -> Result<Option<Value>> {
    if let Some(info) = known_embedding_model_info(model_id) {
        return Ok(Some(info.snapshot()));
    }

    resolve_cached_embedding_layout(model_id, cache, repo).map(|layout| {
        layout.map(|layout| EmbeddingModelInfo::from_layout(model_id, &layout).snapshot())
    })
}

fn resolve_cached_embedding_layout(
    model_id: &str,
    cache: &Cache,
    repo: &Repo,
) -> Result<Option<EmbeddingLayout>> {
    let cache_repo = cache.repo(repo.clone());
    let Some(config_path) = cache_repo.get("config.json") else {
        return Ok(None);
    };
    let raw_config = std::fs::read_to_string(config_path)?;
    let config_json: Value = serde_json::from_str(&raw_config)?;
    let architecture = EmbeddingArchitecture::from_config_json(model_id, &config_json)?;
    let (hidden_size, max_sequence_length) = match architecture {
        EmbeddingArchitecture::Bert => {
            let config: BertConfig = serde_json::from_str(&raw_config)?;
            (config.hidden_size, config.max_position_embeddings)
        }
        EmbeddingArchitecture::DistilBert => {
            let config: DistilBertConfig = serde_json::from_str(&raw_config)?;
            let max_sequence_length = config_json
                .get("max_position_embeddings")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| {
                    Error::ModelAssets(format!(
                        "embedding model '{model_id}' is missing a valid max_position_embeddings value"
                    ))
                })?;

            (config.dim, max_sequence_length)
        }
        EmbeddingArchitecture::XlmRoberta => {
            let config: XLMRobertaConfig = serde_json::from_str(&raw_config)?;
            (config.hidden_size, config.max_position_embeddings)
        }
    };
    let Some((pooling, projection)) =
        load_cached_sentence_transformer_layout(&cache_repo, hidden_size)?
    else {
        return Ok(None);
    };

    Ok(Some(EmbeddingLayout {
        architecture,
        hidden_size,
        max_sequence_length,
        pooling,
        projection,
    }))
}

fn load_cached_sentence_transformer_layout(
    cache_repo: &hf_hub::CacheRepo,
    hidden_size: usize,
) -> Result<Option<(EmbeddingPooling, Option<EmbeddingProjectionSpec>)>> {
    let Some(modules_path) = cache_repo.get("modules.json") else {
        return Ok(Some((EmbeddingPooling::default_mean(), None)));
    };
    let modules: Vec<SentenceTransformerModule> =
        serde_json::from_str(&std::fs::read_to_string(modules_path)?)?;
    let pooling = modules
        .iter()
        .find(|module| module.module_type.ends_with(".Pooling"))
        .map_or(Ok(Some(EmbeddingPooling::default_mean())), |module| {
            let config_path = sentence_transformer_module_path(&module.path, "config.json");
            cache_repo
                .get(&config_path)
                .map(|path| -> Result<EmbeddingPooling> {
                    let config: SentenceTransformerPoolingConfig =
                        serde_json::from_str(&std::fs::read_to_string(path)?)?;
                    EmbeddingPooling::from_sentence_transformer_config(&config)
                })
                .transpose()
        })?;
    let Some(pooling) = pooling else {
        return Ok(None);
    };
    let projection = modules
        .iter()
        .find(|module| module.module_type.ends_with(".Dense"))
        .map(|module| -> Result<Option<EmbeddingProjectionSpec>> {
            let config_path = sentence_transformer_module_path(&module.path, "config.json");
            let Some(path) = cache_repo.get(&config_path) else {
                return Ok(None);
            };
            let config: SentenceTransformerDenseConfig =
                serde_json::from_str(&std::fs::read_to_string(path)?)?;
            let projection = EmbeddingProjectionSpec {
                path: module.path.clone(),
                in_features: config.in_features,
                out_features: config.out_features,
                bias: config.bias,
                activation: EmbeddingProjectionActivation::from_sentence_transformer(
                    config.activation_function.as_deref(),
                )?,
            };

            Ok(Some(projection))
        })
        .transpose()?
        .flatten();

    if let Some(projection) = &projection {
        let pooling_dimension = pooling.output_dimension(hidden_size);
        if pooling_dimension != projection.in_features {
            return Ok(None);
        }
    }

    Ok(Some((pooling, projection)))
}

fn sort_and_dedup_paths(paths: &mut Vec<PathBuf>) {
    paths.sort();
    paths.dedup();
}

fn generation_model_snapshot(model_id: &str, _cache_dir: Option<&str>) -> Value {
    let availability = generation_availability(model_id, Feature::Chat);
    let chat_template = generation_model_spec(model_id).map(|spec| match spec.chat_template {
        ChatTemplate::ChatMl => "chatml",
    });

    json!({
        "supported": availability.available,
        "reason": availability.reason,
        "supported_models": availability.supported_models,
        "chat_template": chat_template,
    })
}

fn local_model_is_loaded(
    model_id: &str,
    lane: LocalModelLane,
    cache_dir: Option<&str>,
    device_key: Option<&str>,
) -> bool {
    let Some(device_key) = device_key else {
        return false;
    };
    let key = local_cache_key(model_id, cache_dir, device_key);

    match lane {
        LocalModelLane::Embedding => {
            EMBEDDING_MODELS.with(|cache| cache.borrow().contains_key(&key))
        }
        LocalModelLane::Generation => {
            GENERATION_MODELS.with(|cache| cache.borrow().contains_key(&key))
        }
    }
}

fn evict_memory_model(
    model_id: &str,
    lane: LocalModelLane,
    cache_dir: Option<&str>,
    device_key: Option<&str>,
) -> bool {
    let Some(device_key) = device_key else {
        return false;
    };
    let key = local_cache_key(model_id, cache_dir, device_key);

    match lane {
        LocalModelLane::Embedding => {
            EMBEDDING_MODELS.with(|cache| cache.borrow_mut().remove(&key).is_some())
        }
        LocalModelLane::Generation => {
            GENERATION_MODELS.with(|cache| cache.borrow_mut().remove(&key).is_some())
        }
    }
}

fn resolve_embedding_layout(
    model_id: &str,
    repo: &ApiRepo,
    sibling_files: &HashSet<String>,
    cache_dir: Option<&str>,
    offline: bool,
    raw_config: &str,
) -> Result<EmbeddingLayout> {
    let config_json: Value = serde_json::from_str(raw_config)?;
    let architecture = EmbeddingArchitecture::from_config_json(model_id, &config_json)?;
    let (hidden_size, max_sequence_length) = match architecture {
        EmbeddingArchitecture::Bert => {
            let config: BertConfig = serde_json::from_str(raw_config)?;
            (config.hidden_size, config.max_position_embeddings)
        }
        EmbeddingArchitecture::DistilBert => {
            let config: DistilBertConfig = serde_json::from_str(raw_config)?;
            (
                config.dim,
                config_json
                    .get("max_position_embeddings")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| {
                        Error::ModelAssets(
                            "distilbert config is missing a usable max_position_embeddings value"
                                .to_owned(),
                        )
                    })?,
            )
        }
        EmbeddingArchitecture::XlmRoberta => {
            let config: XLMRobertaConfig = serde_json::from_str(raw_config)?;
            (config.hidden_size, config.max_position_embeddings)
        }
    };
    let (pooling, projection) = load_sentence_transformer_layout(
        repo,
        sibling_files,
        model_id,
        cache_dir,
        offline,
        hidden_size,
    )?;

    Ok(EmbeddingLayout {
        architecture,
        hidden_size,
        max_sequence_length,
        pooling,
        projection,
    })
}

fn load_sentence_transformer_layout(
    repo: &ApiRepo,
    sibling_files: &HashSet<String>,
    model_id: &str,
    cache_dir: Option<&str>,
    offline: bool,
    hidden_size: usize,
) -> Result<(EmbeddingPooling, Option<EmbeddingProjectionSpec>)> {
    let modules = download_optional_repo_json::<Vec<SentenceTransformerModule>>(
        repo,
        sibling_files,
        model_id,
        cache_dir,
        offline,
        "modules.json",
    )?;
    let Some(modules) = modules else {
        return Ok((EmbeddingPooling::default_mean(), None));
    };

    let pooling = modules
        .iter()
        .find(|module| module.module_type.ends_with(".Pooling"))
        .map_or(Ok(EmbeddingPooling::default_mean()), |module| {
            let config_path = sentence_transformer_module_path(&module.path, "config.json");
            let config = download_optional_repo_json::<SentenceTransformerPoolingConfig>(
                repo,
                sibling_files,
                model_id,
                cache_dir,
                offline,
                &config_path,
            )?
            .ok_or_else(|| {
                Error::ModelAssets(format!(
                    "sentence-transformer pooling module '{}' is missing {config_path}",
                    module.module_type
                ))
            })?;

            EmbeddingPooling::from_sentence_transformer_config(&config)
        })?;
    let projection = modules
        .iter()
        .find(|module| module.module_type.ends_with(".Dense"))
        .map(|module| {
            let config_path = sentence_transformer_module_path(&module.path, "config.json");
            let config = download_optional_repo_json::<SentenceTransformerDenseConfig>(
                repo,
                sibling_files,
                model_id,
                cache_dir,
                offline,
                &config_path,
            )?
            .ok_or_else(|| {
                Error::ModelAssets(format!(
                    "sentence-transformer Dense module '{}' is missing {config_path}",
                    module.module_type
                ))
            })?;

            Ok::<EmbeddingProjectionSpec, Error>(EmbeddingProjectionSpec {
                path: module.path.clone(),
                in_features: config.in_features,
                out_features: config.out_features,
                bias: config.bias,
                activation: EmbeddingProjectionActivation::from_sentence_transformer(
                    config.activation_function.as_deref(),
                )?,
            })
        })
        .transpose()?;

    if let Some(projection) = &projection {
        let pooling_dimension = pooling.output_dimension(hidden_size);
        if pooling_dimension != projection.in_features {
            return Err(Error::ModelAssets(format!(
                "sentence-transformer Dense module expects {} pooled values but pooling produces {}",
                projection.in_features, pooling_dimension
            )));
        }
    }

    Ok((pooling, projection))
}

fn sentence_transformer_module_path(path: &str, filename: &str) -> String {
    if path.trim().is_empty() {
        filename.to_owned()
    } else {
        format!("{}/{}", path.trim_matches('/'), filename)
    }
}

fn download_optional_repo_json<T: DeserializeOwned>(
    repo: &ApiRepo,
    sibling_files: &HashSet<String>,
    model_id: &str,
    cache_dir: Option<&str>,
    offline: bool,
    filename: &str,
) -> Result<Option<T>> {
    if !sibling_files.contains(filename) {
        return Ok(None);
    }

    let path = download_repo_file(repo, model_id, cache_dir, offline, filename)?;
    let value = serde_json::from_str(&std::fs::read_to_string(path)?)?;

    Ok(Some(value))
}

fn repo_sibling_files(
    repo: &ApiRepo,
    model_id: &str,
    cache_dir: Option<&str>,
    offline: bool,
) -> Result<HashSet<String>> {
    local_runtime_checkpoint("model metadata fetch")?;
    if offline {
        let cache = build_cache(cache_dir);
        let cache_state = inspect_cache_state(&cache, &Repo::model(model_id.to_owned()))?;
        if !cache_state.disk_cached() {
            return Err(offline_cache_miss(model_id, cache_dir, "snapshot metadata"));
        }

        return Ok(cache_state
            .cached_files
            .into_iter()
            .map(|entry| entry.path)
            .collect());
    }

    let info = repo
        .info()
        .map_err(|error| Error::ModelAssets(error.to_string()))?;

    Ok(info
        .siblings
        .into_iter()
        .map(|sibling| sibling.rfilename)
        .collect())
}

fn model_weight_paths(
    repo: &ApiRepo,
    sibling_files: &HashSet<String>,
    model_id: &str,
    cache_dir: Option<&str>,
    offline: bool,
    prefix: &str,
    asset_label: &str,
) -> Result<Vec<PathBuf>> {
    let index_name = sentence_transformer_module_path(prefix, "model.safetensors.index.json");
    let single_name = sentence_transformer_module_path(prefix, "model.safetensors");

    local_runtime_checkpoint("model weight resolution")?;
    if sibling_files.contains(&index_name) {
        let index_path = download_repo_file(repo, model_id, cache_dir, offline, &index_name)?;
        let index: SafetensorIndexFile =
            serde_json::from_str(&std::fs::read_to_string(index_path)?)?;

        return sharded_weight_filenames(&index, prefix)
            .into_iter()
            .map(|filename| download_repo_file(repo, model_id, cache_dir, offline, &filename))
            .collect();
    }

    if sibling_files.contains(&single_name) {
        return Ok(vec![download_repo_file(
            repo,
            model_id,
            cache_dir,
            offline,
            &single_name,
        )?]);
    }

    Err(Error::ModelAssets(format!(
        "{asset_label} is missing model.safetensors assets"
    )))
}

fn sharded_weight_filenames(index: &SafetensorIndexFile, prefix: &str) -> Vec<String> {
    let mut filenames = index
        .weight_map
        .values()
        .map(|filename| prefixed_weight_filename(prefix, filename))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    filenames.sort();
    filenames
}

fn prefixed_weight_filename(prefix: &str, filename: &str) -> String {
    if prefix.trim().is_empty() || filename.contains('/') {
        filename.to_owned()
    } else {
        format!("{}/{}", prefix.trim_matches('/'), filename)
    }
}

fn buffered_safetensor_var_builder(
    paths: &[PathBuf],
    dtype: DType,
    device: &Device,
) -> Result<VarBuilder<'static>> {
    let backend = BufferedSafetensorBackend::load(paths)?;

    Ok(VarBuilder::from_backend(
        Box::new(backend),
        dtype,
        device.clone(),
    ))
}

fn candle_dtype_from_config(config_json: &Value) -> Result<DType> {
    match config_json
        .get("torch_dtype")
        .and_then(Value::as_str)
        .unwrap_or("float32")
    {
        "bfloat16" => Ok(DType::BF16),
        "float16" => Ok(DType::F16),
        "float32" | "float" => Ok(DType::F32),
        other => Err(Error::ModelAssets(format!(
            "unsupported torch_dtype '{other}' for local Candle generation"
        ))),
    }
}

fn generation_runtime_dtype(dtype: DType, device: &Device) -> DType {
    if device.is_cpu() && matches!(dtype, DType::BF16 | DType::F16) {
        DType::F32
    } else {
        dtype
    }
}

fn resolve_generation_stop_tokens(
    tokenizer: &Tokenizer,
    config_json: &Value,
    generation_config: Option<&GenerationConfigFile>,
) -> Result<Vec<u32>> {
    let mut stop_token_ids = BTreeSet::new();

    for token_id in token_ids_from_value(config_json.get("eos_token_id")) {
        stop_token_ids.insert(token_id);
    }

    if let Some(generation_config) = generation_config {
        for token_id in token_ids_from_value(generation_config.eos_token_id.as_ref()) {
            stop_token_ids.insert(token_id);
        }
    }

    for token in [QWEN_IM_END_TOKEN, QWEN_END_OF_TEXT_TOKEN] {
        if let Some(token_id) = tokenizer.token_to_id(token) {
            stop_token_ids.insert(token_id);
        }
    }

    if stop_token_ids.is_empty() {
        return Err(Error::ModelAssets(
            "failed to determine any local Candle generation stop tokens".to_owned(),
        ));
    }

    Ok(stop_token_ids.into_iter().collect())
}

fn token_ids_from_value(value: Option<&Value>) -> Vec<u32> {
    match value {
        Some(Value::Number(number)) => number
            .as_u64()
            .and_then(|token_id| u32::try_from(token_id).ok())
            .into_iter()
            .collect(),
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_u64)
            .filter_map(|token_id| u32::try_from(token_id).ok())
            .collect(),
        Some(Value::String(_) | Value::Null | Value::Bool(_) | Value::Object(_)) | None => {
            Vec::new()
        }
    }
}

fn build_local_chat_completion(
    model_id: &str,
    content: &str,
    prompt_tokens: usize,
    completion_tokens: usize,
    finish_reason: &'static str,
) -> Value {
    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());

    json!({
        "id": format!("chatcmpl-candle-{created}"),
        "object": "chat.completion",
        "created": created,
        "model": model_id,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": content,
            },
            "finish_reason": finish_reason,
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    })
}

fn generation_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        })
}

fn load_embedding_model(
    vb: &VarBuilder<'_>,
    raw_config: &str,
    config_json: &Value,
    architecture: EmbeddingArchitecture,
) -> Result<EmbeddingModel> {
    match architecture {
        EmbeddingArchitecture::Bert => {
            let config: BertConfig = serde_json::from_str(raw_config)?;
            Ok(EmbeddingModel::Bert(load_bert_model(vb, &config)?))
        }
        EmbeddingArchitecture::DistilBert => {
            let config: DistilBertConfig = serde_json::from_str(raw_config)?;
            Ok(EmbeddingModel::DistilBert(load_distilbert_model(
                vb, &config,
            )?))
        }
        EmbeddingArchitecture::XlmRoberta => {
            let config: XLMRobertaConfig = serde_json::from_str(raw_config)?;
            Ok(EmbeddingModel::XlmRoberta(load_xlm_roberta_model(
                vb,
                &config,
                config_json,
            )?))
        }
    }
}

fn load_bert_model(vb: &VarBuilder<'_>, config: &BertConfig) -> Result<BertModel> {
    let prefixes = ["", "bert", "0.auto_model", "model", "0.model"];
    let mut last_error = String::new();

    for prefix in prefixes {
        let prefixed_vb = if prefix.is_empty() {
            vb.clone()
        } else {
            vb.pp(prefix)
        };

        match BertModel::load(prefixed_vb, config) {
            Ok(model) => return Ok(model),
            Err(error) => {
                last_error = error.to_string();
            }
        }
    }

    Err(Error::Candle(format!(
        "failed to load a BERT-compatible Candle model from '{prefixes:?}': {last_error}",
    )))
}

fn load_distilbert_model(
    vb: &VarBuilder<'_>,
    config: &DistilBertConfig,
) -> Result<DistilBertModel> {
    let prefixes = ["", "distilbert", "0.auto_model", "model", "0.model"];
    let mut last_error = String::new();

    for prefix in prefixes {
        let prefixed_vb = if prefix.is_empty() {
            vb.clone()
        } else {
            vb.pp(prefix)
        };

        match DistilBertModel::load(prefixed_vb, config) {
            Ok(model) => return Ok(model),
            Err(error) => {
                last_error = error.to_string();
            }
        }
    }

    Err(Error::Candle(format!(
        "failed to load a DistilBERT-compatible Candle model from '{prefixes:?}': {last_error}",
    )))
}

fn load_xlm_roberta_model(
    vb: &VarBuilder<'_>,
    config: &XLMRobertaConfig,
    config_json: &Value,
) -> Result<XLMRobertaModel> {
    let model_type = config_json
        .get("model_type")
        .and_then(Value::as_str)
        .unwrap_or("xlm-roberta");
    let prefixes = [
        "",
        "roberta",
        "xlm_roberta",
        "0.auto_model",
        "model",
        "0.model",
    ];
    let mut last_error = String::new();

    for prefix in prefixes {
        let prefixed_vb = if prefix.is_empty() {
            vb.clone()
        } else {
            vb.pp(prefix)
        };

        match XLMRobertaModel::new(config, prefixed_vb) {
            Ok(model) => return Ok(model),
            Err(error) => {
                last_error = error.to_string();
            }
        }
    }

    Err(Error::Candle(format!(
        "failed to load an XLM-RoBERTa-compatible Candle model for model_type '{model_type}' from '{prefixes:?}': {last_error}",
    )))
}

fn tensor_from_u32(values: &[u32], device: &Device) -> Result<Tensor> {
    Tensor::from_slice(values, (1, values.len()), device)
        .map_err(|error| Error::Candle(error.to_string()))
}

fn last_token_logits(logits: &Tensor) -> Result<Tensor> {
    logits
        .squeeze(0)
        .map_err(|error| Error::Candle(error.to_string()))?
        .squeeze(0)
        .map_err(|error| Error::Candle(error.to_string()))
}

fn token_type_ids(encoding: &Encoding) -> Vec<u32> {
    let ids = encoding.get_type_ids();

    if ids.len() == encoding.get_ids().len() {
        ids.to_vec()
    } else {
        vec![0; encoding.get_ids().len()]
    }
}

fn cls_pool(token_embeddings: &[Vec<f32>]) -> Result<Vec<f32>> {
    token_embeddings
        .first()
        .cloned()
        .ok_or_else(|| Error::Candle("the local model returned zero token embeddings".to_owned()))
}

fn max_pool(token_embeddings: &[Vec<f32>], attention_mask: &[u32]) -> Result<Vec<f32>> {
    let Some(hidden_size) = token_embeddings.first().map(Vec::len) else {
        return Err(Error::Candle(
            "the local model returned zero token embeddings".to_owned(),
        ));
    };
    let mut pooled = vec![f32::NEG_INFINITY; hidden_size];
    let mut token_count = 0_usize;

    for (token_embedding, mask) in token_embeddings.iter().zip(attention_mask.iter().copied()) {
        if mask == 0 {
            continue;
        }

        token_count += 1;

        for (pooled_value, token_value) in pooled.iter_mut().zip(token_embedding) {
            *pooled_value = pooled_value.max(*token_value);
        }
    }

    if token_count == 0 {
        return Err(Error::Candle(
            "the tokenizer attention mask contained no active tokens".to_owned(),
        ));
    }

    Ok(pooled)
}

fn mean_pool(token_embeddings: &[Vec<f32>], attention_mask: &[u32]) -> Result<Vec<f32>> {
    let Some(hidden_size) = token_embeddings.first().map(Vec::len) else {
        return Err(Error::Candle(
            "the local model returned zero token embeddings".to_owned(),
        ));
    };

    let mut pooled = vec![0.0_f32; hidden_size];
    let mut token_count = 0.0_f32;

    for (token_embedding, mask) in token_embeddings.iter().zip(attention_mask.iter().copied()) {
        if mask == 0 {
            continue;
        }

        token_count += 1.0;

        for (pooled_value, token_value) in pooled.iter_mut().zip(token_embedding) {
            *pooled_value += *token_value;
        }
    }

    if token_count == 0.0 {
        return Err(Error::Candle(
            "the tokenizer attention mask contained no active tokens".to_owned(),
        ));
    }

    for pooled_value in &mut pooled {
        *pooled_value /= token_count;
    }

    Ok(pooled)
}

fn mean_sqrt_len_pool(token_embeddings: &[Vec<f32>], attention_mask: &[u32]) -> Result<Vec<f32>> {
    let Some(hidden_size) = token_embeddings.first().map(Vec::len) else {
        return Err(Error::Candle(
            "the local model returned zero token embeddings".to_owned(),
        ));
    };

    let mut pooled = vec![0.0_f32; hidden_size];
    let mut token_count = 0.0_f32;

    for (token_embedding, mask) in token_embeddings.iter().zip(attention_mask.iter().copied()) {
        if mask == 0 {
            continue;
        }

        token_count += 1.0;

        for (pooled_value, token_value) in pooled.iter_mut().zip(token_embedding) {
            *pooled_value += *token_value;
        }
    }

    if token_count == 0.0 {
        return Err(Error::Candle(
            "the tokenizer attention mask contained no active tokens".to_owned(),
        ));
    }

    let divisor = token_count.sqrt();
    for pooled_value in &mut pooled {
        *pooled_value /= divisor;
    }

    Ok(pooled)
}

fn normalize_l2(mut vector: Vec<f32>) -> Vec<f32> {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();

    if norm == 0.0 {
        return vector;
    }

    for value in &mut vector {
        *value /= norm;
    }

    vector
}

fn render_chatml_prompt(messages: &[Value]) -> Result<String> {
    let mut prompt = String::new();
    let first_role = messages
        .first()
        .and_then(|message| message.get("role"))
        .and_then(Value::as_str);

    if !matches!(first_role, Some(role) if role.eq_ignore_ascii_case("system")) {
        prompt.push_str("<|im_start|>system\n");
        prompt.push_str(QWEN_DEFAULT_SYSTEM_PROMPT);
        prompt.push_str("<|im_end|>\n");
    }

    for (index, message) in messages.iter().enumerate() {
        let argument = format!("messages[{index}]");
        let role = message.get("role").and_then(Value::as_str).ok_or_else(|| {
            Error::invalid_argument(
                &format!("{argument}.role"),
                "must contain a string role for local Candle generation",
                "build messages with postllm.message(...), postllm.user(...), or postllm.system(...)",
            )
        })?;
        let content = render_message_content(
            message.get("content").ok_or_else(|| {
                Error::invalid_argument(
                    &format!("{argument}.content"),
                    "must be present for local Candle generation",
                    "provide JSON like {\"role\":\"user\",\"content\":\"...\"}",
                )
            })?,
            index,
        )?;

        prompt.push_str("<|im_start|>");
        prompt.push_str(role);
        prompt.push('\n');
        prompt.push_str(&content);
        prompt.push_str("<|im_end|>\n");
    }

    prompt.push_str("<|im_start|>assistant\n");

    Ok(prompt)
}

fn render_message_content(content: &Value, index: usize) -> Result<String> {
    let argument = format!("messages[{index}].content");

    match content {
        Value::String(text) => Ok(text.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<String>();

            if text.is_empty() {
                Err(Error::invalid_argument(
                    &argument,
                    "must include at least one text part for local Candle generation",
                    "use postllm.text_part(...) for Candle or switch postllm.runtime to 'openai' for image-only messages",
                ))
            } else {
                Ok(text)
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => {
            Err(Error::invalid_argument(
                &argument,
                "must be a string or text-part array for local Candle generation",
                "pass text-only messages or switch postllm.runtime to 'openai' for multimodal/tool-call requests",
            ))
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "Candle tests are clearer with direct indexing and explicit expect messages"
)]
mod tests {
    use super::{
        EmbeddingArchitecture, EmbeddingLayout, EmbeddingModelInfo, EmbeddingPooling,
        EmbeddingProjectionActivation, EmbeddingProjectionSpec, GenerationConfigFile,
        LOCAL_RUNTIME_LIMITS, LocalModelFileIntegrityStatus, LocalRuntimeExecution,
        SafetensorIndexFile, Sha1, Sha256, chat_response, cls_pool,
        enforce_local_input_token_limit, generation_availability, generation_model_spec,
        generation_runtime_dtype, inspect_cache_state, max_pool, mean_pool, mean_sqrt_len_pool,
        normalize_l2, probe_candle_device, rank_embeddings, render_chatml_prompt,
        resolve_runtime_device, sharded_weight_filenames, token_ids_from_value,
    };
    use crate::backend::{CandleDevice, Feature, RequestOptions, Runtime, Settings};
    use candle_core::{DType, Device};
    use hf_hub::{Cache, Repo};
    use serde_json::json;
    use sha1::Digest as _;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "postllm-candle-{label}-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("temporary directory should be created");

            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[cfg(target_family = "unix")]
    fn write_test_cache_file(
        root: &Path,
        model_id: &str,
        commit_hash: &str,
        relative_path: &str,
        blob_name: &str,
        contents: &[u8],
    ) {
        let repo = Repo::model(model_id.to_owned());
        let repo_root = root.join(repo.folder_name());
        let blobs_dir = repo_root.join("blobs");
        let snapshot_root = repo_root.join("snapshots").join(commit_hash);
        let snapshot_path = snapshot_root.join(relative_path);
        let blob_path = blobs_dir.join(blob_name);
        let ref_path = repo_root.join("refs").join("main");

        std::fs::create_dir_all(blob_path.parent().expect("blob parent should exist"))
            .expect("blob directory should exist");
        std::fs::create_dir_all(
            snapshot_path
                .parent()
                .expect("snapshot parent should exist"),
        )
        .expect("snapshot directory should exist");
        std::fs::create_dir_all(ref_path.parent().expect("ref parent should exist"))
            .expect("ref directory should exist");
        std::fs::write(&blob_path, contents).expect("blob should be written");
        std::fs::write(&ref_path, commit_hash).expect("ref should be written");
        std::os::unix::fs::symlink(&blob_path, &snapshot_path)
            .expect("snapshot symlink should be created");
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        super::encode_lower_hex(&hasher.finalize())
    }

    fn git_blob_sha1_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", bytes.len()).as_bytes());
        hasher.update(bytes);
        super::encode_lower_hex(&hasher.finalize())
    }

    fn with_test_local_runtime_execution<T>(
        execution: LocalRuntimeExecution,
        f: impl FnOnce() -> T,
    ) -> T {
        let previous = LOCAL_RUNTIME_LIMITS.with(|limits| limits.replace(Some(execution)));
        let result = f();
        drop(LOCAL_RUNTIME_LIMITS.with(|limits| limits.replace(previous)));
        result
    }

    #[test]
    fn normalize_l2_should_preserve_zero_vectors() {
        assert_eq!(normalize_l2(vec![0.0, 0.0, 0.0]), vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn normalize_l2_should_scale_vectors_to_unit_length() {
        let vector = normalize_l2(vec![3.0, 4.0]);

        assert!((vector[0] - 0.6).abs() < 1e-6);
        assert!((vector[1] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn mean_pool_should_ignore_masked_tokens() {
        let pooled = mean_pool(
            &[vec![2.0, 4.0], vec![6.0, 8.0], vec![100.0, 100.0]],
            &[1, 1, 0],
        )
        .expect("pooling should succeed");

        assert_eq!(pooled, vec![4.0, 6.0]);
    }

    #[test]
    fn embedding_pooling_should_concatenate_enabled_modes_in_sentence_transformer_order() {
        let pooled = EmbeddingPooling {
            cls_token: true,
            mean_tokens: true,
            max_tokens: true,
            mean_sqrt_len_tokens: false,
        }
        .pool(
            &[vec![2.0, 4.0], vec![6.0, 8.0], vec![100.0, 100.0]],
            &[1, 1, 0],
        )
        .expect("pooling should succeed");

        assert_eq!(pooled, vec![2.0, 4.0, 6.0, 8.0, 4.0, 6.0]);
    }

    #[test]
    fn cls_pool_should_return_the_first_token_embedding() {
        let pooled =
            cls_pool(&[vec![2.0, 4.0], vec![6.0, 8.0]]).expect("cls pooling should succeed");

        assert_eq!(pooled, vec![2.0, 4.0]);
    }

    #[test]
    fn max_pool_should_ignore_masked_tokens() {
        let pooled = max_pool(
            &[vec![2.0, 4.0], vec![6.0, 8.0], vec![100.0, 100.0]],
            &[1, 1, 0],
        )
        .expect("max pooling should succeed");

        assert_eq!(pooled, vec![6.0, 8.0]);
    }

    #[test]
    fn mean_sqrt_len_pool_should_scale_by_the_square_root_of_active_tokens() {
        let pooled = mean_sqrt_len_pool(&[vec![2.0, 4.0], vec![6.0, 8.0]], &[1, 1])
            .expect("mean_sqrt_len pooling should succeed");

        let scale = 2.0_f32.sqrt();
        assert!((pooled[0] - (8.0 / scale)).abs() < 1e-6);
        assert!((pooled[1] - (12.0 / scale)).abs() < 1e-6);
    }

    #[test]
    fn embedding_model_info_should_report_dimension_limits_and_normalization() {
        let info = EmbeddingModelInfo::from_layout(
            "sentence-transformers/paraphrase-MiniLM-L3-v2",
            &EmbeddingLayout {
                architecture: EmbeddingArchitecture::Bert,
                hidden_size: 384,
                max_sequence_length: 512,
                pooling: EmbeddingPooling::default_mean(),
                projection: None,
            },
        )
        .snapshot();

        assert_eq!(info["runtime"], "candle");
        assert_eq!(
            info["model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
        assert_eq!(info["architecture"], "bert");
        assert_eq!(info["dimension"], 384);
        assert_eq!(info["max_sequence_length"], 512);
        assert_eq!(info["pooling"], "mean");
        assert_eq!(info["normalization"]["default"], "l2");
        assert_eq!(info["normalization"]["supported"], json!(["l2", "none"]));
    }

    #[test]
    fn embedding_model_info_should_report_projection_metadata() {
        let info = EmbeddingModelInfo::from_layout(
            "sentence-transformers/distiluse-base-multilingual-cased-v2",
            &EmbeddingLayout {
                architecture: EmbeddingArchitecture::DistilBert,
                hidden_size: 768,
                max_sequence_length: 512,
                pooling: EmbeddingPooling::default_mean(),
                projection: Some(EmbeddingProjectionSpec {
                    path: "2_Dense".to_owned(),
                    in_features: 768,
                    out_features: 512,
                    bias: true,
                    activation: EmbeddingProjectionActivation::Tanh,
                }),
            },
        )
        .snapshot();

        assert_eq!(info["architecture"], "distilbert");
        assert_eq!(info["dimension"], 512);
        assert_eq!(info["pooling"], "mean");
        assert_eq!(info["projection"]["in_dimension"], 768);
        assert_eq!(info["projection"]["out_dimension"], 512);
        assert_eq!(info["projection"]["activation"], "tanh");
    }

    #[test]
    fn embedding_architecture_should_detect_supported_model_types() {
        assert_eq!(
            EmbeddingArchitecture::from_config_json(
                "test-distil",
                &json!({"model_type": "distilbert"})
            )
            .expect("distilbert should resolve"),
            EmbeddingArchitecture::DistilBert
        );
        assert_eq!(
            EmbeddingArchitecture::from_config_json(
                "test-xlmr",
                &json!({"model_type": "xlm-roberta"})
            )
            .expect("xlm-roberta should resolve"),
            EmbeddingArchitecture::XlmRoberta
        );
    }

    #[test]
    fn embedding_architecture_should_reject_unsupported_model_types() {
        let error =
            EmbeddingArchitecture::from_config_json("test-mpnet", &json!({"model_type": "mpnet"}))
                .expect_err("unsupported model type should fail");

        assert!(
            error
                .to_string()
                .contains("do not support model_type 'mpnet'")
        );
    }

    #[test]
    fn embedding_projection_activation_should_detect_tanh() {
        assert_eq!(
            EmbeddingProjectionActivation::from_sentence_transformer(Some(
                "torch.nn.modules.activation.Tanh"
            ))
            .expect("tanh should resolve"),
            EmbeddingProjectionActivation::Tanh
        );
    }

    #[test]
    fn embedding_projection_activation_should_reject_unknown_activations() {
        let error = EmbeddingProjectionActivation::from_sentence_transformer(Some(
            "torch.nn.modules.activation.ReLU",
        ))
        .expect_err("unsupported activation should fail");

        assert!(
            error
                .to_string()
                .contains("Dense activation 'torch.nn.modules.activation.ReLU'")
        );
    }

    #[test]
    fn generation_availability_should_list_supported_starter_models() {
        let availability = generation_availability("llama3.2", Feature::Chat);

        assert!(!availability.available);
        assert_eq!(
            availability.reason,
            Some(
                "model 'llama3.2' is not in the local Candle generation starter set; supported starter models are Qwen/Qwen2.5-0.5B-Instruct, Qwen/Qwen2.5-1.5B-Instruct"
                    .to_owned()
            )
        );
        assert_eq!(
            availability.supported_models,
            vec![
                "Qwen/Qwen2.5-0.5B-Instruct".to_owned(),
                "Qwen/Qwen2.5-1.5B-Instruct".to_owned()
            ]
        );
    }

    #[test]
    fn rank_embeddings_should_sort_descending_and_truncate() {
        let ranked = rank_embeddings(
            &[1.0, 0.0],
            &[
                vec![0.2, 0.0],
                vec![0.9, 0.0],
                vec![0.9, 0.1],
                vec![-0.5, 0.0],
            ],
            Some(3),
        )
        .expect("ranking should succeed");

        assert_eq!(ranked.len(), 3);
        assert_eq!(ranked[0].index, 1);
        assert_eq!(ranked[1].index, 2);
        assert_eq!(ranked[2].index, 0);
        assert!((ranked[0].score - 0.9).abs() < 1e-6);
        assert!((ranked[1].score - 0.9).abs() < 1e-6);
        assert!((ranked[2].score - 0.2).abs() < 1e-6);
    }

    #[test]
    fn generation_availability_should_report_chat_support_for_registered_starter_models() {
        let availability = generation_availability("Qwen/Qwen2.5-0.5B-Instruct", Feature::Chat);

        assert!(availability.available);
        assert_eq!(availability.reason, None);
    }

    #[test]
    fn generation_availability_should_report_complete_support_for_registered_starter_models() {
        let availability = generation_availability("Qwen/Qwen2.5-0.5B-Instruct", Feature::Complete);

        assert!(availability.available);
        assert_eq!(availability.reason, None);
    }

    #[test]
    fn generation_model_spec_should_render_chatml_prompts() {
        let spec = generation_model_spec("Qwen/Qwen2.5-0.5B-Instruct")
            .expect("starter model should resolve");

        let prompt = spec
            .render_prompt(&[
                json!({"role": "system", "content": "You are terse."}),
                json!({"role": "user", "content": "Say hi."}),
            ])
            .expect("chatml prompt should render");

        assert_eq!(
            prompt,
            "<|im_start|>system\nYou are terse.<|im_end|>\n<|im_start|>user\nSay hi.<|im_end|>\n<|im_start|>assistant\n"
        );
    }

    #[test]
    fn render_chatml_prompt_should_inject_the_qwen_default_system_prompt() {
        let prompt = render_chatml_prompt(&[json!({"role": "user", "content": "Say hi."})])
            .expect("chatml prompt should render");

        assert_eq!(
            prompt,
            "<|im_start|>system\nYou are Qwen, created by Alibaba Cloud.\nYou are a helpful assistant.<|im_end|>\n<|im_start|>user\nSay hi.<|im_end|>\n<|im_start|>assistant\n"
        );
    }

    #[test]
    fn sharded_weight_filenames_should_be_sorted_and_deduplicated() {
        let index = SafetensorIndexFile {
            weight_map: HashMap::from([
                (
                    "a".to_owned(),
                    "model-00002-of-00003.safetensors".to_owned(),
                ),
                (
                    "b".to_owned(),
                    "model-00001-of-00003.safetensors".to_owned(),
                ),
                (
                    "c".to_owned(),
                    "model-00002-of-00003.safetensors".to_owned(),
                ),
            ]),
        };

        assert_eq!(
            sharded_weight_filenames(&index, ""),
            vec![
                "model-00001-of-00003.safetensors".to_owned(),
                "model-00002-of-00003.safetensors".to_owned(),
            ]
        );
    }

    #[cfg(target_family = "unix")]
    #[test]
    fn inspect_cache_state_should_report_verified_sha256_files() {
        let tempdir = TempDir::new("integrity-sha256");
        let model_id = "sentence-transformers/test-embed";
        let commit_hash = "commit-sha256";
        let contents = b"local candle weights";
        let blob_name = sha256_hex(contents);

        write_test_cache_file(
            tempdir.path(),
            model_id,
            commit_hash,
            "model.safetensors",
            &blob_name,
            contents,
        );

        let cache_state = inspect_cache_state(
            &Cache::new(tempdir.path().to_path_buf()),
            &Repo::model(model_id.to_owned()),
        )
        .expect("cache inspection should succeed");

        assert!(cache_state.disk_cached());
        assert_eq!(cache_state.integrity.status(), "verified");
        assert!(cache_state.integrity.ok());
        assert_eq!(cache_state.integrity.verified, 1);
        assert_eq!(cache_state.integrity.mismatched, 0);
        assert_eq!(cache_state.integrity.unchecked, 0);
        assert_eq!(cache_state.cached_files.len(), 1);
        assert_eq!(
            cache_state.cached_files[0].integrity.status,
            LocalModelFileIntegrityStatus::Verified
        );
        assert_eq!(
            cache_state.cached_files[0].integrity.algorithm,
            Some("sha256")
        );
        assert_eq!(
            cache_state.cached_files[0].integrity.expected.as_deref(),
            Some(blob_name.as_str())
        );
        assert_eq!(
            cache_state.cached_files[0].integrity.actual.as_deref(),
            Some(blob_name.as_str())
        );
    }

    #[cfg(target_family = "unix")]
    #[test]
    fn inspect_cache_state_should_report_verified_git_blob_sha1_files() {
        let tempdir = TempDir::new("integrity-git-sha1");
        let model_id = "sentence-transformers/test-config";
        let commit_hash = "commit-git-sha1";
        let contents = b"{\"pooling\":\"mean\"}\n";
        let blob_name = git_blob_sha1_hex(contents);

        write_test_cache_file(
            tempdir.path(),
            model_id,
            commit_hash,
            "config.json",
            &blob_name,
            contents,
        );

        let cache_state = inspect_cache_state(
            &Cache::new(tempdir.path().to_path_buf()),
            &Repo::model(model_id.to_owned()),
        )
        .expect("cache inspection should succeed");

        assert_eq!(cache_state.integrity.status(), "verified");
        assert_eq!(
            cache_state.cached_files[0].integrity.algorithm,
            Some("git_blob_sha1")
        );
        assert_eq!(
            cache_state.cached_files[0].integrity.expected.as_deref(),
            Some(blob_name.as_str())
        );
        assert_eq!(
            cache_state.cached_files[0].integrity.actual.as_deref(),
            Some(blob_name.as_str())
        );
    }

    #[cfg(target_family = "unix")]
    #[test]
    fn inspect_cache_state_should_report_checksum_mismatches() {
        let tempdir = TempDir::new("integrity-mismatch");
        let model_id = "Qwen/test-generation";
        let commit_hash = "commit-mismatch";
        let contents = b"bad local artifact";
        let blob_name = sha256_hex(b"some other contents");

        write_test_cache_file(
            tempdir.path(),
            model_id,
            commit_hash,
            "model.safetensors",
            &blob_name,
            contents,
        );

        let cache_state = inspect_cache_state(
            &Cache::new(tempdir.path().to_path_buf()),
            &Repo::model(model_id.to_owned()),
        )
        .expect("cache inspection should succeed");

        assert_eq!(cache_state.integrity.status(), "mismatch");
        assert!(!cache_state.integrity.ok());
        assert_eq!(cache_state.integrity.verified, 0);
        assert_eq!(cache_state.integrity.mismatched, 1);
        assert_eq!(
            cache_state.cached_files[0].integrity.status,
            LocalModelFileIntegrityStatus::Mismatch
        );
        assert_eq!(
            cache_state.cached_files[0].integrity.expected.as_deref(),
            Some(blob_name.as_str())
        );
        assert_ne!(
            cache_state.cached_files[0].integrity.actual.as_deref(),
            Some(blob_name.as_str())
        );
    }

    #[test]
    fn local_runtime_limits_should_reject_tokenized_inputs_over_the_configured_cap() {
        let error = with_test_local_runtime_execution(
            LocalRuntimeExecution {
                started_at: Instant::now(),
                timeout_ms: 30_000,
                max_input_tokens: Some(4),
                _concurrency_slot: None,
            },
            || {
                enforce_local_input_token_limit("input", 5)
                    .expect_err("inputs over the configured cap should fail")
            },
        );

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'input' tokenized length 5 exceeds the configured local Candle limit of 4; fix: shorten the input or raise postllm.candle_max_input_tokens"
        );
    }

    #[test]
    fn local_runtime_execution_should_fail_after_the_timeout_budget_expires() {
        let execution = LocalRuntimeExecution {
            started_at: Instant::now()
                .checked_sub(Duration::from_millis(5))
                .expect("test instant subtraction should succeed"),
            timeout_ms: 1,
            max_input_tokens: None,
            _concurrency_slot: None,
        };
        let error = execution
            .ensure_within_timeout("generation request")
            .expect_err("expired local timeout should fail");

        assert_eq!(
            error.to_string(),
            "failed to run the local Candle model: local Candle generation request exceeded postllm.timeout_ms=1ms; fix: verify the selected local model is supported, the request fits the model limits, and the backend has enough memory"
        );
    }

    #[test]
    fn token_ids_from_value_should_support_scalars_and_arrays() {
        assert_eq!(token_ids_from_value(Some(&json!(151_645))), vec![151_645]);
        assert_eq!(
            token_ids_from_value(Some(&json!([151_645, 151_643]))),
            vec![151_645, 151_643]
        );
        assert_eq!(token_ids_from_value(Some(&json!("bad"))), Vec::<u32>::new());
    }

    #[test]
    fn generation_config_file_should_filter_non_finite_top_p_values() {
        let config = GenerationConfigFile {
            eos_token_id: None,
            top_p: Some(f64::NAN),
        };

        assert_eq!(config.top_p(), None);
    }

    #[test]
    fn generation_runtime_dtype_should_promote_cpu_half_precision_to_f32() {
        assert_eq!(
            generation_runtime_dtype(DType::BF16, &Device::Cpu),
            DType::F32
        );
        assert_eq!(
            generation_runtime_dtype(DType::F16, &Device::Cpu),
            DType::F32
        );
        assert_eq!(
            generation_runtime_dtype(DType::F32, &Device::Cpu),
            DType::F32
        );
    }

    #[test]
    fn resolve_runtime_device_should_return_cpu_for_cpu_preference() {
        let resolved = resolve_runtime_device(CandleDevice::Cpu)
            .expect("cpu device preference should always resolve");

        assert_eq!(resolved.cache_key(), "cpu");
        assert!(resolved.device.is_cpu());
    }

    #[test]
    fn probe_candle_device_should_report_cpu_fallback_for_auto_without_gpu_support() {
        if candle_core::utils::cuda_is_available() || candle_core::utils::metal_is_available() {
            return;
        }

        let snapshot = probe_candle_device(CandleDevice::Auto).snapshot();

        assert_eq!(snapshot["requested"], "auto");
        assert_eq!(snapshot["resolved"], "cpu");
        assert_eq!(snapshot["available"], true);
        assert_eq!(snapshot["accelerated"], false);
        assert!(
            snapshot["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("using CPU"))
        );
    }

    #[test]
    fn resolve_runtime_device_should_reject_explicit_cuda_without_cuda_support() {
        if candle_core::utils::cuda_is_available() {
            return;
        }

        let error = resolve_runtime_device(CandleDevice::Cuda)
            .expect_err("explicit cuda preference should fail without cuda support");

        assert_eq!(
            error.to_string(),
            "postllm backend is not available: local Candle device 'cuda' is unavailable: CUDA support is not compiled into this build; rebuild postllm with --features candle-cuda to enable it; set postllm.candle_device = 'cpu' or 'auto' to fall back to CPU"
        );
    }

    #[test]
    fn resolve_runtime_device_should_reject_explicit_metal_without_metal_support() {
        if candle_core::utils::metal_is_available() {
            return;
        }

        let error = resolve_runtime_device(CandleDevice::Metal)
            .expect_err("explicit metal preference should fail without metal support");

        assert_eq!(
            error.to_string(),
            "postllm backend is not available: local Candle device 'metal' is unavailable: Metal support is not compiled into this build; rebuild postllm with --features candle-metal to enable it; set postllm.candle_device = 'cpu' or 'auto' to fall back to CPU"
        );
    }

    #[test]
    fn chat_response_should_reject_unknown_generation_models_before_runtime_work() {
        let error = chat_response(
            &Settings {
                runtime: Runtime::Candle,
                model: "llama3.2".to_owned(),
                base_url: None,
                api_key: None,
                timeout_ms: 30_000,
                max_retries: 2,
                retry_backoff_ms: 250,
                candle_cache_dir: None,
                candle_offline: false,
                candle_device: CandleDevice::Auto,
                candle_max_input_tokens: 0,
                candle_max_concurrency: 0,
            },
            &[json!({"role": "user", "content": "hello"})],
            RequestOptions {
                temperature: 0.2,
                max_tokens: Some(32),
            },
            None,
            None,
            None,
        )
        .expect_err("unknown Candle generation model should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm backend is not available: model 'llama3.2' is not in the local Candle generation starter set; supported starter models are Qwen/Qwen2.5-0.5B-Instruct, Qwen/Qwen2.5-1.5B-Instruct"
        );
    }
}
