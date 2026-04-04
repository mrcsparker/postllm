#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::backend::{CandleDevice, Runtime, Settings};
use crate::error::{Error, Result};
use crate::permissions;
use pgrx::datum::DatumWithOid;
use pgrx::spi::Spi;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use serde_json::{Map, Value, json};
use std::ffi::CString;

pub(crate) const DEFAULT_BASE_URL: &str = "http://127.0.0.1:11434/v1/chat/completions";
pub(crate) const DEFAULT_RUNTIME: &str = Runtime::OPENAI;
pub(crate) const DEFAULT_MODEL: &str = "llama3.2";
pub(crate) const DEFAULT_EMBEDDING_MODEL: &str = "sentence-transformers/paraphrase-MiniLM-L3-v2";
pub(crate) const DEFAULT_TIMEOUT_MS: i32 = 30_000;
pub(crate) const DEFAULT_MAX_RETRIES: i32 = 2;
pub(crate) const DEFAULT_RETRY_BACKOFF_MS: i32 = 250;
pub(crate) const DEFAULT_API_KEY_SECRET: &str = "";
pub(crate) const DEFAULT_CANDLE_DEVICE: &str = CandleDevice::AUTO;
pub(crate) const DEFAULT_CANDLE_MAX_INPUT_TOKENS: i32 = 0;
pub(crate) const DEFAULT_CANDLE_MAX_CONCURRENCY: i32 = 0;
const PROFILE_MANAGED_STRING_DEFAULTS: [(&str, &str); 7] = [
    ("postllm.base_url", DEFAULT_BASE_URL),
    ("postllm.runtime", DEFAULT_RUNTIME),
    ("postllm.model", DEFAULT_MODEL),
    ("postllm.embedding_model", DEFAULT_EMBEDDING_MODEL),
    ("postllm.api_key", ""),
    ("postllm.api_key_secret", DEFAULT_API_KEY_SECRET),
    ("postllm.candle_device", DEFAULT_CANDLE_DEVICE),
];
const PROFILE_MANAGED_INT_DEFAULTS: [(&str, i32); 5] = [
    ("postllm.timeout_ms", DEFAULT_TIMEOUT_MS),
    ("postllm.max_retries", DEFAULT_MAX_RETRIES),
    ("postllm.retry_backoff_ms", DEFAULT_RETRY_BACKOFF_MS),
    (
        "postllm.candle_max_input_tokens",
        DEFAULT_CANDLE_MAX_INPUT_TOKENS,
    ),
    (
        "postllm.candle_max_concurrency",
        DEFAULT_CANDLE_MAX_CONCURRENCY,
    ),
];

static POSTLLM_BASE_URL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"http://127.0.0.1:11434/v1/chat/completions"));
static POSTLLM_RUNTIME: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"openai"));
static POSTLLM_MODEL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"llama3.2"));
static POSTLLM_EMBEDDING_MODEL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"sentence-transformers/paraphrase-MiniLM-L3-v2"));
static POSTLLM_API_KEY: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static POSTLLM_API_KEY_SECRET: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static POSTLLM_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static POSTLLM_MAX_RETRIES: GucSetting<i32> = GucSetting::<i32>::new(2);
static POSTLLM_RETRY_BACKOFF_MS: GucSetting<i32> = GucSetting::<i32>::new(250);
static POSTLLM_HTTP_ALLOWED_HOSTS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static POSTLLM_HTTP_ALLOWED_PROVIDERS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static POSTLLM_CANDLE_CACHE_DIR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static POSTLLM_CANDLE_OFFLINE: GucSetting<bool> = GucSetting::<bool>::new(false);
static POSTLLM_CANDLE_DEVICE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"auto"));
static POSTLLM_CANDLE_MAX_INPUT_TOKENS: GucSetting<i32> = GucSetting::<i32>::new(0);
static POSTLLM_CANDLE_MAX_CONCURRENCY: GucSetting<i32> = GucSetting::<i32>::new(0);

/// Fully resolved setting inputs shared by all request-setting builders.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedSettingInputs {
    base_url: Option<String>,
    api_key: Option<String>,
    timeout_ms: u64,
    max_retries: u32,
    retry_backoff_ms: u64,
    http_allowed_hosts: Vec<String>,
    http_allowed_providers: Vec<String>,
    candle_cache_dir: Option<String>,
    candle_offline: bool,
    candle_device: CandleDevice,
    candle_max_input_tokens: u32,
    candle_max_concurrency: u32,
}

impl ResolvedSettingInputs {
    fn into_settings(self, runtime: Runtime, model: String) -> Settings {
        Settings {
            runtime,
            model,
            base_url: self.base_url,
            api_key: self.api_key,
            timeout_ms: self.timeout_ms,
            max_retries: self.max_retries,
            retry_backoff_ms: self.retry_backoff_ms,
            http_allowed_hosts: self.http_allowed_hosts,
            http_allowed_providers: self.http_allowed_providers,
            candle_cache_dir: self.candle_cache_dir,
            candle_offline: self.candle_offline,
            candle_device: self.candle_device,
            candle_max_input_tokens: self.candle_max_input_tokens,
            candle_max_concurrency: self.candle_max_concurrency,
        }
    }
}

/// Validated session-local `postllm` overrides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SessionOverrides {
    pub(crate) base_url: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) embedding_model: Option<String>,
    pub(crate) api_key: Option<String>,
    pub(crate) api_key_secret: Option<String>,
    pub(crate) clear_api_key: bool,
    pub(crate) clear_api_key_secret: bool,
    pub(crate) timeout_ms: Option<i32>,
    pub(crate) max_retries: Option<i32>,
    pub(crate) retry_backoff_ms: Option<i32>,
    pub(crate) runtime: Option<String>,
    pub(crate) candle_cache_dir: Option<String>,
    pub(crate) candle_offline: Option<bool>,
    pub(crate) candle_device: Option<String>,
    pub(crate) candle_max_input_tokens: Option<i32>,
    pub(crate) candle_max_concurrency: Option<i32>,
}

impl SessionOverrides {
    #[expect(
        clippy::too_many_arguments,
        reason = "the SQL configure(...) entry point intentionally maps one-to-one onto setting overrides"
    )]
    pub(crate) fn from_configure_args(
        base_url: Option<&str>,
        model: Option<&str>,
        embedding_model: Option<&str>,
        api_key: Option<&str>,
        api_key_secret: Option<&str>,
        timeout_ms: Option<i32>,
        max_retries: Option<i32>,
        retry_backoff_ms: Option<i32>,
        runtime: Option<&str>,
        candle_cache_dir: Option<&str>,
        candle_offline: Option<bool>,
        candle_device: Option<&str>,
        candle_max_input_tokens: Option<i32>,
        candle_max_concurrency: Option<i32>,
    ) -> Result<Self> {
        let api_key = api_key.map(|value| value.trim().to_owned());
        let api_key_secret = api_key_secret.map(|value| value.trim().to_owned());
        let direct_api_key = api_key
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let named_api_key_secret = api_key_secret
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_owned);

        if direct_api_key.is_some() && named_api_key_secret.is_some() {
            return Err(Error::invalid_argument(
                "api_key_secret",
                "must not be combined with api_key in the same configure(...) call",
                "pass either api_key => '...' for a direct session secret or api_key_secret => '...' for a named stored secret",
            ));
        }

        Ok(Self {
            base_url: sanitize_non_blank_string("base_url", base_url)?,
            model: sanitize_non_blank_string("model", model)?,
            embedding_model: sanitize_non_blank_string("embedding_model", embedding_model)?,
            api_key: direct_api_key,
            api_key_secret: named_api_key_secret,
            clear_api_key: api_key.as_ref().is_some_and(String::is_empty)
                || api_key_secret.is_some(),
            clear_api_key_secret: api_key.is_some() || api_key_secret.is_some(),
            timeout_ms: sanitize_positive_int(
                "timeout_ms",
                timeout_ms,
                "pass a positive integer number of milliseconds",
            )?,
            max_retries: sanitize_non_negative_int(
                "max_retries",
                max_retries,
                "pass zero to disable retries or a positive retry count",
            )?,
            retry_backoff_ms: sanitize_non_negative_int(
                "retry_backoff_ms",
                retry_backoff_ms,
                "pass zero to retry immediately or a positive integer number of milliseconds",
            )?,
            runtime: sanitize_runtime(runtime)?,
            candle_cache_dir: candle_cache_dir.map(|value| value.trim().to_owned()),
            candle_offline,
            candle_device: sanitize_candle_device(candle_device)?,
            candle_max_input_tokens: sanitize_non_negative_int(
                "candle_max_input_tokens",
                candle_max_input_tokens,
                "pass zero to disable the local Candle token cap or a positive integer token limit",
            )?,
            candle_max_concurrency: sanitize_non_negative_int(
                "candle_max_concurrency",
                candle_max_concurrency,
                "pass zero to disable the local Candle concurrency cap or a positive integer slot count",
            )?,
        })
    }

    pub(crate) fn from_profile_json(value: &Value) -> Result<Self> {
        let object = value.as_object().ok_or_else(|| {
            Error::Config(
                "stored profile config must be a JSON object; fix: rewrite it with postllm.profile_set(...)"
                    .to_owned(),
            )
        })?;
        validate_profile_keys(object)?;

        Ok(Self {
            base_url: parse_profile_string(object, "base_url", true)?,
            model: parse_profile_string(object, "model", true)?,
            embedding_model: parse_profile_string(object, "embedding_model", true)?,
            api_key: None,
            api_key_secret: parse_profile_string(object, "api_key_secret", true)?,
            clear_api_key: false,
            clear_api_key_secret: false,
            timeout_ms: parse_profile_int(object, "timeout_ms", false)?,
            max_retries: parse_profile_int(object, "max_retries", true)?,
            retry_backoff_ms: parse_profile_int(object, "retry_backoff_ms", true)?,
            runtime: parse_profile_runtime(object)?,
            candle_cache_dir: parse_profile_string(object, "candle_cache_dir", false)?,
            candle_offline: parse_profile_bool(object, "candle_offline")?,
            candle_device: parse_profile_candle_device(object)?,
            candle_max_input_tokens: parse_profile_int(object, "candle_max_input_tokens", true)?,
            candle_max_concurrency: parse_profile_int(object, "candle_max_concurrency", true)?,
        })
    }

    #[must_use]
    pub(crate) fn profile_is_empty(&self) -> bool {
        self.to_profile_json().as_object().is_none_or(Map::is_empty)
    }

    #[must_use]
    pub(crate) fn to_profile_json(&self) -> Value {
        let mut object = Map::new();

        insert_optional_string(&mut object, "base_url", self.base_url.as_deref());
        insert_optional_string(&mut object, "model", self.model.as_deref());
        insert_optional_string(
            &mut object,
            "embedding_model",
            self.embedding_model.as_deref(),
        );
        insert_optional_string(
            &mut object,
            "api_key_secret",
            self.api_key_secret.as_deref(),
        );
        insert_optional_i32(&mut object, "timeout_ms", self.timeout_ms);
        insert_optional_i32(&mut object, "max_retries", self.max_retries);
        insert_optional_i32(&mut object, "retry_backoff_ms", self.retry_backoff_ms);
        insert_optional_string(&mut object, "runtime", self.runtime.as_deref());
        insert_optional_string(
            &mut object,
            "candle_cache_dir",
            self.candle_cache_dir.as_deref(),
        );
        insert_optional_bool(&mut object, "candle_offline", self.candle_offline);
        insert_optional_string(&mut object, "candle_device", self.candle_device.as_deref());
        insert_optional_i32(
            &mut object,
            "candle_max_input_tokens",
            self.candle_max_input_tokens,
        );
        insert_optional_i32(
            &mut object,
            "candle_max_concurrency",
            self.candle_max_concurrency,
        );

        Value::Object(object)
    }
}

/// Registers all `PostgreSQL` GUCs used by the extension.
pub(crate) fn register() {
    register_core_runtime_gucs();
    register_hosted_runtime_gucs();
    register_candle_runtime_gucs();
}

fn register_core_runtime_gucs() {
    GucRegistry::define_string_guc(
        c"postllm.base_url",
        c"OpenAI-compatible hosted endpoint.",
        c"Fully qualified HTTP endpoint used by postllm for hosted chat completions, streaming, tools, structured outputs, and reranking.",
        &POSTLLM_BASE_URL,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"postllm.runtime",
        c"Backend runtime used by postllm.",
        c"Supported values are 'openai' for OpenAI-compatible HTTP endpoints and 'candle' for the local Candle lane. Candle embeddings and reranking are live today, and starter-model local chat and complete generation are both available.",
        &POSTLLM_RUNTIME,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"postllm.model",
        c"Default model name for postllm requests.",
        c"Used when SQL callers do not override the target model explicitly.",
        &POSTLLM_MODEL,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"postllm.embedding_model",
        c"Default local embedding model for Candle-backed embedding and reranking requests.",
        c"Used by postllm.embed, postllm.embed_many, and Candle-backed postllm.rerank unless SQL callers override the target model explicitly.",
        &POSTLLM_EMBEDDING_MODEL,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"postllm.api_key_secret",
        c"Named provider secret used to resolve postllm.api_key for the current session.",
        c"Metadata-only secret reference set by postllm.configure(api_key_secret => ...) or postllm.profile_apply(...); the referenced secret is stored encrypted in postllm.provider_secrets.",
        &POSTLLM_API_KEY_SECRET,
        GucContext::Suset,
        GucFlags::NO_SHOW_ALL | GucFlags::SUPERUSER_ONLY,
    );
}

fn register_hosted_runtime_gucs() {
    GucRegistry::define_int_guc(
        c"postllm.timeout_ms",
        c"Runtime timeout for postllm requests in milliseconds.",
        c"Upper bound on how long a PostgreSQL backend will wait for hosted HTTP responses or local Candle inference work.",
        &POSTLLM_TIMEOUT_MS,
        100,
        300_000,
        GucContext::Userset,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_int_guc(
        c"postllm.max_retries",
        c"Maximum number of transient HTTP retries for hosted runtimes.",
        c"Applies to OpenAI-compatible HTTP requests when transport failures or transient upstream statuses are classified as retryable.",
        &POSTLLM_MAX_RETRIES,
        0,
        10,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"postllm.retry_backoff_ms",
        c"Base retry backoff for transient HTTP retries in milliseconds.",
        c"Each transient retry waits this long before the first retry, then doubles on subsequent retries for OpenAI-compatible HTTP requests.",
        &POSTLLM_RETRY_BACKOFF_MS,
        0,
        60_000,
        GucContext::Userset,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_string_guc(
        c"postllm.http_allowed_hosts",
        c"Optional hosted HTTP host allowlist.",
        c"Comma-separated host, host:port, or *.suffix entries that limit which OpenAI-compatible endpoints postllm may contact. Empty means unrestricted.",
        &POSTLLM_HTTP_ALLOWED_HOSTS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"postllm.http_allowed_providers",
        c"Optional hosted HTTP provider safelist.",
        c"Comma-separated provider identities such as openai, ollama, and openai-compatible that limit which hosted provider families postllm may use. Empty means unrestricted.",
        &POSTLLM_HTTP_ALLOWED_PROVIDERS,
        GucContext::Suset,
        GucFlags::default(),
    );
}

fn register_candle_runtime_gucs() {
    GucRegistry::define_string_guc(
        c"postllm.candle_cache_dir",
        c"Optional cache directory for Candle-managed model artifacts.",
        c"Used by Candle-backed local embeddings and starter-model local chat generation so models, weights, and tokenizers can be cached locally.",
        &POSTLLM_CANDLE_CACHE_DIR,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"postllm.candle_offline",
        c"Whether Candle should use cached model artifacts only.",
        c"When enabled, Candle-backed local embeddings, reranking, model lifecycle commands, and starter-model local generation refuse all network fetches and require model artifacts to already exist in the local cache.",
        &POSTLLM_CANDLE_OFFLINE,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"postllm.candle_device",
        c"Preferred execution device for local Candle requests.",
        c"Supported values are 'auto' to prefer an available accelerator and fall back to CPU, 'cpu', 'cuda', and 'metal'. CUDA and Metal require builds that enable the optional candle-cuda or candle-metal crate feature.",
        &POSTLLM_CANDLE_DEVICE,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"postllm.candle_max_input_tokens",
        c"Optional cap on local Candle tokenized input size.",
        c"When greater than zero, each local Candle embedding input, rerank query/document, or generation prompt must stay within this many tokenized input tokens.",
        &POSTLLM_CANDLE_MAX_INPUT_TOKENS,
        0,
        262_144,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"postllm.candle_max_concurrency",
        c"Optional cap on concurrent local Candle requests.",
        c"When greater than zero, local Candle embedding, rerank, and generation requests will wait for one of this many global concurrency slots before starting work.",
        &POSTLLM_CANDLE_MAX_CONCURRENCY,
        0,
        128,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"postllm.api_key",
        c"Bearer token for authenticated LLM providers.",
        c"Optional API key forwarded as an Authorization header.",
        &POSTLLM_API_KEY,
        GucContext::Suset,
        GucFlags::NO_SHOW_ALL | GucFlags::SUPERUSER_ONLY,
    );
}

/// Resolves the current backend settings, optionally overriding the model for one request.
pub(crate) fn resolve(model_override: Option<&str>) -> Result<Settings> {
    let inputs = resolve_setting_inputs()?;
    let runtime = resolve_runtime()?;
    permissions::ensure_runtime_allowed(runtime)?;
    let model = resolve_generation_model(model_override)?;
    ensure_active_privileged_settings_allowed()?;
    let settings = inputs.into_settings(runtime, model);

    crate::http_policy::enforce_settings(&settings)?;

    Ok(settings)
}

/// Resolves settings for reranking requests.
pub(crate) fn resolve_rerank(model_override: Option<&str>) -> Result<Settings> {
    let inputs = resolve_setting_inputs()?;
    let runtime = resolve_runtime()?;
    permissions::ensure_runtime_allowed(runtime)?;
    let model = resolve_rerank_model(runtime, model_override)?;
    ensure_active_privileged_settings_allowed()?;
    let settings = inputs.into_settings(runtime, model);

    crate::http_policy::enforce_settings(&settings)?;

    Ok(settings)
}

/// Resolves settings for local Candle embedding requests.
pub(crate) fn resolve_embedding_settings(model_override: Option<&str>) -> Result<Settings> {
    let inputs = resolve_setting_inputs()?;
    ensure_active_privileged_settings_allowed()?;
    let model = resolve_embedding_model(model_override)?;

    Ok(inputs.into_settings(Runtime::Candle, model))
}

/// Resolves the current Candle embedding model, optionally overriding it for one request.
pub(crate) fn resolve_embedding_model(model_override: Option<&str>) -> Result<String> {
    let model = model_override.and_then(trimmed_or_none).map_or_else(
        || {
            required_setting(
                "postllm.embedding_model",
                string_setting(&POSTLLM_EMBEDDING_MODEL),
            )
        },
        |model| Ok(model.to_owned()),
    )?;

    let resolved = resolve_embedding_alias(&model)?;
    permissions::ensure_embedding_model_allowed(&resolved)?;
    Ok(resolved)
}

/// Resolves the optional Candle cache directory.
#[must_use]
pub(crate) fn resolve_candle_cache_dir() -> Option<String> {
    string_setting(&POSTLLM_CANDLE_CACHE_DIR)
}

/// Resolves whether Candle should use cached artifacts only.
#[must_use]
pub(crate) fn resolve_candle_offline() -> bool {
    POSTLLM_CANDLE_OFFLINE.get()
}

/// Resolves the configured local Candle device preference.
pub(crate) fn resolve_candle_device() -> Result<CandleDevice> {
    let value = required_setting(
        "postllm.candle_device",
        string_setting(&POSTLLM_CANDLE_DEVICE),
    )?;
    let normalized = value.trim().to_ascii_lowercase();

    CandleDevice::parse(&normalized).ok_or_else(|| {
        Error::invalid_setting(
            "postllm.candle_device",
            format!(
                "must be one of {}, got '{normalized}'",
                CandleDevice::ACCEPTED_VALUES,
            ),
            "SET postllm.candle_device = 'auto', 'cpu', 'cuda', or 'metal'",
        )
    })
}

pub(crate) fn resolve_http_allowed_hosts() -> Result<Vec<String>> {
    crate::http_policy::parse_allowed_hosts(
        string_setting(&POSTLLM_HTTP_ALLOWED_HOSTS).as_deref(),
        "postllm.http_allowed_hosts",
    )
}

pub(crate) fn resolve_http_allowed_providers() -> Result<Vec<String>> {
    crate::http_policy::parse_allowed_providers(
        string_setting(&POSTLLM_HTTP_ALLOWED_PROVIDERS).as_deref(),
        "postllm.http_allowed_providers",
    )
}

fn resolve_setting_inputs() -> Result<ResolvedSettingInputs> {
    Ok(ResolvedSettingInputs {
        base_url: string_setting(&POSTLLM_BASE_URL),
        api_key: string_setting(&POSTLLM_API_KEY),
        timeout_ms: resolve_timeout_ms()?,
        max_retries: resolve_max_retries()?,
        retry_backoff_ms: resolve_retry_backoff_ms()?,
        http_allowed_hosts: resolve_http_allowed_hosts()?,
        http_allowed_providers: resolve_http_allowed_providers()?,
        candle_cache_dir: string_setting(&POSTLLM_CANDLE_CACHE_DIR),
        candle_offline: POSTLLM_CANDLE_OFFLINE.get(),
        candle_device: resolve_candle_device()?,
        candle_max_input_tokens: resolve_candle_max_input_tokens()?,
        candle_max_concurrency: resolve_candle_max_concurrency()?,
    })
}

fn resolve_runtime() -> Result<Runtime> {
    let runtime_value = required_setting("postllm.runtime", string_setting(&POSTLLM_RUNTIME))?;

    Runtime::parse(&runtime_value).map_err(|_| {
        Error::invalid_setting(
            "postllm.runtime",
            format!(
                "must be '{}' or '{}', got '{}'",
                Runtime::OPENAI,
                Runtime::CANDLE,
                runtime_value.trim().to_ascii_lowercase(),
            ),
            "SET postllm.runtime = 'openai' or 'candle'",
        )
    })
}

fn resolve_rerank_model(runtime: Runtime, model_override: Option<&str>) -> Result<String> {
    match runtime {
        Runtime::OpenAi => match model_override.and_then(trimmed_or_none) {
            Some(model) => {
                let resolved = resolve_generation_alias(model)?;
                permissions::ensure_generation_model_allowed(&resolved)?;
                Ok(resolved)
            }
            None => resolve_generation_model(None),
        },
        Runtime::Candle => resolve_embedding_model(model_override),
    }
}

fn resolve_timeout_ms() -> Result<u64> {
    let timeout_ms = POSTLLM_TIMEOUT_MS.get();
    if timeout_ms <= 0 {
        return Err(Error::invalid_setting(
            "postllm.timeout_ms",
            "must be greater than zero",
            "SET postllm.timeout_ms = 30000 or another positive integer",
        ));
    }

    u64::try_from(timeout_ms).map_err(|_| {
        Error::invalid_setting(
            "postllm.timeout_ms",
            "must be representable as a u64",
            "SET postllm.timeout_ms = 30000 or another positive integer",
        )
    })
}

fn resolve_max_retries() -> Result<u32> {
    u32::try_from(POSTLLM_MAX_RETRIES.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.max_retries",
            "must be representable as a u32",
            "SET postllm.max_retries = 2 or another non-negative integer",
        )
    })
}

fn resolve_retry_backoff_ms() -> Result<u64> {
    u64::try_from(POSTLLM_RETRY_BACKOFF_MS.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.retry_backoff_ms",
            "must be representable as a u64",
            "SET postllm.retry_backoff_ms = 250 or another non-negative integer",
        )
    })
}

fn resolve_candle_max_input_tokens() -> Result<u32> {
    u32::try_from(POSTLLM_CANDLE_MAX_INPUT_TOKENS.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.candle_max_input_tokens",
            "must be representable as a u32",
            "SET postllm.candle_max_input_tokens = 0 to disable the cap or another non-negative integer",
        )
    })
}

fn resolve_candle_max_concurrency() -> Result<u32> {
    u32::try_from(POSTLLM_CANDLE_MAX_CONCURRENCY.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.candle_max_concurrency",
            "must be representable as a u32",
            "SET postllm.candle_max_concurrency = 0 to disable the cap or another non-negative integer",
        )
    })
}

/// Returns a JSON snapshot of the current backend-visible settings.
pub(crate) fn snapshot() -> Value {
    json!({
        "runtime": string_setting(&POSTLLM_RUNTIME),
        "base_url": string_setting(&POSTLLM_BASE_URL),
        "model": string_setting(&POSTLLM_MODEL),
        "embedding_model": string_setting(&POSTLLM_EMBEDDING_MODEL),
        "timeout_ms": POSTLLM_TIMEOUT_MS.get(),
        "max_retries": POSTLLM_MAX_RETRIES.get(),
        "retry_backoff_ms": POSTLLM_RETRY_BACKOFF_MS.get(),
        "http_allowed_hosts": string_setting(&POSTLLM_HTTP_ALLOWED_HOSTS),
        "http_allowed_providers": string_setting(&POSTLLM_HTTP_ALLOWED_PROVIDERS),
        "has_api_key": string_setting(&POSTLLM_API_KEY).is_some(),
        "api_key_source": api_key_source(),
        "api_key_secret": string_setting(&POSTLLM_API_KEY_SECRET),
        "candle_cache_dir": string_setting(&POSTLLM_CANDLE_CACHE_DIR),
        "candle_offline": POSTLLM_CANDLE_OFFLINE.get(),
        "candle_device": string_setting(&POSTLLM_CANDLE_DEVICE),
        "candle_max_input_tokens": POSTLLM_CANDLE_MAX_INPUT_TOKENS.get(),
        "candle_max_concurrency": POSTLLM_CANDLE_MAX_CONCURRENCY.get(),
        "capabilities": capabilities_snapshot(),
    })
}

/// Returns a JSON capability snapshot based on the current GUC state.
#[must_use]
pub(crate) fn capabilities_snapshot() -> Value {
    let model = string_setting(&POSTLLM_MODEL)
        .map(|model| resolve_generation_alias(&model).unwrap_or(model));
    let embedding_model = string_setting(&POSTLLM_EMBEDDING_MODEL)
        .map(|model| resolve_embedding_alias(&model).unwrap_or(model));

    crate::backend::CapabilitySnapshot::from_raw(
        string_setting(&POSTLLM_RUNTIME),
        model,
        embedding_model,
    )
    .snapshot()
}

/// Applies session-local configuration overrides and returns the resulting settings snapshot.
#[expect(
    clippy::too_many_arguments,
    reason = "the SQL configure(...) entry point maps directly onto these optional session overrides"
)]
pub(crate) fn configure_session(
    base_url: Option<&str>,
    model: Option<&str>,
    embedding_model: Option<&str>,
    api_key: Option<&str>,
    api_key_secret: Option<&str>,
    timeout_ms: Option<i32>,
    max_retries: Option<i32>,
    retry_backoff_ms: Option<i32>,
    runtime: Option<&str>,
    candle_cache_dir: Option<&str>,
    candle_offline: Option<bool>,
    candle_device: Option<&str>,
    candle_max_input_tokens: Option<i32>,
    candle_max_concurrency: Option<i32>,
) -> Result<Value> {
    let overrides = SessionOverrides::from_configure_args(
        base_url,
        model,
        embedding_model,
        api_key,
        api_key_secret,
        timeout_ms,
        max_retries,
        retry_backoff_ms,
        runtime,
        candle_cache_dir,
        candle_offline,
        candle_device,
        candle_max_input_tokens,
        candle_max_concurrency,
    )?;

    apply_session_overrides(&overrides)
}

/// Applies validated session-local overrides and returns the resulting settings snapshot.
pub(crate) fn apply_session_overrides(overrides: &SessionOverrides) -> Result<Value> {
    apply_session_overrides_internal(overrides)
}

/// Resets non-secret settings to their extension defaults, then applies the provided profile.
pub(crate) fn apply_profile_overrides(overrides: &SessionOverrides) -> Result<Value> {
    reset_profile_managed_settings_to_defaults()?;
    apply_session_overrides_internal(overrides)
}

fn apply_session_overrides_internal(overrides: &SessionOverrides) -> Result<Value> {
    apply_runtime_identity_overrides(overrides)?;
    apply_auth_overrides(overrides)?;
    apply_hosted_runtime_overrides(overrides)?;
    apply_candle_runtime_overrides(overrides)?;

    Ok(snapshot())
}

fn apply_runtime_identity_overrides(overrides: &SessionOverrides) -> Result<()> {
    if let Some(base_url) = overrides.base_url.as_deref() {
        if base_url != DEFAULT_BASE_URL {
            permissions::ensure_setting_change_allowed("base_url")?;
        }
        set_session_string("postllm.base_url", base_url)?;
    }

    if let Some(runtime) = overrides.runtime.as_deref() {
        permissions::ensure_runtime_allowed(Runtime::parse(runtime)?)?;
        set_session_string("postllm.runtime", runtime)?;
    }

    if let Some(model) = overrides.model.as_deref() {
        let resolved = resolve_generation_alias(model)?;
        permissions::ensure_generation_model_allowed(&resolved)?;
        set_session_string("postllm.model", model)?;
    }

    if let Some(embedding_model) = overrides.embedding_model.as_deref() {
        let resolved = resolve_embedding_alias(embedding_model)?;
        permissions::ensure_embedding_model_allowed(&resolved)?;
        set_session_string("postllm.embedding_model", embedding_model)?;
    }

    Ok(())
}

fn apply_auth_overrides(overrides: &SessionOverrides) -> Result<()> {
    if overrides.clear_api_key {
        set_session_string("postllm.api_key", "")?;
    }

    if overrides.clear_api_key_secret {
        set_session_string("postllm.api_key_secret", "")?;
    }

    if let Some(api_key) = overrides.api_key.as_deref() {
        permissions::ensure_setting_change_allowed("api_key")?;
        set_session_string("postllm.api_key", api_key)?;
    }

    if let Some(api_key_secret) = overrides.api_key_secret.as_deref() {
        permissions::ensure_setting_change_allowed("api_key_secret")?;
        let resolved_api_key = crate::catalog::secret_value(api_key_secret)?;
        set_session_string("postllm.api_key", &resolved_api_key)?;
        set_session_string("postllm.api_key_secret", api_key_secret)?;
    }

    Ok(())
}

fn apply_hosted_runtime_overrides(overrides: &SessionOverrides) -> Result<()> {
    apply_optional_i32_override(
        "postllm.timeout_ms",
        "timeout_ms",
        overrides.timeout_ms,
        DEFAULT_TIMEOUT_MS,
    )?;
    apply_optional_i32_override(
        "postllm.max_retries",
        "max_retries",
        overrides.max_retries,
        DEFAULT_MAX_RETRIES,
    )?;
    apply_optional_i32_override(
        "postllm.retry_backoff_ms",
        "retry_backoff_ms",
        overrides.retry_backoff_ms,
        DEFAULT_RETRY_BACKOFF_MS,
    )?;

    Ok(())
}

fn apply_candle_runtime_overrides(overrides: &SessionOverrides) -> Result<()> {
    if let Some(candle_cache_dir) = overrides.candle_cache_dir.as_deref() {
        if !candle_cache_dir.trim().is_empty() {
            permissions::ensure_setting_change_allowed("candle_cache_dir")?;
        }
        set_session_string("postllm.candle_cache_dir", candle_cache_dir)?;
    }

    if let Some(candle_offline) = overrides.candle_offline {
        if candle_offline {
            permissions::ensure_setting_change_allowed("candle_offline")?;
        }
        set_session_string(
            "postllm.candle_offline",
            if candle_offline { "on" } else { "off" },
        )?;
    }

    if let Some(candle_device) = overrides.candle_device.as_deref() {
        if candle_device != DEFAULT_CANDLE_DEVICE {
            permissions::ensure_setting_change_allowed("candle_device")?;
        }
        set_session_string("postllm.candle_device", candle_device)?;
    }

    apply_optional_i32_override(
        "postllm.candle_max_input_tokens",
        "candle_max_input_tokens",
        overrides.candle_max_input_tokens,
        DEFAULT_CANDLE_MAX_INPUT_TOKENS,
    )?;
    apply_optional_i32_override(
        "postllm.candle_max_concurrency",
        "candle_max_concurrency",
        overrides.candle_max_concurrency,
        DEFAULT_CANDLE_MAX_CONCURRENCY,
    )?;

    Ok(())
}

fn apply_optional_i32_override(
    setting_name: &str,
    permission_name: &str,
    value: Option<i32>,
    default_value: i32,
) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };

    if value != default_value {
        permissions::ensure_setting_change_allowed(permission_name)?;
    }
    set_session_string(setting_name, &value.to_string())
}

fn reset_profile_managed_settings_to_defaults() -> Result<()> {
    for (setting_name, default_value) in PROFILE_MANAGED_STRING_DEFAULTS {
        set_session_string(setting_name, default_value)?;
    }

    for (setting_name, default_value) in PROFILE_MANAGED_INT_DEFAULTS {
        set_session_string(setting_name, &default_value.to_string())?;
    }

    set_session_string("postllm.candle_cache_dir", "")?;
    set_session_string("postllm.candle_offline", "off")?;
    Ok(())
}

fn set_session_string(name: &str, value: &str) -> Result<()> {
    drop(Spi::get_one_with_args::<String>(
        "SELECT set_config($1, $2, false)",
        &[DatumWithOid::from(name), DatumWithOid::from(value)],
    )?);

    Ok(())
}

fn resolve_generation_model(model_override: Option<&str>) -> Result<String> {
    let model = model_override.and_then(trimmed_or_none).map_or_else(
        || required_setting("postllm.model", string_setting(&POSTLLM_MODEL)),
        |model| Ok(model.to_owned()),
    )?;

    let resolved = resolve_generation_alias(&model)?;
    permissions::ensure_generation_model_allowed(&resolved)?;
    Ok(resolved)
}

pub(crate) fn ensure_active_privileged_settings_allowed() -> Result<()> {
    ensure_present_string_setting_allowed(string_setting(&POSTLLM_API_KEY), "api_key")?;
    ensure_present_string_setting_allowed(
        string_setting(&POSTLLM_API_KEY_SECRET),
        "api_key_secret",
    )?;
    ensure_present_string_setting_allowed(
        string_setting(&POSTLLM_CANDLE_CACHE_DIR),
        "candle_cache_dir",
    )?;
    ensure_non_default_string_setting_allowed(
        string_setting(&POSTLLM_BASE_URL),
        DEFAULT_BASE_URL,
        "base_url",
    )?;
    ensure_non_default_string_setting_allowed(
        string_setting(&POSTLLM_CANDLE_DEVICE),
        DEFAULT_CANDLE_DEVICE,
        "candle_device",
    )?;
    ensure_non_default_i32_setting_allowed(
        POSTLLM_TIMEOUT_MS.get(),
        DEFAULT_TIMEOUT_MS,
        "timeout_ms",
    )?;
    ensure_non_default_i32_setting_allowed(
        POSTLLM_MAX_RETRIES.get(),
        DEFAULT_MAX_RETRIES,
        "max_retries",
    )?;
    ensure_non_default_i32_setting_allowed(
        POSTLLM_RETRY_BACKOFF_MS.get(),
        DEFAULT_RETRY_BACKOFF_MS,
        "retry_backoff_ms",
    )?;
    ensure_non_default_i32_setting_allowed(
        POSTLLM_CANDLE_MAX_INPUT_TOKENS.get(),
        DEFAULT_CANDLE_MAX_INPUT_TOKENS,
        "candle_max_input_tokens",
    )?;
    ensure_non_default_i32_setting_allowed(
        POSTLLM_CANDLE_MAX_CONCURRENCY.get(),
        DEFAULT_CANDLE_MAX_CONCURRENCY,
        "candle_max_concurrency",
    )?;
    ensure_true_setting_allowed(POSTLLM_CANDLE_OFFLINE.get(), "candle_offline")?;

    Ok(())
}

fn ensure_present_string_setting_allowed(
    value: Option<String>,
    permission_name: &str,
) -> Result<()> {
    if value.is_some() {
        permissions::ensure_setting_change_allowed(permission_name)?;
    }

    Ok(())
}

fn ensure_non_default_string_setting_allowed(
    value: Option<String>,
    default_value: &str,
    permission_name: &str,
) -> Result<()> {
    if value
        .as_deref()
        .is_some_and(|current| current != default_value)
    {
        permissions::ensure_setting_change_allowed(permission_name)?;
    }

    Ok(())
}

fn ensure_non_default_i32_setting_allowed(
    value: i32,
    default_value: i32,
    permission_name: &str,
) -> Result<()> {
    if value != default_value {
        permissions::ensure_setting_change_allowed(permission_name)?;
    }

    Ok(())
}

fn ensure_true_setting_allowed(value: bool, permission_name: &str) -> Result<()> {
    if value {
        permissions::ensure_setting_change_allowed(permission_name)?;
    }

    Ok(())
}

fn resolve_generation_alias(model: &str) -> Result<String> {
    crate::catalog::resolve_model_alias(model, crate::catalog::ModelAliasLane::Generation)
        .map(|resolved| resolved.unwrap_or_else(|| model.to_owned()))
}

fn resolve_embedding_alias(model: &str) -> Result<String> {
    crate::catalog::resolve_model_alias(model, crate::catalog::ModelAliasLane::Embedding)
        .map(|resolved| resolved.unwrap_or_else(|| model.to_owned()))
}

fn sanitize_non_blank_string(name: &str, value: Option<&str>) -> Result<Option<String>> {
    value
        .map(|value| require_non_blank(name, value).map(str::to_owned))
        .transpose()
}

fn sanitize_positive_int(name: &str, value: Option<i32>, fix: &str) -> Result<Option<i32>> {
    value
        .map(|value| {
            if value <= 0 {
                Err(Error::invalid_argument(
                    name,
                    format!("must be greater than zero, got {value}"),
                    fix,
                ))
            } else {
                Ok(value)
            }
        })
        .transpose()
}

fn sanitize_non_negative_int(name: &str, value: Option<i32>, fix: &str) -> Result<Option<i32>> {
    value
        .map(|value| {
            if value < 0 {
                Err(Error::invalid_argument(
                    name,
                    format!("must be greater than or equal to zero, got {value}"),
                    fix,
                ))
            } else {
                Ok(value)
            }
        })
        .transpose()
}

fn sanitize_runtime(value: Option<&str>) -> Result<Option<String>> {
    value
        .map(|runtime| {
            let runtime = require_non_blank("runtime", runtime)?;
            let parsed = Runtime::parse(runtime)?;
            Ok(parsed.as_str().to_owned())
        })
        .transpose()
}

fn sanitize_candle_device(value: Option<&str>) -> Result<Option<String>> {
    value
        .map(|value| {
            let normalized = require_non_blank("candle_device", value)?
                .trim()
                .to_ascii_lowercase();
            CandleDevice::parse(&normalized)
                .map(|device| device.as_str().to_owned())
                .ok_or_else(|| {
                    Error::invalid_argument(
                        "candle_device",
                        format!(
                            "must be one of {}, got '{normalized}'",
                            CandleDevice::ACCEPTED_VALUES,
                        ),
                        "pass candle_device => 'auto', 'cpu', 'cuda', or 'metal'",
                    )
                })
        })
        .transpose()
}

fn validate_profile_keys(object: &Map<String, Value>) -> Result<()> {
    const ALLOWED_KEYS: [&str; 13] = [
        "base_url",
        "model",
        "embedding_model",
        "api_key_secret",
        "timeout_ms",
        "max_retries",
        "retry_backoff_ms",
        "runtime",
        "candle_cache_dir",
        "candle_offline",
        "candle_device",
        "candle_max_input_tokens",
        "candle_max_concurrency",
    ];

    for key in object.keys() {
        if !ALLOWED_KEYS.contains(&key.as_str()) {
            return Err(Error::Config(format!(
                "stored profile config includes unsupported key '{key}'; fix: rewrite it with postllm.profile_set(...)"
            )));
        }
    }

    Ok(())
}

fn parse_profile_string(
    object: &Map<String, Value>,
    key: &str,
    require_non_blank_value: bool,
) -> Result<Option<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            if require_non_blank_value {
                Ok(Some(require_non_blank(key, value)?.to_owned()))
            } else {
                Ok(Some(value.trim().to_owned()))
            }
        }
        Some(_) => Err(Error::Config(format!(
            "stored profile config field '{key}' must be a string or null; fix: rewrite it with postllm.profile_set(...)"
        ))),
    }
}

fn parse_profile_int(
    object: &Map<String, Value>,
    key: &str,
    allow_zero: bool,
) -> Result<Option<i32>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => {
            let value = number.as_i64().ok_or_else(|| {
                Error::Config(format!(
                    "stored profile config field '{key}' must be an integer or null; fix: rewrite it with postllm.profile_set(...)"
                ))
            })?;
            let value = i32::try_from(value).map_err(|_| {
                Error::Config(format!(
                    "stored profile config field '{key}' must fit into a PostgreSQL integer; fix: rewrite it with postllm.profile_set(...)"
                ))
            })?;
            if allow_zero {
                sanitize_non_negative_int(key, Some(value), "rewrite the profile with a non-negative integer")?
            } else {
                sanitize_positive_int(key, Some(value), "rewrite the profile with a positive integer")?
            }
            .ok_or_else(|| {
                Error::Config(format!(
                    "stored profile config field '{key}' did not validate; fix: rewrite it with postllm.profile_set(...)"
                ))
            })
            .map(Some)
        }
        Some(_) => Err(Error::Config(format!(
            "stored profile config field '{key}' must be an integer or null; fix: rewrite it with postllm.profile_set(...)"
        ))),
    }
}

fn parse_profile_bool(object: &Map<String, Value>, key: &str) -> Result<Option<bool>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(Error::Config(format!(
            "stored profile config field '{key}' must be a boolean or null; fix: rewrite it with postllm.profile_set(...)"
        ))),
    }
}

fn parse_profile_runtime(object: &Map<String, Value>) -> Result<Option<String>> {
    sanitize_runtime(parse_profile_string(object, "runtime", true)?.as_deref())
}

fn parse_profile_candle_device(object: &Map<String, Value>) -> Result<Option<String>> {
    sanitize_candle_device(parse_profile_string(object, "candle_device", true)?.as_deref())
}

fn insert_optional_string(object: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        object.insert(key.to_owned(), Value::String(value.to_owned()));
    }
}

fn insert_optional_i32(object: &mut Map<String, Value>, key: &str, value: Option<i32>) {
    if let Some(value) = value {
        object.insert(key.to_owned(), json!(value));
    }
}

fn insert_optional_bool(object: &mut Map<String, Value>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        object.insert(key.to_owned(), json!(value));
    }
}

fn string_setting(setting: &'static GucSetting<Option<CString>>) -> Option<String> {
    setting
        .get()
        .map(|value| value.to_string_lossy().into_owned())
        .and_then(|value| trimmed_or_none(&value).map(str::to_owned))
}

fn api_key_source() -> &'static str {
    if string_setting(&POSTLLM_API_KEY_SECRET).is_some() {
        "secret"
    } else if string_setting(&POSTLLM_API_KEY).is_some() {
        "direct"
    } else {
        "none"
    }
}

fn require_non_blank<'value>(name: &str, value: &'value str) -> Result<&'value str> {
    trimmed_or_none(value).ok_or_else(|| {
        Error::invalid_argument(
            name,
            "must not be empty or whitespace-only",
            format!("pass a non-empty value for '{name}'"),
        )
    })
}

fn required_setting(name: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| Error::missing_setting(name, missing_setting_fix(name)))
}

fn trimmed_or_none(value: &str) -> Option<&str> {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn missing_setting_fix(name: &str) -> String {
    match name {
        "postllm.runtime" => "SET postllm.runtime = 'openai' or 'candle'".to_owned(),
        "postllm.model" => {
            "SET postllm.model = 'llama3.2' or pass model => '...' to the SQL function"
                .to_owned()
        }
        "postllm.embedding_model" => "SET postllm.embedding_model = 'sentence-transformers/paraphrase-MiniLM-L3-v2' or pass model => '...' to postllm.embed/postllm.embed_many".to_owned(),
        _ => format!("SET {name} = '...'"),
    }
}
