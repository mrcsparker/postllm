#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::backend::{CandleDevice, Runtime, Settings};
use crate::error::{Error, Result};
use pgrx::datum::DatumWithOid;
use pgrx::spi::Spi;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use serde_json::{Value, json};
use std::ffi::CString;

static POSTLLM_BASE_URL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"http://127.0.0.1:11434/v1/chat/completions"));
static POSTLLM_RUNTIME: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"openai"));
static POSTLLM_MODEL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"llama3.2"));
static POSTLLM_EMBEDDING_MODEL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"sentence-transformers/paraphrase-MiniLM-L3-v2"));
static POSTLLM_API_KEY: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static POSTLLM_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static POSTLLM_MAX_RETRIES: GucSetting<i32> = GucSetting::<i32>::new(2);
static POSTLLM_RETRY_BACKOFF_MS: GucSetting<i32> = GucSetting::<i32>::new(250);
static POSTLLM_CANDLE_CACHE_DIR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static POSTLLM_CANDLE_OFFLINE: GucSetting<bool> = GucSetting::<bool>::new(false);
static POSTLLM_CANDLE_DEVICE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"auto"));
static POSTLLM_CANDLE_MAX_INPUT_TOKENS: GucSetting<i32> = GucSetting::<i32>::new(0);
static POSTLLM_CANDLE_MAX_CONCURRENCY: GucSetting<i32> = GucSetting::<i32>::new(0);

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
    let timeout_ms = POSTLLM_TIMEOUT_MS.get();
    if timeout_ms <= 0 {
        return Err(Error::invalid_setting(
            "postllm.timeout_ms",
            "must be greater than zero",
            "SET postllm.timeout_ms = 30000 or another positive integer",
        ));
    }

    let timeout_ms = u64::try_from(timeout_ms).map_err(|_| {
        Error::invalid_setting(
            "postllm.timeout_ms",
            "must be representable as a u64",
            "SET postllm.timeout_ms = 30000 or another positive integer",
        )
    })?;
    let max_retries = POSTLLM_MAX_RETRIES.get();
    let max_retries = u32::try_from(max_retries).map_err(|_| {
        Error::invalid_setting(
            "postllm.max_retries",
            "must be representable as a u32",
            "SET postllm.max_retries = 2 or another non-negative integer",
        )
    })?;
    let retry_backoff_ms = POSTLLM_RETRY_BACKOFF_MS.get();
    let retry_backoff_ms = u64::try_from(retry_backoff_ms).map_err(|_| {
        Error::invalid_setting(
            "postllm.retry_backoff_ms",
            "must be representable as a u64",
            "SET postllm.retry_backoff_ms = 250 or another non-negative integer",
        )
    })?;
    let candle_max_input_tokens = u32::try_from(POSTLLM_CANDLE_MAX_INPUT_TOKENS.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.candle_max_input_tokens",
            "must be representable as a u32",
            "SET postllm.candle_max_input_tokens = 0 to disable the cap or another non-negative integer",
        )
    })?;
    let candle_max_concurrency = u32::try_from(POSTLLM_CANDLE_MAX_CONCURRENCY.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.candle_max_concurrency",
            "must be representable as a u32",
            "SET postllm.candle_max_concurrency = 0 to disable the cap or another non-negative integer",
        )
    })?;
    let candle_device = resolve_candle_device()?;

    let runtime_value = required_setting("postllm.runtime", string_setting(&POSTLLM_RUNTIME))?;
    let runtime = Runtime::parse(&runtime_value).map_err(|_| {
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
    })?;
    let model = match model_override.and_then(trimmed_or_none) {
        Some(model) => model.to_owned(),
        None => required_setting("postllm.model", string_setting(&POSTLLM_MODEL))?,
    };

    Ok(Settings {
        runtime,
        model,
        base_url: string_setting(&POSTLLM_BASE_URL),
        api_key: string_setting(&POSTLLM_API_KEY),
        timeout_ms,
        max_retries,
        retry_backoff_ms,
        candle_cache_dir: string_setting(&POSTLLM_CANDLE_CACHE_DIR),
        candle_offline: POSTLLM_CANDLE_OFFLINE.get(),
        candle_device,
        candle_max_input_tokens,
        candle_max_concurrency,
    })
}

/// Resolves settings for reranking requests.
pub(crate) fn resolve_rerank(model_override: Option<&str>) -> Result<Settings> {
    let timeout_ms = POSTLLM_TIMEOUT_MS.get();
    if timeout_ms <= 0 {
        return Err(Error::invalid_setting(
            "postllm.timeout_ms",
            "must be greater than zero",
            "SET postllm.timeout_ms = 30000 or another positive integer",
        ));
    }

    let timeout_ms = u64::try_from(timeout_ms).map_err(|_| {
        Error::invalid_setting(
            "postllm.timeout_ms",
            "must be representable as a u64",
            "SET postllm.timeout_ms = 30000 or another positive integer",
        )
    })?;
    let max_retries = POSTLLM_MAX_RETRIES.get();
    let max_retries = u32::try_from(max_retries).map_err(|_| {
        Error::invalid_setting(
            "postllm.max_retries",
            "must be representable as a u32",
            "SET postllm.max_retries = 2 or another non-negative integer",
        )
    })?;
    let retry_backoff_ms = POSTLLM_RETRY_BACKOFF_MS.get();
    let retry_backoff_ms = u64::try_from(retry_backoff_ms).map_err(|_| {
        Error::invalid_setting(
            "postllm.retry_backoff_ms",
            "must be representable as a u64",
            "SET postllm.retry_backoff_ms = 250 or another non-negative integer",
        )
    })?;
    let candle_max_input_tokens = u32::try_from(POSTLLM_CANDLE_MAX_INPUT_TOKENS.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.candle_max_input_tokens",
            "must be representable as a u32",
            "SET postllm.candle_max_input_tokens = 0 to disable the cap or another non-negative integer",
        )
    })?;
    let candle_max_concurrency = u32::try_from(POSTLLM_CANDLE_MAX_CONCURRENCY.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.candle_max_concurrency",
            "must be representable as a u32",
            "SET postllm.candle_max_concurrency = 0 to disable the cap or another non-negative integer",
        )
    })?;
    let candle_device = resolve_candle_device()?;

    let runtime_value = required_setting("postllm.runtime", string_setting(&POSTLLM_RUNTIME))?;
    let runtime = Runtime::parse(&runtime_value).map_err(|_| {
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
    })?;
    let model = match runtime {
        Runtime::OpenAi => match model_override.and_then(trimmed_or_none) {
            Some(model) => model.to_owned(),
            None => required_setting("postllm.model", string_setting(&POSTLLM_MODEL))?,
        },
        Runtime::Candle => resolve_embedding_model(model_override)?,
    };

    Ok(Settings {
        runtime,
        model,
        base_url: string_setting(&POSTLLM_BASE_URL),
        api_key: string_setting(&POSTLLM_API_KEY),
        timeout_ms,
        max_retries,
        retry_backoff_ms,
        candle_cache_dir: string_setting(&POSTLLM_CANDLE_CACHE_DIR),
        candle_offline: POSTLLM_CANDLE_OFFLINE.get(),
        candle_device,
        candle_max_input_tokens,
        candle_max_concurrency,
    })
}

/// Resolves settings for local Candle embedding requests.
pub(crate) fn resolve_embedding_settings(model_override: Option<&str>) -> Result<Settings> {
    let timeout_ms = POSTLLM_TIMEOUT_MS.get();
    if timeout_ms <= 0 {
        return Err(Error::invalid_setting(
            "postllm.timeout_ms",
            "must be greater than zero",
            "SET postllm.timeout_ms = 30000 or another positive integer",
        ));
    }

    let timeout_ms = u64::try_from(timeout_ms).map_err(|_| {
        Error::invalid_setting(
            "postllm.timeout_ms",
            "must be representable as a u64",
            "SET postllm.timeout_ms = 30000 or another positive integer",
        )
    })?;
    let max_retries = u32::try_from(POSTLLM_MAX_RETRIES.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.max_retries",
            "must be representable as a u32",
            "SET postllm.max_retries = 2 or another non-negative integer",
        )
    })?;
    let retry_backoff_ms = u64::try_from(POSTLLM_RETRY_BACKOFF_MS.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.retry_backoff_ms",
            "must be representable as a u64",
            "SET postllm.retry_backoff_ms = 250 or another non-negative integer",
        )
    })?;
    let candle_max_input_tokens = u32::try_from(POSTLLM_CANDLE_MAX_INPUT_TOKENS.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.candle_max_input_tokens",
            "must be representable as a u32",
            "SET postllm.candle_max_input_tokens = 0 to disable the cap or another non-negative integer",
        )
    })?;
    let candle_max_concurrency = u32::try_from(POSTLLM_CANDLE_MAX_CONCURRENCY.get()).map_err(|_| {
        Error::invalid_setting(
            "postllm.candle_max_concurrency",
            "must be representable as a u32",
            "SET postllm.candle_max_concurrency = 0 to disable the cap or another non-negative integer",
        )
    })?;
    let candle_device = resolve_candle_device()?;

    Ok(Settings {
        runtime: Runtime::Candle,
        model: resolve_embedding_model(model_override)?,
        base_url: string_setting(&POSTLLM_BASE_URL),
        api_key: string_setting(&POSTLLM_API_KEY),
        timeout_ms,
        max_retries,
        retry_backoff_ms,
        candle_cache_dir: string_setting(&POSTLLM_CANDLE_CACHE_DIR),
        candle_offline: POSTLLM_CANDLE_OFFLINE.get(),
        candle_device,
        candle_max_input_tokens,
        candle_max_concurrency,
    })
}

/// Resolves the current Candle embedding model, optionally overriding it for one request.
pub(crate) fn resolve_embedding_model(model_override: Option<&str>) -> Result<String> {
    model_override.and_then(trimmed_or_none).map_or_else(
        || {
            required_setting(
                "postllm.embedding_model",
                string_setting(&POSTLLM_EMBEDDING_MODEL),
            )
        },
        |model| Ok(model.to_owned()),
    )
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
        "has_api_key": string_setting(&POSTLLM_API_KEY).is_some(),
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
    crate::backend::CapabilitySnapshot::from_raw(
        string_setting(&POSTLLM_RUNTIME),
        string_setting(&POSTLLM_MODEL),
        string_setting(&POSTLLM_EMBEDDING_MODEL),
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
    if let Some(base_url) = base_url {
        set_session_string("postllm.base_url", require_non_blank("base_url", base_url)?)?;
    }

    if let Some(runtime) = runtime {
        let runtime = require_non_blank("runtime", runtime)?;
        let _ = Runtime::parse(runtime)?;
        set_session_string("postllm.runtime", runtime)?;
    }

    if let Some(model) = model {
        set_session_string("postllm.model", require_non_blank("model", model)?)?;
    }

    if let Some(embedding_model) = embedding_model {
        set_session_string(
            "postllm.embedding_model",
            require_non_blank("embedding_model", embedding_model)?,
        )?;
    }

    if let Some(api_key) = api_key {
        set_session_string("postllm.api_key", api_key.trim())?;
    }

    if let Some(timeout_ms) = timeout_ms {
        if timeout_ms <= 0 {
            return Err(Error::invalid_argument(
                "timeout_ms",
                format!("must be greater than zero, got {timeout_ms}"),
                "pass a positive integer number of milliseconds",
            ));
        }

        set_session_string("postllm.timeout_ms", &timeout_ms.to_string())?;
    }

    if let Some(max_retries) = max_retries {
        if max_retries < 0 {
            return Err(Error::invalid_argument(
                "max_retries",
                format!("must be greater than or equal to zero, got {max_retries}"),
                "pass zero to disable retries or a positive retry count",
            ));
        }

        set_session_string("postllm.max_retries", &max_retries.to_string())?;
    }

    if let Some(retry_backoff_ms) = retry_backoff_ms {
        if retry_backoff_ms < 0 {
            return Err(Error::invalid_argument(
                "retry_backoff_ms",
                format!("must be greater than or equal to zero, got {retry_backoff_ms}"),
                "pass zero to retry immediately or a positive integer number of milliseconds",
            ));
        }

        set_session_string("postllm.retry_backoff_ms", &retry_backoff_ms.to_string())?;
    }

    if let Some(candle_cache_dir) = candle_cache_dir {
        set_session_string("postllm.candle_cache_dir", candle_cache_dir.trim())?;
    }

    if let Some(candle_offline) = candle_offline {
        set_session_string(
            "postllm.candle_offline",
            if candle_offline { "on" } else { "off" },
        )?;
    }

    if let Some(candle_device) = candle_device {
        set_candle_device_session(candle_device)?;
    }

    if let Some(candle_max_input_tokens) = candle_max_input_tokens {
        if candle_max_input_tokens < 0 {
            return Err(Error::invalid_argument(
                "candle_max_input_tokens",
                format!("must be greater than or equal to zero, got {candle_max_input_tokens}"),
                "pass zero to disable the local Candle token cap or a positive integer token limit",
            ));
        }

        set_session_string(
            "postllm.candle_max_input_tokens",
            &candle_max_input_tokens.to_string(),
        )?;
    }

    if let Some(candle_max_concurrency) = candle_max_concurrency {
        if candle_max_concurrency < 0 {
            return Err(Error::invalid_argument(
                "candle_max_concurrency",
                format!("must be greater than or equal to zero, got {candle_max_concurrency}"),
                "pass zero to disable the local Candle concurrency cap or a positive integer slot count",
            ));
        }

        set_session_string(
            "postllm.candle_max_concurrency",
            &candle_max_concurrency.to_string(),
        )?;
    }

    Ok(snapshot())
}

fn set_session_string(name: &str, value: &str) -> Result<()> {
    drop(Spi::get_one_with_args::<String>(
        "SELECT set_config($1, $2, false)",
        &[DatumWithOid::from(name), DatumWithOid::from(value)],
    )?);

    Ok(())
}

fn set_candle_device_session(candle_device: &str) -> Result<()> {
    let candle_device = require_non_blank("candle_device", candle_device)?;
    let normalized = candle_device.trim().to_ascii_lowercase();
    let candle_device = CandleDevice::parse(&normalized).ok_or_else(|| {
        Error::invalid_argument(
            "candle_device",
            format!(
                "must be one of {}, got '{normalized}'",
                CandleDevice::ACCEPTED_VALUES,
            ),
            "pass candle_device => 'auto', 'cpu', 'cuda', or 'metal'",
        )
    })?;
    set_session_string("postllm.candle_device", candle_device.as_str())
}

fn string_setting(setting: &'static GucSetting<Option<CString>>) -> Option<String> {
    setting
        .get()
        .map(|value| value.to_string_lossy().into_owned())
        .and_then(|value| trimmed_or_none(&value).map(str::to_owned))
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
