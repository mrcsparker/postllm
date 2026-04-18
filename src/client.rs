#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::error::{Error, Result};
use reqwest::StatusCode;
use reqwest::Url;
use reqwest::blocking::{Client, Response};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

const ANTHROPIC_API_VERSION: &str = "2023-06-01";
const ANTHROPIC_DEFAULT_MAX_TOKENS: i32 = 1024;
const INTERRUPT_POLL_INTERVAL_MS: u64 = 25;
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(INTERRUPT_POLL_INTERVAL_MS);

type HttpRequestResult<T> = core::result::Result<T, HttpRequestError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostedEndpointFlavor {
    ChatCompletions,
    Responses,
    AnthropicMessages,
}

#[derive(Debug)]
enum HttpRequestError {
    Transport(reqwest::Error),
    ResponseRead(std::io::Error),
    Upstream { status: StatusCode, body: String },
    JsonDecode(serde_json::Error),
    MalformedStream(String),
    Postllm(Error),
    Interrupted,
    Internal(String),
}

impl HttpRequestError {
    fn is_transient(&self) -> bool {
        match self {
            Self::Transport(error) => {
                error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
            }
            Self::ResponseRead(_) => true,
            Self::Upstream { status, .. } => is_transient_http_status(*status),
            Self::JsonDecode(_)
            | Self::MalformedStream(_)
            | Self::Postllm(_)
            | Self::Interrupted
            | Self::Internal(_) => false,
        }
    }

    fn into_error(self) -> Error {
        match self {
            Self::Transport(error) => Error::Http(error),
            Self::ResponseRead(error) => Error::HttpRead(error.to_string()),
            Self::Upstream { status, body } => Error::Upstream { status, body },
            Self::JsonDecode(error) => Error::Json(error),
            Self::MalformedStream(message) => Error::MalformedStream(message),
            Self::Postllm(error) => error,
            Self::Interrupted => Error::Interrupted,
            Self::Internal(message) => Error::Internal(message),
        }
    }
}

/// Executes a chat-completions request and returns the raw JSON response.
pub(crate) fn chat_response(
    settings: &crate::backend::Settings,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
) -> Result<Value> {
    let settings = settings.clone();
    let messages = messages.to_vec();
    let response_format = response_format.cloned();
    let tools = tools.map(<[Value]>::to_vec);
    let tool_choice = tool_choice.cloned();

    run_interruptible_request(move |cancelled| {
        execute_chat_response(
            &settings,
            &messages,
            options,
            response_format.as_ref(),
            tools.as_deref(),
            tool_choice.as_ref(),
            cancelled.as_ref(),
        )
    })
}

/// Executes a hosted rerank request and returns parsed document scores.
pub(crate) fn rerank_response(
    settings: &crate::backend::Settings,
    query: &str,
    documents: &[String],
    top_n: Option<usize>,
) -> Result<Vec<crate::backend::RerankResult>> {
    let settings = settings.clone();
    let query = query.to_owned();
    let documents = documents.to_vec();

    run_interruptible_request(move |cancelled| {
        execute_rerank_response(&settings, &query, &documents, top_n, cancelled.as_ref())
    })
}

/// Executes a hosted embeddings request and returns normalized vectors.
pub(crate) fn embed_response(
    settings: &crate::backend::Settings,
    inputs: &[String],
    normalize: bool,
) -> Result<Vec<Vec<f32>>> {
    let settings = settings.clone();
    let inputs = inputs.to_vec();

    run_interruptible_request(move |cancelled| {
        execute_embedding_response(&settings, &inputs, normalize, cancelled.as_ref())
    })
}

/// Executes a streaming chat-completions request and returns parsed chunk events.
pub(crate) fn chat_stream_response(
    settings: &crate::backend::Settings,
    messages: &[Value],
    options: crate::backend::RequestOptions,
) -> Result<Vec<Value>> {
    let settings = settings.clone();
    let messages = messages.to_vec();

    run_interruptible_request(move |cancelled| {
        execute_chat_stream_response(&settings, &messages, options, cancelled.as_ref())
    })
}

/// Probes the configured OpenAI-compatible runtime and reports discovery metadata.
#[must_use]
#[allow(
    clippy::too_many_lines,
    reason = "this probes multiple readiness failure modes and returns a single SQL-facing snapshot"
)]
pub(crate) fn discover_openai_runtime(settings: &crate::backend::Settings) -> Value {
    let endpoint = crate::http_policy::summarize(settings);
    let provider = endpoint.provider.clone();
    let base_url_host = endpoint.base_url_host.clone();
    let base_url_kind = endpoint.base_url_kind;
    let discovery_url = endpoint.discovery_url.clone();

    if let Err(error) = crate::http_policy::enforce_settings(settings) {
        return json!({
            "runtime": settings.runtime.as_str(),
            "provider": provider,
            "ready": false,
            "reason": error.to_string(),
            "base_url": settings.base_url.as_deref(),
            "discovery_url": Value::Null,
            "reachable": false,
            "authorized": Value::Null,
            "status_code": Value::Null,
            "model": settings.model.as_str(),
            "model_listed": Value::Null,
            "listed_models": Vec::<String>::new(),
            "base_url_host": base_url_host,
            "base_url_kind": base_url_kind,
        });
    }

    let Some(base_url) = settings.base_url.as_deref() else {
        return json!({
            "runtime": settings.runtime.as_str(),
            "provider": provider,
            "ready": false,
            "reason": "postllm.base_url is not set",
            "base_url": Value::Null,
            "discovery_url": Value::Null,
            "reachable": false,
            "authorized": Value::Null,
            "status_code": Value::Null,
            "model": settings.model,
            "model_listed": Value::Null,
            "listed_models": Vec::<String>::new(),
            "base_url_host": Value::Null,
            "base_url_kind": Value::Null,
        });
    };

    let Some(discovery_url) = discovery_url else {
        return json!({
            "runtime": settings.runtime.as_str(),
            "provider": provider,
            "ready": false,
            "reason": format!("could not derive a discovery endpoint from postllm.base_url='{base_url}'"),
            "base_url": base_url,
            "discovery_url": Value::Null,
            "reachable": false,
            "authorized": Value::Null,
            "status_code": Value::Null,
            "model": settings.model,
            "model_listed": Value::Null,
            "listed_models": Vec::<String>::new(),
            "base_url_host": base_url_host,
            "base_url_kind": base_url_kind,
        });
    };

    let timeout_ms = settings.timeout_ms.min(5_000);
    let provider = crate::http_policy::provider_identity(settings);
    let client = match Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return json!({
                "runtime": settings.runtime.as_str(),
                "provider": provider,
                "ready": false,
                "reason": error.to_string(),
                "base_url": base_url,
                "discovery_url": discovery_url,
                "reachable": false,
                "authorized": Value::Null,
                "status_code": Value::Null,
                "model": settings.model,
                "model_listed": Value::Null,
                "listed_models": Vec::<String>::new(),
                "base_url_host": base_url_host,
                "base_url_kind": base_url_kind,
            });
        }
    };

    let request = apply_provider_discovery_headers(client.get(&discovery_url), settings, &provider);

    let response = match request.send() {
        Ok(response) => response,
        Err(error) => {
            return json!({
                "runtime": settings.runtime.as_str(),
                "provider": provider,
                "ready": false,
                "reason": error.to_string(),
                "base_url": base_url,
                "discovery_url": discovery_url,
                "reachable": false,
                "authorized": Value::Null,
                "status_code": Value::Null,
                "model": settings.model,
                "model_listed": Value::Null,
                "listed_models": Vec::<String>::new(),
                "base_url_host": base_url_host,
                "base_url_kind": base_url_kind,
            });
        }
    };

    let status = response.status();
    let status_code = status.as_u16();
    let authorized = Some(!matches!(
        status,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
    ));
    let body = match response.text() {
        Ok(body) => body,
        Err(error) => {
            return json!({
                "runtime": settings.runtime.as_str(),
                "provider": provider,
                "ready": false,
                "reason": error.to_string(),
                "base_url": base_url,
                "discovery_url": discovery_url,
                "reachable": true,
                "authorized": authorized,
                "status_code": status_code,
                "model": settings.model,
                "model_listed": Value::Null,
                "listed_models": Vec::<String>::new(),
                "base_url_host": base_url_host,
                "base_url_kind": base_url_kind,
            });
        }
    };

    if !status.is_success() {
        return json!({
            "runtime": settings.runtime.as_str(),
            "provider": provider,
            "ready": false,
            "reason": format!("discovery endpoint returned HTTP {status_code}: {}", truncate_body(&body)),
            "base_url": base_url,
            "discovery_url": discovery_url,
            "reachable": true,
            "authorized": authorized,
            "status_code": status_code,
            "model": settings.model,
            "model_listed": Value::Null,
            "listed_models": Vec::<String>::new(),
            "base_url_host": base_url_host,
            "base_url_kind": base_url_kind,
        });
    }

    let response = match serde_json::from_str::<Value>(&body) {
        Ok(response) => response,
        Err(error) => {
            return json!({
                "runtime": settings.runtime.as_str(),
                "provider": provider,
                "ready": false,
                "reason": format!("discovery endpoint did not return valid JSON: {error}"),
                "base_url": base_url,
                "discovery_url": discovery_url,
                "reachable": true,
                "authorized": authorized,
                "status_code": status_code,
                "model": settings.model,
                "model_listed": Value::Null,
                "listed_models": Vec::<String>::new(),
                "base_url_host": base_url_host,
                "base_url_kind": base_url_kind,
            });
        }
    };

    let listed_models = extract_model_ids(&response);
    let model_listed = (!listed_models.is_empty()).then(|| {
        listed_models
            .iter()
            .any(|model| model.as_str() == settings.model.as_str())
    });
    let ready = model_listed.unwrap_or(true);
    let reason = if ready {
        None
    } else {
        Some(format!(
            "configured model '{}' was not listed by the discovery endpoint",
            settings.model
        ))
    };

    json!({
        "runtime": settings.runtime.as_str(),
        "provider": provider,
        "ready": ready,
        "reason": reason,
        "base_url": base_url,
        "discovery_url": discovery_url,
        "reachable": true,
        "authorized": authorized,
        "status_code": status_code,
        "model": settings.model,
        "model_listed": model_listed,
        "listed_models": listed_models,
        "base_url_host": base_url_host,
        "base_url_kind": base_url_kind,
    })
}

fn execute_chat_response(
    settings: &crate::backend::Settings,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
    cancelled: &AtomicBool,
) -> Result<Value> {
    retry_http_request(settings, cancelled, || {
        execute_chat_response_once(
            settings,
            messages,
            options,
            response_format,
            tools,
            tool_choice,
            cancelled,
        )
    })
}

fn extract_model_ids(response: &Value) -> Vec<String> {
    response
        .get("data")
        .and_then(Value::as_array)
        .map(|data| {
            data.iter()
                .filter_map(|entry| entry.get("id").and_then(Value::as_str))
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn execute_chat_response_once(
    settings: &crate::backend::Settings,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
    cancelled: &AtomicBool,
) -> HttpRequestResult<Value> {
    crate::http_policy::enforce_settings(settings).map_err(HttpRequestError::Postllm)?;

    let Some(base_url) = settings.base_url.as_deref() else {
        return Err(HttpRequestError::Postllm(Error::invalid_setting(
            "postllm.base_url",
            format!(
                "is required when runtime is '{}' and model is '{}'",
                settings.runtime.as_str(),
                settings.model,
            ),
            "SET postllm.base_url = 'http://127.0.0.1:11434/v1/chat/completions' or pass base_url => '...' to postllm.configure(...)",
        )));
    };

    let client = Client::builder()
        .timeout(Duration::from_millis(settings.timeout_ms))
        .build()
        .map_err(HttpRequestError::Transport)?;
    let endpoint = hosted_endpoint_flavor(settings);
    let payload = build_request_payload_for_endpoint(
        endpoint,
        &settings.model,
        messages,
        options,
        response_format,
        tools,
        tool_choice,
    )
    .map_err(HttpRequestError::Postllm)?;
    let request = client
        .post(base_url)
        .header(CONTENT_TYPE, "application/json")
        .json(&payload);
    let request = apply_provider_request_headers(request, settings, endpoint);

    let response = request.send().map_err(HttpRequestError::Transport)?;
    let status = response.status();
    let body = read_response_body(response, cancelled)?;

    if !status.is_success() {
        return Err(HttpRequestError::Upstream {
            status,
            body: truncate_body(&String::from_utf8_lossy(&body)),
        });
    }

    let response = serde_json::from_slice(&body).map_err(HttpRequestError::JsonDecode)?;

    normalize_response_for_endpoint(endpoint, &response).map_err(HttpRequestError::Postllm)
}

fn execute_chat_stream_response(
    settings: &crate::backend::Settings,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>> {
    retry_http_request(settings, cancelled, || {
        execute_chat_stream_response_once(settings, messages, options, cancelled)
    })
}

fn execute_chat_stream_response_once(
    settings: &crate::backend::Settings,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    cancelled: &AtomicBool,
) -> HttpRequestResult<Vec<Value>> {
    crate::http_policy::enforce_settings(settings).map_err(HttpRequestError::Postllm)?;

    let Some(base_url) = settings.base_url.as_deref() else {
        return Err(HttpRequestError::Postllm(Error::invalid_setting(
            "postllm.base_url",
            format!(
                "is required when runtime is '{}' and model is '{}'",
                settings.runtime.as_str(),
                settings.model,
            ),
            "SET postllm.base_url = 'http://127.0.0.1:11434/v1/chat/completions' or pass base_url => '...' to postllm.configure(...)",
        )));
    };

    let client = Client::builder()
        .timeout(Duration::from_millis(settings.timeout_ms))
        .build()
        .map_err(HttpRequestError::Transport)?;
    let endpoint = hosted_endpoint_flavor(settings);
    let payload =
        build_stream_request_payload_for_endpoint(endpoint, &settings.model, messages, options)
            .map_err(HttpRequestError::Postllm)?;
    let request = client
        .post(base_url)
        .header(CONTENT_TYPE, "application/json")
        .json(&payload);
    let request = apply_provider_request_headers(request, settings, endpoint);

    let response = request.send().map_err(HttpRequestError::Transport)?;
    let status = response.status();

    if !status.is_success() {
        let body = read_response_body(response, cancelled)?;
        return Err(HttpRequestError::Upstream {
            status,
            body: truncate_body(&String::from_utf8_lossy(&body)),
        });
    }

    let events = parse_sse_json_events_interruptible(BufReader::new(response), cancelled)?;

    normalize_stream_events_for_endpoint(endpoint, &events).map_err(HttpRequestError::Postllm)
}

fn execute_rerank_response(
    settings: &crate::backend::Settings,
    query: &str,
    documents: &[String],
    top_n: Option<usize>,
    cancelled: &AtomicBool,
) -> Result<Vec<crate::backend::RerankResult>> {
    retry_http_request(settings, cancelled, || {
        execute_rerank_response_once(settings, query, documents, top_n, cancelled)
    })
}

fn execute_embedding_response(
    settings: &crate::backend::Settings,
    inputs: &[String],
    normalize: bool,
    cancelled: &AtomicBool,
) -> Result<Vec<Vec<f32>>> {
    retry_http_request(settings, cancelled, || {
        execute_embedding_response_once(settings, inputs, normalize, cancelled)
    })
}

fn execute_embedding_response_once(
    settings: &crate::backend::Settings,
    inputs: &[String],
    normalize: bool,
    cancelled: &AtomicBool,
) -> HttpRequestResult<Vec<Vec<f32>>> {
    crate::http_policy::enforce_settings(settings).map_err(HttpRequestError::Postllm)?;
    let endpoint = hosted_endpoint_flavor(settings);

    if endpoint == HostedEndpointFlavor::AnthropicMessages {
        return Err(HttpRequestError::Postllm(Error::Unsupported(
            "embeddings are not implemented by the Anthropic adapter".to_owned(),
        )));
    }

    let Some(base_url) = settings.base_url.as_deref() else {
        return Err(HttpRequestError::Postllm(Error::invalid_setting(
            "postllm.base_url",
            format!(
                "is required when runtime is '{}' and embedding_model is '{}'",
                settings.runtime.as_str(),
                settings.model,
            ),
            "SET postllm.base_url = 'https://api.openai.com/v1/embeddings' or pass base_url => '...' to postllm.configure(...)",
        )));
    };

    let embeddings_url = derive_embeddings_url(base_url).map_err(HttpRequestError::Postllm)?;
    let client = Client::builder()
        .timeout(Duration::from_millis(settings.timeout_ms))
        .build()
        .map_err(HttpRequestError::Transport)?;
    let payload = build_embedding_request_payload(&settings.model, inputs);
    let request = client
        .post(&embeddings_url)
        .header(CONTENT_TYPE, "application/json")
        .json(&payload);
    let request = apply_provider_request_headers(request, settings, endpoint);

    let response = request.send().map_err(HttpRequestError::Transport)?;
    let status = response.status();
    let body = read_response_body(response, cancelled)?;

    if !status.is_success() {
        return Err(HttpRequestError::Upstream {
            status,
            body: truncate_body(&String::from_utf8_lossy(&body)),
        });
    }

    let response = serde_json::from_slice::<Value>(&body).map_err(HttpRequestError::JsonDecode)?;

    parse_embedding_response(&response, normalize).map_err(HttpRequestError::Postllm)
}

fn execute_rerank_response_once(
    settings: &crate::backend::Settings,
    query: &str,
    documents: &[String],
    top_n: Option<usize>,
    cancelled: &AtomicBool,
) -> HttpRequestResult<Vec<crate::backend::RerankResult>> {
    crate::http_policy::enforce_settings(settings).map_err(HttpRequestError::Postllm)?;
    let endpoint = hosted_endpoint_flavor(settings);

    if endpoint == HostedEndpointFlavor::AnthropicMessages {
        return Err(HttpRequestError::Postllm(Error::Unsupported(
            "reranking is not implemented by the Anthropic adapter".to_owned(),
        )));
    }

    let Some(base_url) = settings.base_url.as_deref() else {
        return Err(HttpRequestError::Postllm(Error::invalid_setting(
            "postllm.base_url",
            format!(
                "is required when runtime is '{}' and model is '{}'",
                settings.runtime.as_str(),
                settings.model,
            ),
            "SET postllm.base_url = 'https://example.com/v1/rerank' or point it at a hosted rerank endpoint",
        )));
    };

    let client = Client::builder()
        .timeout(Duration::from_millis(settings.timeout_ms))
        .build()
        .map_err(HttpRequestError::Transport)?;
    let payload = build_rerank_request_payload(&settings.model, query, documents, top_n);
    let request = client
        .post(base_url)
        .header(CONTENT_TYPE, "application/json")
        .json(&payload);
    let request = apply_provider_request_headers(request, settings, endpoint);

    let response = request.send().map_err(HttpRequestError::Transport)?;
    let status = response.status();
    let body = read_response_body(response, cancelled)?;

    if !status.is_success() {
        return Err(HttpRequestError::Upstream {
            status,
            body: truncate_body(&String::from_utf8_lossy(&body)),
        });
    }

    let response = serde_json::from_slice::<Value>(&body).map_err(HttpRequestError::JsonDecode)?;
    let mut ranked =
        parse_rerank_response(&response, documents.len()).map_err(HttpRequestError::Postllm)?;

    if let Some(top_n) = top_n {
        ranked.truncate(top_n);
    }

    Ok(ranked)
}

fn run_interruptible_request<T>(
    operation: impl FnOnce(Arc<AtomicBool>) -> Result<T> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = Arc::clone(&cancelled);
    let (sender, receiver) = mpsc::sync_channel(1);
    let handle = thread::spawn(move || {
        let result = operation(worker_cancelled);
        let _ = sender.send(result);
    });

    loop {
        crate::interrupt::checkpoint_with_cancellation(cancelled.as_ref());

        match receiver.recv_timeout(INTERRUPT_POLL_INTERVAL) {
            Ok(result) => {
                handle.join().map_err(|_| {
                    Error::Internal(
                        "the HTTP worker thread panicked after returning a result".to_owned(),
                    )
                })?;

                return result;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return match handle.join() {
                    Ok(()) => Err(Error::Internal(
                        "the HTTP worker thread exited before sending a result".to_owned(),
                    )),
                    Err(_) => Err(Error::Internal(
                        "the HTTP worker thread panicked".to_owned(),
                    )),
                };
            }
        }
    }
}

fn retry_http_request<T>(
    settings: &crate::backend::Settings,
    cancelled: &AtomicBool,
    mut operation: impl FnMut() -> HttpRequestResult<T>,
) -> Result<T> {
    let mut attempt = 0_u32;

    loop {
        crate::interrupt::ensure_not_cancelled(cancelled)?;

        match operation() {
            Ok(value) => return Ok(value),
            Err(error) => {
                if attempt >= settings.max_retries || !error.is_transient() {
                    return Err(error.into_error());
                }

                sleep_retry_backoff(settings.retry_backoff_ms, attempt, cancelled)?;
                attempt += 1;
            }
        }
    }
}

/// Extracts the first textual completion from a provider response.
pub(crate) fn extract_text(response: &Value) -> Result<String> {
    if let Some(text) = extract_chat_completion_text(response) {
        return Ok(text);
    }

    if let Some(text) = extract_responses_output_text(response) {
        return Ok(text);
    }

    Err(Error::MalformedResponse)
}

/// Builds the JSON payload sent to an OpenAI-compatible chat endpoint.
pub(crate) fn build_request_payload(
    model: &str,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
) -> Value {
    let mut payload = json!({
        "model": model,
        "messages": messages,
        "temperature": options.temperature,
    });

    if let Some(max_tokens) = options.max_tokens
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("max_tokens".to_owned(), json!(max_tokens));
    }

    if let Some(response_format) = response_format
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("response_format".to_owned(), response_format.clone());
    }

    if let Some(tools) = tools
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("tools".to_owned(), json!(tools));
    }

    if let Some(tool_choice) = tool_choice
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("tool_choice".to_owned(), tool_choice.clone());
    }

    payload
}

fn build_embedding_request_payload(model: &str, inputs: &[String]) -> Value {
    json!({
        "model": model,
        "input": inputs,
    })
}

fn build_request_payload_for_endpoint(
    endpoint: HostedEndpointFlavor,
    model: &str,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
) -> Result<Value> {
    match endpoint {
        HostedEndpointFlavor::ChatCompletions => Ok(build_request_payload(
            model,
            messages,
            options,
            response_format,
            tools,
            tool_choice,
        )),
        HostedEndpointFlavor::Responses => build_responses_request_payload(
            model,
            messages,
            options,
            response_format,
            tools,
            tool_choice,
        ),
        HostedEndpointFlavor::AnthropicMessages => build_anthropic_request_payload(
            model,
            messages,
            options,
            response_format,
            tools,
            tool_choice,
        ),
    }
}

/// Builds the JSON payload sent to a hosted rerank endpoint.
pub(crate) fn build_rerank_request_payload(
    model: &str,
    query: &str,
    documents: &[String],
    top_n: Option<usize>,
) -> Value {
    let mut payload = json!({
        "model": model,
        "query": query,
        "documents": documents,
    });

    if let Some(top_n) = top_n
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("top_n".to_owned(), json!(top_n));
    }

    payload
}

fn build_stream_request_payload(
    model: &str,
    messages: &[Value],
    options: crate::backend::RequestOptions,
) -> Value {
    let mut payload = build_request_payload(model, messages, options, None, None, None);

    if let Some(object) = payload.as_object_mut() {
        object.insert("stream".to_owned(), json!(true));
    }

    payload
}

fn messages_to_anthropic_payload(messages: &[Value]) -> Result<(Option<String>, Vec<Value>)> {
    let mut system_messages = Vec::new();
    let mut anthropic_messages = Vec::new();

    for message in messages {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .ok_or(Error::MalformedResponse)?;

        match role {
            "system" => {
                system_messages.push(stringify_anthropic_message_content(message.get("content"))?);
            }
            "user" | "assistant" => anthropic_messages.push(json!({
                "role": role,
                "content": anthropic_message_content(message.get("content"))?,
            })),
            "tool" => {
                return Err(Error::Unsupported(
                    "tool result messages are not implemented by the Anthropic adapter".to_owned(),
                ));
            }
            _ => return Err(Error::MalformedResponse),
        }

        if message.get("tool_calls").is_some() {
            return Err(Error::Unsupported(
                "assistant tool calls are not implemented by the Anthropic adapter".to_owned(),
            ));
        }
    }

    let system = (!system_messages.is_empty()).then(|| system_messages.join("\n\n"));

    Ok((system, anthropic_messages))
}

fn anthropic_message_content(content: Option<&Value>) -> Result<Value> {
    match content.unwrap_or(&Value::Null) {
        Value::String(text) => Ok(json!([{
            "type": "text",
            "text": text,
        }])),
        Value::Array(parts) => parts
            .iter()
            .map(anthropic_message_part)
            .collect::<Result<Vec<Value>>>()
            .map(Value::Array),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => {
            Err(Error::MalformedResponse)
        }
    }
}

fn anthropic_message_part(part: &Value) -> Result<Value> {
    let part_type = part
        .get("type")
        .and_then(Value::as_str)
        .ok_or(Error::MalformedResponse)?;

    match part_type {
        "text" => Ok(json!({
            "type": "text",
            "text": part
                .get("text")
                .and_then(Value::as_str)
                .ok_or(Error::MalformedResponse)?,
        })),
        "image_url" => Err(Error::Unsupported(
            "multimodal inputs are not implemented by the Anthropic adapter".to_owned(),
        )),
        _ => Err(Error::MalformedResponse),
    }
}

fn stringify_anthropic_message_content(content: Option<&Value>) -> Result<String> {
    match content.unwrap_or(&Value::Null) {
        Value::String(text) => Ok(text.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .map(|part| {
                    let part_type = part
                        .get("type")
                        .and_then(Value::as_str)
                        .ok_or(Error::MalformedResponse)?;

                    match part_type {
                        "text" => part
                            .get("text")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .ok_or(Error::MalformedResponse),
                        "image_url" => Err(Error::Unsupported(
                            "multimodal inputs are not implemented by the Anthropic adapter"
                                .to_owned(),
                        )),
                        _ => Err(Error::MalformedResponse),
                    }
                })
                .collect::<Result<Vec<String>>>()?
                .join("");

            if text.is_empty() {
                Err(Error::MalformedResponse)
            } else {
                Ok(text)
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => {
            Err(Error::MalformedResponse)
        }
    }
}

fn derive_embeddings_url(base_url: &str) -> Result<String> {
    let mut url = Url::parse(base_url).map_err(|_| {
        Error::invalid_setting(
            "postllm.base_url",
            format!("must be a valid absolute URL, got '{base_url}'"),
            "SET postllm.base_url = 'https://api.openai.com/v1/embeddings' or another valid absolute URL",
        )
    })?;

    let Some(mut segments) = url
        .path_segments()
        .map(|segments| segments.map(str::to_owned).collect::<Vec<String>>())
    else {
        return Err(Error::invalid_setting(
            "postllm.base_url",
            format!("must include a path segment, got '{base_url}'"),
            "SET postllm.base_url = 'https://api.openai.com/v1/embeddings' or another endpoint under /v1/",
        ));
    };

    if matches!(segments.last().map(String::as_str), Some("completions")) {
        segments.pop();
    }
    if matches!(segments.last().map(String::as_str), Some("chat")) {
        segments.pop();
    }
    if matches!(
        segments.last().map(String::as_str),
        Some("responses" | "rerank" | "embeddings")
    ) {
        segments.pop();
    }
    if segments.is_empty() {
        segments.push("v1".to_owned());
    }
    segments.push("embeddings".to_owned());
    url.set_path(&format!("/{}", segments.join("/")));

    Ok(url.to_string())
}

fn build_stream_request_payload_for_endpoint(
    endpoint: HostedEndpointFlavor,
    model: &str,
    messages: &[Value],
    options: crate::backend::RequestOptions,
) -> Result<Value> {
    match endpoint {
        HostedEndpointFlavor::ChatCompletions => {
            Ok(build_stream_request_payload(model, messages, options))
        }
        HostedEndpointFlavor::Responses => {
            let mut payload =
                build_responses_request_payload(model, messages, options, None, None, None)?;

            if let Some(object) = payload.as_object_mut() {
                object.insert("stream".to_owned(), json!(true));
            }

            Ok(payload)
        }
        HostedEndpointFlavor::AnthropicMessages => {
            let mut payload =
                build_anthropic_request_payload(model, messages, options, None, None, None)?;

            if let Some(object) = payload.as_object_mut() {
                object.insert("stream".to_owned(), json!(true));
            }

            Ok(payload)
        }
    }
}

fn build_anthropic_request_payload(
    model: &str,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
) -> Result<Value> {
    if response_format.is_some() {
        return Err(Error::Unsupported(
            "structured-output requests are not implemented by the Anthropic adapter".to_owned(),
        ));
    }
    if tools.is_some() || tool_choice.is_some() {
        return Err(Error::Unsupported(
            "tool-calling requests are not implemented by the Anthropic adapter".to_owned(),
        ));
    }

    let (system, anthropic_messages) = messages_to_anthropic_payload(messages)?;
    let mut payload = json!({
        "model": model,
        "messages": anthropic_messages,
        "max_tokens": options.max_tokens.unwrap_or(ANTHROPIC_DEFAULT_MAX_TOKENS),
        "temperature": options.temperature,
    });

    if let Some(system) = system
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("system".to_owned(), json!(system));
    }

    Ok(payload)
}

fn messages_to_responses_input(messages: &[Value]) -> Result<Vec<Value>> {
    let mut input = Vec::new();

    for message in messages {
        input.extend(message_to_responses_items(message)?);
    }

    Ok(input)
}

fn message_to_responses_items(message: &Value) -> Result<Vec<Value>> {
    let role = message
        .get("role")
        .and_then(Value::as_str)
        .ok_or(Error::MalformedResponse)?;

    match role {
        "system" | "user" => Ok(vec![json!({
            "role": role,
            "content": responses_message_content(message.get("content"))?,
        })]),
        "assistant" => assistant_message_to_responses_items(message),
        "tool" => Ok(vec![json!({
            "type": "function_call_output",
            "call_id": message
                .get("tool_call_id")
                .and_then(Value::as_str)
                .ok_or(Error::MalformedResponse)?,
            "output": stringify_message_content(message.get("content"))?,
        })]),
        _ => Err(Error::MalformedResponse),
    }
}

fn assistant_message_to_responses_items(message: &Value) -> Result<Vec<Value>> {
    let mut items = Vec::new();

    if let Some(content) = message.get("content")
        && !content.is_null()
    {
        items.push(json!({
            "role": "assistant",
            "content": responses_message_content(Some(content))?,
        }));
    }

    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for tool_call in tool_calls {
            let function = tool_call.get("function").ok_or(Error::MalformedResponse)?;
            items.push(json!({
                "type": "function_call",
                "call_id": tool_call
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or(Error::MalformedResponse)?,
                "name": function
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or(Error::MalformedResponse)?,
                "arguments": function
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or(Error::MalformedResponse)?,
            }));
        }
    }

    Ok(items)
}

fn responses_message_content(content: Option<&Value>) -> Result<Value> {
    match content.unwrap_or(&Value::Null) {
        Value::String(text) => Ok(json!([{
            "type": "input_text",
            "text": text,
        }])),
        Value::Array(parts) => parts
            .iter()
            .map(response_input_part)
            .collect::<Result<Vec<Value>>>()
            .map(Value::Array),
        Value::Null => Ok(Value::Array(Vec::new())),
        Value::Bool(_) | Value::Number(_) | Value::Object(_) => Err(Error::MalformedResponse),
    }
}

fn response_input_part(part: &Value) -> Result<Value> {
    let part_type = part
        .get("type")
        .and_then(Value::as_str)
        .ok_or(Error::MalformedResponse)?;

    match part_type {
        "text" => Ok(json!({
            "type": "input_text",
            "text": part
                .get("text")
                .and_then(Value::as_str)
                .ok_or(Error::MalformedResponse)?,
        })),
        "image_url" => {
            let image_url = part.get("image_url").ok_or(Error::MalformedResponse)?;
            let mut normalized = json!({
                "type": "input_image",
                "image_url": image_url
                    .get("url")
                    .and_then(Value::as_str)
                    .ok_or(Error::MalformedResponse)?,
            });

            if let Some(detail) = image_url.get("detail").and_then(Value::as_str)
                && let Some(object) = normalized.as_object_mut()
            {
                object.insert("detail".to_owned(), json!(detail));
            }

            Ok(normalized)
        }
        _ => Err(Error::MalformedResponse),
    }
}

fn stringify_message_content(content: Option<&Value>) -> Result<String> {
    match content.unwrap_or(&Value::Null) {
        Value::String(text) => Ok(text.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<String>();

            if text.is_empty() {
                Err(Error::MalformedResponse)
            } else {
                Ok(text)
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => {
            Err(Error::MalformedResponse)
        }
    }
}

fn hosted_endpoint_flavor(settings: &crate::backend::Settings) -> HostedEndpointFlavor {
    if crate::http_policy::provider_identity(settings) == crate::http_policy::PROVIDER_ANTHROPIC {
        return HostedEndpointFlavor::AnthropicMessages;
    }

    let Some(base_url) = settings.base_url.as_deref() else {
        return HostedEndpointFlavor::ChatCompletions;
    };
    let Ok(url) = Url::parse(base_url) else {
        return HostedEndpointFlavor::ChatCompletions;
    };
    let Some(last_segment) = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
    else {
        return HostedEndpointFlavor::ChatCompletions;
    };

    if last_segment == "responses" {
        HostedEndpointFlavor::Responses
    } else {
        HostedEndpointFlavor::ChatCompletions
    }
}

fn normalize_response_for_endpoint(
    endpoint: HostedEndpointFlavor,
    response: &Value,
) -> Result<Value> {
    match endpoint {
        HostedEndpointFlavor::ChatCompletions => Ok(response.clone()),
        HostedEndpointFlavor::Responses => normalize_responses_api_response(response),
        HostedEndpointFlavor::AnthropicMessages => normalize_anthropic_response(response),
    }
}

fn normalize_stream_events_for_endpoint(
    endpoint: HostedEndpointFlavor,
    events: &[Value],
) -> Result<Vec<Value>> {
    match endpoint {
        HostedEndpointFlavor::ChatCompletions => Ok(events.to_vec()),
        HostedEndpointFlavor::Responses => normalize_responses_stream_events(events),
        HostedEndpointFlavor::AnthropicMessages => normalize_anthropic_stream_events(events),
    }
}

fn normalize_anthropic_response(response: &Value) -> Result<Value> {
    let tool_calls = extract_anthropic_tool_calls(response)?;
    let content = extract_anthropic_output_text(response).map_or(Value::Null, Value::String);
    let finish_reason = anthropic_finish_reason(response, !tool_calls.is_empty());
    let mut message = json!({
        "role": "assistant",
        "content": content,
    });

    if !tool_calls.is_empty()
        && let Some(object) = message.as_object_mut()
    {
        object.insert("tool_calls".to_owned(), Value::Array(tool_calls));
    }

    Ok(json!({
        "id": response.get("id").cloned().unwrap_or(Value::Null),
        "object": "chat.completion",
        "model": response.get("model").cloned().unwrap_or(Value::Null),
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason,
        }],
        "usage": anthropic_usage(response),
    }))
}

fn normalize_anthropic_stream_events(events: &[Value]) -> Result<Vec<Value>> {
    let mut normalized = Vec::new();
    let mut response_id = Value::Null;
    let mut model = Value::Null;

    for event in events {
        let event_type =
            event
                .get("type")
                .and_then(Value::as_str)
                .ok_or(Error::MalformedStream(
                    "anthropic stream event did not include a type".to_owned(),
                ))?;

        match event_type {
            "message_start" => {
                let Some(message) = event.get("message") else {
                    return Err(Error::MalformedStream(
                        "anthropic message_start event did not include a message".to_owned(),
                    ));
                };
                response_id = message.get("id").cloned().unwrap_or(Value::Null);
                model = message.get("model").cloned().unwrap_or(Value::Null);

                normalized.push(json!({
                    "id": response_id.clone(),
                    "object": "chat.completion.chunk",
                    "model": model.clone(),
                    "choices": [{
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                        },
                        "finish_reason": Value::Null,
                    }],
                }));
            }
            "content_block_delta" => {
                let Some(delta) = event.get("delta") else {
                    return Err(Error::MalformedStream(
                        "anthropic content_block_delta event did not include a delta".to_owned(),
                    ));
                };

                if delta.get("type").and_then(Value::as_str) == Some("text_delta") {
                    normalized.push(json!({
                        "id": response_id.clone(),
                        "object": "chat.completion.chunk",
                        "model": model.clone(),
                        "choices": [{
                            "index": 0,
                            "delta": {
                                "content": delta.get("text").cloned().unwrap_or(Value::Null),
                            },
                            "finish_reason": Value::Null,
                        }],
                    }));
                }
            }
            "message_delta" => normalized.push(json!({
                "id": response_id.clone(),
                "object": "chat.completion.chunk",
                "model": model.clone(),
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": anthropic_stream_finish_reason(event),
                }],
                "usage": anthropic_stream_usage(event),
            })),
            _ => {}
        }
    }

    if normalized.is_empty() {
        return Err(Error::MalformedStream(
            "anthropic stream did not contain any text or completion events".to_owned(),
        ));
    }

    Ok(normalized)
}

fn normalize_responses_api_response(response: &Value) -> Result<Value> {
    let tool_calls = extract_responses_tool_calls(response)?;
    let content = extract_responses_output_text(response).map_or(Value::Null, Value::String);
    let finish_reason = responses_finish_reason(response, !tool_calls.is_empty());

    let mut message = json!({
        "role": "assistant",
        "content": content,
    });

    if !tool_calls.is_empty()
        && let Some(object) = message.as_object_mut()
    {
        object.insert("tool_calls".to_owned(), Value::Array(tool_calls));
    }

    Ok(json!({
        "id": response.get("id").cloned().unwrap_or(Value::Null),
        "object": "chat.completion",
        "model": response.get("model").cloned().unwrap_or(Value::Null),
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason,
        }],
        "usage": responses_usage(response),
    }))
}

fn normalize_responses_stream_events(events: &[Value]) -> Result<Vec<Value>> {
    let mut normalized = Vec::new();

    for event in events {
        let event_type =
            event
                .get("type")
                .and_then(Value::as_str)
                .ok_or(Error::MalformedStream(
                    "responses stream event did not include a type".to_owned(),
                ))?;

        match event_type {
            "response.output_text.delta" => normalized.push(json!({
                "id": event.get("response_id").cloned().unwrap_or(Value::Null),
                "object": "chat.completion.chunk",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "content": event.get("delta").cloned().unwrap_or(Value::Null),
                    },
                    "finish_reason": Value::Null,
                }],
            })),
            "response.output_item.added" => {
                if let Some(item) = event.get("item")
                    && item.get("type").and_then(Value::as_str) == Some("function_call")
                {
                    normalized.push(json!({
                        "id": event.get("response_id").cloned().unwrap_or(Value::Null),
                        "object": "chat.completion.chunk",
                        "choices": [{
                            "index": 0,
                            "delta": {
                                "tool_calls": [{
                                    "index": 0,
                                    "id": item.get("call_id").cloned().unwrap_or(Value::Null),
                                    "type": "function",
                                    "function": {
                                        "name": item.get("name").cloned().unwrap_or(Value::Null),
                                    },
                                }],
                            },
                            "finish_reason": Value::Null,
                        }],
                    }));
                }
            }
            "response.function_call_arguments.delta" => normalized.push(json!({
                "id": event.get("response_id").cloned().unwrap_or(Value::Null),
                "object": "chat.completion.chunk",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "tool_calls": [{
                            "index": 0,
                            "id": event.get("call_id").cloned().unwrap_or(Value::Null),
                            "type": "function",
                            "function": {
                                "arguments": event.get("delta").cloned().unwrap_or(Value::Null),
                            },
                        }],
                    },
                    "finish_reason": Value::Null,
                }],
            })),
            "response.completed" | "response.done" => {
                let response = event.get("response").unwrap_or(event);
                normalized.push(json!({
                    "id": response.get("id").cloned().unwrap_or(Value::Null),
                    "object": "chat.completion.chunk",
                    "model": response.get("model").cloned().unwrap_or(Value::Null),
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": responses_finish_reason(
                            response,
                            !extract_responses_tool_calls(response)?.is_empty(),
                        ),
                    }],
                }));
            }
            _ => {}
        }
    }

    if normalized.is_empty() {
        return Err(Error::MalformedStream(
            "responses stream did not contain any text, tool, or completion events".to_owned(),
        ));
    }

    Ok(normalized)
}

fn extract_chat_completion_text(response: &Value) -> Option<String> {
    let choices = response.get("choices").and_then(Value::as_array)?;
    let first_choice = choices.first()?;

    if let Some(text) = first_choice.get("text").and_then(Value::as_str) {
        return Some(text.to_owned());
    }

    let content = first_choice
        .get("message")
        .and_then(|message| message.get("content"))?;

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

fn extract_responses_output_text(response: &Value) -> Option<String> {
    if let Some(text) = response.get("output_text").and_then(Value::as_str)
        && !text.is_empty()
    {
        return Some(text.to_owned());
    }

    let mut collected = String::new();
    let output = response.get("output").and_then(Value::as_array)?;

    for item in output {
        if item.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }

        let Some(content) = item.get("content").and_then(Value::as_array) else {
            continue;
        };

        for part in content {
            let is_text_part = matches!(
                part.get("type").and_then(Value::as_str),
                Some("output_text" | "text")
            );

            if is_text_part && let Some(text) = part.get("text").and_then(Value::as_str) {
                collected.push_str(text);
            }
        }
    }

    (!collected.is_empty()).then_some(collected)
}

fn extract_responses_tool_calls(response: &Value) -> Result<Vec<Value>> {
    let Some(output) = response.get("output").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };

    output
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("function_call"))
        .map(|item| {
            Ok(json!({
                "id": item
                    .get("call_id")
                    .cloned()
                    .or_else(|| item.get("id").cloned())
                    .unwrap_or(Value::Null),
                "type": "function",
                "function": {
                    "name": item
                        .get("name")
                        .cloned()
                        .ok_or(Error::MalformedResponse)?,
                    "arguments": item
                        .get("arguments")
                        .cloned()
                        .unwrap_or_else(|| Value::String("{}".to_owned())),
                }
            }))
        })
        .collect()
}

fn extract_anthropic_output_text(response: &Value) -> Option<String> {
    let content = response.get("content").and_then(Value::as_array)?;
    let text = content
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<String>();

    (!text.is_empty()).then_some(text)
}

fn extract_anthropic_tool_calls(response: &Value) -> Result<Vec<Value>> {
    let Some(content) = response.get("content").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };

    content
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("tool_use"))
        .map(|part| {
            let arguments =
                serde_json::to_string(part.get("input").ok_or(Error::MalformedResponse)?)?;

            Ok(json!({
                "id": part
                    .get("id")
                    .cloned()
                    .unwrap_or(Value::Null),
                "type": "function",
                "function": {
                    "name": part
                        .get("name")
                        .cloned()
                        .ok_or(Error::MalformedResponse)?,
                    "arguments": arguments,
                }
            }))
        })
        .collect()
}

fn responses_usage(response: &Value) -> Value {
    let Some(usage) = response.get("usage").and_then(Value::as_object) else {
        return Value::Null;
    };

    json!({
        "prompt_tokens": usage.get("input_tokens").cloned().unwrap_or(Value::Null),
        "completion_tokens": usage.get("output_tokens").cloned().unwrap_or(Value::Null),
        "total_tokens": usage.get("total_tokens").cloned().unwrap_or(Value::Null),
    })
}

fn anthropic_usage(response: &Value) -> Value {
    let Some(usage) = response.get("usage").and_then(Value::as_object) else {
        return Value::Null;
    };
    let prompt_tokens = usage.get("input_tokens").cloned().unwrap_or(Value::Null);
    let completion_tokens = usage.get("output_tokens").cloned().unwrap_or(Value::Null);
    let total_tokens = prompt_tokens
        .as_i64()
        .zip(completion_tokens.as_i64())
        .map_or(Value::Null, |(prompt, completion)| {
            Value::from(prompt + completion)
        });

    json!({
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
    })
}

fn responses_finish_reason(response: &Value, has_tool_calls: bool) -> Value {
    if has_tool_calls {
        return Value::String("tool_calls".to_owned());
    }

    match response.get("status").and_then(Value::as_str) {
        Some("completed") => Value::String("stop".to_owned()),
        Some("incomplete") => response
            .get("incomplete_details")
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str)
            .map_or_else(
                || Value::String("length".to_owned()),
                |reason| match reason {
                    "max_output_tokens" => Value::String("length".to_owned()),
                    _ => Value::String(reason.to_owned()),
                },
            ),
        Some(status) => Value::String(status.to_owned()),
        None => Value::Null,
    }
}

fn anthropic_finish_reason(response: &Value, has_tool_calls: bool) -> Value {
    if has_tool_calls {
        return Value::String("tool_calls".to_owned());
    }

    match response.get("stop_reason").and_then(Value::as_str) {
        Some("end_turn" | "stop_sequence") => Value::String("stop".to_owned()),
        Some("max_tokens" | "model_context_window_exceeded") => Value::String("length".to_owned()),
        Some(reason) => Value::String(reason.to_owned()),
        None => Value::Null,
    }
}

fn anthropic_stream_finish_reason(event: &Value) -> Value {
    anthropic_finish_reason(event.get("delta").unwrap_or(&Value::Null), false)
}

fn anthropic_stream_usage(event: &Value) -> Value {
    anthropic_usage(event)
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "provider embedding payloads decode as f64, but postllm exposes embeddings as f32 vectors"
)]
fn parse_embedding_response(response: &Value, normalize: bool) -> Result<Vec<Vec<f32>>> {
    let Some(data) = response.get("data").and_then(Value::as_array) else {
        return Err(Error::MalformedResponse);
    };
    if data.is_empty() {
        return Err(Error::MalformedResponse);
    }

    let mut indexed = data
        .iter()
        .enumerate()
        .map(|(position, item)| {
            let index = item
                .get("index")
                .and_then(Value::as_u64)
                .and_then(|index| usize::try_from(index).ok())
                .unwrap_or(position);
            let embedding = item
                .get("embedding")
                .and_then(Value::as_array)
                .ok_or(Error::MalformedResponse)?
                .iter()
                .map(|value| {
                    value
                        .as_f64()
                        .map(|number| number as f32)
                        .ok_or(Error::MalformedResponse)
                })
                .collect::<Result<Vec<f32>>>()?;

            Ok::<(usize, Vec<f32>), Error>((index, maybe_normalize_embedding(embedding, normalize)))
        })
        .collect::<Result<Vec<_>>>()?;

    indexed.sort_unstable_by_key(|(index, _)| *index);

    Ok(indexed
        .into_iter()
        .map(|(_, embedding)| embedding)
        .collect::<Vec<_>>())
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "vector normalization keeps the f32 embedding shape expected by SQL callers"
)]
fn maybe_normalize_embedding(mut embedding: Vec<f32>, normalize: bool) -> Vec<f32> {
    if !normalize {
        return embedding;
    }

    let magnitude = embedding
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();

    if magnitude == 0.0 {
        return embedding;
    }

    let scale = magnitude as f32;
    for value in &mut embedding {
        *value /= scale;
    }

    embedding
}

fn build_responses_request_payload(
    model: &str,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
) -> Result<Value> {
    let mut payload = json!({
        "model": model,
        "input": messages_to_responses_input(messages)?,
        "temperature": options.temperature,
    });

    if let Some(max_tokens) = options.max_tokens
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("max_output_tokens".to_owned(), json!(max_tokens));
    }

    if let Some(response_format) = response_format
        && let Some(object) = payload.as_object_mut()
    {
        object.insert(
            "text".to_owned(),
            json!({
                "format": response_format,
            }),
        );
    }

    if let Some(tools) = tools
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("tools".to_owned(), json!(tools));
    }

    if let Some(tool_choice) = tool_choice
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("tool_choice".to_owned(), tool_choice.clone());
    }

    Ok(payload)
}

#[cfg(test)]
fn parse_sse_json_events(reader: impl BufRead) -> HttpRequestResult<Vec<Value>> {
    let cancelled = AtomicBool::new(false);

    parse_sse_json_events_interruptible(reader, &cancelled)
}

fn parse_sse_json_events_interruptible(
    reader: impl BufRead,
    cancelled: &AtomicBool,
) -> HttpRequestResult<Vec<Value>> {
    let mut events = Vec::new();
    let mut data_lines = Vec::new();

    for line in reader.lines() {
        crate::interrupt::ensure_not_cancelled(cancelled)
            .map_err(|_| HttpRequestError::Interrupted)?;
        let line = line.map_err(HttpRequestError::ResponseRead)?;

        if line.is_empty() {
            push_sse_event(&mut events, &mut data_lines)?;
            continue;
        }

        if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.trim_start().to_owned());
        }
    }

    push_sse_event(&mut events, &mut data_lines)?;

    if events.is_empty() {
        return Err(HttpRequestError::MalformedStream(
            "response did not contain any SSE data events".to_owned(),
        ));
    }

    Ok(events)
}

fn read_response_body(
    mut response: Response,
    cancelled: &AtomicBool,
) -> HttpRequestResult<Vec<u8>> {
    let mut body = Vec::new();
    let mut buffer = [0_u8; 8192];

    loop {
        crate::interrupt::ensure_not_cancelled(cancelled)
            .map_err(|_| HttpRequestError::Interrupted)?;
        let bytes_read = response
            .read(&mut buffer)
            .map_err(HttpRequestError::ResponseRead)?;

        if bytes_read == 0 {
            break;
        }

        let Some(bytes) = buffer.get(..bytes_read) else {
            return Err(HttpRequestError::Internal(
                "HTTP response reader returned an out-of-bounds byte count".to_owned(),
            ));
        };
        body.extend_from_slice(bytes);
    }

    Ok(body)
}

fn push_sse_event(events: &mut Vec<Value>, data_lines: &mut Vec<String>) -> HttpRequestResult<()> {
    if data_lines.is_empty() {
        return Ok(());
    }

    let data = data_lines.join("\n");
    data_lines.clear();

    if data == "[DONE]" || data.trim().is_empty() {
        return Ok(());
    }

    let event = serde_json::from_str::<Value>(&data).map_err(|error| {
        HttpRequestError::MalformedStream(format!("stream event was not valid JSON: {error}"))
    })?;
    events.push(event);

    Ok(())
}

fn sleep_retry_backoff(retry_backoff_ms: u64, attempt: u32, cancelled: &AtomicBool) -> Result<()> {
    let mut remaining = retry_delay_ms(retry_backoff_ms, attempt);

    while remaining > 0 {
        crate::interrupt::ensure_not_cancelled(cancelled)?;
        let chunk = remaining.min(INTERRUPT_POLL_INTERVAL_MS);
        thread::sleep(Duration::from_millis(chunk));
        remaining -= chunk;
    }

    Ok(())
}

fn retry_delay_ms(retry_backoff_ms: u64, attempt: u32) -> u64 {
    if retry_backoff_ms == 0 {
        return 0;
    }

    let multiplier_shift = attempt.min(20);
    retry_backoff_ms.saturating_mul(1_u64 << multiplier_shift)
}

const fn is_transient_http_status(status: StatusCode) -> bool {
    matches!(
        status.as_u16(),
        408 | 409 | 425 | 429 | 500 | 502 | 503 | 504
    )
}

fn truncate_body(body: &str) -> String {
    const MAX_CHARS: usize = 512;

    let mut truncated = body.chars().take(MAX_CHARS).collect::<String>();
    if body.chars().count() > MAX_CHARS {
        truncated.push_str("...");
    }

    truncated
}

fn apply_provider_request_headers(
    request: reqwest::blocking::RequestBuilder,
    settings: &crate::backend::Settings,
    endpoint: HostedEndpointFlavor,
) -> reqwest::blocking::RequestBuilder {
    match endpoint {
        HostedEndpointFlavor::AnthropicMessages => {
            let request = if let Some(api_key) = settings.api_key.as_deref() {
                request.header("x-api-key", api_key)
            } else {
                request
            };

            request.header("anthropic-version", ANTHROPIC_API_VERSION)
        }
        HostedEndpointFlavor::ChatCompletions | HostedEndpointFlavor::Responses => {
            if let Some(api_key) = settings.api_key.as_deref() {
                request.header(AUTHORIZATION, format!("Bearer {api_key}"))
            } else {
                request
            }
        }
    }
}

fn apply_provider_discovery_headers(
    request: reqwest::blocking::RequestBuilder,
    settings: &crate::backend::Settings,
    provider: &str,
) -> reqwest::blocking::RequestBuilder {
    if provider == crate::http_policy::PROVIDER_ANTHROPIC {
        let request = if let Some(api_key) = settings.api_key.as_deref() {
            request.header("x-api-key", api_key)
        } else {
            request
        };

        request.header("anthropic-version", ANTHROPIC_API_VERSION)
    } else if let Some(api_key) = settings.api_key.as_deref() {
        request.header(AUTHORIZATION, format!("Bearer {api_key}"))
    } else {
        request
    }
}

fn parse_rerank_response(
    response: &Value,
    documents_len: usize,
) -> Result<Vec<crate::backend::RerankResult>> {
    let Some(results) = response
        .get("results")
        .or_else(|| response.get("data"))
        .and_then(Value::as_array)
    else {
        return Err(Error::MalformedRerankResponse(
            "response did not contain a results[] or data[] array".to_owned(),
        ));
    };

    results
        .iter()
        .map(|result| {
            let Some(index) = result.get("index").and_then(Value::as_u64) else {
                return Err(Error::MalformedRerankResponse(
                    "each rerank result must include an integer index".to_owned(),
                ));
            };
            let index = usize::try_from(index).map_err(|_| {
                Error::MalformedRerankResponse(
                    "rerank result index could not be represented as usize".to_owned(),
                )
            })?;

            if index >= documents_len {
                return Err(Error::MalformedRerankResponse(format!(
                    "rerank result index {index} is out of range for {documents_len} input documents"
                )));
            }

            let Some(score) = result
                .get("relevance_score")
                .or_else(|| result.get("score"))
                .and_then(Value::as_f64)
            else {
                return Err(Error::MalformedRerankResponse(
                    "each rerank result must include a numeric relevance_score or score"
                        .to_owned(),
                ));
            };

            Ok(crate::backend::RerankResult { index, score })
        })
        .collect()
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    reason = "tests should fail loudly with focused messages when fixtures break"
)]
mod tests {
    use super::{
        HostedEndpointFlavor, build_anthropic_request_payload, build_embedding_request_payload,
        build_request_payload, build_request_payload_for_endpoint, build_rerank_request_payload,
        build_stream_request_payload, chat_response, chat_stream_response, derive_embeddings_url,
        embed_response, extract_text, hosted_endpoint_flavor, normalize_response_for_endpoint,
        normalize_stream_events_for_endpoint, parse_embedding_response, parse_rerank_response,
        parse_sse_json_events, rerank_response,
    };
    use crate::backend::test_support::SettingsBuilder;
    use crate::backend::{RequestOptions, RerankResult, Runtime, Settings};
    use reqwest::StatusCode;
    use serde_json::{Value, json};
    use std::io::{BufRead, BufReader, Cursor, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;

    #[derive(Debug)]
    struct ObservedRequest {
        authorization: Option<String>,
        x_api_key: Option<String>,
        anthropic_version: Option<String>,
        body: Value,
    }

    #[derive(Debug)]
    struct MockResponse {
        status_code: u16,
        content_type: String,
        body: String,
    }

    fn settings(base_url: String) -> Settings {
        settings_with_retries(base_url, 2, 250)
    }

    fn settings_with_retries(
        base_url: String,
        max_retries: u32,
        retry_backoff_ms: u64,
    ) -> Settings {
        SettingsBuilder::new()
            .runtime(Runtime::OpenAi)
            .base_url(&base_url)
            .api_key(Some("secret-token"))
            .retries(max_retries, retry_backoff_ms)
            .build()
    }

    fn request_options() -> RequestOptions {
        RequestOptions {
            temperature: 0.2,
            max_tokens: Some(64),
        }
    }

    fn start_mock_server(response_body: &str) -> (String, mpsc::Receiver<ObservedRequest>) {
        start_mock_server_at_path("/v1/chat/completions", response_body, "application/json")
    }

    fn start_mock_stream_server(response_body: &str) -> (String, mpsc::Receiver<ObservedRequest>) {
        start_mock_server_at_path("/v1/chat/completions", response_body, "text/event-stream")
    }

    fn start_mock_server_at_path(
        path: &str,
        response_body: &str,
        content_type: &str,
    ) -> (String, mpsc::Receiver<ObservedRequest>) {
        let (address, receiver) = start_mock_server_sequence_at_path(
            path,
            vec![MockResponse::new(200, content_type, response_body)],
        );

        let (sender, single_receiver) = mpsc::channel();
        thread::spawn(move || {
            let observed_request = receiver
                .recv()
                .expect("mock server should send the observed requests")
                .into_iter()
                .next()
                .expect("single-response server should observe exactly one request");
            sender
                .send(observed_request)
                .expect("request should be sent back to the test");
        });

        (address, single_receiver)
    }

    fn start_mock_server_sequence(
        responses: Vec<MockResponse>,
    ) -> (String, mpsc::Receiver<Vec<ObservedRequest>>) {
        start_mock_server_sequence_at_path("/v1/chat/completions", responses)
    }

    fn start_mock_server_sequence_at_path(
        path: &str,
        responses: Vec<MockResponse>,
    ) -> (String, mpsc::Receiver<Vec<ObservedRequest>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = format!(
            "http://{}{}",
            listener
                .local_addr()
                .expect("listener should have a local address"),
            path,
        );
        let expected_path = path.to_owned();
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let mut observed_requests = Vec::with_capacity(responses.len());
            for response in responses {
                let (stream, _) = listener
                    .accept()
                    .expect("server should accept one connection per configured response");
                let observed_request = read_request(stream, &expected_path, &response);
                observed_requests.push(observed_request);
            }
            sender
                .send(observed_requests)
                .expect("requests should be sent back to the test");
        });

        (address, receiver)
    }

    fn read_request(
        mut stream: TcpStream,
        expected_path: &str,
        response: &MockResponse,
    ) -> ObservedRequest {
        let mut reader = BufReader::new(stream.try_clone().expect("stream should clone"));
        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .expect("request line should read");
        assert!(
            request_line.starts_with(&format!("POST {expected_path} HTTP/1.1")),
            "unexpected request line: {request_line}"
        );

        let mut content_length = None;
        let mut authorization = None;
        let mut x_api_key = None;
        let mut anthropic_version = None;

        loop {
            let mut header_line = String::new();
            reader
                .read_line(&mut header_line)
                .expect("header line should read");

            if header_line == "\r\n" {
                break;
            }

            let lower_header = header_line.to_ascii_lowercase();
            if lower_header.starts_with("content-length:") {
                let parsed = header_line
                    .split_once(':')
                    .expect("content-length header should contain a separator")
                    .1
                    .trim()
                    .parse::<usize>()
                    .expect("content-length should parse");
                content_length = Some(parsed);
            }

            if lower_header.starts_with("authorization:") {
                authorization = Some(
                    header_line
                        .split_once(':')
                        .expect("authorization header should contain a separator")
                        .1
                        .trim()
                        .to_owned(),
                );
            }

            if lower_header.starts_with("x-api-key:") {
                x_api_key = Some(
                    header_line
                        .split_once(':')
                        .expect("x-api-key header should contain a separator")
                        .1
                        .trim()
                        .to_owned(),
                );
            }

            if lower_header.starts_with("anthropic-version:") {
                anthropic_version = Some(
                    header_line
                        .split_once(':')
                        .expect("anthropic-version header should contain a separator")
                        .1
                        .trim()
                        .to_owned(),
                );
            }
        }

        let body_length = content_length.expect("request should include content-length");
        let mut body = vec![0_u8; body_length];
        reader
            .read_exact(&mut body)
            .expect("request body should read");

        write!(
            stream,
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
            response.status_code,
            response.reason_phrase(),
            response.content_type,
            response.body.len(),
            response.body
        )
        .expect("response should write");
        stream.flush().expect("response should flush");

        ObservedRequest {
            authorization,
            x_api_key,
            anthropic_version,
            body: serde_json::from_slice(&body).expect("request body should be valid JSON"),
        }
    }

    impl MockResponse {
        fn new(status_code: u16, content_type: &str, body: &str) -> Self {
            Self {
                status_code,
                content_type: content_type.to_owned(),
                body: body.to_owned(),
            }
        }

        fn reason_phrase(&self) -> &'static str {
            StatusCode::from_u16(self.status_code)
                .ok()
                .and_then(|status| status.canonical_reason())
                .unwrap_or("OK")
        }
    }

    #[test]
    fn build_request_payload_should_include_sampling_controls() {
        let payload = build_request_payload(
            "llama3.2",
            &[json!({"role": "user", "content": "Say hi"})],
            request_options(),
            None,
            None,
            None,
        );

        assert_eq!(payload["model"], "llama3.2");
        assert_eq!(payload["messages"][0]["role"], "user");
        assert_eq!(payload["max_tokens"], 64);
    }

    #[test]
    fn hosted_endpoint_flavor_should_detect_responses_urls() {
        assert_eq!(
            hosted_endpoint_flavor(&settings("https://api.openai.com/v1/responses".to_owned())),
            HostedEndpointFlavor::Responses
        );
        assert_eq!(
            hosted_endpoint_flavor(&settings(
                "https://api.openai.com/v1/chat/completions".to_owned()
            )),
            HostedEndpointFlavor::ChatCompletions
        );
        assert_eq!(
            hosted_endpoint_flavor(&settings(
                "https://api.anthropic.com/v1/messages".to_owned()
            )),
            HostedEndpointFlavor::AnthropicMessages
        );
    }

    #[test]
    fn build_rerank_request_payload_should_include_top_n() {
        let payload = build_rerank_request_payload(
            "jina-reranker-v2-base-multilingual",
            "What controls table bloat?",
            &[
                "Autovacuum removes dead tuples.".to_owned(),
                "Bananas are yellow.".to_owned(),
            ],
            Some(1),
        );

        assert_eq!(payload["model"], "jina-reranker-v2-base-multilingual");
        assert_eq!(payload["query"], "What controls table bloat?");
        assert_eq!(payload["documents"][0], "Autovacuum removes dead tuples.");
        assert_eq!(payload["top_n"], 1);
    }

    #[test]
    fn build_embedding_request_payload_should_include_model_and_inputs() {
        let payload = build_embedding_request_payload(
            "text-embedding-3-small",
            &["hello".to_owned(), "world".to_owned()],
        );

        assert_eq!(payload["model"], "text-embedding-3-small");
        assert_eq!(payload["input"], json!(["hello", "world"]));
    }

    #[test]
    fn build_stream_request_payload_should_enable_streaming() {
        let payload = build_stream_request_payload(
            "llama3.2",
            &[json!({"role": "user", "content": "Say hi"})],
            request_options(),
        );

        assert_eq!(payload["stream"], true);
        assert_eq!(payload["messages"][0]["content"], "Say hi");
    }

    #[test]
    fn parse_sse_json_events_should_skip_done_markers_and_parse_json_chunks() {
        let events = parse_sse_json_events(Cursor::new(concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"hel\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        )))
        .expect("SSE payload should parse");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["choices"][0]["delta"]["content"], "hel");
        assert_eq!(events[1]["choices"][0]["finish_reason"], "stop");
    }

    #[test]
    fn derive_embeddings_url_should_resolve_sibling_endpoint_from_chat_and_responses_paths() {
        assert_eq!(
            derive_embeddings_url("https://api.openai.com/v1/chat/completions")
                .expect("chat completions path should derive"),
            "https://api.openai.com/v1/embeddings"
        );
        assert_eq!(
            derive_embeddings_url("https://api.openai.com/v1/responses")
                .expect("responses path should derive"),
            "https://api.openai.com/v1/embeddings"
        );
        assert_eq!(
            derive_embeddings_url("https://api.openai.com/v1/rerank")
                .expect("rerank path should derive"),
            "https://api.openai.com/v1/embeddings"
        );
    }

    #[test]
    fn parse_embedding_response_should_sort_by_index_and_optionally_normalize() {
        let response = json!({
            "data": [
                {
                    "index": 1,
                    "embedding": [3.0, 4.0]
                },
                {
                    "index": 0,
                    "embedding": [0.0, 5.0]
                }
            ]
        });

        let raw = parse_embedding_response(&response, false).expect("raw embeddings should parse");
        let normalized =
            parse_embedding_response(&response, true).expect("normalized embeddings should parse");

        assert_eq!(raw, vec![vec![0.0, 5.0], vec![3.0, 4.0]]);
        assert!((normalized[0][0] - 0.0).abs() < 1.0e-6);
        assert!((normalized[0][1] - 1.0).abs() < 1.0e-6);
        assert!((normalized[1][0] - 0.6).abs() < 1.0e-6);
        assert!((normalized[1][1] - 0.8).abs() < 1.0e-6);
    }

    #[test]
    fn extract_text_should_accept_text_part_arrays() {
        let response = json!({
            "choices": [
                {
                    "message": {
                        "content": [
                            { "type": "text", "text": "hello" },
                            { "type": "text", "text": " world" }
                        ]
                    }
                }
            ]
        });

        let text = extract_text(&response).expect("response should parse");

        assert_eq!(text, "hello world");
    }

    #[test]
    fn parse_rerank_response_should_accept_results_arrays() {
        let response = json!({
            "results": [
                {"index": 1, "relevance_score": 0.97},
                {"index": 0, "relevance_score": 0.41}
            ]
        });

        let ranked = parse_rerank_response(&response, 2).expect("rerank response should parse");

        assert_eq!(
            ranked,
            vec![
                RerankResult {
                    index: 1,
                    score: 0.97
                },
                RerankResult {
                    index: 0,
                    score: 0.41
                }
            ]
        );
    }

    #[test]
    fn parse_rerank_response_should_accept_data_arrays_and_score_keys() {
        let response = json!({
            "data": [
                {"index": 0, "score": 0.88}
            ]
        });

        let ranked = parse_rerank_response(&response, 1).expect("rerank response should parse");

        assert_eq!(
            ranked,
            vec![RerankResult {
                index: 0,
                score: 0.88
            }]
        );
    }

    #[test]
    fn chat_response_should_forward_auth_and_return_json() {
        let (base_url, receiver) = start_mock_server(
            r#"{"choices":[{"message":{"content":"hello from the mock server"}}]}"#,
        );
        let messages = vec![json!({"role": "user", "content": "Hello"})];

        let response = chat_response(
            &settings(base_url),
            &messages,
            request_options(),
            None,
            None,
            None,
        )
        .expect("chat request should succeed");
        let observed_request = receiver
            .recv()
            .expect("mock server should observe the request");

        assert_eq!(
            observed_request.authorization.as_deref(),
            Some("Bearer secret-token")
        );
        assert_eq!(observed_request.body["model"], "llama3.2");
        assert_eq!(observed_request.body["messages"][0]["content"], "Hello");
        assert_eq!(
            response["choices"][0]["message"]["content"],
            "hello from the mock server"
        );
    }

    #[test]
    fn embed_response_should_use_embeddings_endpoint_and_parse_vectors() {
        let (embeddings_url, receiver) = start_mock_server_at_path(
            "/v1/embeddings",
            r#"{
                "data":[
                    {"index":1,"embedding":[1.0,2.0]},
                    {"index":0,"embedding":[5.0,0.0]}
                ]
            }"#,
            "application/json",
        );
        let base_url = embeddings_url.replace("/v1/embeddings", "/v1/chat/completions");
        let settings = SettingsBuilder::new()
            .runtime(Runtime::OpenAi)
            .model("text-embedding-3-small")
            .base_url(&base_url)
            .api_key(Some("secret-token"))
            .build();

        let vectors = embed_response(&settings, &["hello".to_owned(), "world".to_owned()], true)
            .expect("embedding request should succeed");
        let observed_request = receiver
            .recv()
            .expect("mock server should observe the embedding request");

        assert_eq!(
            observed_request.authorization.as_deref(),
            Some("Bearer secret-token")
        );
        assert_eq!(observed_request.body["model"], "text-embedding-3-small");
        assert_eq!(observed_request.body["input"], json!(["hello", "world"]));
        assert!((vectors[0][0] - 1.0).abs() < 1.0e-6);
        assert!((vectors[0][1] - 0.0).abs() < 1.0e-6);
        assert!((vectors[1][0] - 0.447_213_6).abs() < 1.0e-5);
        assert!((vectors[1][1] - 0.894_427_2).abs() < 1.0e-5);
    }

    #[test]
    fn rerank_response_should_forward_auth_and_parse_results() {
        let (base_url, receiver) = start_mock_server(
            r#"{"results":[{"index":1,"relevance_score":0.98},{"index":0,"relevance_score":0.42}]}"#,
        );
        let documents = vec![
            "Bananas are yellow.".to_owned(),
            "Autovacuum removes dead tuples.".to_owned(),
        ];

        let ranked = rerank_response(
            &settings(base_url),
            "What controls table bloat?",
            &documents,
            Some(1),
        )
        .expect("rerank request should succeed");
        let observed_request = receiver
            .recv()
            .expect("mock server should observe the rerank request");

        assert_eq!(
            observed_request.authorization.as_deref(),
            Some("Bearer secret-token")
        );
        assert_eq!(observed_request.body["model"], "llama3.2");
        assert_eq!(observed_request.body["query"], "What controls table bloat?");
        assert_eq!(
            observed_request.body["documents"][1],
            "Autovacuum removes dead tuples."
        );
        assert_eq!(observed_request.body["top_n"], 1);
        assert_eq!(
            ranked,
            vec![RerankResult {
                index: 1,
                score: 0.98
            }]
        );
    }

    #[test]
    fn chat_response_should_forward_tools_and_tool_choice() {
        let (base_url, receiver) = start_mock_server(
            r#"{"choices":[{"message":{"tool_calls":[{"id":"call_123","type":"function","function":{"name":"lookup_weather","arguments":"{\"city\":\"Austin\"}"}}]}}]}"#,
        );
        let messages = vec![json!({"role": "user", "content": "Hello"})];
        let tools = vec![json!({
            "type": "function",
            "function": {
                "name": "lookup_weather",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"}
                    }
                }
            }
        })];
        let tool_choice = json!({
            "type": "function",
            "function": {
                "name": "lookup_weather"
            }
        });

        let response = chat_response(
            &settings(base_url),
            &messages,
            request_options(),
            None,
            Some(&tools),
            Some(&tool_choice),
        )
        .expect("chat request should succeed");
        let observed_request = receiver
            .recv()
            .expect("mock server should observe the request");

        assert_eq!(observed_request.body["tools"], json!(tools));
        assert_eq!(observed_request.body["tool_choice"], tool_choice);
        assert_eq!(
            response["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "lookup_weather"
        );
    }

    #[test]
    fn chat_stream_response_should_forward_stream_flag_and_parse_sse_events() {
        let (base_url, receiver) = start_mock_stream_server(concat!(
            "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"model\":\"llama3.2\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"model\":\"llama3.2\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hel\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"model\":\"llama3.2\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        ));
        let messages = vec![json!({"role": "user", "content": "Hello"})];

        let events = chat_stream_response(&settings(base_url), &messages, request_options())
            .expect("streaming chat request should succeed");
        let observed_request = receiver
            .recv()
            .expect("mock server should observe the request");

        assert_eq!(observed_request.body["stream"], true);
        assert_eq!(events.len(), 3);
        assert_eq!(events[1]["choices"][0]["delta"]["content"], "hel");
        assert_eq!(events[2]["choices"][0]["finish_reason"], "stop");
    }

    #[test]
    fn chat_response_should_retry_transient_upstream_statuses_before_succeeding() {
        let (base_url, receiver) = start_mock_server_sequence(vec![
            MockResponse::new(503, "application/json", r#"{"error":"please retry"}"#),
            MockResponse::new(
                200,
                "application/json",
                r#"{"choices":[{"message":{"content":"retried successfully"}}]}"#,
            ),
        ]);
        let messages = vec![json!({"role": "user", "content": "Hello"})];

        let response = chat_response(
            &settings_with_retries(base_url, 1, 0),
            &messages,
            request_options(),
            None,
            None,
            None,
        )
        .expect("chat request should succeed after retry");
        let observed_requests = receiver
            .recv()
            .expect("mock server should observe both requests");

        assert_eq!(observed_requests.len(), 2);
        assert_eq!(
            response["choices"][0]["message"]["content"],
            "retried successfully"
        );
    }

    #[test]
    fn chat_response_should_not_retry_non_transient_upstream_statuses() {
        let (base_url, receiver) = start_mock_server_sequence(vec![MockResponse::new(
            400,
            "application/json",
            r#"{"error":"bad request"}"#,
        )]);
        let messages = vec![json!({"role": "user", "content": "Hello"})];

        let error = chat_response(
            &settings_with_retries(base_url, 3, 0),
            &messages,
            request_options(),
            None,
            None,
            None,
        )
        .expect_err("chat request should fail without retrying");
        let observed_requests = receiver
            .recv()
            .expect("mock server should observe the request");

        assert_eq!(observed_requests.len(), 1);
        assert!(
            matches!(
                &error,
                crate::error::Error::Upstream { status, body }
                    if *status == StatusCode::BAD_REQUEST
                        && body == r#"{"error":"bad request"}"#
            ),
            "expected upstream error, got {error:?}"
        );
    }

    #[test]
    fn chat_stream_response_should_retry_transient_upstream_statuses_before_succeeding() {
        let (base_url, receiver) = start_mock_server_sequence(vec![
            MockResponse::new(502, "application/json", r#"{"error":"try again"}"#),
            MockResponse::new(
                200,
                "text/event-stream",
                concat!(
                    "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"model\":\"llama3.2\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"model\":\"llama3.2\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"retry worked\"},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                ),
            ),
        ]);
        let messages = vec![json!({"role": "user", "content": "Hello"})];

        let events = chat_stream_response(
            &settings_with_retries(base_url, 1, 0),
            &messages,
            request_options(),
        )
        .expect("streaming chat request should succeed after retry");
        let observed_requests = receiver
            .recv()
            .expect("mock server should observe both requests");

        assert_eq!(observed_requests.len(), 2);
        assert_eq!(events.len(), 2);
        assert_eq!(events[1]["choices"][0]["delta"]["content"], "retry worked");
    }

    #[test]
    fn build_request_payload_should_include_response_format_when_present() {
        let response_format = json!({
            "type": "json_schema",
            "json_schema": {
                "name": "person",
                "strict": true,
                "schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"}
                    },
                    "required": ["name"],
                    "additionalProperties": false
                }
            }
        });
        let payload = build_request_payload(
            "llama3.2",
            &[json!({"role": "user", "content": "Say hi"})],
            request_options(),
            Some(&response_format),
            None,
            None,
        );

        assert_eq!(payload["response_format"], response_format);
    }

    #[test]
    fn build_request_payload_should_include_tools_and_tool_choice_when_present() {
        let tools = vec![json!({
            "type": "function",
            "function": {
                "name": "lookup_weather",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"}
                    }
                }
            }
        })];
        let tool_choice = json!({
            "type": "function",
            "function": {
                "name": "lookup_weather"
            }
        });
        let payload = build_request_payload(
            "llama3.2",
            &[json!({"role": "user", "content": "Say hi"})],
            request_options(),
            None,
            Some(&tools),
            Some(&tool_choice),
        );

        assert_eq!(payload["tools"], json!(tools));
        assert_eq!(payload["tool_choice"], tool_choice);
    }

    #[test]
    fn build_request_payload_for_responses_should_translate_input_and_text_format() {
        let response_format = json!({
            "type": "json_schema",
            "json_schema": {
                "name": "person",
                "strict": true,
                "schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"}
                    },
                    "required": ["name"],
                    "additionalProperties": false
                }
            }
        });
        let payload = build_request_payload_for_endpoint(
            HostedEndpointFlavor::Responses,
            "gpt-4.1-mini",
            &[json!({"role": "user", "content": "Say hi"})],
            request_options(),
            Some(&response_format),
            None,
            None,
        )
        .expect("responses payload should build");

        assert_eq!(payload["model"], "gpt-4.1-mini");
        assert_eq!(payload["input"][0]["role"], "user");
        assert_eq!(payload["input"][0]["content"][0]["type"], "input_text");
        assert_eq!(payload["input"][0]["content"][0]["text"], "Say hi");
        assert_eq!(payload["max_output_tokens"], 64);
        assert_eq!(payload["text"]["format"], response_format);
    }

    #[test]
    fn build_anthropic_request_payload_should_lift_system_messages() {
        let payload = build_anthropic_request_payload(
            "claude-3-5-sonnet-latest",
            &[
                json!({"role": "system", "content": "You are concise."}),
                json!({"role": "user", "content": "Explain MVCC."}),
            ],
            request_options(),
            None,
            None,
            None,
        )
        .expect("anthropic payload should build");

        assert_eq!(payload["model"], "claude-3-5-sonnet-latest");
        assert_eq!(payload["system"], "You are concise.");
        assert_eq!(payload["messages"][0]["role"], "user");
        assert_eq!(payload["messages"][0]["content"][0]["type"], "text");
        assert_eq!(
            payload["messages"][0]["content"][0]["text"],
            "Explain MVCC."
        );
        assert_eq!(payload["max_tokens"], 64);
    }

    #[test]
    fn normalize_response_for_responses_should_produce_chat_completion_shape() {
        let normalized = normalize_response_for_endpoint(
            HostedEndpointFlavor::Responses,
            &json!({
                "id": "resp_123",
                "model": "gpt-4.1-mini",
                "status": "completed",
                "output_text": "hello from responses",
                "usage": {
                    "input_tokens": 7,
                    "output_tokens": 5,
                    "total_tokens": 12
                },
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": "hello from responses"
                    }]
                }]
            }),
        )
        .expect("responses payload should normalize");

        assert_eq!(
            normalized["choices"][0]["message"]["content"],
            "hello from responses"
        );
        assert_eq!(normalized["choices"][0]["finish_reason"], "stop");
        assert_eq!(normalized["usage"]["prompt_tokens"], 7);
        assert_eq!(normalized["usage"]["completion_tokens"], 5);
    }

    #[test]
    fn normalize_response_for_anthropic_should_produce_chat_completion_shape() {
        let normalized = normalize_response_for_endpoint(
            HostedEndpointFlavor::AnthropicMessages,
            &json!({
                "id": "msg_123",
                "model": "claude-3-5-sonnet-latest",
                "role": "assistant",
                "stop_reason": "end_turn",
                "content": [{
                    "type": "text",
                    "text": "hello from anthropic"
                }],
                "usage": {
                    "input_tokens": 7,
                    "output_tokens": 5
                }
            }),
        )
        .expect("anthropic payload should normalize");

        assert_eq!(
            normalized["choices"][0]["message"]["content"],
            "hello from anthropic"
        );
        assert_eq!(normalized["choices"][0]["finish_reason"], "stop");
        assert_eq!(normalized["usage"]["prompt_tokens"], 7);
        assert_eq!(normalized["usage"]["completion_tokens"], 5);
        assert_eq!(normalized["usage"]["total_tokens"], 12);
    }

    #[test]
    fn normalize_response_for_responses_should_produce_tool_calls() {
        let normalized = normalize_response_for_endpoint(
            HostedEndpointFlavor::Responses,
            &json!({
                "id": "resp_456",
                "model": "gpt-4.1-mini",
                "status": "completed",
                "output": [{
                    "type": "function_call",
                    "call_id": "call_123",
                    "name": "lookup_weather",
                    "arguments": "{\"city\":\"Austin\"}"
                }]
            }),
        )
        .expect("responses tool call should normalize");

        assert_eq!(normalized["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(
            normalized["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "lookup_weather"
        );
    }

    #[test]
    fn normalize_stream_events_for_responses_should_emit_chunk_shapes() {
        let events = normalize_stream_events_for_endpoint(
            HostedEndpointFlavor::Responses,
            &[
                json!({
                    "type": "response.output_text.delta",
                    "response_id": "resp_123",
                    "delta": "hel"
                }),
                json!({
                    "type": "response.output_text.delta",
                    "response_id": "resp_123",
                    "delta": "lo"
                }),
                json!({
                    "type": "response.completed",
                    "response": {
                        "id": "resp_123",
                        "model": "gpt-4.1-mini",
                        "status": "completed",
                        "output_text": "hello"
                    }
                }),
            ],
        )
        .expect("responses stream events should normalize");

        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["choices"][0]["delta"]["content"], "hel");
        assert_eq!(events[2]["choices"][0]["finish_reason"], "stop");
    }

    #[test]
    fn normalize_stream_events_for_anthropic_should_emit_chunk_shapes() {
        let events = normalize_stream_events_for_endpoint(
            HostedEndpointFlavor::AnthropicMessages,
            &[
                json!({
                    "type": "message_start",
                    "message": {
                        "id": "msg_123",
                        "model": "claude-3-5-sonnet-latest"
                    }
                }),
                json!({
                    "type": "content_block_delta",
                    "delta": {
                        "type": "text_delta",
                        "text": "hel"
                    }
                }),
                json!({
                    "type": "message_delta",
                    "delta": {
                        "stop_reason": "end_turn"
                    },
                    "usage": {
                        "input_tokens": 7,
                        "output_tokens": 5
                    }
                }),
            ],
        )
        .expect("anthropic stream events should normalize");

        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["choices"][0]["delta"]["role"], "assistant");
        assert_eq!(events[1]["choices"][0]["delta"]["content"], "hel");
        assert_eq!(events[2]["choices"][0]["finish_reason"], "stop");
        assert_eq!(events[2]["usage"]["total_tokens"], 12);
    }

    #[test]
    fn chat_response_should_normalize_responses_api_output() {
        let (base_url, receiver) = start_mock_server_at_path(
            "/v1/responses",
            r#"{
                "id":"resp_123",
                "model":"gpt-4.1-mini",
                "status":"completed",
                "output_text":"hello from responses",
                "usage":{"input_tokens":7,"output_tokens":5,"total_tokens":12},
                "output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello from responses"}]}]
            }"#,
            "application/json",
        );
        let messages = vec![json!({"role": "user", "content": "Hello"})];

        let response = chat_response(
            &settings(base_url),
            &messages,
            request_options(),
            None,
            None,
            None,
        )
        .expect("responses chat request should succeed");
        let observed_request = receiver
            .recv()
            .expect("mock server should observe the request");

        assert_eq!(observed_request.body["input"][0]["role"], "user");
        assert_eq!(observed_request.body["max_output_tokens"], 64);
        assert_eq!(
            response["choices"][0]["message"]["content"],
            "hello from responses"
        );
    }

    #[test]
    fn chat_response_should_normalize_anthropic_messages_output() {
        let (base_url, receiver) = start_mock_server_at_path(
            "/v1/messages",
            r#"{
                "id":"msg_123",
                "model":"claude-3-5-sonnet-latest",
                "role":"assistant",
                "stop_reason":"end_turn",
                "content":[{"type":"text","text":"hello from anthropic"}],
                "usage":{"input_tokens":7,"output_tokens":5}
            }"#,
            "application/json",
        );
        let messages = vec![
            json!({"role": "system", "content": "You are concise."}),
            json!({"role": "user", "content": "Hello"}),
        ];
        let settings = SettingsBuilder::new()
            .runtime(Runtime::OpenAi)
            .model("claude-3-5-sonnet-latest")
            .base_url(&base_url)
            .api_key(Some("secret-token"))
            .build();

        let response = chat_response(&settings, &messages, request_options(), None, None, None)
            .expect("anthropic chat request should succeed");
        let observed_request = receiver
            .recv()
            .expect("mock server should observe the request");

        assert_eq!(observed_request.authorization, None);
        assert_eq!(observed_request.x_api_key.as_deref(), Some("secret-token"));
        assert_eq!(
            observed_request.anthropic_version.as_deref(),
            Some(super::ANTHROPIC_API_VERSION)
        );
        assert_eq!(observed_request.body["system"], "You are concise.");
        assert_eq!(observed_request.body["messages"][0]["role"], "user");
        assert_eq!(
            observed_request.body["messages"][0]["content"][0]["text"],
            "Hello"
        );
        assert_eq!(
            response["choices"][0]["message"]["content"],
            "hello from anthropic"
        );
        assert_eq!(response["usage"]["total_tokens"], 12);
    }

    #[test]
    fn chat_stream_response_should_normalize_responses_stream_events() {
        let (base_url, receiver) = start_mock_server_at_path(
            "/v1/responses",
            concat!(
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\",\"model\":\"gpt-4.1-mini\",\"status\":\"in_progress\"}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_123\",\"delta\":\"hel\"}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_123\",\"delta\":\"lo\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_123\",\"model\":\"gpt-4.1-mini\",\"status\":\"completed\",\"output_text\":\"hello\"}}\n\n"
            ),
            "text/event-stream",
        );
        let messages = vec![json!({"role": "user", "content": "Hello"})];

        let events = chat_stream_response(&settings(base_url), &messages, request_options())
            .expect("responses streaming chat request should succeed");
        let observed_request = receiver
            .recv()
            .expect("mock server should observe the request");

        assert_eq!(observed_request.body["stream"], true);
        assert_eq!(observed_request.body["input"][0]["role"], "user");
        assert_eq!(events[0]["choices"][0]["delta"]["content"], "hel");
        assert_eq!(events[2]["choices"][0]["finish_reason"], "stop");
    }

    #[test]
    fn chat_stream_response_should_normalize_anthropic_stream_events() {
        let (base_url, receiver) = start_mock_server_at_path(
            "/v1/messages",
            concat!(
                "event: message_start\n",
                "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_123\",\"model\":\"claude-3-5-sonnet-latest\"}}\n\n",
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hel\"}}\n\n",
                "event: message_delta\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":7,\"output_tokens\":5}}\n\n",
                "event: message_stop\n",
                "data: {\"type\":\"message_stop\"}\n\n"
            ),
            "text/event-stream",
        );
        let settings = SettingsBuilder::new()
            .runtime(Runtime::OpenAi)
            .model("claude-3-5-sonnet-latest")
            .base_url(&base_url)
            .api_key(Some("secret-token"))
            .build();
        let messages = vec![json!({"role": "user", "content": "Hello"})];

        let events = chat_stream_response(&settings, &messages, request_options())
            .expect("anthropic streaming chat request should succeed");
        let observed_request = receiver
            .recv()
            .expect("mock server should observe the request");

        assert_eq!(observed_request.authorization, None);
        assert_eq!(observed_request.x_api_key.as_deref(), Some("secret-token"));
        assert_eq!(observed_request.body["messages"][0]["role"], "user");
        assert_eq!(observed_request.body["stream"], true);
        assert_eq!(events[0]["choices"][0]["delta"]["role"], "assistant");
        assert_eq!(events[1]["choices"][0]["delta"]["content"], "hel");
        assert_eq!(events[2]["choices"][0]["finish_reason"], "stop");
    }
}
