#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::error::{Error, Result};
use reqwest::StatusCode;
use reqwest::blocking::{Client, Response};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

const INTERRUPT_POLL_INTERVAL_MS: u64 = 25;
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(INTERRUPT_POLL_INTERVAL_MS);

type HttpRequestResult<T> = core::result::Result<T, HttpRequestError>;

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

fn execute_chat_response_once(
    settings: &crate::backend::Settings,
    messages: &[Value],
    options: crate::backend::RequestOptions,
    response_format: Option<&Value>,
    tools: Option<&[Value]>,
    tool_choice: Option<&Value>,
    cancelled: &AtomicBool,
) -> HttpRequestResult<Value> {
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
    let payload = build_request_payload(
        &settings.model,
        messages,
        options,
        response_format,
        tools,
        tool_choice,
    );
    let mut request = client
        .post(base_url)
        .header(CONTENT_TYPE, "application/json")
        .json(&payload);

    if let Some(api_key) = settings.api_key.as_deref() {
        request = request.header(AUTHORIZATION, format!("Bearer {api_key}"));
    }

    let response = request.send().map_err(HttpRequestError::Transport)?;
    let status = response.status();
    let body = read_response_body(response, cancelled)?;

    if !status.is_success() {
        return Err(HttpRequestError::Upstream {
            status,
            body: truncate_body(&String::from_utf8_lossy(&body)),
        });
    }

    serde_json::from_slice(&body).map_err(HttpRequestError::JsonDecode)
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
    let payload = build_stream_request_payload(&settings.model, messages, options);
    let mut request = client
        .post(base_url)
        .header(CONTENT_TYPE, "application/json")
        .json(&payload);

    if let Some(api_key) = settings.api_key.as_deref() {
        request = request.header(AUTHORIZATION, format!("Bearer {api_key}"));
    }

    let response = request.send().map_err(HttpRequestError::Transport)?;
    let status = response.status();

    if !status.is_success() {
        let body = read_response_body(response, cancelled)?;
        return Err(HttpRequestError::Upstream {
            status,
            body: truncate_body(&String::from_utf8_lossy(&body)),
        });
    }

    parse_sse_json_events_interruptible(BufReader::new(response), cancelled)
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

fn execute_rerank_response_once(
    settings: &crate::backend::Settings,
    query: &str,
    documents: &[String],
    top_n: Option<usize>,
    cancelled: &AtomicBool,
) -> HttpRequestResult<Vec<crate::backend::RerankResult>> {
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
    let mut request = client
        .post(base_url)
        .header(CONTENT_TYPE, "application/json")
        .json(&payload);

    if let Some(api_key) = settings.api_key.as_deref() {
        request = request.header(AUTHORIZATION, format!("Bearer {api_key}"));
    }

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
    let Some(choices) = response.get("choices").and_then(Value::as_array) else {
        return Err(Error::MalformedResponse);
    };

    let Some(first_choice) = choices.first() else {
        return Err(Error::MalformedResponse);
    };

    if let Some(text) = first_choice.get("text").and_then(Value::as_str) {
        return Ok(text.to_owned());
    }

    let Some(content) = first_choice
        .get("message")
        .and_then(|message| message.get("content"))
    else {
        return Err(Error::MalformedResponse);
    };

    match content {
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
    reason = "tests should fail loudly with focused messages when fixtures break"
)]
mod tests {
    use super::{
        build_request_payload, build_rerank_request_payload, build_stream_request_payload,
        chat_response, chat_stream_response, extract_text, parse_rerank_response,
        parse_sse_json_events, rerank_response,
    };
    use crate::backend::{CandleDevice, RequestOptions, RerankResult, Runtime, Settings};
    use reqwest::StatusCode;
    use serde_json::{Value, json};
    use std::io::{BufRead, BufReader, Cursor, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;

    #[derive(Debug)]
    struct ObservedRequest {
        authorization: Option<String>,
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
        Settings {
            runtime: Runtime::OpenAi,
            model: "llama3.2".to_owned(),
            base_url: Some(base_url),
            api_key: Some("secret-token".to_owned()),
            timeout_ms: 30_000,
            max_retries,
            retry_backoff_ms,
            candle_cache_dir: None,
            candle_offline: false,
            candle_device: CandleDevice::Auto,
            candle_max_input_tokens: 0,
            candle_max_concurrency: 0,
        }
    }

    fn request_options() -> RequestOptions {
        RequestOptions {
            temperature: 0.2,
            max_tokens: Some(64),
        }
    }

    fn start_mock_server(response_body: &str) -> (String, mpsc::Receiver<ObservedRequest>) {
        start_mock_server_with_content_type(response_body, "application/json")
    }

    fn start_mock_stream_server(response_body: &str) -> (String, mpsc::Receiver<ObservedRequest>) {
        start_mock_server_with_content_type(response_body, "text/event-stream")
    }

    fn start_mock_server_with_content_type(
        response_body: &str,
        content_type: &str,
    ) -> (String, mpsc::Receiver<ObservedRequest>) {
        let (address, receiver) =
            start_mock_server_sequence(vec![MockResponse::new(200, content_type, response_body)]);

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
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = format!(
            "http://{}/v1/chat/completions",
            listener
                .local_addr()
                .expect("listener should have a local address")
        );
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let mut observed_requests = Vec::with_capacity(responses.len());
            for response in responses {
                let (stream, _) = listener
                    .accept()
                    .expect("server should accept one connection per configured response");
                let observed_request = read_request(stream, &response);
                observed_requests.push(observed_request);
            }
            sender
                .send(observed_requests)
                .expect("requests should be sent back to the test");
        });

        (address, receiver)
    }

    fn read_request(mut stream: TcpStream, response: &MockResponse) -> ObservedRequest {
        let mut reader = BufReader::new(stream.try_clone().expect("stream should clone"));
        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .expect("request line should read");
        assert!(
            request_line.starts_with("POST /v1/chat/completions HTTP/1.1"),
            "unexpected request line: {request_line}"
        );

        let mut content_length = None;
        let mut authorization = None;

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
}
