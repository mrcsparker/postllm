#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::backend::Settings;
use crate::error::{Error, Result};
use crate::operator_policy;
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::spi::Spi;
use serde_json::{Map, Value};
use std::time::Duration;

const REDACTED_VALUE: &str = "[redacted]";
const REDACTABLE_FIELDS: [&str; 11] = [
    "arguments",
    "completion",
    "content",
    "delta",
    "document",
    "documents",
    "input",
    "inputs",
    "prompt",
    "query",
    "text",
];

/// Audit-log controls resolved from session GUCs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AuditConfig {
    /// Whether request audit logging is enabled for the current session.
    pub(crate) enabled: bool,
    /// Whether request payload fields should be redacted before persistence.
    pub(crate) redact_inputs: bool,
    /// Whether response payload fields should be redacted before persistence.
    pub(crate) redact_outputs: bool,
}

/// Persists one request audit row when logging is enabled.
pub(crate) fn record_request(
    config: AuditConfig,
    operation: &str,
    settings: Option<&Settings>,
    request_payload: Value,
    response_payload: Option<Value>,
    error: Option<&Error>,
    duration: Duration,
) {
    if !config.enabled {
        return;
    }

    let role_name = operator_policy::caller_role_name();
    let status = if error.is_some() { "error" } else { "ok" };
    let request_payload = Some(JsonB(request_payload));
    let response_payload = response_payload.map(JsonB);
    let error_message = error.map(ToString::to_string);
    let duration_ms = duration_ms(duration);

    if let Err(error) = Spi::get_one_with_args::<bool>(
        "SELECT postllm._request_audit_insert(
            role_name => $1,
            operation => $2,
            runtime => $3,
            model => $4,
            base_url => $5,
            status => $6,
            duration_ms => $7,
            input_redacted => $8,
            output_redacted => $9,
            request_payload => $10,
            response_payload => $11,
            error_message => $12
        )",
        &[
            DatumWithOid::from(role_name.as_str()),
            DatumWithOid::from(operation),
            DatumWithOid::from(settings.map(|settings| settings.runtime.as_str())),
            DatumWithOid::from(settings.map(|settings| settings.model.as_str())),
            DatumWithOid::from(settings.and_then(|settings| settings.base_url.as_deref())),
            DatumWithOid::from(status),
            DatumWithOid::from(duration_ms),
            DatumWithOid::from(config.redact_inputs),
            DatumWithOid::from(config.redact_outputs),
            DatumWithOid::from(request_payload),
            DatumWithOid::from(response_payload),
            DatumWithOid::from(error_message.as_deref()),
        ],
    ) {
        pgrx::warning!("postllm request audit log write failed: {error}");
    }
}

/// Inserts one request audit row. Intended for the internal security-definer SQL shim.
#[expect(
    clippy::too_many_arguments,
    reason = "the internal SQL shim maps directly onto the persisted audit row shape"
)]
pub(crate) fn insert_request_audit_row(
    role_name: &str,
    operation: &str,
    runtime: Option<&str>,
    model: Option<&str>,
    base_url: Option<&str>,
    status: &str,
    duration_ms: i64,
    input_redacted: bool,
    output_redacted: bool,
    request_payload: Option<&Value>,
    response_payload: Option<&Value>,
    error_message: Option<&str>,
) -> Result<()> {
    let role_name = require_non_blank("role_name", role_name)?;
    let operation = require_non_blank("operation", operation)?;
    let status = match require_non_blank("status", status)? {
        "ok" | "error" => status,
        other => {
            return Err(Error::invalid_argument(
                "status",
                format!("must be 'ok' or 'error', got '{other}'"),
                "pass status => 'ok' or status => 'error'",
            ));
        }
    };

    Spi::connect_mut(|client| {
        let request_payload = request_payload.cloned().map(JsonB);
        let response_payload = response_payload.cloned().map(JsonB);

        client.update(
            "INSERT INTO postllm.request_audit_log (
                role_name,
                backend_pid,
                operation,
                runtime,
                model,
                base_url,
                status,
                duration_ms,
                input_redacted,
                output_redacted,
                request_payload,
                response_payload,
                error_message
            )
            VALUES (
                $1,
                pg_backend_pid(),
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11,
                $12
            )",
            None,
            &[
                DatumWithOid::from(role_name),
                DatumWithOid::from(operation),
                DatumWithOid::from(runtime),
                DatumWithOid::from(model),
                DatumWithOid::from(base_url),
                DatumWithOid::from(status),
                DatumWithOid::from(duration_ms),
                DatumWithOid::from(input_redacted),
                DatumWithOid::from(output_redacted),
                DatumWithOid::from(request_payload),
                DatumWithOid::from(response_payload),
                DatumWithOid::from(error_message),
            ],
        )?;

        Ok::<(), pgrx::spi::Error>(())
    })?;

    Ok(())
}

/// Redacts prompt and response fields in a JSON payload while preserving high-level structure.
#[must_use]
pub(crate) fn redact_payload_fields(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| {
                    let value = if REDACTABLE_FIELDS.contains(&key.as_str()) && !value.is_null() {
                        Value::String(REDACTED_VALUE.to_owned())
                    } else {
                        redact_payload_fields(value)
                    };

                    (key.clone(), value)
                })
                .collect::<Map<_, _>>(),
        ),
        Value::Array(values) => {
            Value::Array(values.iter().map(redact_payload_fields).collect::<Vec<_>>())
        }
        _ => value.clone(),
    }
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn require_non_blank<'value>(name: &str, value: &'value str) -> Result<&'value str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(Error::invalid_argument(
            name,
            "must not be empty or whitespace-only",
            format!("pass a non-empty value for '{name}'"),
        ))
    } else {
        Ok(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::REDACTED_VALUE;
    use super::redact_payload_fields;
    use serde_json::json;

    #[test]
    fn redact_payload_fields_should_replace_prompt_and_response_text_fields() {
        let payload = json!({
            "messages": [
                {
                    "role": "user",
                    "content": "hello",
                    "tool_calls": [
                        {
                            "function": {
                                "arguments": "{\"city\":\"Chicago\"}"
                            }
                        }
                    ]
                }
            ],
            "query": "postgres",
            "documents": ["alpha", "beta"],
            "nested": {
                "delta": "stream chunk",
                "text": "streamed text",
                "safe": true
            }
        });

        let redacted = redact_payload_fields(&payload);

        assert_eq!(redacted["messages"][0]["content"], REDACTED_VALUE);
        assert_eq!(
            redacted["messages"][0]["tool_calls"][0]["function"]["arguments"],
            REDACTED_VALUE
        );
        assert_eq!(redacted["query"], REDACTED_VALUE);
        assert_eq!(redacted["documents"], REDACTED_VALUE);
        assert_eq!(redacted["nested"]["delta"], REDACTED_VALUE);
        assert_eq!(redacted["nested"]["text"], REDACTED_VALUE);
        assert_eq!(redacted["nested"]["safe"], true);
    }
}
