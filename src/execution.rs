#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::audit;
use crate::backend::{self, Settings};
use crate::error::{Error, Result};
use crate::guc;
use pgrx::datum::DatumWithOid;
use pgrx::spi::Spi;
use serde_json::Value;
use std::thread;
use std::time::{Duration, Instant};

pub(crate) const REQUEST_CONCURRENCY_LOCK_NAMESPACE: i32 = i32::from_be_bytes(*b"pllm");
const REQUEST_CONCURRENCY_POLL_INTERVAL_MS: u64 = 25;

/// Shared request requirements for generation-oriented entrypoints.
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "SQL request capability checks are clearer when each optional feature stays explicit"
)]
pub(crate) struct GenerationRequirements {
    pub(crate) feature: backend::Feature,
    pub(crate) temperature: f64,
    pub(crate) max_tokens: Option<i32>,
    pub(crate) streaming: bool,
    pub(crate) structured_outputs: bool,
    pub(crate) tools: bool,
    pub(crate) multimodal_inputs: bool,
}

/// Shared lifecycle owner for request setup, policy enforcement, and audit finalization.
#[derive(Debug, Clone)]
pub(crate) struct ExecutionContext {
    operation: &'static str,
    audit_config: audit::AuditConfig,
    request_payload: Option<Value>,
    started: Instant,
}

impl ExecutionContext {
    /// Builds one lifecycle context for a SQL-facing request.
    pub(crate) fn new(operation: &'static str, request_payload: impl FnOnce() -> Value) -> Self {
        let audit_config = guc::audit_config();
        let request_payload = audit_config
            .enabled
            .then(request_payload)
            .map(|payload| redact_payload(payload, audit_config.redact_inputs));

        Self {
            operation,
            audit_config,
            request_payload,
            started: Instant::now(),
        }
    }

    /// Resolves, validates, and executes one generation or streaming request.
    pub(crate) fn run_generation<T>(
        self,
        model_override: Option<&str>,
        requirements: GenerationRequirements,
        execute: impl FnOnce(&Settings) -> Result<T>,
        response_payload: impl FnOnce(&T) -> Value,
    ) -> Result<T> {
        self.run_with_settings(
            || {
                let settings = guc::resolve(model_override)?;
                let capabilities = backend::CapabilitySnapshot::from_settings(&settings, None);
                capabilities.require(requirements.feature)?;

                if requirements.streaming {
                    capabilities.require(backend::Feature::Streaming)?;
                }

                if requirements.structured_outputs {
                    capabilities.require(backend::Feature::StructuredOutputs)?;
                }

                if requirements.tools {
                    capabilities.require(backend::Feature::Tools)?;
                }

                if requirements.multimodal_inputs {
                    capabilities.require(backend::Feature::MultimodalInputs)?;
                }

                Ok(settings)
            },
            execute,
            response_payload,
        )
    }

    /// Resolves, validates, and executes one embedding request.
    pub(crate) fn run_embedding<T, Prepared>(
        self,
        model_override: Option<&str>,
        prepare_request: impl FnOnce() -> Result<Prepared>,
        execute: impl FnOnce(&Settings, Prepared) -> Result<T>,
        response_payload: impl FnOnce(&T) -> Value,
    ) -> Result<T> {
        let mut settings = None;
        let result = (|| {
            let prepared = prepare_request()?;
            let resolved = guc::resolve_embedding_settings(model_override)?;
            let capabilities = backend::CapabilitySnapshot::from_settings(
                &resolved,
                Some(resolved.model.as_str()),
            );
            capabilities.require(backend::Feature::Embeddings)?;
            let _request_concurrency_slot = acquire_request_concurrency_slot(
                resolved.request_max_concurrency,
                resolved.timeout_ms,
                self.operation,
            )?;
            settings = Some(resolved);
            let settings_ref = settings.as_ref().ok_or_else(|| {
                Error::Internal("resolved embedding settings should exist".to_owned())
            })?;

            execute(settings_ref, prepared)
        })();

        self.finish(
            settings.as_ref(),
            &result,
            result.as_ref().ok().map(response_payload),
        );

        result
    }

    /// Resolves, validates, and executes one reranking request.
    pub(crate) fn run_rerank<T>(
        self,
        model_override: Option<&str>,
        execute: impl FnOnce(&Settings) -> Result<T>,
        response_payload: impl FnOnce(&T) -> Value,
    ) -> Result<T> {
        self.run_with_settings(
            || {
                let settings = guc::resolve_rerank(model_override)?;
                let embedding_model = matches!(settings.runtime, backend::Runtime::Candle)
                    .then_some(settings.model.as_str());
                let capabilities =
                    backend::CapabilitySnapshot::from_settings(&settings, embedding_model);
                capabilities.require(backend::Feature::Reranking)?;
                Ok(settings)
            },
            execute,
            response_payload,
        )
    }

    fn run_with_settings<T>(
        self,
        resolve_settings: impl FnOnce() -> Result<Settings>,
        execute: impl FnOnce(&Settings) -> Result<T>,
        response_payload: impl FnOnce(&T) -> Value,
    ) -> Result<T> {
        let mut settings = None;
        let result = (|| {
            let resolved = resolve_settings()?;
            let _request_concurrency_slot = acquire_request_concurrency_slot(
                resolved.request_max_concurrency,
                resolved.timeout_ms,
                self.operation,
            )?;
            settings = Some(resolved);
            let settings_ref = settings
                .as_ref()
                .ok_or_else(|| Error::Internal("resolved settings should exist".to_owned()))?;

            execute(settings_ref)
        })();

        self.finish(
            settings.as_ref(),
            &result,
            result.as_ref().ok().map(response_payload),
        );

        result
    }

    fn finish<T>(
        &self,
        settings: Option<&Settings>,
        result: &Result<T>,
        response_payload: Option<Value>,
    ) {
        let Some(request_payload) = self.request_payload.clone() else {
            return;
        };

        let response_payload = response_payload
            .map(|payload| redact_payload(payload, self.audit_config.redact_outputs));

        audit::record_request(
            self.audit_config,
            self.operation,
            settings,
            request_payload,
            response_payload,
            result.as_ref().err(),
            self.started.elapsed(),
        );
    }
}

/// Returns the audit operation name for generation requests.
#[must_use]
pub(crate) const fn generation_operation(
    feature: backend::Feature,
    streaming: bool,
) -> &'static str {
    if streaming {
        match feature {
            backend::Feature::Complete => "complete_stream",
            backend::Feature::Chat
            | backend::Feature::Embeddings
            | backend::Feature::Reranking
            | backend::Feature::Tools
            | backend::Feature::StructuredOutputs
            | backend::Feature::Streaming
            | backend::Feature::MultimodalInputs => "chat_stream",
        }
    } else {
        match feature {
            backend::Feature::Complete => "complete",
            backend::Feature::Chat
            | backend::Feature::Embeddings
            | backend::Feature::Reranking
            | backend::Feature::Tools
            | backend::Feature::StructuredOutputs
            | backend::Feature::Streaming
            | backend::Feature::MultimodalInputs => "chat",
        }
    }
}

fn redact_payload(payload: Value, redact: bool) -> Value {
    if redact {
        audit::redact_payload_fields(&payload)
    } else {
        payload
    }
}

struct RequestConcurrencySlotGuard {
    slot: i32,
}

impl Drop for RequestConcurrencySlotGuard {
    fn drop(&mut self) {
        release_request_concurrency_slot(self.slot);
    }
}

fn acquire_request_concurrency_slot(
    max_concurrency: u32,
    timeout_ms: u64,
    operation: &str,
) -> Result<Option<RequestConcurrencySlotGuard>> {
    if max_concurrency == 0 {
        return Ok(None);
    }

    let started_at = Instant::now();
    loop {
        crate::interrupt::checkpoint();
        if started_at.elapsed() > Duration::from_millis(timeout_ms) {
            return Err(Error::Backpressure(format!(
                "request {operation} could not start because postllm.request_max_concurrency={max_concurrency} stayed saturated until postllm.timeout_ms={timeout_ms}ms elapsed"
            )));
        }

        for slot in 0..max_concurrency {
            let slot = i32::try_from(slot).map_err(|_| {
                Error::invalid_setting(
                    "postllm.request_max_concurrency",
                    "must fit into PostgreSQL advisory lock slot identifiers",
                    "SET postllm.request_max_concurrency = 512 or another smaller non-negative integer",
                )
            })?;

            if try_acquire_request_concurrency_slot(slot)? {
                return Ok(Some(RequestConcurrencySlotGuard { slot }));
            }
        }

        thread::sleep(Duration::from_millis(REQUEST_CONCURRENCY_POLL_INTERVAL_MS));
    }
}

fn try_acquire_request_concurrency_slot(slot: i32) -> Result<bool> {
    Spi::get_one_with_args::<bool>(
        "SELECT pg_try_advisory_lock($1, $2)",
        &[
            DatumWithOid::from(REQUEST_CONCURRENCY_LOCK_NAMESPACE),
            DatumWithOid::from(slot),
        ],
    )?
    .ok_or_else(|| Error::Internal("pg_try_advisory_lock did not return a result row".to_owned()))
}

fn release_request_concurrency_slot(slot: i32) {
    let _ = Spi::get_one_with_args::<bool>(
        "SELECT pg_advisory_unlock($1, $2)",
        &[
            DatumWithOid::from(REQUEST_CONCURRENCY_LOCK_NAMESPACE),
            DatumWithOid::from(slot),
        ],
    );
}
