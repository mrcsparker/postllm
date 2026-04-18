#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::audit;
use crate::backend::{self, Settings};
use crate::error::Result;
use crate::guc;
use serde_json::Value;
use std::time::Instant;

/// Shared request requirements for generation-oriented entrypoints.
#[derive(Debug, Clone, Copy, PartialEq)]
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
            settings = Some(resolved);

            execute(
                settings
                    .as_ref()
                    .expect("resolved embedding settings should exist"),
                prepared,
            )
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
            settings = Some(resolved);

            execute(settings.as_ref().expect("resolved settings should exist"))
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
pub(crate) fn generation_operation(feature: backend::Feature, streaming: bool) -> &'static str {
    match (feature, streaming) {
        (backend::Feature::Chat, false) => "chat",
        (backend::Feature::Complete, false) => "complete",
        (backend::Feature::Chat, true) => "chat_stream",
        (backend::Feature::Complete, true) => "complete_stream",
        _ => "chat",
    }
}

fn redact_payload(payload: Value, redact: bool) -> Value {
    if redact {
        audit::redact_payload_fields(&payload)
    } else {
        payload
    }
}
