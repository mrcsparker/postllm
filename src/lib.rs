#![doc = include_str!("../README.md")]

pub(crate) mod api;
pub(crate) mod audit;
pub(crate) mod backend;
pub(crate) mod candle;
pub(crate) mod catalog;
pub(crate) mod client;
pub(crate) mod conversations;
pub(crate) mod enum_parser;
pub(crate) mod error;
pub(crate) mod evals;
pub(crate) mod execution;
pub(crate) mod guc;
pub(crate) mod http_policy;
pub(crate) mod interrupt;
pub(crate) mod jobs;
pub(crate) mod operator_policy;
pub(crate) mod permissions;
pub(crate) mod prompts;
pub(crate) mod secrets;

use crate::error::{Error, Result};
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::iter::TableIterator;
use pgrx::spi::Spi;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::Path;

::pgrx::pg_module_magic!(name, version);

/// Registers `PostgreSQL` configuration settings during extension load.
#[pgrx::pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register();
}

pgrx::extension_sql!(
    r"
    COMMENT ON SCHEMA postllm IS
        'PostgreSQL-native LLM orchestration primitives for prompts, chats, and session configuration.';

    CREATE TABLE postllm.config_profiles (
        name text PRIMARY KEY,
        description text,
        config jsonb NOT NULL DEFAULT '{}'::jsonb,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        CHECK (jsonb_typeof(config) = 'object')
    );

    COMMENT ON TABLE postllm.config_profiles IS
        'Named postllm session profiles for switching between local, staging, and hosted setups, including optional provider-secret references.';
    COMMENT ON COLUMN postllm.config_profiles.config IS
        'Validated postllm session overrides stored as jsonb.';

    CREATE TABLE postllm.provider_secrets (
        name text PRIMARY KEY,
        description text,
        algorithm text NOT NULL,
        nonce text NOT NULL,
        ciphertext text NOT NULL,
        key_fingerprint text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    );

    COMMENT ON TABLE postllm.provider_secrets IS
        'Encrypted provider credentials referenced by postllm session settings and profiles.';
    COMMENT ON COLUMN postllm.provider_secrets.key_fingerprint IS
        'Fingerprint of the POSTLLM_SECRET_KEY material used to encrypt the secret.';

    CREATE TABLE postllm.model_aliases (
        alias text NOT NULL,
        lane text NOT NULL,
        model text NOT NULL,
        description text,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (alias, lane),
        CHECK (lane IN ('generation', 'embedding'))
    );

    COMMENT ON TABLE postllm.model_aliases IS
        'Lane-aware generation and embedding model aliases used by postllm resolution paths.';

    CREATE TABLE postllm.role_permissions (
        role_name text NOT NULL,
        object_type text NOT NULL,
        target text NOT NULL,
        description text,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (role_name, object_type, target),
        CHECK (object_type IN ('runtime', 'generation_model', 'embedding_model', 'setting'))
    );

    COMMENT ON TABLE postllm.role_permissions IS
        'Role-aware postllm allowlist rules for runtimes, models, and privileged settings.';

    CREATE TABLE postllm.request_audit_log (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        logged_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        role_name text NOT NULL,
        backend_pid integer NOT NULL,
        operation text NOT NULL,
        runtime text,
        model text,
        base_url text,
        status text NOT NULL,
        duration_ms bigint NOT NULL,
        input_redacted boolean NOT NULL DEFAULT true,
        output_redacted boolean NOT NULL DEFAULT true,
        request_payload jsonb,
        response_payload jsonb,
        error_message text,
        CHECK (status IN ('ok', 'error'))
    );

    COMMENT ON TABLE postllm.request_audit_log IS
        'Opt-in audit trail for postllm request execution, including optional redaction of request and response payload fields.';

    CREATE TABLE postllm.async_jobs (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        submitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        started_at timestamptz,
        finished_at timestamptz,
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        submitted_by text NOT NULL,
        kind text NOT NULL,
        status text NOT NULL DEFAULT 'queued',
        worker_pid integer,
        request_payload jsonb NOT NULL,
        result_payload jsonb,
        settings_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
        error_message text,
        CHECK (kind IN ('chat', 'complete', 'embed', 'rerank')),
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
        CHECK (jsonb_typeof(request_payload) = 'object'),
        CHECK (jsonb_typeof(settings_snapshot) = 'object')
    );

    CREATE INDEX postllm_async_jobs_status_submitted_at_idx
        ON postllm.async_jobs (status, submitted_at DESC);
    CREATE INDEX postllm_async_jobs_submitted_by_submitted_at_idx
        ON postllm.async_jobs (submitted_by, submitted_at DESC);

    COMMENT ON TABLE postllm.async_jobs IS
        'Durable async postllm jobs submitted for background execution, polling, result fetch, and cancellation.';
    COMMENT ON COLUMN postllm.async_jobs.settings_snapshot IS
        'Stored postllm session settings replayed into the async worker before the job runs.';

    CREATE TABLE postllm.conversations (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        created_by text NOT NULL,
        title text,
        metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
        message_count integer NOT NULL DEFAULT 0,
        last_message_at timestamptz,
        CHECK (jsonb_typeof(metadata) = 'object')
    );

    CREATE TABLE postllm.conversation_messages (
        conversation_id bigint NOT NULL REFERENCES postllm.conversations(id) ON DELETE CASCADE,
        message_no integer NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        role text NOT NULL,
        message jsonb NOT NULL,
        metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
        PRIMARY KEY (conversation_id, message_no),
        CHECK (jsonb_typeof(message) = 'object'),
        CHECK (jsonb_typeof(metadata) = 'object')
    );

    CREATE INDEX postllm_conversations_created_by_updated_at_idx
        ON postllm.conversations (created_by, updated_at DESC);
    CREATE INDEX postllm_conversation_messages_conversation_id_message_no_idx
        ON postllm.conversation_messages (conversation_id, message_no);

    COMMENT ON TABLE postllm.conversations IS
        'Durable multi-turn conversations owned by the submitting role.';
    COMMENT ON TABLE postllm.conversation_messages IS
        'Stored normalized chat messages that belong to a durable postllm conversation.';

    CREATE TABLE postllm.prompt_registries (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        created_by text NOT NULL,
        name text NOT NULL,
        title text,
        active_version integer NOT NULL DEFAULT 1,
        UNIQUE (created_by, name)
    );

    CREATE TABLE postllm.prompt_versions (
        prompt_id bigint NOT NULL REFERENCES postllm.prompt_registries(id) ON DELETE CASCADE,
        version integer NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        role text,
        template text NOT NULL,
        description text,
        metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
        PRIMARY KEY (prompt_id, version),
        CHECK (jsonb_typeof(metadata) = 'object')
    );

    CREATE INDEX postllm_prompt_registries_created_by_updated_at_idx
        ON postllm.prompt_registries (created_by, updated_at DESC);
    CREATE INDEX postllm_prompt_versions_prompt_id_version_idx
        ON postllm.prompt_versions (prompt_id, version DESC);

    COMMENT ON TABLE postllm.prompt_registries IS
        'Durable prompt registries owned by the creating role, with one active version pointer.';
    COMMENT ON TABLE postllm.prompt_versions IS
        'Append-only prompt template versions and metadata for one durable prompt registry.';

    CREATE TABLE postllm.eval_datasets (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        created_by text NOT NULL,
        name text NOT NULL,
        description text,
        metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
        UNIQUE (created_by, name),
        CHECK (jsonb_typeof(metadata) = 'object')
    );

    CREATE TABLE postllm.eval_cases (
        dataset_id bigint NOT NULL REFERENCES postllm.eval_datasets(id) ON DELETE CASCADE,
        case_name text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        input_payload jsonb NOT NULL,
        expected_payload jsonb NOT NULL,
        scorer text NOT NULL DEFAULT 'exact_text',
        threshold double precision NOT NULL DEFAULT 1.0,
        metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
        PRIMARY KEY (dataset_id, case_name),
        CHECK (scorer IN ('exact_text', 'contains_text', 'exact_json', 'json_subset')),
        CHECK (jsonb_typeof(metadata) = 'object')
    );

    CREATE INDEX postllm_eval_datasets_created_by_updated_at_idx
        ON postllm.eval_datasets (created_by, updated_at DESC);
    CREATE INDEX postllm_eval_cases_dataset_id_updated_at_idx
        ON postllm.eval_cases (dataset_id, updated_at DESC);

    COMMENT ON TABLE postllm.eval_datasets IS
        'Durable evaluation datasets owned by the current role for prompt and model regression fixtures.';
    COMMENT ON TABLE postllm.eval_cases IS
        'Stored evaluation cases containing inputs, expectations, scorer selection, and metadata.';

    CREATE VIEW postllm.request_metrics AS
    SELECT
        id,
        logged_at,
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
        CASE
            WHEN jsonb_typeof(
                COALESCE(
                    response_payload #> '{_postllm,usage,prompt_tokens}',
                    response_payload #> '{usage,prompt_tokens}'
                )
            ) = 'number'
            THEN (
                COALESCE(
                    response_payload #>> '{_postllm,usage,prompt_tokens}',
                    response_payload #>> '{usage,prompt_tokens}'
                )
            )::bigint
        END AS prompt_tokens,
        CASE
            WHEN jsonb_typeof(
                COALESCE(
                    response_payload #> '{_postllm,usage,completion_tokens}',
                    response_payload #> '{usage,completion_tokens}'
                )
            ) = 'number'
            THEN (
                COALESCE(
                    response_payload #>> '{_postllm,usage,completion_tokens}',
                    response_payload #>> '{usage,completion_tokens}'
                )
            )::bigint
        END AS completion_tokens,
        CASE
            WHEN jsonb_typeof(
                COALESCE(
                    response_payload #> '{_postllm,usage,total_tokens}',
                    response_payload #> '{usage,total_tokens}'
                )
            ) = 'number'
            THEN (
                COALESCE(
                    response_payload #>> '{_postllm,usage,total_tokens}',
                    response_payload #>> '{usage,total_tokens}'
                )
            )::bigint
        END AS total_tokens,
        error_message
    FROM postllm.request_audit_log;

    COMMENT ON VIEW postllm.request_metrics IS
        'Normalized per-request metrics derived from postllm.request_audit_log, including latency, status, and extracted token usage.';

    CREATE VIEW postllm.request_count_metrics AS
    SELECT
        role_name,
        operation,
        runtime,
        model,
        base_url,
        count(*) AS request_count,
        count(*) FILTER (WHERE status = 'ok') AS ok_count,
        count(*) FILTER (WHERE status = 'error') AS error_count,
        min(logged_at) AS first_logged_at,
        max(logged_at) AS last_logged_at
    FROM postllm.request_metrics
    GROUP BY role_name, operation, runtime, model, base_url;

    COMMENT ON VIEW postllm.request_count_metrics IS
        'All-time request counts grouped by role, operation, runtime, model, and base URL.';

    CREATE VIEW postllm.request_error_metrics AS
    SELECT
        role_name,
        operation,
        runtime,
        model,
        base_url,
        count(*) AS request_count,
        count(*) FILTER (WHERE status = 'error') AS error_count,
        (count(*) FILTER (WHERE status = 'error'))::double precision
            / count(*)::double precision AS error_rate,
        max(logged_at) FILTER (WHERE status = 'error') AS last_error_at,
        (
            array_agg(error_message ORDER BY logged_at DESC)
            FILTER (WHERE status = 'error' AND error_message IS NOT NULL)
        )[1] AS last_error_message
    FROM postllm.request_metrics
    GROUP BY role_name, operation, runtime, model, base_url;

    COMMENT ON VIEW postllm.request_error_metrics IS
        'All-time error counts and most recent error details grouped by role, operation, runtime, model, and base URL.';

    CREATE VIEW postllm.request_latency_metrics AS
    SELECT
        role_name,
        operation,
        runtime,
        model,
        base_url,
        count(*) AS request_count,
        round(avg(duration_ms)::numeric, 2) AS avg_duration_ms,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms) AS p50_duration_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_duration_ms,
        max(duration_ms) AS max_duration_ms
    FROM postllm.request_metrics
    GROUP BY role_name, operation, runtime, model, base_url;

    COMMENT ON VIEW postllm.request_latency_metrics IS
        'All-time request latency rollups grouped by role, operation, runtime, model, and base URL.';

    CREATE VIEW postllm.request_token_usage_metrics AS
    SELECT
        role_name,
        operation,
        runtime,
        model,
        base_url,
        count(*) AS request_count,
        count(*) FILTER (WHERE total_tokens IS NOT NULL) AS requests_with_usage,
        COALESCE(sum(prompt_tokens), 0) AS prompt_tokens,
        COALESCE(sum(completion_tokens), 0) AS completion_tokens,
        COALESCE(sum(total_tokens), 0) AS total_tokens,
        round(avg(total_tokens)::numeric, 2) AS avg_total_tokens,
        max(total_tokens) AS max_total_tokens
    FROM postllm.request_metrics
    GROUP BY role_name, operation, runtime, model, base_url;

    COMMENT ON VIEW postllm.request_token_usage_metrics IS
        'All-time token-usage rollups grouped by role, operation, runtime, model, and base URL.';
    ",
    name = "postllm_catalog_tables",
    requires = [postllm::settings],
    finalize
);

#[pgrx::pg_schema]
#[allow(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL-facing values as owned Rust types for exported SQL functions"
)]
mod postllm {
    use crate::api::{config, evals, inference, jobs, messages, ops, retrieval};
    use pgrx::iter::TableIterator;
    use pgrx::{
        Aggregate, AggregateName, JsonB, ParallelOption, default, pg_aggregate, pg_extern,
        search_path,
    };

    /// Returns the current backend-visible `postllm` settings.
    #[pg_extern]
    fn settings() -> JsonB {
        config::settings()
    }

    /// Returns the active runtime and feature capability snapshot.
    #[pg_extern]
    fn capabilities() -> JsonB {
        config::capabilities()
    }

    /// Reports the active runtime discovery and readiness snapshot without raising probe failures.
    #[pg_extern]
    fn runtime_discover() -> JsonB {
        config::runtime_discover()
    }

    /// Returns whether the active runtime currently appears ready to serve requests.
    #[pg_extern]
    fn runtime_ready() -> bool {
        config::runtime_ready()
    }

    /// Submits one durable async job and returns the queued job row.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn job_submit(kind: &str, request: JsonB) -> JsonB {
        jobs::submit(kind, request)
    }

    /// Returns the latest persisted state for one async job.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn job_poll(job_id: i64) -> JsonB {
        jobs::poll(job_id)
    }

    /// Returns the completed result payload for one succeeded async job.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn job_result(job_id: i64) -> JsonB {
        jobs::result(job_id)
    }

    /// Cancels one queued or running async job and returns the updated job row.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn job_cancel(job_id: i64) -> JsonB {
        jobs::cancel(job_id)
    }

    #[doc(hidden)]
    #[pg_extern(name = "_request_audit_insert", security_definer)]
    #[search_path(postllm, pg_catalog)]
    #[allow(
        clippy::too_many_arguments,
        reason = "the SQL shim mirrors the persisted audit row shape one-for-one"
    )]
    fn request_audit_insert(
        role_name: &str,
        operation: &str,
        status: &str,
        duration_ms: i64,
        input_redacted: bool,
        output_redacted: bool,
        runtime: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        base_url: default!(Option<&str>, "NULL"),
        request_payload: default!(Option<JsonB>, "NULL"),
        response_payload: default!(Option<JsonB>, "NULL"),
        error_message: default!(Option<&str>, "NULL"),
    ) -> bool {
        match crate::audit::insert_request_audit_row(
            role_name,
            operation,
            runtime,
            model,
            base_url,
            status,
            duration_ms,
            input_redacted,
            output_redacted,
            request_payload.as_ref().map(|payload| &payload.0),
            response_payload.as_ref().map(|payload| &payload.0),
            error_message,
        ) {
            Ok(()) => true,
            Err(error) => pgrx::error!("{error}"),
        }
    }

    #[doc(hidden)]
    #[pg_extern(name = "_async_job_claim", security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn async_job_claim(job_id: i64) -> Option<JsonB> {
        match crate::jobs::worker_claim(job_id) {
            Ok(row) => row.map(JsonB),
            Err(error) => pgrx::error!("{error}"),
        }
    }

    #[doc(hidden)]
    #[pg_extern(name = "_async_job_finish", security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn async_job_finish(
        job_id: i64,
        status: &str,
        result_payload: default!(Option<JsonB>, "NULL"),
        error_message: default!(Option<&str>, "NULL"),
    ) -> bool {
        match crate::jobs::worker_finish(
            job_id,
            status,
            result_payload.as_ref().map(|payload| &payload.0),
            error_message,
        ) {
            Ok(updated) => updated,
            Err(error) => pgrx::error!("{error}"),
        }
    }

    #[doc(hidden)]
    #[pg_extern(name = "_async_job_claim_wait", security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn async_job_claim_wait(job_id: i64) -> bool {
        match crate::jobs::claim_spawned_job(job_id) {
            Ok(updated) => updated,
            Err(error) => pgrx::error!("{error}"),
        }
    }

    #[doc(hidden)]
    #[pg_extern(name = "_async_job_mark_failed", security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn async_job_mark_failed(job_id: i64, error_message: &str) -> bool {
        match crate::jobs::worker_mark_failed(job_id, error_message) {
            Ok(updated) => updated,
            Err(error) => pgrx::error!("{error}"),
        }
    }

    #[doc(hidden)]
    #[pg_extern(name = "_async_job_run", security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn async_job_run(job_id: i64) -> bool {
        match crate::jobs::run_spawned_job(job_id) {
            Ok(updated) => updated,
            Err(error) => {
                let error_message = error.to_string();
                let _ = crate::jobs::worker_finish(job_id, "failed", None, Some(&error_message));
                false
            }
        }
    }

    /// Applies session-level configuration overrides and returns the resulting settings snapshot.
    #[expect(
        clippy::too_many_arguments,
        reason = "the SQL surface intentionally exposes a flat configure(...) API instead of forcing callers through JSON"
    )]
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn configure(
        base_url: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        embedding_model: default!(Option<&str>, "NULL"),
        api_key: default!(Option<&str>, "NULL"),
        api_key_secret: default!(Option<&str>, "NULL"),
        timeout_ms: default!(Option<i32>, "NULL"),
        max_retries: default!(Option<i32>, "NULL"),
        retry_backoff_ms: default!(Option<i32>, "NULL"),
        request_max_concurrency: default!(Option<i32>, "NULL"),
        request_token_budget: default!(Option<i32>, "NULL"),
        request_runtime_budget_ms: default!(Option<i32>, "NULL"),
        request_spend_budget_microusd: default!(Option<i32>, "NULL"),
        output_token_price_microusd_per_1k: default!(Option<i32>, "NULL"),
        runtime: default!(Option<&str>, "NULL"),
        candle_cache_dir: default!(Option<&str>, "NULL"),
        candle_offline: default!(Option<bool>, "NULL"),
        candle_device: default!(Option<&str>, "NULL"),
        candle_max_input_tokens: default!(Option<i32>, "NULL"),
        candle_max_concurrency: default!(Option<i32>, "NULL"),
    ) -> JsonB {
        config::configure(
            base_url,
            model,
            embedding_model,
            api_key,
            api_key_secret,
            timeout_ms,
            max_retries,
            retry_backoff_ms,
            request_max_concurrency,
            request_token_budget,
            request_runtime_budget_ms,
            request_spend_budget_microusd,
            output_token_price_microusd_per_1k,
            runtime,
            candle_cache_dir,
            candle_offline,
            candle_device,
            candle_max_input_tokens,
            candle_max_concurrency,
        )
    }

    /// Returns all named configuration profiles.
    #[pg_extern]
    fn profiles() -> JsonB {
        config::profiles()
    }

    /// Returns one named configuration profile.
    #[pg_extern]
    fn profile(name: &str) -> JsonB {
        config::profile(name)
    }

    /// Stores or updates a named configuration profile.
    #[expect(
        clippy::too_many_arguments,
        reason = "the SQL surface intentionally exposes a flat profile_set(...) API aligned with configure(...)"
    )]
    #[pg_extern]
    fn profile_set(
        name: &str,
        description: default!(Option<&str>, "NULL"),
        base_url: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        embedding_model: default!(Option<&str>, "NULL"),
        api_key_secret: default!(Option<&str>, "NULL"),
        timeout_ms: default!(Option<i32>, "NULL"),
        max_retries: default!(Option<i32>, "NULL"),
        retry_backoff_ms: default!(Option<i32>, "NULL"),
        request_max_concurrency: default!(Option<i32>, "NULL"),
        request_token_budget: default!(Option<i32>, "NULL"),
        request_runtime_budget_ms: default!(Option<i32>, "NULL"),
        request_spend_budget_microusd: default!(Option<i32>, "NULL"),
        output_token_price_microusd_per_1k: default!(Option<i32>, "NULL"),
        runtime: default!(Option<&str>, "NULL"),
        candle_cache_dir: default!(Option<&str>, "NULL"),
        candle_offline: default!(Option<bool>, "NULL"),
        candle_device: default!(Option<&str>, "NULL"),
        candle_max_input_tokens: default!(Option<i32>, "NULL"),
        candle_max_concurrency: default!(Option<i32>, "NULL"),
    ) -> JsonB {
        config::profile_set(
            name,
            description,
            base_url,
            model,
            embedding_model,
            api_key_secret,
            timeout_ms,
            max_retries,
            retry_backoff_ms,
            request_max_concurrency,
            request_token_budget,
            request_runtime_budget_ms,
            request_spend_budget_microusd,
            output_token_price_microusd_per_1k,
            runtime,
            candle_cache_dir,
            candle_offline,
            candle_device,
            candle_max_input_tokens,
            candle_max_concurrency,
        )
    }

    /// Applies a named configuration profile to the current session.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn profile_apply(name: &str) -> JsonB {
        config::profile_apply(name)
    }

    /// Deletes a named configuration profile.
    #[pg_extern]
    fn profile_delete(name: &str) -> JsonB {
        config::profile_delete(name)
    }

    /// Returns all stored provider secret metadata.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn secrets() -> JsonB {
        config::secrets()
    }

    /// Returns one stored provider secret metadata record.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn secret(name: &str) -> JsonB {
        config::secret(name)
    }

    /// Stores or updates a named encrypted provider secret.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn secret_set(name: &str, value: &str, description: default!(Option<&str>, "NULL")) -> JsonB {
        config::secret_set(name, value, description)
    }

    /// Deletes a stored provider secret.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn secret_delete(name: &str) -> JsonB {
        config::secret_delete(name)
    }

    /// Returns all configured postllm role permissions.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn permissions() -> JsonB {
        config::permissions()
    }

    /// Returns one configured postllm role permission.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn permission(role_name: &str, object_type: &str, target: &str) -> JsonB {
        config::permission(role_name, object_type, target)
    }

    /// Stores or updates a postllm role permission.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn permission_set(
        role_name: &str,
        object_type: &str,
        target: &str,
        description: default!(Option<&str>, "NULL"),
    ) -> JsonB {
        config::permission_set(role_name, object_type, target, description)
    }

    /// Deletes a postllm role permission.
    #[pg_extern(security_definer)]
    #[search_path(postllm, pg_catalog)]
    fn permission_delete(role_name: &str, object_type: &str, target: &str) -> JsonB {
        config::permission_delete(role_name, object_type, target)
    }

    /// Returns all configured model aliases.
    #[pg_extern]
    fn model_aliases() -> JsonB {
        config::model_aliases()
    }

    /// Returns one configured model alias.
    #[pg_extern]
    fn model_alias(alias: &str, lane: &str) -> JsonB {
        config::model_alias(alias, lane)
    }

    /// Stores or updates a lane-aware model alias.
    #[pg_extern]
    fn model_alias_set(
        alias: &str,
        lane: &str,
        model: &str,
        description: default!(Option<&str>, "NULL"),
    ) -> JsonB {
        config::model_alias_set(alias, lane, model, description)
    }

    /// Deletes a lane-aware model alias.
    #[pg_extern]
    fn model_alias_delete(alias: &str, lane: &str) -> JsonB {
        config::model_alias_delete(alias, lane)
    }

    /// Builds a chat message object that can be used with [`chat`].
    #[pg_extern]
    fn message(role: &str, content: &str) -> JsonB {
        messages::message(role, content)
    }

    /// Builds a `system` message object for [`chat`].
    #[pg_extern]
    fn system(content: &str) -> JsonB {
        messages::system(content)
    }

    /// Builds a `user` message object for [`chat`].
    #[pg_extern]
    fn user(content: &str) -> JsonB {
        messages::user(content)
    }

    /// Builds an `assistant` message object for [`chat`].
    #[pg_extern]
    fn assistant(content: &str) -> JsonB {
        messages::assistant(content)
    }

    /// Renders a prompt template by substituting `{{name}}` placeholders from a JSON object.
    #[pg_extern]
    fn render_template(template: &str, variables: default!(Option<JsonB>, "NULL")) -> String {
        messages::render_template(template, variables)
    }

    /// Builds a chat message by first rendering a prompt template.
    #[pg_extern]
    fn message_template(
        role: &str,
        template: &str,
        variables: default!(Option<JsonB>, "NULL"),
    ) -> JsonB {
        messages::message_template(role, template, variables)
    }

    /// Builds a `system` message by first rendering a prompt template.
    #[pg_extern]
    fn system_template(template: &str, variables: default!(Option<JsonB>, "NULL")) -> JsonB {
        messages::system_template(template, variables)
    }

    /// Builds a `user` message by first rendering a prompt template.
    #[pg_extern]
    fn user_template(template: &str, variables: default!(Option<JsonB>, "NULL")) -> JsonB {
        messages::user_template(template, variables)
    }

    /// Builds an `assistant` message by first rendering a prompt template.
    #[pg_extern]
    fn assistant_template(template: &str, variables: default!(Option<JsonB>, "NULL")) -> JsonB {
        messages::assistant_template(template, variables)
    }

    /// Builds a text content part for multimodal chat messages.
    #[pg_extern]
    fn text_part(text: &str) -> JsonB {
        messages::text_part(text)
    }

    /// Builds an image-url content part for multimodal chat messages.
    #[pg_extern]
    fn image_url_part(url: &str, detail: default!(Option<&str>, "NULL")) -> JsonB {
        messages::image_url_part(url, detail)
    }

    /// Builds a chat message whose content is an array of content parts.
    #[pg_extern]
    fn message_parts(role: &str, parts: Vec<JsonB>) -> JsonB {
        messages::message_parts(role, parts)
    }

    /// Builds a `system` message whose content is an array of content parts.
    #[pg_extern]
    fn system_parts(parts: Vec<JsonB>) -> JsonB {
        messages::system_parts(parts)
    }

    /// Builds a `user` message whose content is an array of content parts.
    #[pg_extern]
    fn user_parts(parts: Vec<JsonB>) -> JsonB {
        messages::user_parts(parts)
    }

    /// Builds an `assistant` message whose content is an array of content parts.
    #[pg_extern]
    fn assistant_parts(parts: Vec<JsonB>) -> JsonB {
        messages::assistant_parts(parts)
    }

    /// Builds a function-style tool call object.
    #[pg_extern]
    fn tool_call(id: &str, name: &str, arguments: JsonB) -> JsonB {
        messages::tool_call(id, name, arguments)
    }

    /// Builds an assistant message that carries tool calls.
    #[pg_extern]
    fn assistant_tool_calls(
        tool_calls: Vec<JsonB>,
        content: default!(Option<&str>, "NULL"),
    ) -> JsonB {
        messages::assistant_tool_calls(tool_calls, content)
    }

    /// Builds a tool-result message linked to a prior tool call.
    #[pg_extern]
    fn tool_result(tool_call_id: &str, content: &str) -> JsonB {
        messages::tool_result(tool_call_id, content)
    }

    /// Builds an OpenAI-compatible function-tool definition for future tool-calling requests.
    #[pg_extern]
    fn function_tool(
        name: &str,
        parameters: JsonB,
        description: default!(Option<&str>, "NULL"),
    ) -> JsonB {
        messages::function_tool(name, parameters, description)
    }

    /// Builds a `tool_choice` payload requesting automatic tool selection.
    #[pg_extern]
    fn tool_choice_auto() -> JsonB {
        messages::tool_choice_auto()
    }

    /// Builds a `tool_choice` payload disabling tool selection.
    #[pg_extern]
    fn tool_choice_none() -> JsonB {
        messages::tool_choice_none()
    }

    /// Builds a `tool_choice` payload requiring the model to call a tool.
    #[pg_extern]
    fn tool_choice_required() -> JsonB {
        messages::tool_choice_required()
    }

    /// Builds a `tool_choice` payload forcing one named function tool.
    #[pg_extern]
    fn tool_choice_function(name: &str) -> JsonB {
        messages::tool_choice_function(name)
    }

    /// Builds a JSON-schema response-format contract for structured generation.
    #[pg_extern]
    fn json_schema(name: &str, schema: JsonB, strict: default!(bool, true)) -> JsonB {
        messages::json_schema(name, schema, strict)
    }

    /// Lists durable conversations created by the current role.
    #[pg_extern]
    fn conversations() -> JsonB {
        messages::conversations()
    }

    /// Returns one durable conversation plus its stored message rows.
    #[pg_extern]
    fn conversation(conversation_id: i64) -> JsonB {
        messages::conversation(conversation_id)
    }

    /// Creates one durable conversation for multi-turn workflows.
    #[pg_extern]
    fn conversation_create(
        title: default!(Option<&str>, "NULL"),
        metadata: default!(Option<JsonB>, "NULL"),
    ) -> JsonB {
        messages::conversation_create(title, metadata)
    }

    /// Appends one normalized message to a durable conversation.
    #[pg_extern]
    fn conversation_append(
        conversation_id: i64,
        message: JsonB,
        metadata: default!(Option<JsonB>, "NULL"),
    ) -> JsonB {
        messages::conversation_append(conversation_id, message, metadata)
    }

    /// Returns the stored conversation transcript as `jsonb[]` ready for chat-style APIs.
    #[pg_extern]
    fn conversation_history(conversation_id: i64) -> Vec<JsonB> {
        messages::conversation_history(conversation_id)
    }

    /// Optionally appends one message, runs chat against the stored transcript, and saves the assistant reply.
    #[pg_extern]
    fn conversation_reply(
        conversation_id: i64,
        message: default!(Option<JsonB>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> JsonB {
        messages::conversation_reply(conversation_id, message, model, temperature, max_tokens)
    }

    /// Lists durable prompt registries created by the current role.
    #[pg_extern]
    fn prompts() -> JsonB {
        messages::prompts()
    }

    /// Returns one durable prompt registry, optionally pinned to a specific version.
    #[pg_extern]
    fn prompt(name: &str, version: default!(Option<i32>, "NULL")) -> JsonB {
        messages::prompt(name, version)
    }

    /// Appends one new prompt version and marks it active for the current role.
    #[pg_extern]
    fn prompt_set(
        name: &str,
        template: &str,
        role: default!(Option<&str>, "NULL"),
        title: default!(Option<&str>, "NULL"),
        description: default!(Option<&str>, "NULL"),
        metadata: default!(Option<JsonB>, "NULL"),
    ) -> JsonB {
        messages::prompt_set(name, template, role, title, description, metadata)
    }

    /// Renders one stored prompt version as text with named variables.
    #[pg_extern]
    fn prompt_render(
        name: &str,
        variables: default!(Option<JsonB>, "NULL"),
        version: default!(Option<i32>, "NULL"),
    ) -> String {
        messages::prompt_render(name, variables, version)
    }

    /// Renders one stored prompt version as a chat message when it declares a role.
    #[pg_extern]
    fn prompt_message(
        name: &str,
        variables: default!(Option<JsonB>, "NULL"),
        version: default!(Option<i32>, "NULL"),
    ) -> JsonB {
        messages::prompt_message(name, variables, version)
    }

    /// Deletes one durable prompt registry and all of its stored versions for the current role.
    #[pg_extern]
    fn prompt_delete(name: &str) -> JsonB {
        messages::prompt_delete(name)
    }

    /// Lists durable evaluation datasets created by the current role.
    #[pg_extern]
    fn eval_datasets() -> JsonB {
        evals::eval_datasets()
    }

    /// Returns one durable evaluation dataset plus its stored cases.
    #[pg_extern]
    fn eval_dataset(name: &str) -> JsonB {
        evals::eval_dataset(name)
    }

    /// Creates or updates one durable evaluation dataset for the current role.
    #[pg_extern]
    fn eval_dataset_set(
        name: &str,
        description: default!(Option<&str>, "NULL"),
        metadata: default!(Option<JsonB>, "NULL"),
    ) -> JsonB {
        evals::eval_dataset_set(name, description, metadata)
    }

    /// Deletes one durable evaluation dataset and all of its stored cases.
    #[pg_extern]
    fn eval_dataset_delete(name: &str) -> JsonB {
        evals::eval_dataset_delete(name)
    }

    /// Returns one stored evaluation case from a durable dataset.
    #[pg_extern]
    fn eval_case(dataset_name: &str, case_name: &str) -> JsonB {
        evals::eval_case(dataset_name, case_name)
    }

    /// Creates or updates one stored evaluation case in a durable dataset.
    #[pg_extern]
    fn eval_case_set(
        dataset_name: &str,
        case_name: &str,
        input_payload: JsonB,
        expected_payload: JsonB,
        scorer: default!(&str, "'exact_text'"),
        threshold: default!(f64, 1.0),
        metadata: default!(Option<JsonB>, "NULL"),
    ) -> JsonB {
        evals::eval_case_set(
            dataset_name,
            case_name,
            input_payload,
            expected_payload,
            scorer,
            threshold,
            metadata,
        )
    }

    /// Deletes one stored evaluation case from a durable dataset.
    #[pg_extern]
    fn eval_case_delete(dataset_name: &str, case_name: &str) -> JsonB {
        evals::eval_case_delete(dataset_name, case_name)
    }

    /// Scores one actual payload against one expected payload with a built-in scorer.
    #[pg_extern]
    fn eval_score(
        actual: JsonB,
        expected: JsonB,
        scorer: default!(&str, "'exact_text'"),
        threshold: default!(f64, 1.0),
    ) -> JsonB {
        evals::eval_score(actual, expected, scorer, threshold)
    }

    /// Scores one actual payload against the stored expectation for a named evaluation case.
    #[pg_extern]
    fn eval_case_score(dataset_name: &str, case_name: &str, actual: JsonB) -> JsonB {
        evals::eval_case_score(dataset_name, case_name, actual)
    }

    #[derive(AggregateName)]
    #[aggregate_name = "messages_agg"]
    struct MessagesAggregate;

    #[expect(
        clippy::use_self,
        reason = "the Aggregate marker type must be named explicitly in the trait parameter"
    )]
    #[pg_aggregate]
    impl Aggregate<MessagesAggregate> for MessagesAggregate {
        const INITIAL_CONDITION: Option<&'static str> = Some("[]");
        const PARALLEL: Option<ParallelOption> = Some(ParallelOption::Unsafe);

        type State = JsonB;
        type Args = Option<JsonB>;
        type Finalize = Vec<JsonB>;

        fn state(
            mut current: Self::State,
            message: Self::Args,
            _fcinfo: pgrx::pg_sys::FunctionCallInfo,
        ) -> Self::State {
            let Some(message) = message else {
                pgrx::error!(
                    "{}",
                    crate::error::Error::invalid_argument(
                        "message",
                        "must not be null in postllm.messages_agg",
                        "filter out NULL rows before aggregating",
                    )
                );
            };
            let normalized = match crate::validate_aggregate_message(&message.0) {
                Ok(message) => message,
                Err(error) => pgrx::error!("{error}"),
            };
            let Some(messages) = current.0.as_array_mut() else {
                pgrx::error!("postllm internal error: messages_agg state must be a JSON array");
            };
            messages.push(normalized);
            current
        }

        fn finalize(
            current: Self::State,
            _direct_args: Self::OrderedSetArgs,
            _fcinfo: pgrx::pg_sys::FunctionCallInfo,
        ) -> Self::Finalize {
            let Some(messages) = current.0.as_array() else {
                pgrx::error!("postllm internal error: messages_agg state must be a JSON array");
            };

            messages.iter().cloned().map(JsonB).collect()
        }
    }

    /// Sends a prepared conversation to the configured LLM and returns provider JSON plus normalized `_postllm` metadata.
    #[pg_extern]
    fn chat(
        messages: Vec<JsonB>,
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> JsonB {
        inference::chat(messages, model, temperature, max_tokens)
    }

    /// Sends a prepared conversation to the configured LLM and returns the first textual answer.
    #[pg_extern]
    fn chat_text(
        messages: Vec<JsonB>,
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> String {
        inference::chat_text(messages, model, temperature, max_tokens)
    }

    /// Streams a prepared conversation and returns one row per provider chunk with a normalized text delta.
    #[pg_extern]
    fn chat_stream(
        messages: Vec<JsonB>,
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> TableIterator<
        'static,
        (
            pgrx::name!(index, i32),
            pgrx::name!(delta, Option<String>),
            pgrx::name!(event, JsonB),
        ),
    > {
        inference::chat_stream(messages, model, temperature, max_tokens)
    }

    /// Sends a prepared conversation to the configured LLM with a structured-output contract and returns parsed `jsonb`.
    #[pg_extern]
    fn chat_structured(
        messages: Vec<JsonB>,
        response_format: JsonB,
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> JsonB {
        inference::chat_structured(messages, response_format, model, temperature, max_tokens)
    }

    /// Sends a prepared conversation plus OpenAI-compatible tool definitions and returns the raw provider response.
    #[pg_extern]
    fn chat_tools(
        messages: Vec<JsonB>,
        tools: Vec<JsonB>,
        tool_choice: default!(Option<JsonB>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> JsonB {
        inference::chat_tools(messages, tools, tool_choice, model, temperature, max_tokens)
    }

    /// Returns normalized token-usage metadata for a provider response.
    #[pg_extern]
    fn usage(response: JsonB) -> JsonB {
        inference::usage(response)
    }

    /// Returns a specific choice object from a provider response.
    #[pg_extern]
    fn choice(response: JsonB, index: i32) -> JsonB {
        inference::choice(response, index)
    }

    /// Returns the normalized finish reason for a provider response when one is available.
    #[pg_extern]
    fn finish_reason(response: JsonB) -> Option<String> {
        inference::finish_reason(response)
    }

    /// Extracts the first textual completion from a provider response object.
    #[pg_extern]
    fn extract_text(response: JsonB) -> String {
        inference::extract_text(response)
    }

    /// Computes an embedding using the active runtime and returns it as a `real[]`.
    #[pg_extern]
    fn embed(
        input: &str,
        model: default!(Option<&str>, "NULL"),
        normalize: default!(bool, true),
    ) -> Vec<f32> {
        retrieval::embed(input, model, normalize)
    }

    /// Computes embeddings for multiple inputs using the active runtime and returns them as JSON.
    #[pg_extern]
    fn embed_many(
        inputs: Vec<String>,
        model: default!(Option<&str>, "NULL"),
        normalize: default!(bool, true),
    ) -> JsonB {
        retrieval::embed_many(inputs, model, normalize)
    }

    /// Returns metadata for the active or requested embedding model.
    #[pg_extern]
    fn embedding_model_info(model: default!(Option<&str>, "NULL")) -> JsonB {
        retrieval::embedding_model_info(model)
    }

    /// Installs local Candle model artifacts into the configured cache directory.
    #[pg_extern]
    fn model_install(
        model: default!(Option<&str>, "NULL"),
        lane: default!(Option<&str>, "NULL"),
    ) -> JsonB {
        ops::model_install(model, lane)
    }

    /// Loads a local Candle model into the current backend process so later calls avoid cold-start work.
    #[pg_extern]
    fn model_prewarm(
        model: default!(Option<&str>, "NULL"),
        lane: default!(Option<&str>, "NULL"),
    ) -> JsonB {
        ops::model_prewarm(model, lane)
    }

    /// Reports the on-disk and in-memory state of a local Candle model.
    #[pg_extern]
    fn model_inspect(
        model: default!(Option<&str>, "NULL"),
        lane: default!(Option<&str>, "NULL"),
    ) -> JsonB {
        ops::model_inspect(model, lane)
    }

    /// Evicts a local Candle model from backend memory, on-disk cache, or both.
    #[pg_extern]
    fn model_evict(
        model: default!(Option<&str>, "NULL"),
        lane: default!(Option<&str>, "NULL"),
        scope: default!(&str, "'all'"),
    ) -> JsonB {
        ops::model_evict(model, lane, scope)
    }

    /// Chunks a document, computes embeddings, and returns canonical row data for embedding tables.
    #[pg_extern]
    fn embed_document(
        doc_id: &str,
        input: &str,
        metadata: default!(Option<JsonB>, "NULL"),
        chunk_chars: default!(i32, 1000),
        overlap_chars: default!(i32, 200),
        model: default!(Option<&str>, "NULL"),
        normalize: default!(bool, true),
    ) -> TableIterator<
        'static,
        (
            pgrx::name!(chunk_id, String),
            pgrx::name!(doc_id, String),
            pgrx::name!(chunk_no, i32),
            pgrx::name!(content, String),
            pgrx::name!(metadata, JsonB),
            pgrx::name!(embedding, Vec<f32>),
        ),
    > {
        retrieval::embed_document(
            doc_id,
            input,
            metadata,
            chunk_chars,
            overlap_chars,
            model,
            normalize,
        )
    }

    /// Upserts canonical chunk rows into a caller-owned embedding table and optionally prunes stale chunks.
    #[expect(
        clippy::too_many_arguments,
        reason = "the SQL surface intentionally keeps ingestion configuration flat instead of forcing callers through a JSON argument"
    )]
    #[pg_extern]
    fn ingest_document(
        target_table: &str,
        doc_id: &str,
        input: &str,
        metadata: default!(Option<JsonB>, "NULL"),
        chunk_chars: default!(i32, 1000),
        overlap_chars: default!(i32, 200),
        model: default!(Option<&str>, "NULL"),
        normalize: default!(bool, true),
        delete_missing: default!(bool, true),
    ) -> JsonB {
        retrieval::ingest_document(
            target_table,
            doc_id,
            input,
            metadata,
            chunk_chars,
            overlap_chars,
            model,
            normalize,
            delete_missing,
        )
    }

    /// Splits text into overlapping chunks using character-count targets with boundary-aware fallbacks.
    #[pg_extern]
    fn chunk_text(
        input: &str,
        chunk_chars: default!(i32, 1000),
        overlap_chars: default!(i32, 200),
    ) -> Vec<String> {
        retrieval::chunk_text(input, chunk_chars, overlap_chars)
    }

    /// Splits text into chunk rows and propagates caller metadata onto every emitted chunk.
    #[pg_extern]
    fn chunk_document(
        input: &str,
        metadata: default!(Option<JsonB>, "NULL"),
        chunk_chars: default!(i32, 1000),
        overlap_chars: default!(i32, 200),
    ) -> TableIterator<
        'static,
        (
            pgrx::name!(index, i32),
            pgrx::name!(chunk, String),
            pgrx::name!(metadata, JsonB),
        ),
    > {
        retrieval::chunk_document(input, metadata, chunk_chars, overlap_chars)
    }

    /// Computes `PostgreSQL` full-text keyword ranks for candidate documents.
    #[allow(
        clippy::type_complexity,
        reason = "pgrx SQL generation requires the exported TableIterator shape inline"
    )]
    #[pg_extern]
    fn keyword_rank(
        query: &str,
        documents: Vec<String>,
        top_n: default!(Option<i32>, "NULL"),
        text_search_config: default!(Option<&str>, "NULL"),
        normalization: default!(i32, 32),
    ) -> TableIterator<
        'static,
        (
            pgrx::name!(rank, i32),
            pgrx::name!(index, i32),
            pgrx::name!(document, String),
            pgrx::name!(score, f64),
        ),
    > {
        retrieval::keyword_rank(query, documents, top_n, text_search_config, normalization)
    }

    /// Computes a reciprocal-rank-fusion score from semantic and keyword ranks.
    #[pg_extern]
    fn rrf_score(
        semantic_rank: default!(Option<i32>, "NULL"),
        keyword_rank: default!(Option<i32>, "NULL"),
        semantic_weight: default!(f64, 1.0),
        keyword_weight: default!(f64, 1.0),
        rrf_k: default!(i32, 60),
    ) -> f64 {
        retrieval::rrf_score(
            semantic_rank,
            keyword_rank,
            semantic_weight,
            keyword_weight,
            rrf_k,
        )
    }

    /// Fuses semantic reranking with `PostgreSQL` keyword search over the same candidate documents.
    #[expect(
        clippy::too_many_arguments,
        reason = "the SQL surface keeps hybrid retrieval controls flat instead of forcing a JSON wrapper"
    )]
    #[allow(
        clippy::type_complexity,
        reason = "pgrx SQL generation requires the exported TableIterator shape inline"
    )]
    #[pg_extern]
    fn hybrid_rank(
        query: &str,
        documents: Vec<String>,
        top_n: default!(Option<i32>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        text_search_config: default!(Option<&str>, "NULL"),
        semantic_weight: default!(f64, 1.0),
        keyword_weight: default!(f64, 1.0),
        rrf_k: default!(i32, 60),
        normalization: default!(i32, 32),
    ) -> TableIterator<
        'static,
        (
            pgrx::name!(rank, i32),
            pgrx::name!(index, i32),
            pgrx::name!(document, String),
            pgrx::name!(score, f64),
            pgrx::name!(semantic_rank, Option<i32>),
            pgrx::name!(keyword_rank, Option<i32>),
            pgrx::name!(semantic_score, Option<f64>),
            pgrx::name!(keyword_score, Option<f64>),
        ),
    > {
        retrieval::hybrid_rank(
            query,
            documents,
            top_n,
            model,
            text_search_config,
            semantic_weight,
            keyword_weight,
            rrf_k,
            normalization,
        )
    }

    /// Reranks candidate documents for one query and returns ordered rows with the original document index.
    #[allow(
        clippy::type_complexity,
        reason = "pgrx SQL generation requires the exported TableIterator shape inline"
    )]
    #[pg_extern]
    fn rerank(
        query: &str,
        documents: Vec<String>,
        top_n: default!(Option<i32>, "NULL"),
        model: default!(Option<&str>, "NULL"),
    ) -> TableIterator<
        'static,
        (
            pgrx::name!(rank, i32),
            pgrx::name!(index, i32),
            pgrx::name!(document, String),
            pgrx::name!(score, f64),
        ),
    > {
        retrieval::rerank(query, documents, top_n, model)
    }

    /// Retrieves context documents, builds a prompt, and returns the answer plus retrieval metadata.
    #[expect(
        clippy::too_many_arguments,
        reason = "the SQL surface keeps the batteries-included RAG helper flat instead of forcing a JSON wrapper"
    )]
    #[pg_extern]
    fn rag(
        query: &str,
        documents: Vec<String>,
        system_prompt: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        retrieval: default!(Option<&str>, "NULL"),
        retrieval_model: default!(Option<&str>, "NULL"),
        top_n: default!(i32, 5),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
        text_search_config: default!(Option<&str>, "NULL"),
        semantic_weight: default!(f64, 1.0),
        keyword_weight: default!(f64, 1.0),
        rrf_k: default!(i32, 60),
        normalization: default!(i32, 32),
    ) -> JsonB {
        retrieval::rag(
            query,
            documents,
            system_prompt,
            model,
            retrieval,
            retrieval_model,
            top_n,
            temperature,
            max_tokens,
            text_search_config,
            semantic_weight,
            keyword_weight,
            rrf_k,
            normalization,
        )
    }

    /// Retrieves context documents, builds a prompt, and returns only the answer text.
    #[expect(
        clippy::too_many_arguments,
        reason = "the SQL surface keeps the batteries-included RAG helper flat instead of forcing a JSON wrapper"
    )]
    #[pg_extern]
    fn rag_text(
        query: &str,
        documents: Vec<String>,
        system_prompt: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        retrieval: default!(Option<&str>, "NULL"),
        retrieval_model: default!(Option<&str>, "NULL"),
        top_n: default!(i32, 5),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
        text_search_config: default!(Option<&str>, "NULL"),
        semantic_weight: default!(f64, 1.0),
        keyword_weight: default!(f64, 1.0),
        rrf_k: default!(i32, 60),
        normalization: default!(i32, 32),
    ) -> String {
        retrieval::rag_text(
            query,
            documents,
            system_prompt,
            model,
            retrieval,
            retrieval_model,
            top_n,
            temperature,
            max_tokens,
            text_search_config,
            semantic_weight,
            keyword_weight,
            rrf_k,
            normalization,
        )
    }

    /// Sends a single prompt, optionally preceded by a system prompt, and returns the text result.
    #[pg_extern]
    fn complete(
        prompt: &str,
        system_prompt: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> String {
        inference::complete(prompt, system_prompt, model, temperature, max_tokens)
    }

    /// Sends a single prompt with a structured-output contract and returns parsed `jsonb`.
    #[pg_extern]
    fn complete_structured(
        prompt: &str,
        response_format: JsonB,
        system_prompt: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> JsonB {
        inference::complete_structured(
            prompt,
            response_format,
            system_prompt,
            model,
            temperature,
            max_tokens,
        )
    }

    /// Streams a prompt and returns one row per provider chunk with a normalized text delta.
    #[pg_extern]
    fn complete_stream(
        prompt: &str,
        system_prompt: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> TableIterator<
        'static,
        (
            pgrx::name!(index, i32),
            pgrx::name!(delta, Option<String>),
            pgrx::name!(event, JsonB),
        ),
    > {
        inference::complete_stream(prompt, system_prompt, model, temperature, max_tokens)
    }

    /// Sends a prompt plus OpenAI-compatible tool definitions and returns the raw provider response.
    #[pg_extern]
    fn complete_tools(
        prompt: &str,
        tools: Vec<JsonB>,
        system_prompt: default!(Option<&str>, "NULL"),
        tool_choice: default!(Option<JsonB>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> JsonB {
        inference::complete_tools(
            prompt,
            tools,
            system_prompt,
            tool_choice,
            model,
            temperature,
            max_tokens,
        )
    }

    /// Sends multiple prompts, optionally preceded by the same system prompt, and returns the text results in input order.
    #[pg_extern]
    fn complete_many(
        prompts: Vec<String>,
        system_prompt: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> Vec<String> {
        inference::complete_many(prompts, system_prompt, model, temperature, max_tokens)
    }

    /// Sends multiple prompts and returns one row per completion for set-oriented SQL workflows.
    #[pg_extern]
    fn complete_many_rows(
        prompts: Vec<String>,
        system_prompt: default!(Option<&str>, "NULL"),
        model: default!(Option<&str>, "NULL"),
        temperature: default!(f64, 0.2),
        max_tokens: default!(Option<i32>, "NULL"),
    ) -> TableIterator<
        'static,
        (
            pgrx::name!(index, i32),
            pgrx::name!(prompt, String),
            pgrx::name!(completion, String),
        ),
    > {
        inference::complete_many_rows(prompts, system_prompt, model, temperature, max_tokens)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StructuredOutputContract {
    response_format: Value,
    name: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct ChatRequestExtensions<'value> {
    response_format: Option<&'value Value>,
    tools: Option<&'value [Value]>,
    tool_choice: Option<&'value Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChunkingOptions {
    chunk_chars: usize,
    overlap_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TextChunk {
    start_char: usize,
    end_char_exclusive: usize,
    text: String,
}

#[derive(Debug, Clone, PartialEq)]
struct PreparedDocumentEmbedding {
    chunk_id: String,
    doc_id: String,
    chunk_no: i32,
    content: String,
    metadata: Value,
    embedding: Vec<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct RankedDocument {
    index: usize,
    score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RagRetrievalStrategy {
    Hybrid,
    Semantic,
    Keyword,
}

impl RagRetrievalStrategy {
    const VARIANTS: [(&'static str, Self); 3] = [
        ("hybrid", Self::Hybrid),
        ("semantic", Self::Semantic),
        ("keyword", Self::Keyword),
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Hybrid => "hybrid",
            Self::Semantic => "semantic",
            Self::Keyword => "keyword",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        enum_parser::parse_case_insensitive_required("retrieval", value, &Self::VARIANTS)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RagContextRow {
    rank: i32,
    index: i32,
    document: String,
    score: f64,
    semantic_rank: Option<i32>,
    keyword_rank: Option<i32>,
    semantic_score: Option<f64>,
    keyword_score: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
struct RagResult {
    query: String,
    retrieval: RagRetrievalStrategy,
    prompt: String,
    system_prompt: String,
    answer: String,
    documents_considered: usize,
    top_n: usize,
    context: Vec<RagContextRow>,
    response: Value,
}

#[derive(Debug, Clone, Copy)]
struct DocumentEmbeddingRequest<'value> {
    doc_id: &'value str,
    input: &'value str,
    metadata: Option<&'value Value>,
    chunk_chars: i32,
    overlap_chars: i32,
    model: Option<&'value str>,
    normalize: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalModelTarget {
    model: String,
    lane: candle::LocalModelLane,
}

#[derive(Debug, Clone)]
struct LocalModelRequestContext {
    target: LocalModelTarget,
    candle_cache_dir: Option<String>,
    candle_offline: bool,
    candle_device: backend::CandleDevice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalModelLaneSelector {
    Auto,
    Embedding,
    Generation,
}

impl LocalModelLaneSelector {
    const EMBEDDING: &'static str = "embedding";
    const GENERATION: &'static str = "generation";
    const ALL_VARIANTS: &'static str = "'auto', 'embedding', or 'generation'";
    const VARIANTS: [(&'static str, Self); 3] = [
        ("auto", Self::Auto),
        (Self::EMBEDDING, Self::Embedding),
        (Self::GENERATION, Self::Generation),
    ];
    const EXPLICIT_VARIANTS: &'static str = "'embedding' or 'generation'";

    fn parse(value: &str) -> Result<Self> {
        let normalized = enum_parser::normalize_input(value);
        if normalized.is_empty() {
            return Err(Error::invalid_argument(
                "lane",
                "must not be empty or whitespace-only",
                format!(
                    "omit lane for auto selection or pass lane => {}",
                    Self::EXPLICIT_VARIANTS
                ),
            ));
        }

        enum_parser::parse_case_insensitive(&normalized, &Self::VARIANTS).map_err(|unknown| {
            Error::invalid_argument(
                "lane",
                format!("must be one of {}, got '{unknown}'", Self::ALL_VARIANTS),
                format!(
                    "omit lane for auto selection or pass lane => {}",
                    Self::EXPLICIT_VARIANTS
                ),
            )
        })
    }

    fn parse_or_default(value: Option<&str>) -> Result<Self> {
        value.map_or_else(|| Ok(Self::Auto), Self::parse)
    }
}

#[derive(Debug, Clone)]
struct CandleGenerationReadiness {
    ready: bool,
    cold_start: bool,
    reason: Option<String>,
}

type RankRow = (i32, i32, String, f64);
type HybridRankRow = (
    i32,
    i32,
    String,
    f64,
    Option<i32>,
    Option<i32>,
    Option<f64>,
    Option<f64>,
);
type RankRowsIterator = TableIterator<
    'static,
    (
        pgrx::name!(rank, i32),
        pgrx::name!(index, i32),
        pgrx::name!(document, String),
        pgrx::name!(score, f64),
    ),
>;
type HybridRankRowsIterator = TableIterator<
    'static,
    (
        pgrx::name!(rank, i32),
        pgrx::name!(index, i32),
        pgrx::name!(document, String),
        pgrx::name!(score, f64),
        pgrx::name!(semantic_rank, Option<i32>),
        pgrx::name!(keyword_rank, Option<i32>),
        pgrx::name!(semantic_score, Option<f64>),
        pgrx::name!(keyword_score, Option<f64>),
    ),
>;

const DEFAULT_RAG_SYSTEM_PROMPT: &str =
    "You answer using only the retrieved context. If the context is insufficient, say so.";

fn complete_impl(
    prompt: &str,
    system_prompt: Option<&str>,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<String> {
    let prompt_message = build_message("user", prompt)?;
    let mut messages = Vec::with_capacity(usize::from(system_prompt.is_some()) + 1);

    if let Some(system_prompt) = system_prompt {
        messages.push(build_message("system", system_prompt)?);
    }

    messages.push(prompt_message);

    let response = chat_impl_from_values(
        &messages,
        model,
        temperature,
        max_tokens,
        backend::Feature::Complete,
        ChatRequestExtensions::default(),
    )?;

    backend::extract_text(&response)
}

fn complete_structured_impl(
    prompt: &str,
    response_format: &Value,
    system_prompt: Option<&str>,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Value> {
    let contract = validate_structured_output_contract(response_format)?;
    let prompt_message = build_message("user", prompt)?;
    let mut messages = Vec::with_capacity(usize::from(system_prompt.is_some()) + 1);

    if let Some(system_prompt) = system_prompt {
        messages.push(build_message("system", system_prompt)?);
    }

    messages.push(prompt_message);

    let response = chat_impl_from_values(
        &messages,
        model,
        temperature,
        max_tokens,
        backend::Feature::Complete,
        ChatRequestExtensions {
            response_format: Some(&contract.response_format),
            ..ChatRequestExtensions::default()
        },
    )?;

    parse_structured_output_response(&response, &contract)
}

fn complete_stream_impl(
    prompt: &str,
    system_prompt: Option<&str>,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Vec<(i32, Option<String>, JsonB)>> {
    let prompt_message = build_message("user", prompt)?;
    let mut messages = Vec::with_capacity(usize::from(system_prompt.is_some()) + 1);

    if let Some(system_prompt) = system_prompt {
        messages.push(build_message("system", system_prompt)?);
    }

    messages.push(prompt_message);

    stream_impl_from_values(
        &messages,
        model,
        temperature,
        max_tokens,
        backend::Feature::Complete,
    )
}

fn complete_tools_impl(
    prompt: &str,
    tools: &[JsonB],
    system_prompt: Option<&str>,
    tool_choice: Option<&Value>,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Value> {
    let validated_tools = validate_tools(tools)?;
    let validated_tool_choice = validate_tool_choice(tool_choice, &validated_tools)?;
    let prompt_message = build_message("user", prompt)?;
    let mut messages = Vec::with_capacity(usize::from(system_prompt.is_some()) + 1);

    if let Some(system_prompt) = system_prompt {
        messages.push(build_message("system", system_prompt)?);
    }

    messages.push(prompt_message);

    chat_impl_from_values(
        &messages,
        model,
        temperature,
        max_tokens,
        backend::Feature::Complete,
        ChatRequestExtensions {
            tools: Some(&validated_tools),
            tool_choice: validated_tool_choice.as_ref(),
            ..ChatRequestExtensions::default()
        },
    )
}

fn complete_many_impl(
    prompts: &[String],
    system_prompt: Option<&str>,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Vec<String>> {
    let validated_prompts = validate_batch_prompts(prompts)?;
    let system_prompt = validate_optional_system_prompt(system_prompt)?;

    complete_many_from_validated_prompts(
        &validated_prompts,
        system_prompt.as_deref(),
        model,
        temperature,
        max_tokens,
    )
}

fn complete_many_from_validated_prompts(
    prompts: &[String],
    system_prompt: Option<&str>,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Vec<String>> {
    prompts
        .iter()
        .map(|prompt| complete_impl(prompt, system_prompt, model, temperature, max_tokens))
        .collect()
}

fn validate_optional_system_prompt(system_prompt: Option<&str>) -> Result<Option<String>> {
    let system_prompt = system_prompt
        .map(|system_prompt| require_non_blank("system_prompt", system_prompt).map(str::to_owned))
        .transpose()?;

    Ok(system_prompt)
}

fn complete_many_rows_impl(
    prompts: &[String],
    system_prompt: Option<&str>,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Vec<(i32, String, String)>> {
    let validated_prompts = validate_batch_prompts(prompts)?;
    let system_prompt = validate_optional_system_prompt(system_prompt)?;
    let completions = complete_many_from_validated_prompts(
        &validated_prompts,
        system_prompt.as_deref(),
        model,
        temperature,
        max_tokens,
    )?;

    validated_prompts
        .into_iter()
        .zip(completions)
        .enumerate()
        .map(|(index, (prompt, completion))| {
            let index = i32::try_from(index + 1).map_err(|_| {
                Error::invalid_argument(
                    "prompts",
                    "contains too many entries to return 1-based row indexes safely",
                    format!("pass fewer than {} prompts", i32::MAX),
                )
            })?;

            Ok((index, prompt, completion))
        })
        .collect()
}

fn chat_impl(
    messages: &[JsonB],
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Value> {
    let validated_messages = validate_messages(messages)?;

    chat_impl_from_values(
        &validated_messages,
        model,
        temperature,
        max_tokens,
        backend::Feature::Chat,
        ChatRequestExtensions::default(),
    )
}

fn chat_text_impl(
    messages: &[JsonB],
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<String> {
    let response = chat_impl(messages, model, temperature, max_tokens)?;

    backend::extract_text(&response)
}

fn chat_stream_impl(
    messages: &[JsonB],
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Vec<(i32, Option<String>, JsonB)>> {
    let validated_messages = validate_messages(messages)?;

    stream_impl_from_values(
        &validated_messages,
        model,
        temperature,
        max_tokens,
        backend::Feature::Chat,
    )
}

fn chat_structured_impl(
    messages: &[JsonB],
    response_format: &Value,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Value> {
    let validated_messages = validate_messages(messages)?;
    let contract = validate_structured_output_contract(response_format)?;
    let response = chat_impl_from_values(
        &validated_messages,
        model,
        temperature,
        max_tokens,
        backend::Feature::Chat,
        ChatRequestExtensions {
            response_format: Some(&contract.response_format),
            ..ChatRequestExtensions::default()
        },
    )?;

    parse_structured_output_response(&response, &contract)
}

fn chat_tools_impl(
    messages: &[JsonB],
    tools: &[JsonB],
    tool_choice: Option<&Value>,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Value> {
    let validated_messages = validate_messages(messages)?;
    let validated_tools = validate_tools(tools)?;
    let validated_tool_choice = validate_tool_choice(tool_choice, &validated_tools)?;

    chat_impl_from_values(
        &validated_messages,
        model,
        temperature,
        max_tokens,
        backend::Feature::Chat,
        ChatRequestExtensions {
            tools: Some(&validated_tools),
            tool_choice: validated_tool_choice.as_ref(),
            ..ChatRequestExtensions::default()
        },
    )
}

fn usage_impl(response: &Value) -> Value {
    backend::usage(response)
}

fn choice_impl(response: &Value, index: i32) -> Result<Value> {
    let index = usize::try_from(index).map_err(|_| {
        Error::invalid_argument(
            "index",
            format!("must be greater than or equal to zero, got {index}"),
            "pass 0 for the first choice",
        )
    })?;

    backend::choice(response, index)
}

fn chat_impl_from_values(
    messages: &[Value],
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
    feature: backend::Feature,
    extensions: ChatRequestExtensions<'_>,
) -> Result<Value> {
    let controls = validate_request_controls(temperature, max_tokens)?;
    let max_tokens = controls.max_tokens;

    execution::ExecutionContext::new(execution::generation_operation(feature, false), || {
        chat_request_audit_payload(messages, temperature, max_tokens, extensions)
    })
    .run_generation(
        model,
        execution::GenerationRequirements {
            feature,
            temperature,
            max_tokens,
            streaming: false,
            structured_outputs: extensions.response_format.is_some(),
            tools: extensions.tools.is_some() || extensions.tool_choice.is_some(),
            multimodal_inputs: messages_require_multimodal_inputs(messages),
        },
        |settings| {
            backend::chat_response(
                settings,
                messages,
                backend::RequestOptions {
                    temperature,
                    max_tokens,
                },
                extensions.response_format,
                extensions.tools,
                extensions.tool_choice,
            )
        },
        Clone::clone,
    )
}

fn stream_impl_from_values(
    messages: &[Value],
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
    feature: backend::Feature,
) -> Result<Vec<(i32, Option<String>, JsonB)>> {
    let controls = validate_request_controls(temperature, max_tokens)?;
    let max_tokens = controls.max_tokens;

    execution::ExecutionContext::new(execution::generation_operation(feature, true), || {
        chat_request_audit_payload(
            messages,
            temperature,
            max_tokens,
            ChatRequestExtensions::default(),
        )
    })
    .run_generation(
        model,
        execution::GenerationRequirements {
            feature,
            temperature,
            max_tokens,
            streaming: true,
            structured_outputs: false,
            tools: false,
            multimodal_inputs: messages_require_multimodal_inputs(messages),
        },
        |settings| {
            let events = backend::chat_stream_response(
                settings,
                messages,
                backend::RequestOptions {
                    temperature,
                    max_tokens,
                },
            )?;

            build_stream_rows(events)
        },
        |rows| stream_rows_audit_payload(rows),
    )
}

fn build_stream_rows(events: Vec<Value>) -> Result<Vec<(i32, Option<String>, JsonB)>> {
    events
        .into_iter()
        .enumerate()
        .map(|(index, event)| {
            let index = i32::try_from(index + 1).map_err(|_| {
                Error::invalid_argument(
                    "stream",
                    "contains too many events to return 1-based row indexes safely",
                    format!("pass fewer than {} streamed chunks", i32::MAX),
                )
            })?;
            let delta = backend::stream_text_delta(&event);

            Ok((index, delta, JsonB(event)))
        })
        .collect()
}

fn chat_request_audit_payload(
    messages: &[Value],
    temperature: f64,
    max_tokens: Option<i32>,
    extensions: ChatRequestExtensions<'_>,
) -> Value {
    json!({
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": extensions.response_format,
        "tools": extensions.tools,
        "tool_choice": extensions.tool_choice,
    })
}

fn stream_rows_audit_payload(rows: &[(i32, Option<String>, JsonB)]) -> Value {
    let text = rows
        .iter()
        .filter_map(|(_, delta, _)| delta.as_deref())
        .collect::<Vec<_>>()
        .join("");
    let payload = json!({
        "event_count": rows.len(),
        "text": text,
    });

    payload
}

fn embed_impl(input: &str, model: Option<&str>, normalize: bool) -> Result<Vec<f32>> {
    let input = require_non_blank("input", input)?.to_owned();
    let mut vectors = embed_many_impl(&[input], model, normalize)?;

    vectors
        .pop()
        .ok_or_else(|| Error::Candle("the local embedding backend returned no vectors".to_owned()))
}

fn profiles_impl() -> Result<Value> {
    catalog::profiles()
}

fn profile_impl(name: &str) -> Result<Value> {
    catalog::profile(name)
}

fn secrets_impl() -> Result<Value> {
    operator_policy::run_operator_operation("postllm.secrets", catalog::secrets)
}

fn secret_impl(name: &str) -> Result<Value> {
    operator_policy::run_operator_operation("postllm.secret", || catalog::secret(name))
}

fn secret_set_impl(name: &str, value: &str, description: Option<&str>) -> Result<Value> {
    operator_policy::run_operator_operation("postllm.secret_set", || {
        catalog::secret_set(name, value, description)
    })
}

fn secret_delete_impl(name: &str) -> Result<Value> {
    operator_policy::run_operator_operation("postllm.secret_delete", || {
        catalog::secret_delete(name)
    })
}

fn permissions_impl() -> Result<Value> {
    operator_policy::run_operator_operation("postllm.permissions", permissions::permissions)
}

fn permission_impl(role_name: &str, object_type: &str, target: &str) -> Result<Value> {
    operator_policy::run_operator_operation("postllm.permission", || {
        let object_type = permissions::PermissionObjectType::parse("object_type", object_type)?;
        permissions::permission(role_name, object_type, target)
    })
}

fn permission_set_impl(
    role_name: &str,
    object_type: &str,
    target: &str,
    description: Option<&str>,
) -> Result<Value> {
    operator_policy::run_operator_operation("postllm.permission_set", || {
        let object_type = permissions::PermissionObjectType::parse("object_type", object_type)?;
        permissions::permission_set(role_name, object_type, target, description)
    })
}

fn permission_delete_impl(role_name: &str, object_type: &str, target: &str) -> Result<Value> {
    operator_policy::run_operator_operation("postllm.permission_delete", || {
        let object_type = permissions::PermissionObjectType::parse("object_type", object_type)?;
        permissions::permission_delete(role_name, object_type, target)
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the SQL surface intentionally exposes a flat profile_set(...) API aligned with configure(...)"
)]
fn profile_set_impl(
    name: &str,
    description: Option<&str>,
    base_url: Option<&str>,
    model: Option<&str>,
    embedding_model: Option<&str>,
    api_key_secret: Option<&str>,
    timeout_ms: Option<i32>,
    max_retries: Option<i32>,
    retry_backoff_ms: Option<i32>,
    request_max_concurrency: Option<i32>,
    request_token_budget: Option<i32>,
    request_runtime_budget_ms: Option<i32>,
    request_spend_budget_microusd: Option<i32>,
    output_token_price_microusd_per_1k: Option<i32>,
    runtime: Option<&str>,
    candle_cache_dir: Option<&str>,
    candle_offline: Option<bool>,
    candle_device: Option<&str>,
    candle_max_input_tokens: Option<i32>,
    candle_max_concurrency: Option<i32>,
) -> Result<Value> {
    let overrides = guc::SessionOverrides::from_configure_args(
        base_url,
        model,
        embedding_model,
        None,
        api_key_secret,
        timeout_ms,
        max_retries,
        retry_backoff_ms,
        request_max_concurrency,
        request_token_budget,
        request_runtime_budget_ms,
        request_spend_budget_microusd,
        output_token_price_microusd_per_1k,
        runtime,
        candle_cache_dir,
        candle_offline,
        candle_device,
        candle_max_input_tokens,
        candle_max_concurrency,
    )?;

    catalog::profile_set(name, description, &overrides)
}

fn profile_apply_impl(name: &str) -> Result<Value> {
    catalog::profile_apply(name)
}

fn profile_delete_impl(name: &str) -> Result<Value> {
    catalog::profile_delete(name)
}

fn model_aliases_impl() -> Result<Value> {
    catalog::model_aliases()
}

fn model_alias_impl(alias: &str, lane: &str) -> Result<Value> {
    let lane = catalog::ModelAliasLane::parse("lane", lane)?;
    catalog::model_alias(alias, lane)
}

fn model_alias_set_impl(
    alias: &str,
    lane: &str,
    model: &str,
    description: Option<&str>,
) -> Result<Value> {
    let lane = catalog::ModelAliasLane::parse("lane", lane)?;
    catalog::model_alias_set(alias, lane, model, description)
}

fn model_alias_delete_impl(alias: &str, lane: &str) -> Result<Value> {
    let lane = catalog::ModelAliasLane::parse("lane", lane)?;
    catalog::model_alias_delete(alias, lane)
}

fn embedding_model_info_impl(model: Option<&str>) -> Result<Value> {
    guc::ensure_active_privileged_settings_allowed()?;
    let settings = guc::resolve_embedding_settings(model)?;

    match settings.runtime {
        backend::Runtime::OpenAi => Ok(json!({
            "runtime": settings.runtime.as_str(),
            "model": settings.model,
            "dimension": Value::Null,
            "max_sequence_length": Value::Null,
            "pooling": Value::Null,
            "normalization": {
                "default": "provider-defined",
                "supported": ["provider-defined", "l2", "none"],
            },
            "metadata_source": "provider-defined",
        })),
        backend::Runtime::Candle => {
            let candle_cache_dir = guc::resolve_candle_cache_dir();
            let candle_offline = guc::resolve_candle_offline();

            candle::embedding_model_info(
                &settings.model,
                candle_cache_dir.as_deref(),
                candle_offline,
            )
        }
    }
}

fn model_install_impl(model: Option<&str>, lane: Option<&str>) -> Result<Value> {
    let request = resolve_local_model_request(model, lane)?;
    candle::install_model(
        &request.target.model,
        request.target.lane,
        request.candle_cache_dir.as_deref(),
        request.candle_offline,
        request.candle_device,
    )
}

fn model_prewarm_impl(model: Option<&str>, lane: Option<&str>) -> Result<Value> {
    let request = resolve_local_model_request(model, lane)?;
    candle::prewarm_model(
        &request.target.model,
        request.target.lane,
        request.candle_cache_dir.as_deref(),
        request.candle_offline,
        request.candle_device,
    )
}

fn model_inspect_impl(model: Option<&str>, lane: Option<&str>) -> Result<Value> {
    let request = resolve_local_model_request(model, lane)?;
    candle::inspect_model(
        &request.target.model,
        request.target.lane,
        request.candle_cache_dir.as_deref(),
        request.candle_offline,
        request.candle_device,
    )
}

fn model_evict_impl(model: Option<&str>, lane: Option<&str>, scope: &str) -> Result<Value> {
    let request = resolve_local_model_request(model, lane)?;
    let scope = parse_local_model_evict_scope(scope)?;

    candle::evict_model(
        &request.target.model,
        request.target.lane,
        scope,
        request.candle_cache_dir.as_deref(),
        request.candle_offline,
        request.candle_device,
    )
}

fn runtime_discover_impl() -> Value {
    match guc::resolve(None) {
        Ok(settings) => match settings.runtime {
            backend::Runtime::OpenAi => discover_hosted_runtime(&settings),
            backend::Runtime::Candle => discover_candle_runtime(&settings),
        },
        Err(error) => runtime_discover_error(&error),
    }
}

fn runtime_ready_impl() -> bool {
    runtime_discover_impl()
        .get("ready")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn execution_environment() -> &'static str {
    if Path::new("/.dockerenv").exists() {
        "docker"
    } else {
        "local"
    }
}

fn resolve_local_model_request(
    model: Option<&str>,
    lane: Option<&str>,
) -> Result<LocalModelRequestContext> {
    guc::ensure_active_privileged_settings_allowed()?;

    Ok(LocalModelRequestContext {
        target: resolve_local_model_target(model, lane)?,
        candle_cache_dir: guc::resolve_candle_cache_dir(),
        candle_offline: guc::resolve_candle_offline(),
        candle_device: guc::resolve_candle_device()?,
    })
}

fn discover_hosted_runtime(settings: &backend::Settings) -> Value {
    attach_execution_environment(client::discover_openai_runtime(settings))
}

fn runtime_discover_error(error: &Error) -> Value {
    json!({
        "runtime": guc::snapshot().get("runtime").cloned().unwrap_or(Value::Null),
        "provider": Value::Null,
        "ready": false,
        "reason": error.to_string(),
        "execution_environment": execution_environment(),
        "settings": guc::snapshot(),
        "capabilities": guc::capabilities_snapshot(),
    })
}

fn attach_execution_environment(mut discovery: Value) -> Value {
    if let Some(object) = discovery.as_object_mut() {
        object.insert(
            "execution_environment".to_owned(),
            json!(execution_environment()),
        );
    }

    discovery
}

fn discover_candle_runtime(settings: &backend::Settings) -> Value {
    let embedding_model = match guc::resolve_embedding_model(None) {
        Ok(model) => model,
        Err(error) => return candle_runtime_failure(settings, None, &error, None),
    };
    let generation = match inspect_candle_model(
        &settings.model,
        candle::LocalModelLane::Generation,
        settings,
    ) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return candle_runtime_failure(settings, Some(&embedding_model), &error, None);
        }
    };
    let embedding = match inspect_candle_model(
        &embedding_model,
        candle::LocalModelLane::Embedding,
        settings,
    ) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return candle_runtime_failure(
                settings,
                Some(&embedding_model),
                &error,
                Some(generation),
            );
        }
    };
    let readiness = candle_generation_readiness(settings, &generation);

    json!({
        "runtime": settings.runtime.as_str(),
        "provider": "candle",
        "ready": readiness.ready,
        "reason": readiness.reason,
        "execution_environment": execution_environment(),
        "model": settings.model,
        "embedding_model": embedding_model,
        "offline": settings.candle_offline,
        "cold_start": readiness.cold_start,
        "capabilities": candle_runtime_capabilities(settings, Some(&embedding_model)),
        "generation": generation,
        "embedding": embedding,
    })
}

fn inspect_candle_model(
    model: &str,
    lane: candle::LocalModelLane,
    settings: &backend::Settings,
) -> Result<Value> {
    candle::inspect_model(
        model,
        lane,
        settings.candle_cache_dir.as_deref(),
        settings.candle_offline,
        settings.candle_device,
    )
}

fn candle_runtime_failure(
    settings: &backend::Settings,
    embedding_model: Option<&str>,
    error: &Error,
    generation: Option<Value>,
) -> Value {
    let mut discovery = json!({
        "runtime": settings.runtime.as_str(),
        "provider": "candle",
        "ready": false,
        "reason": error.to_string(),
        "execution_environment": execution_environment(),
        "model": settings.model,
        "embedding_model": embedding_model,
        "offline": settings.candle_offline,
        "capabilities": candle_runtime_capabilities(settings, embedding_model),
    });

    if let Some(generation) = generation
        && let Some(object) = discovery.as_object_mut()
    {
        object.insert("generation".to_owned(), generation);
    }

    discovery
}

fn candle_runtime_capabilities(
    settings: &backend::Settings,
    embedding_model: Option<&str>,
) -> Value {
    backend::CapabilitySnapshot::from_settings(settings, embedding_model).snapshot()
}

fn candle_generation_readiness(
    settings: &backend::Settings,
    generation: &Value,
) -> CandleGenerationReadiness {
    let generation_device_available = generation
        .get("device")
        .and_then(|device| device.get("available"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let generation_supported = generation
        .get("metadata")
        .and_then(|metadata| metadata.get("supported"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let generation_cached = generation
        .get("disk_cached")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || generation
            .get("memory_cached")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let cache_ready = !settings.candle_offline || generation_cached;

    CandleGenerationReadiness {
        ready: generation_device_available && generation_supported && cache_ready,
        cold_start: !generation_cached,
        reason: candle_generation_unready_reason(
            settings,
            generation,
            generation_device_available,
            generation_supported,
            cache_ready,
        ),
    }
}

fn candle_generation_unready_reason(
    settings: &backend::Settings,
    generation: &Value,
    generation_device_available: bool,
    generation_supported: bool,
    cache_ready: bool,
) -> Option<String> {
    if !generation_device_available {
        return generation
            .get("device")
            .and_then(|device| device.get("reason"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| Some("the requested Candle device is not available".to_owned()));
    }

    if !generation_supported {
        return guc::capabilities_snapshot()
            .get("features")
            .and_then(|features| features.get("chat"))
            .and_then(|chat| chat.get("reason"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                Some("the configured Candle generation model is not supported".to_owned())
            });
    }

    if !cache_ready {
        return Some(format!(
            "postllm.candle_offline is enabled and model '{}' is not cached locally",
            settings.model
        ));
    }

    None
}

fn resolve_local_model_target(model: Option<&str>, lane: Option<&str>) -> Result<LocalModelTarget> {
    let requested_model = model
        .map(|model| require_non_blank("model", model).map(str::to_owned))
        .transpose()?;
    let lane = parse_local_model_lane_selector(lane)?;

    match (requested_model, lane) {
        (Some(model), LocalModelLaneSelector::Embedding) => Ok(LocalModelTarget {
            model: resolve_aliased_local_model(&model, catalog::ModelAliasLane::Embedding)?,
            lane: candle::LocalModelLane::Embedding,
        }),
        (Some(model), LocalModelLaneSelector::Generation) => Ok(LocalModelTarget {
            model: resolve_aliased_local_model(&model, catalog::ModelAliasLane::Generation)?,
            lane: candle::LocalModelLane::Generation,
        }),
        (Some(model), LocalModelLaneSelector::Auto) => auto_requested_local_model_target(&model),
        (None, LocalModelLaneSelector::Embedding) => {
            let model = guc::resolve_embedding_model(None)?;
            Ok(LocalModelTarget {
                model,
                lane: candle::LocalModelLane::Embedding,
            })
        }
        (None, LocalModelLaneSelector::Generation) => {
            let settings = guc::resolve(None)?;
            Ok(LocalModelTarget {
                model: settings.model,
                lane: candle::LocalModelLane::Generation,
            })
        }
        (None, LocalModelLaneSelector::Auto) => auto_local_model_target(),
    }
}

fn auto_local_model_target() -> Result<LocalModelTarget> {
    let settings = guc::resolve(None)?;
    if settings.runtime == backend::Runtime::Candle
        && auto_local_model_lane_for_model(&settings.model) == candle::LocalModelLane::Generation
    {
        return Ok(LocalModelTarget {
            model: settings.model,
            lane: candle::LocalModelLane::Generation,
        });
    }

    let model = guc::resolve_embedding_model(None)?;
    Ok(LocalModelTarget {
        model,
        lane: candle::LocalModelLane::Embedding,
    })
}

fn auto_requested_local_model_target(model: &str) -> Result<LocalModelTarget> {
    if let Some(generation_model) =
        catalog::resolve_model_alias(model, catalog::ModelAliasLane::Generation)?
    {
        return Ok(LocalModelTarget {
            model: generation_model,
            lane: candle::LocalModelLane::Generation,
        });
    }

    if let Some(embedding_model) =
        catalog::resolve_model_alias(model, catalog::ModelAliasLane::Embedding)?
    {
        return Ok(LocalModelTarget {
            model: embedding_model,
            lane: candle::LocalModelLane::Embedding,
        });
    }

    Ok(LocalModelTarget {
        lane: auto_local_model_lane_for_model(model),
        model: model.to_owned(),
    })
}

fn resolve_aliased_local_model(model: &str, lane: catalog::ModelAliasLane) -> Result<String> {
    catalog::resolve_model_alias(model, lane)
        .map(|resolved| resolved.unwrap_or_else(|| model.to_owned()))
}

fn auto_local_model_lane_for_model(model: &str) -> candle::LocalModelLane {
    if candle::generation_availability(model, backend::Feature::Chat).available {
        candle::LocalModelLane::Generation
    } else {
        candle::LocalModelLane::Embedding
    }
}

fn parse_local_model_lane_selector(lane: Option<&str>) -> Result<LocalModelLaneSelector> {
    LocalModelLaneSelector::parse_or_default(lane)
}

fn parse_local_model_evict_scope(scope: &str) -> Result<candle::LocalModelEvictionScope> {
    candle::LocalModelEvictionScope::parse(scope)
}

fn embed_document_impl(
    request: DocumentEmbeddingRequest<'_>,
) -> Result<Vec<PreparedDocumentEmbedding>> {
    let doc_id = require_non_blank("doc_id", request.doc_id)?.to_owned();
    let input = require_non_blank("input", request.input)?;
    let options = validate_chunking_options(request.chunk_chars, request.overlap_chars)?;
    let metadata = validate_chunk_metadata(request.metadata)?;
    let total_chars = input.chars().count();
    let chunks = chunk_text_value(input, options);
    let inputs = chunks
        .iter()
        .map(|chunk| chunk.text.clone())
        .collect::<Vec<_>>();
    let embeddings = embed_many_impl(&inputs, request.model, request.normalize)?;

    chunks
        .into_iter()
        .zip(embeddings)
        .enumerate()
        .map(|(index, (chunk, embedding))| {
            let chunk_no = i32::try_from(index + 1).map_err(|_| {
                Error::invalid_argument(
                    "input",
                    "contains too many chunks to return 1-based row indexes safely",
                    format!("pass fewer than {} chunks", i32::MAX),
                )
            })?;
            let chunk_id = deterministic_chunk_id(&doc_id, &chunk);
            let metadata = annotate_chunk_metadata(
                &metadata,
                &chunk,
                chunk_no,
                total_chars,
                options.overlap_chars,
            );

            Ok(PreparedDocumentEmbedding {
                chunk_id,
                doc_id: doc_id.clone(),
                chunk_no,
                content: chunk.text,
                metadata,
                embedding,
            })
        })
        .collect()
}

fn ingest_document_impl(
    target_table: &str,
    request: DocumentEmbeddingRequest<'_>,
    delete_missing: bool,
) -> Result<Value> {
    let doc_id = require_non_blank("doc_id", request.doc_id)?.to_owned();
    let target_table = resolve_target_table(target_table)?;
    validate_ingest_table_columns(&target_table)?;
    let rows = embed_document_impl(DocumentEmbeddingRequest {
        doc_id: &doc_id,
        ..request
    })?;
    let live_chunk_ids = rows
        .iter()
        .map(|row| row.chunk_id.clone())
        .collect::<HashSet<_>>();

    let upsert_sql = format!(
        "INSERT INTO {target_table} AS target (chunk_id, doc_id, chunk_no, content, metadata, embedding)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (chunk_id) DO UPDATE
         SET doc_id = EXCLUDED.doc_id,
             chunk_no = EXCLUDED.chunk_no,
             content = EXCLUDED.content,
             metadata = EXCLUDED.metadata,
             embedding = EXCLUDED.embedding
         WHERE target.doc_id IS DISTINCT FROM EXCLUDED.doc_id
            OR target.chunk_no IS DISTINCT FROM EXCLUDED.chunk_no
            OR target.content IS DISTINCT FROM EXCLUDED.content
            OR target.metadata IS DISTINCT FROM EXCLUDED.metadata
            OR target.embedding IS DISTINCT FROM EXCLUDED.embedding
         RETURNING 1"
    );
    let existing_ids_sql = format!(
        "SELECT COALESCE(array_agg(chunk_id ORDER BY chunk_no, chunk_id), ARRAY[]::text[])
         FROM {target_table}
         WHERE doc_id = $1"
    );
    let delete_sql = format!("DELETE FROM {target_table} WHERE doc_id = $1 AND chunk_id = $2");
    let mut written = 0_usize;
    let mut deleted = 0_usize;

    Spi::connect_mut(|client| {
        let existing_chunk_ids = if delete_missing {
            client
                .select(
                    &existing_ids_sql,
                    Some(1),
                    &[DatumWithOid::from(doc_id.as_str())],
                )?
                .first()
                .get_one::<Vec<String>>()?
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        for row in &rows {
            let metadata = JsonB(row.metadata.clone());
            let updated = client.update(
                &upsert_sql,
                Some(1),
                &[
                    DatumWithOid::from(row.chunk_id.as_str()),
                    DatumWithOid::from(row.doc_id.as_str()),
                    DatumWithOid::from(row.chunk_no),
                    DatumWithOid::from(row.content.as_str()),
                    DatumWithOid::from(metadata),
                    DatumWithOid::from(row.embedding.clone()),
                ],
            )?;
            written += updated.len();
        }

        if delete_missing {
            for existing_chunk_id in existing_chunk_ids {
                if live_chunk_ids.contains(&existing_chunk_id) {
                    continue;
                }

                deleted += client
                    .update(
                        &delete_sql,
                        None,
                        &[
                            DatumWithOid::from(doc_id.as_str()),
                            DatumWithOid::from(existing_chunk_id.as_str()),
                        ],
                    )?
                    .len();
            }
        }

        Ok::<(), pgrx::spi::Error>(())
    })?;

    Ok(json!({
        "table": target_table,
        "doc_id": doc_id,
        "chunk_count": rows.len(),
        "written": written,
        "unchanged": rows.len().saturating_sub(written),
        "deleted": deleted,
        "delete_missing": delete_missing,
    }))
}

fn chunk_text_impl(input: &str, chunk_chars: i32, overlap_chars: i32) -> Result<Vec<String>> {
    let input = require_non_blank("input", input)?;
    let options = validate_chunking_options(chunk_chars, overlap_chars)?;

    Ok(chunk_text_value(input, options)
        .into_iter()
        .map(|chunk| chunk.text)
        .collect())
}

fn chunk_document_impl(
    input: &str,
    metadata: Option<&Value>,
    chunk_chars: i32,
    overlap_chars: i32,
) -> Result<Vec<(i32, String, JsonB)>> {
    let input = require_non_blank("input", input)?;
    let options = validate_chunking_options(chunk_chars, overlap_chars)?;
    let metadata = validate_chunk_metadata(metadata)?;
    let total_chars = input.chars().count();

    chunk_text_value(input, options)
        .into_iter()
        .enumerate()
        .map(|(index, chunk)| {
            let index = i32::try_from(index + 1).map_err(|_| {
                Error::invalid_argument(
                    "input",
                    "contains too many chunks to return 1-based row indexes safely",
                    format!("pass fewer than {} chunks", i32::MAX),
                )
            })?;
            let metadata = annotate_chunk_metadata(
                &metadata,
                &chunk,
                index,
                total_chars,
                options.overlap_chars,
            );

            Ok((index, chunk.text, JsonB(metadata)))
        })
        .collect()
}

fn embed_many_impl(
    inputs: &[String],
    model: Option<&str>,
    normalize: bool,
) -> Result<Vec<Vec<f32>>> {
    if inputs.is_empty() {
        return Err(Error::invalid_argument(
            "inputs",
            "must contain at least one text value",
            "pass a non-empty text[] array",
        ));
    }

    execution::ExecutionContext::new("embed", || embed_request_audit_payload(inputs, normalize))
        .run_embedding(
            model,
            || {
                inputs
                    .iter()
                    .map(|input| require_non_blank("input", input).map(str::to_owned))
                    .collect::<Result<Vec<_>>>()
            },
            |settings, validated_inputs| {
                backend::embed_response(settings, &validated_inputs, normalize)
            },
            |vectors| embed_response_audit_payload(vectors, normalize),
        )
}

fn embed_request_audit_payload(inputs: &[String], normalize: bool) -> Value {
    json!({
        "inputs": inputs,
        "normalize": normalize,
    })
}

fn embed_response_audit_payload(vectors: &[Vec<f32>], normalize: bool) -> Value {
    json!({
        "vector_count": vectors.len(),
        "dimension": vectors.first().map(Vec::len),
        "normalize": normalize,
    })
}

fn keyword_rank_impl(
    query: &str,
    documents: &[String],
    top_n: Option<i32>,
    text_search_config: Option<&str>,
    normalization: i32,
) -> Result<Vec<RankRow>> {
    let query = require_non_blank("query", query)?;
    let documents = validate_rerank_documents(documents)?;
    let top_n = validate_rerank_top_n(top_n)?;
    let text_search_config = resolve_text_search_config(text_search_config)?;
    let normalization = validate_keyword_normalization(normalization)?;
    let ranked = keyword_rank_results(query, &documents, &text_search_config, normalization)?;

    build_rank_rows(&documents, &ranked, top_n)
}

fn rrf_score_impl(
    semantic_rank: Option<i32>,
    keyword_rank: Option<i32>,
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
) -> Result<f64> {
    let semantic_rank = validate_optional_rank("semantic_rank", semantic_rank)?;
    let keyword_rank = validate_optional_rank("keyword_rank", keyword_rank)?;
    let (semantic_weight, keyword_weight) =
        validate_hybrid_weights(semantic_weight, keyword_weight)?;
    let rrf_k = validate_rrf_k(rrf_k)?;

    if semantic_rank.is_none() && keyword_rank.is_none() {
        return Err(Error::invalid_argument(
            "semantic_rank",
            "must be present when keyword_rank is null",
            "pass at least one 1-based rank value",
        ));
    }

    Ok(rrf_score_from_ranks(
        semantic_rank,
        keyword_rank,
        semantic_weight,
        keyword_weight,
        rrf_k,
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "hybrid retrieval needs a few explicit controls and the SQL API keeps them flat"
)]
fn hybrid_rank_impl(
    query: &str,
    documents: &[String],
    top_n: Option<i32>,
    model: Option<&str>,
    text_search_config: Option<&str>,
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
    normalization: i32,
) -> Result<Vec<HybridRankRow>> {
    let query = require_non_blank("query", query)?;
    let documents = validate_rerank_documents(documents)?;
    let top_n = validate_rerank_top_n(top_n)?;
    let text_search_config = resolve_text_search_config(text_search_config)?;
    let normalization = validate_keyword_normalization(normalization)?;
    let (semantic_weight, keyword_weight) =
        validate_hybrid_weights(semantic_weight, keyword_weight)?;
    let rrf_k = validate_rrf_k(rrf_k)?;
    let semantic = semantic_rank_results(query, &documents, model, None)?;
    let keyword = keyword_rank_results(query, &documents, &text_search_config, normalization)?;

    fuse_hybrid_rankings(
        &documents,
        &semantic,
        &keyword,
        top_n,
        semantic_weight,
        keyword_weight,
        rrf_k,
    )
}

fn rerank_impl(
    query: &str,
    documents: &[String],
    top_n: Option<i32>,
    model: Option<&str>,
) -> Result<Vec<RankRow>> {
    let query = require_non_blank("query", query)?;
    let documents = validate_rerank_documents(documents)?;
    let top_n = validate_rerank_top_n(top_n)?;
    let ranked = semantic_rank_results(query, &documents, model, top_n)?;

    build_rank_rows(&documents, &ranked, None)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the batteries-included RAG helper exposes retrieval and generation controls directly"
)]
fn rag_impl(
    query: &str,
    documents: &[String],
    system_prompt: Option<&str>,
    model: Option<&str>,
    retrieval: Option<&str>,
    retrieval_model: Option<&str>,
    top_n: i32,
    temperature: f64,
    max_tokens: Option<i32>,
    text_search_config: Option<&str>,
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
    normalization: i32,
) -> Result<Value> {
    let result = rag_result_impl(
        query,
        documents,
        system_prompt,
        model,
        retrieval,
        retrieval_model,
        top_n,
        temperature,
        max_tokens,
        text_search_config,
        semantic_weight,
        keyword_weight,
        rrf_k,
        normalization,
    )?;

    Ok(rag_result_to_value(&result))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the batteries-included RAG helper exposes retrieval and generation controls directly"
)]
fn rag_text_impl(
    query: &str,
    documents: &[String],
    system_prompt: Option<&str>,
    model: Option<&str>,
    retrieval: Option<&str>,
    retrieval_model: Option<&str>,
    top_n: i32,
    temperature: f64,
    max_tokens: Option<i32>,
    text_search_config: Option<&str>,
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
    normalization: i32,
) -> Result<String> {
    rag_result_impl(
        query,
        documents,
        system_prompt,
        model,
        retrieval,
        retrieval_model,
        top_n,
        temperature,
        max_tokens,
        text_search_config,
        semantic_weight,
        keyword_weight,
        rrf_k,
        normalization,
    )
    .map(|result| result.answer)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the batteries-included RAG helper exposes retrieval and generation controls directly"
)]
fn rag_result_impl(
    query: &str,
    documents: &[String],
    system_prompt: Option<&str>,
    model: Option<&str>,
    retrieval: Option<&str>,
    retrieval_model: Option<&str>,
    top_n: i32,
    temperature: f64,
    max_tokens: Option<i32>,
    text_search_config: Option<&str>,
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
    normalization: i32,
) -> Result<RagResult> {
    let query = require_non_blank("query", query)?;
    let documents = validate_rerank_documents(documents)?;
    let system_prompt = validate_optional_system_prompt(system_prompt)?;
    let retrieval = validate_rag_retrieval(retrieval)?;
    let top_n = validate_rag_top_n(top_n)?;
    let context = retrieve_rag_context(
        retrieval,
        query,
        &documents,
        top_n,
        retrieval_model,
        text_search_config,
        semantic_weight,
        keyword_weight,
        rrf_k,
        normalization,
    )?;
    let prompt = build_rag_prompt(query, &context);
    let effective_system_prompt = system_prompt
        .as_deref()
        .unwrap_or(DEFAULT_RAG_SYSTEM_PROMPT)
        .to_owned();
    let messages = vec![
        build_message("system", &effective_system_prompt)?,
        build_message("user", &prompt)?,
    ];
    let response = chat_impl_from_values(
        &messages,
        model,
        temperature,
        max_tokens,
        backend::Feature::Complete,
        ChatRequestExtensions::default(),
    )?;
    let answer = backend::extract_text(&response)?;

    Ok(RagResult {
        query: query.to_owned(),
        retrieval,
        prompt,
        system_prompt: effective_system_prompt,
        answer,
        documents_considered: documents.len(),
        top_n,
        context,
        response,
    })
}

fn semantic_rank_results(
    query: &str,
    documents: &[String],
    model: Option<&str>,
    top_n: Option<usize>,
) -> Result<Vec<RankedDocument>> {
    execution::ExecutionContext::new("rerank", || {
        rerank_request_audit_payload(query, documents, top_n)
    })
    .run_rerank(
        model,
        |settings| {
            let ranked = backend::rerank_response(settings, query, documents, top_n)?
                .into_iter()
                .map(|result| RankedDocument {
                    index: result.index,
                    score: result.score,
                })
                .collect::<Vec<_>>();

            Ok(ranked)
        },
        |ranked| rerank_response_audit_payload(documents, ranked),
    )
}

fn rerank_request_audit_payload(query: &str, documents: &[String], top_n: Option<usize>) -> Value {
    json!({
        "query": query,
        "documents": documents,
        "top_n": top_n,
    })
}

fn rerank_response_audit_payload(documents: &[String], ranked: &[RankedDocument]) -> Value {
    json!(
        ranked
            .iter()
            .enumerate()
            .map(|(position, ranked)| {
                json!({
                    "rank": position + 1,
                    "index": ranked.index + 1,
                    "score": ranked.score,
                    "document": documents.get(ranked.index),
                })
            })
            .collect::<Vec<_>>()
    )
}

fn keyword_rank_results(
    query: &str,
    documents: &[String],
    text_search_config: &str,
    normalization: i32,
) -> Result<Vec<RankedDocument>> {
    let ranked = Spi::get_one_with_args::<JsonB>(
        r"WITH docs AS (
            SELECT (ord - 1)::int AS index, document
            FROM unnest($1::text[]) WITH ORDINALITY AS docs(document, ord)
        ),
        doc_vectors AS (
            SELECT
                index,
                to_tsvector($2::regconfig, document) AS document_vector
            FROM docs
        ),
        query_terms AS (
            SELECT DISTINCT term
            FROM unnest(
                tsvector_to_array(to_tsvector($2::regconfig, $3))
            ) AS query_terms(term)
        ),
        ranked AS (
            SELECT
                docs.index,
                COALESCE(
                    SUM(
                        ts_rank_cd(
                            docs.document_vector,
                            plainto_tsquery($2::regconfig, query_terms.term),
                            $4
                        )::float8
                    ),
                    0.0
                ) AS score
            FROM doc_vectors AS docs
            LEFT JOIN query_terms ON TRUE
            GROUP BY docs.index
        )
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object('index', index, 'score', score)
                ORDER BY score DESC, index ASC
            ),
            '[]'::jsonb
        )
        FROM ranked
        WHERE score > 0",
        &[
            DatumWithOid::from(documents.to_vec()),
            DatumWithOid::from(text_search_config),
            DatumWithOid::from(query),
            DatumWithOid::from(normalization),
        ],
    )?
    .map_or_else(|| json!([]), |ranked| ranked.0);

    parse_ranked_documents(&ranked, documents.len(), "keyword ranking")
}

fn build_rank_rows(
    documents: &[String],
    ranked: &[RankedDocument],
    top_n: Option<usize>,
) -> Result<Vec<RankRow>> {
    ranked
        .iter()
        .take(top_n.unwrap_or(usize::MAX))
        .enumerate()
        .map(|(rank, result)| {
            let rank = one_based_i32(
                "documents",
                rank + 1,
                "pass fewer than 2147483647 documents",
            )?;
            let index = one_based_i32(
                "documents",
                result.index + 1,
                "pass fewer than 2147483647 documents",
            )?;
            let document = documents.get(result.index).cloned().ok_or_else(|| {
                Error::Internal(format!(
                    "ranked document index {} was out of bounds for {} input documents",
                    result.index,
                    documents.len()
                ))
            })?;

            Ok((rank, index, document, result.score))
        })
        .collect()
}

#[expect(
    clippy::too_many_arguments,
    reason = "the RAG helper threads retrieval controls through one shared implementation"
)]
fn retrieve_rag_context(
    retrieval: RagRetrievalStrategy,
    query: &str,
    documents: &[String],
    top_n: usize,
    retrieval_model: Option<&str>,
    text_search_config: Option<&str>,
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: i32,
    normalization: i32,
) -> Result<Vec<RagContextRow>> {
    match retrieval {
        RagRetrievalStrategy::Semantic => {
            let ranked = semantic_rank_results(query, documents, retrieval_model, Some(top_n))?;
            let rows = build_rank_rows(documents, &ranked, None)?;

            Ok(rows
                .into_iter()
                .map(rank_row_to_rag_context_semantic)
                .collect())
        }
        RagRetrievalStrategy::Keyword => {
            let text_search_config = resolve_text_search_config(text_search_config)?;
            let normalization = validate_keyword_normalization(normalization)?;
            let ranked =
                keyword_rank_results(query, documents, &text_search_config, normalization)?;
            let rows = build_rank_rows(documents, &ranked, Some(top_n))?;

            Ok(rows
                .into_iter()
                .map(rank_row_to_rag_context_keyword)
                .collect())
        }
        RagRetrievalStrategy::Hybrid => {
            let text_search_config = resolve_text_search_config(text_search_config)?;
            let normalization = validate_keyword_normalization(normalization)?;
            let (semantic_weight, keyword_weight) =
                validate_hybrid_weights(semantic_weight, keyword_weight)?;
            let rrf_k = validate_rrf_k(rrf_k)?;
            let semantic = semantic_rank_results(query, documents, retrieval_model, None)?;
            let keyword =
                keyword_rank_results(query, documents, &text_search_config, normalization)?;
            let rows = fuse_hybrid_rankings(
                documents,
                &semantic,
                &keyword,
                Some(top_n),
                semantic_weight,
                keyword_weight,
                rrf_k,
            )?;

            Ok(rows
                .into_iter()
                .map(hybrid_rank_row_to_rag_context)
                .collect())
        }
    }
}

fn rank_row_to_rag_context_semantic(row: RankRow) -> RagContextRow {
    let (rank, index, document, score) = row;

    RagContextRow {
        rank,
        index,
        document,
        score,
        semantic_rank: Some(rank),
        keyword_rank: None,
        semantic_score: Some(score),
        keyword_score: None,
    }
}

fn rank_row_to_rag_context_keyword(row: RankRow) -> RagContextRow {
    let (rank, index, document, score) = row;

    RagContextRow {
        rank,
        index,
        document,
        score,
        semantic_rank: None,
        keyword_rank: Some(rank),
        semantic_score: None,
        keyword_score: Some(score),
    }
}

fn hybrid_rank_row_to_rag_context(row: HybridRankRow) -> RagContextRow {
    let (rank, index, document, score, semantic_rank, keyword_rank, semantic_score, keyword_score) =
        row;

    RagContextRow {
        rank,
        index,
        document,
        score,
        semantic_rank,
        keyword_rank,
        semantic_score,
        keyword_score,
    }
}

fn build_rag_prompt(query: &str, context: &[RagContextRow]) -> String {
    let context_block = if context.is_empty() {
        "[no retrieved context]".to_owned()
    } else {
        context
            .iter()
            .map(|row| format!("[{}] {}", row.rank, row.document))
            .collect::<Vec<_>>()
            .join("\n\n")
    };

    format!("Question: {query}\n\nRetrieved context:\n{context_block}")
}

fn rag_result_to_value(result: &RagResult) -> Value {
    let context = result
        .context
        .iter()
        .map(|row| {
            json!({
                "rank": row.rank,
                "index": row.index,
                "document": &row.document,
                "score": row.score,
                "semantic_rank": row.semantic_rank,
                "keyword_rank": row.keyword_rank,
                "semantic_score": row.semantic_score,
                "keyword_score": row.keyword_score,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "query": &result.query,
        "retrieval": result.retrieval.as_str(),
        "top_n": result.top_n,
        "documents_considered": result.documents_considered,
        "context_documents": result.context.len(),
        "system_prompt": &result.system_prompt,
        "prompt": &result.prompt,
        "answer": &result.answer,
        "context": context,
        "response": &result.response,
    })
}

fn parse_ranked_documents(
    ranked: &Value,
    documents_len: usize,
    context: &str,
) -> Result<Vec<RankedDocument>> {
    let Some(ranked) = ranked.as_array() else {
        return Err(Error::Internal(format!(
            "{context} returned a non-array payload"
        )));
    };

    ranked
        .iter()
        .map(|result| {
            let Some(index) = result.get("index").and_then(Value::as_u64) else {
                return Err(Error::Internal(format!(
                    "{context} returned a row without an integer index"
                )));
            };
            let index = usize::try_from(index).map_err(|_| {
                Error::Internal(format!(
                    "{context} returned an index that could not be represented as usize"
                ))
            })?;

            if index >= documents_len {
                return Err(Error::Internal(format!(
                    "{context} returned index {index} for {documents_len} input documents"
                )));
            }

            let Some(score) = result.get("score").and_then(Value::as_f64) else {
                return Err(Error::Internal(format!(
                    "{context} returned a row without a numeric score"
                )));
            };

            Ok(RankedDocument { index, score })
        })
        .collect()
}

fn fuse_hybrid_rankings(
    documents: &[String],
    semantic: &[RankedDocument],
    keyword: &[RankedDocument],
    top_n: Option<usize>,
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: u32,
) -> Result<Vec<HybridRankRow>> {
    let semantic_details = collect_rank_details(documents.len(), semantic, "semantic")?;
    let keyword_details = collect_rank_details(documents.len(), keyword, "keyword")?;

    let mut rows = documents
        .iter()
        .enumerate()
        .filter_map(|(index, document)| {
            let semantic_detail = semantic_details.get(index).copied().flatten();
            let keyword_detail = keyword_details.get(index).copied().flatten();

            if semantic_detail.is_none() && keyword_detail.is_none() {
                return None;
            }

            let score = rrf_score_from_ranks(
                semantic_detail.map(|(rank, _)| rank),
                keyword_detail.map(|(rank, _)| rank),
                semantic_weight,
                keyword_weight,
                rrf_k,
            );

            Some((
                index,
                document.clone(),
                score,
                semantic_detail,
                keyword_detail,
            ))
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        right
            .2
            .total_cmp(&left.2)
            .then(option_rank_sort_key(left.3).cmp(&option_rank_sort_key(right.3)))
            .then(option_rank_sort_key(left.4).cmp(&option_rank_sort_key(right.4)))
            .then(left.0.cmp(&right.0))
    });

    if let Some(top_n) = top_n {
        rows.truncate(top_n);
    }

    rows.into_iter()
        .enumerate()
        .map(
            |(rank, (index, document, score, semantic_detail, keyword_detail))| {
                let rank = one_based_i32(
                    "documents",
                    rank + 1,
                    "pass fewer than 2147483647 documents",
                )?;
                let index = one_based_i32(
                    "documents",
                    index + 1,
                    "pass fewer than 2147483647 documents",
                )?;
                let semantic_rank = semantic_detail
                    .map(|(rank, _)| rank_detail_to_i32(rank, "semantic"))
                    .transpose()?;
                let keyword_rank = keyword_detail
                    .map(|(rank, _)| rank_detail_to_i32(rank, "keyword"))
                    .transpose()?;

                Ok((
                    rank,
                    index,
                    document,
                    score,
                    semantic_rank,
                    keyword_rank,
                    semantic_detail.map(|(_, score)| score),
                    keyword_detail.map(|(_, score)| score),
                ))
            },
        )
        .collect()
}

fn collect_rank_details(
    documents_len: usize,
    ranked: &[RankedDocument],
    signal: &str,
) -> Result<Vec<Option<(u32, f64)>>> {
    let mut details = vec![None; documents_len];

    for (rank, result) in ranked.iter().enumerate() {
        let rank = u32::try_from(rank + 1).map_err(|_| {
            Error::invalid_argument(
                "documents",
                format!("contains too many {signal} ranks to fuse safely"),
                "pass fewer than 4294967295 documents",
            )
        })?;
        let entry = details.get_mut(result.index).ok_or_else(|| {
            Error::Internal(format!(
                "{signal} rank index {} was out of bounds for {documents_len} input documents",
                result.index
            ))
        })?;
        *entry = Some((rank, result.score));
    }

    Ok(details)
}

fn rank_detail_to_i32(rank: u32, signal: &str) -> Result<i32> {
    i32::try_from(rank).map_err(|_| {
        Error::invalid_argument(
            "documents",
            format!("contains too many {signal} ranks to return 1-based row indexes safely"),
            "pass fewer than 2147483647 documents",
        )
    })
}

fn validate_rag_retrieval(retrieval: Option<&str>) -> Result<RagRetrievalStrategy> {
    let retrieval = match retrieval {
        Some(retrieval) => require_non_blank("retrieval", retrieval)?,
        None => RagRetrievalStrategy::Hybrid.as_str(),
    };

    RagRetrievalStrategy::parse(retrieval)
}

fn resolve_text_search_config(text_search_config: Option<&str>) -> Result<String> {
    let text_search_config = match text_search_config {
        Some(text_search_config) => require_non_blank("text_search_config", text_search_config)?,
        None => "english",
    };

    let exists = match text_search_config.split_once('.') {
        Some((schema_name, config_name)) => {
            if schema_name.is_empty() || config_name.is_empty() || config_name.contains('.') {
                return Err(Error::invalid_argument(
                    "text_search_config",
                    format!(
                        "must be an unqualified config name or schema-qualified as schema.config, got '{text_search_config}'"
                    ),
                    "pass a valid configuration like 'english', 'simple', or 'pg_catalog.english'",
                ));
            }

            Spi::get_one_with_args::<bool>(
                r"SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_ts_config AS cfg
                    JOIN pg_catalog.pg_namespace AS nsp
                        ON nsp.oid = cfg.cfgnamespace
                    WHERE nsp.nspname = $1
                        AND cfg.cfgname = $2
                )",
                &[
                    DatumWithOid::from(schema_name),
                    DatumWithOid::from(config_name),
                ],
            )?
            .unwrap_or(false)
        }
        None => Spi::get_one_with_args::<bool>(
            r"SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_ts_config AS cfg
                WHERE cfg.cfgname = $1
            )",
            &[DatumWithOid::from(text_search_config)],
        )?
        .unwrap_or(false),
    };

    if exists {
        Ok(text_search_config.to_owned())
    } else {
        Err(Error::invalid_argument(
            "text_search_config",
            format!("text search configuration '{text_search_config}' was not found"),
            "pass a valid configuration like 'english' or 'simple'",
        ))
    }
}

fn validate_rag_top_n(top_n: i32) -> Result<usize> {
    validate_rerank_top_n(Some(top_n))?
        .ok_or_else(|| Error::Internal("validated RAG top_n should always be present".to_owned()))
}

fn validate_keyword_normalization(normalization: i32) -> Result<i32> {
    if normalization < 0 {
        Err(Error::invalid_argument(
            "normalization",
            format!("must be greater than or equal to zero, got {normalization}"),
            "pass a non-negative ts_rank_cd normalization bitmask like 32",
        ))
    } else {
        Ok(normalization)
    }
}

fn validate_hybrid_weights(semantic_weight: f64, keyword_weight: f64) -> Result<(f64, f64)> {
    validate_non_negative_f64("semantic_weight", semantic_weight)?;
    validate_non_negative_f64("keyword_weight", keyword_weight)?;

    if semantic_weight == 0.0 && keyword_weight == 0.0 {
        return Err(Error::invalid_argument(
            "semantic_weight",
            "must be positive when keyword_weight is also zero",
            "pass a positive weight for semantic_weight, keyword_weight, or both",
        ));
    }

    Ok((semantic_weight, keyword_weight))
}

fn validate_non_negative_f64(argument: &str, value: f64) -> Result<()> {
    if !value.is_finite() {
        return Err(Error::invalid_argument(
            argument,
            format!("must be finite, got {value}"),
            "pass a finite floating-point value like 1.0",
        ));
    }

    if value < 0.0 {
        return Err(Error::invalid_argument(
            argument,
            format!("must be greater than or equal to zero, got {value}"),
            "pass zero or a positive floating-point value like 1.0",
        ));
    }

    Ok(())
}

fn validate_rrf_k(rrf_k: i32) -> Result<u32> {
    if rrf_k <= 0 {
        return Err(Error::invalid_argument(
            "rrf_k",
            format!("must be greater than zero, got {rrf_k}"),
            "pass a positive integer like 60",
        ));
    }

    u32::try_from(rrf_k).map_err(|_| {
        Error::invalid_argument(
            "rrf_k",
            "must be representable as u32",
            "pass a positive integer like 60",
        )
    })
}

fn validate_optional_rank(argument: &str, rank: Option<i32>) -> Result<Option<u32>> {
    rank.map(|rank| {
        if rank <= 0 {
            return Err(Error::invalid_argument(
                argument,
                format!("must be greater than zero when present, got {rank}"),
                "pass a 1-based rank like 1, 2, or 3",
            ));
        }

        u32::try_from(rank).map_err(|_| {
            Error::invalid_argument(
                argument,
                "must be representable as u32",
                "pass a positive rank like 1, 2, or 3",
            )
        })
    })
    .transpose()
}

fn rrf_score_from_ranks(
    semantic_rank: Option<u32>,
    keyword_rank: Option<u32>,
    semantic_weight: f64,
    keyword_weight: f64,
    rrf_k: u32,
) -> f64 {
    let rrf_k = f64::from(rrf_k);
    semantic_rank.map_or(0.0, |semantic_rank| {
        semantic_weight / (rrf_k + f64::from(semantic_rank))
    }) + keyword_rank.map_or(0.0, |keyword_rank| {
        keyword_weight / (rrf_k + f64::from(keyword_rank))
    })
}

fn option_rank_sort_key(detail: Option<(u32, f64)>) -> u32 {
    detail.map_or(u32::MAX, |(rank, _)| rank)
}

fn one_based_i32(argument: &str, value: usize, fix: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| {
        Error::invalid_argument(
            argument,
            "contains too many entries to return 1-based row indexes safely",
            fix,
        )
    })
}

fn validate_batch_prompts(prompts: &[String]) -> Result<Vec<String>> {
    if prompts.is_empty() {
        return Err(Error::invalid_argument(
            "prompts",
            "must contain at least one text value",
            "pass a non-empty text[] array",
        ));
    }

    prompts
        .iter()
        .map(|prompt| require_non_blank("prompt", prompt).map(str::to_owned))
        .collect()
}

fn validate_rerank_documents(documents: &[String]) -> Result<Vec<String>> {
    if documents.is_empty() {
        return Err(Error::invalid_argument(
            "documents",
            "must contain at least one text value",
            "pass a non-empty text[] array",
        ));
    }

    documents
        .iter()
        .enumerate()
        .map(|(index, document)| {
            require_non_blank(&format!("documents[{index}]"), document).map(str::to_owned)
        })
        .collect()
}

fn validate_rerank_top_n(top_n: Option<i32>) -> Result<Option<usize>> {
    top_n
        .map(|top_n| {
            if top_n <= 0 {
                return Err(Error::invalid_argument(
                    "top_n",
                    format!("must be greater than zero when present, got {top_n}"),
                    "omit top_n or pass a positive integer like 5",
                ));
            }

            usize::try_from(top_n).map_err(|_| {
                Error::invalid_argument(
                    "top_n",
                    "must be representable as usize",
                    "omit top_n or pass a positive integer like 5",
                )
            })
        })
        .transpose()
}

fn validate_chunking_options(chunk_chars: i32, overlap_chars: i32) -> Result<ChunkingOptions> {
    if chunk_chars <= 0 {
        return Err(Error::invalid_argument(
            "chunk_chars",
            format!("must be greater than zero, got {chunk_chars}"),
            "pass a positive character count like 1000",
        ));
    }

    if overlap_chars < 0 {
        return Err(Error::invalid_argument(
            "overlap_chars",
            format!("must be greater than or equal to zero, got {overlap_chars}"),
            "pass zero or a positive character count like 200",
        ));
    }

    if overlap_chars >= chunk_chars {
        return Err(Error::invalid_argument(
            "overlap_chars",
            format!(
                "must be smaller than chunk_chars, got overlap_chars={overlap_chars} and chunk_chars={chunk_chars}"
            ),
            "pass an overlap smaller than the chunk size, for example chunk_chars => 1000 and overlap_chars => 200",
        ));
    }

    Ok(ChunkingOptions {
        chunk_chars: usize::try_from(chunk_chars).map_err(|_| {
            Error::invalid_argument(
                "chunk_chars",
                "must be representable as usize",
                "pass a positive character count like 1000",
            )
        })?,
        overlap_chars: usize::try_from(overlap_chars).map_err(|_| {
            Error::invalid_argument(
                "overlap_chars",
                "must be representable as usize",
                "pass zero or a positive character count like 200",
            )
        })?,
    })
}

fn validate_chunk_metadata(metadata: Option<&Value>) -> Result<Value> {
    let metadata = metadata.cloned().unwrap_or_else(|| json!({}));

    if metadata.is_object() {
        Ok(metadata)
    } else {
        Err(Error::invalid_argument(
            "metadata",
            "must be a JSON object or null",
            r#"pass jsonb like '{"doc_id":"vacuum-guide"}'::jsonb or omit metadata"#,
        ))
    }
}

fn resolve_target_table(target_table: &str) -> Result<String> {
    let target_table = require_non_blank("target_table", target_table)?;

    Spi::get_one_with_args::<String>(
        "SELECT to_regclass($1)::text",
        &[DatumWithOid::from(target_table)],
    )?
    .ok_or_else(|| {
        Error::invalid_argument(
            "target_table",
            format!("relation '{target_table}' was not found"),
            "pass an existing table name like 'public.doc_chunks'",
        )
    })
}

fn validate_ingest_table_columns(target_table: &str) -> Result<()> {
    const REQUIRED_COLUMNS: [&str; 6] = [
        "chunk_id",
        "doc_id",
        "chunk_no",
        "content",
        "metadata",
        "embedding",
    ];

    let columns = Spi::get_one_with_args::<Vec<String>>(
        "SELECT COALESCE(array_agg(attname::text ORDER BY attnum), ARRAY[]::text[])
         FROM pg_attribute
         WHERE attrelid = to_regclass($1)
           AND attnum > 0
           AND NOT attisdropped",
        &[DatumWithOid::from(target_table)],
    )?
    .unwrap_or_default();
    let missing_columns = REQUIRED_COLUMNS
        .into_iter()
        .filter(|required| !columns.iter().any(|column| column == required))
        .collect::<Vec<_>>();

    if missing_columns.is_empty() {
        Ok(())
    } else {
        Err(Error::invalid_argument(
            "target_table",
            format!(
                "must expose canonical ingestion columns chunk_id, doc_id, chunk_no, content, metadata, and embedding; missing {}",
                missing_columns.join(", ")
            ),
            "create a table with those columns or use postllm.embed_document(...) and write the INSERT yourself",
        ))
    }
}

fn chunk_text_value(input: &str, options: ChunkingOptions) -> Vec<TextChunk> {
    let boundaries = char_boundary_indices(input);
    let total_chars = boundaries.len().saturating_sub(1);
    let tail_slack = options.chunk_chars.div_ceil(10).clamp(1, 32);

    if total_chars <= options.chunk_chars {
        return vec![build_text_chunk(input, &boundaries, 0, total_chars)];
    }

    let mut chunks = Vec::new();
    let mut start_char = 0;

    while start_char < total_chars {
        let ideal_end = total_chars.min(start_char + options.chunk_chars);
        let end_char = if total_chars > ideal_end && total_chars - ideal_end <= tail_slack {
            // Let the final chunk absorb a short tail instead of creating a tiny fragment.
            total_chars
        } else {
            choose_chunk_end(input, &boundaries, start_char, ideal_end, options)
        };
        let chunk = build_text_chunk(input, &boundaries, start_char, end_char);

        if !chunk.text.is_empty() {
            chunks.push(chunk);
        }

        if end_char >= total_chars {
            break;
        }

        let next_start = end_char.saturating_sub(options.overlap_chars);
        start_char = if next_start > start_char {
            next_start
        } else {
            end_char
        };
    }

    chunks
}

fn choose_chunk_end(
    input: &str,
    boundaries: &[usize],
    start_char: usize,
    ideal_end: usize,
    options: ChunkingOptions,
) -> usize {
    if ideal_end <= start_char + 1 || ideal_end == boundaries.len().saturating_sub(1) {
        return ideal_end;
    }

    let min_end = (start_char + options.chunk_chars / 2)
        .max(start_char + options.overlap_chars + 1)
        .min(ideal_end);
    let mut best_line_break = None;
    let mut best_sentence_break = None;
    let mut best_whitespace_break = None;

    for end_char in (min_end..=ideal_end).rev() {
        let boundary = classify_chunk_boundary(input, boundaries, end_char);

        if boundary.is_paragraph() {
            return end_char;
        }

        if boundary.is_line_break() && best_line_break.is_none() {
            best_line_break = Some(end_char);
        }

        if boundary.is_sentence_break() && best_sentence_break.is_none() {
            best_sentence_break = Some(end_char);
        }

        if boundary.is_whitespace() && best_whitespace_break.is_none() {
            best_whitespace_break = Some(end_char);
        }
    }

    best_line_break
        .or(best_sentence_break)
        .or(best_whitespace_break)
        .unwrap_or(ideal_end)
}

fn build_text_chunk(
    input: &str,
    boundaries: &[usize],
    start_char: usize,
    end_char: usize,
) -> TextChunk {
    let (start_char, end_char) = trim_chunk_bounds(input, boundaries, start_char, end_char);
    let start_byte = boundary_byte(boundaries, start_char, input.len());
    let end_byte = boundary_byte(boundaries, end_char, input.len());

    TextChunk {
        start_char,
        end_char_exclusive: end_char,
        text: input[start_byte..end_byte].to_owned(),
    }
}

fn trim_chunk_bounds(
    input: &str,
    boundaries: &[usize],
    mut start_char: usize,
    mut end_char: usize,
) -> (usize, usize) {
    while start_char < end_char
        && char_at(input, boundaries, start_char).is_some_and(char::is_whitespace)
    {
        start_char += 1;
    }

    while end_char > start_char
        && char_at(input, boundaries, end_char - 1).is_some_and(char::is_whitespace)
    {
        end_char -= 1;
    }

    (start_char, end_char)
}

fn char_boundary_indices(input: &str) -> Vec<usize> {
    let mut boundaries = input
        .char_indices()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    boundaries.push(input.len());
    boundaries
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkBoundary {
    None,
    Whitespace,
    Sentence,
    LineBreak,
    Paragraph,
}

impl ChunkBoundary {
    const fn is_whitespace(self) -> bool {
        matches!(self, Self::Whitespace)
    }

    const fn is_sentence_break(self) -> bool {
        matches!(self, Self::Sentence)
    }

    const fn is_line_break(self) -> bool {
        matches!(self, Self::LineBreak)
    }

    const fn is_paragraph(self) -> bool {
        matches!(self, Self::Paragraph)
    }
}

fn classify_chunk_boundary(input: &str, boundaries: &[usize], end_char: usize) -> ChunkBoundary {
    let end_byte = boundary_byte(boundaries, end_char, input.len());
    let prefix = &input[..end_byte];

    if prefix.ends_with("\n\n") {
        return ChunkBoundary::Paragraph;
    }

    if prefix.ends_with('\n') {
        return ChunkBoundary::LineBreak;
    }

    let previous_char = end_char
        .checked_sub(1)
        .and_then(|index| char_at(input, boundaries, index));
    let next_char = char_at(input, boundaries, end_char);

    if previous_char.is_some_and(|character| matches!(character, '.' | '!' | '?' | ';' | ':'))
        && next_char.is_none_or(char::is_whitespace)
    {
        return ChunkBoundary::Sentence;
    }

    if previous_char.is_some_and(char::is_whitespace) {
        return ChunkBoundary::Whitespace;
    }

    ChunkBoundary::None
}

fn char_at(input: &str, boundaries: &[usize], char_index: usize) -> Option<char> {
    let start = *boundaries.get(char_index)?;
    let end = *boundaries.get(char_index + 1)?;

    input[start..end].chars().next()
}

fn boundary_byte(boundaries: &[usize], char_index: usize, fallback: usize) -> usize {
    boundaries.get(char_index).copied().unwrap_or(fallback)
}

fn annotate_chunk_metadata(
    metadata: &Value,
    chunk: &TextChunk,
    index: i32,
    source_chars: usize,
    overlap_chars: usize,
) -> Value {
    let mut metadata = metadata.clone();

    if let Some(object) = metadata.as_object_mut() {
        object.insert(
            "_postllm_chunk".to_owned(),
            json!({
                "index": index,
                "start_char": chunk.start_char,
                "end_char_exclusive": chunk.end_char_exclusive,
                "chunk_chars": chunk.end_char_exclusive - chunk.start_char,
                "source_chars": source_chars,
                "overlap_chars": overlap_chars,
            }),
        );
    }

    metadata
}

fn deterministic_chunk_id(doc_id: &str, chunk: &TextChunk) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"postllm.chunk.v1");
    hasher.update(b"\0");
    hasher.update(doc_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(chunk.start_char.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(chunk.end_char_exclusive.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(chunk.text.as_bytes());

    let digest = hasher.finalize();
    format!("plc_{}", lower_hex(&digest))
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(hex_digit(byte >> 4));
        encoded.push(hex_digit(byte & 0x0f));
    }

    encoded
}

const fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '0',
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GenerationControls {
    pub(crate) max_tokens: Option<i32>,
}

pub(crate) fn validate_request_controls(
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<GenerationControls> {
    if !(0.0..=2.0).contains(&temperature) {
        return Err(Error::invalid_argument(
            "temperature",
            format!("must be between 0.0 and 2.0, got {temperature}"),
            "pass a value between 0.0 and 2.0",
        ));
    }

    let max_tokens = guc::resolve_generation_max_tokens(max_tokens)?;

    Ok(GenerationControls { max_tokens })
}

fn validate_messages(messages: &[JsonB]) -> Result<Vec<Value>> {
    if messages.is_empty() {
        return Err(Error::invalid_argument(
            "messages",
            "must contain at least one chat message",
            "pass a non-empty jsonb[] array built from postllm.message(...) values",
        ));
    }

    messages
        .iter()
        .enumerate()
        .map(|(index, message)| validate_message(&message.0, index))
        .collect()
}

pub(crate) fn messages_require_multimodal_inputs(messages: &[Value]) -> bool {
    messages.iter().any(message_requires_multimodal_inputs)
}

fn message_requires_multimodal_inputs(message: &Value) -> bool {
    message
        .get("content")
        .and_then(Value::as_array)
        .is_some_and(|parts| {
            parts.iter().any(|part| {
                part.get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|part_type| part_type != "text")
            })
        })
}

fn validate_message(message: &Value, index: usize) -> Result<Value> {
    validate_message_with_argument(message, &format!("messages[{index}]"))
}

fn validate_aggregate_message(message: &Value) -> Result<Value> {
    validate_message_with_argument(message, "message")
}

fn render_template_impl(template: &str, variables: Option<&Value>) -> Result<String> {
    let _ = require_non_blank("template", template)?;

    let empty_variables = json!({});
    let variables = variables.unwrap_or(&empty_variables);
    let Some(variables) = variables.as_object() else {
        return Err(Error::invalid_argument(
            "variables",
            "must be a JSON object",
            r#"pass jsonb like '{"name":"value"}'::jsonb"#,
        ));
    };

    let mut rendered = String::with_capacity(template.len());
    let mut cursor = 0;

    while let Some(relative_start) = template[cursor..].find("{{") {
        let start = cursor + relative_start;
        rendered.push_str(&template[cursor..start]);

        let placeholder_start = start + 2;
        let Some(relative_end) = template[placeholder_start..].find("}}") else {
            return Err(Error::invalid_argument(
                "template",
                "contains an unterminated '{{...}}' placeholder",
                "close every placeholder with '}}'",
            ));
        };
        let placeholder_end = placeholder_start + relative_end;
        let name = template[placeholder_start..placeholder_end].trim();

        if name.is_empty() {
            return Err(Error::invalid_argument(
                "template",
                "contains an empty placeholder",
                "use a named placeholder like {{topic}}",
            ));
        }

        let Some(value) = variables.get(name) else {
            return Err(Error::invalid_argument(
                "variables",
                format!("is missing template variable '{name}'"),
                format!("include key '{name}' in the variables jsonb object"),
            ));
        };

        rendered.push_str(&render_template_value(value)?);
        cursor = placeholder_end + 2;
    }

    rendered.push_str(&template[cursor..]);

    Ok(rendered)
}

pub(crate) fn validate_message_with_argument(message: &Value, argument: &str) -> Result<Value> {
    let Some(role) = message.get("role").and_then(Value::as_str) else {
        return Err(Error::invalid_argument(
            &format!("{argument}.role"),
            "must contain a string role",
            r#"provide JSON like {"role":"user","content":"..."}"#,
        ));
    };

    if role.trim().is_empty() {
        return Err(Error::invalid_argument(
            &format!("{argument}.role"),
            "must not be empty or whitespace-only",
            "use role values like 'system', 'user', 'assistant', or 'tool'",
        ));
    }

    let Some(content) = message.get("content") else {
        return Err(Error::invalid_argument(
            &format!("{argument}.content"),
            "must be present",
            r#"provide JSON like {"role":"user","content":"..."}"#,
        ));
    };

    let has_tool_calls = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .is_some_and(|tool_calls| !tool_calls.is_empty());

    match content {
        Value::String(text) => {
            if text.trim().is_empty() {
                return Err(Error::invalid_argument(
                    &format!("{argument}.content"),
                    "must not be empty or whitespace-only",
                    "pass non-empty text or a non-empty content-part array",
                ));
            }
        }
        Value::Null => {
            if !has_tool_calls {
                return Err(Error::invalid_argument(
                    &format!("{argument}.content"),
                    "may be null only when tool_calls are present",
                    "provide non-null content or include tool_calls on assistant messages",
                ));
            }
        }
        Value::Array(parts) => {
            if parts.is_empty() {
                return Err(Error::invalid_argument(
                    &format!("{argument}.content"),
                    "must not be an empty array",
                    "include at least one content part",
                ));
            }
        }
        Value::Bool(_) | Value::Number(_) | Value::Object(_) => {
            return Err(Error::invalid_argument(
                &format!("{argument}.content"),
                "must be a string, JSON array, or null only for assistant tool-call messages",
                "pass text, content parts, or null only alongside tool_calls",
            ));
        }
    }

    Ok(message.clone())
}

fn build_message(role: &str, content: &str) -> Result<Value> {
    let role = require_non_blank("role", role)?;
    let content = require_non_blank("content", content)?;

    Ok(json!({
        "role": role,
        "content": content,
    }))
}

fn build_message_template(role: &str, template: &str, variables: Option<&Value>) -> Result<Value> {
    let rendered = render_template_impl(template, variables)?;

    build_message(role, &rendered)
}

fn render_template_value(value: &Value) -> Result<String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        other @ (Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::Array(_)
        | Value::Object(_)) => Ok(serde_json::to_string(other)?),
    }
}

fn build_text_part(text: &str) -> Result<Value> {
    let text = require_non_blank("text", text)?;

    Ok(json!({
        "type": "text",
        "text": text,
    }))
}

fn build_image_url_part(url: &str, detail: Option<&str>) -> Result<Value> {
    let url = require_non_blank("url", url)?;
    let mut image_url = json!({
        "url": url,
    });

    if let Some(detail) = detail {
        let detail = require_non_blank("detail", detail)?;
        if let Some(object) = image_url.as_object_mut() {
            object.insert("detail".to_owned(), json!(detail));
        }
    }

    Ok(json!({
        "type": "image_url",
        "image_url": image_url,
    }))
}

fn build_parts_message(role: &str, parts: &[JsonB]) -> Result<Value> {
    let role = require_non_blank("role", role)?;
    let parts = validate_message_parts(parts)?;

    Ok(json!({
        "role": role,
        "content": parts,
    }))
}

fn validate_message_parts(parts: &[JsonB]) -> Result<Vec<Value>> {
    if parts.is_empty() {
        return Err(Error::invalid_argument(
            "parts",
            "must contain at least one content part",
            "pass a non-empty jsonb[] array built from postllm.text_part(...) or postllm.image_url_part(...)",
        ));
    }

    parts
        .iter()
        .enumerate()
        .map(|(index, part)| validate_message_part(&part.0, index))
        .collect()
}

fn validate_message_part(part: &Value, index: usize) -> Result<Value> {
    let argument = format!("parts[{index}]");
    let Some(part_type) = part.get("type").and_then(Value::as_str) else {
        return Err(Error::invalid_argument(
            &format!("{argument}.type"),
            "must contain a string type",
            "use postllm.text_part(...) or postllm.image_url_part(...) to build content parts",
        ));
    };

    match part_type {
        "text" => {
            let Some(text) = part.get("text").and_then(Value::as_str) else {
                return Err(Error::invalid_argument(
                    &format!("{argument}.text"),
                    "must contain a string text value when type is 'text'",
                    "use postllm.text_part(...) or provide JSON like {\"type\":\"text\",\"text\":\"...\"}",
                ));
            };
            require_non_blank("text", text)?;
        }
        "image_url" => {
            let Some(image_url) = part.get("image_url").and_then(Value::as_object) else {
                return Err(Error::invalid_argument(
                    &format!("{argument}.image_url"),
                    "must contain an object when type is 'image_url'",
                    "use postllm.image_url_part(...) or provide JSON like {\"type\":\"image_url\",\"image_url\":{\"url\":\"...\"}}",
                ));
            };
            let Some(url) = image_url.get("url").and_then(Value::as_str) else {
                return Err(Error::invalid_argument(
                    &format!("{argument}.image_url.url"),
                    "must contain a string URL when type is 'image_url'",
                    "provide image_url.url as a non-empty string",
                ));
            };
            require_non_blank("url", url)?;

            if let Some(detail) = image_url.get("detail") {
                let Some(detail) = detail.as_str() else {
                    return Err(Error::invalid_argument(
                        &format!("{argument}.image_url.detail"),
                        "must be a string when present",
                        "use detail values like 'low', 'high', or omit detail",
                    ));
                };
                require_non_blank("detail", detail)?;
            }
        }
        _ => {
            return Err(Error::invalid_argument(
                &format!("{argument}.type"),
                format!("must be 'text' or 'image_url', got '{part_type}'"),
                "use postllm.text_part(...) or postllm.image_url_part(...)",
            ));
        }
    }

    Ok(part.clone())
}

fn build_tool_call(id: &str, name: &str, arguments: &Value) -> Result<Value> {
    let id = require_non_blank("id", id)?;
    let name = require_non_blank("name", name)?;
    let arguments = serde_json::to_string(arguments)?;

    Ok(json!({
        "id": id,
        "type": "function",
        "function": {
            "name": name,
            "arguments": arguments,
        },
    }))
}

fn build_assistant_tool_calls(tool_calls: &[JsonB], content: Option<&str>) -> Result<Value> {
    if tool_calls.is_empty() {
        return Err(Error::invalid_argument(
            "tool_calls",
            "must contain at least one tool call",
            "pass a non-empty jsonb[] array built from postllm.tool_call(...)",
        ));
    }

    let normalized_tool_calls = tool_calls
        .iter()
        .enumerate()
        .map(|(index, tool_call)| normalize_tool_call(&tool_call.0, index))
        .collect::<Result<Vec<_>>>()?;
    let content = content
        .map(|content| require_non_blank("content", content).map(str::to_owned))
        .transpose()?;

    Ok(json!({
        "role": "assistant",
        "content": content,
        "tool_calls": normalized_tool_calls,
    }))
}

fn normalize_tool_call(tool_call: &Value, index: usize) -> Result<Value> {
    let argument = format!("tool_calls[{index}]");
    let Some(id) = tool_call.get("id").and_then(Value::as_str) else {
        return Err(Error::invalid_argument(
            &format!("{argument}.id"),
            "must contain a string id",
            "use postllm.tool_call(...) or provide JSON like {\"id\":\"call_123\",\"type\":\"function\",\"function\":{...}}",
        ));
    };
    let id = require_non_blank("id", id)?;
    let tool_type = tool_call
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("function");

    if tool_type != "function" {
        return Err(Error::invalid_argument(
            &format!("{argument}.type"),
            format!("must be 'function', got '{tool_type}'"),
            "use function-style tool calls only",
        ));
    }

    let Some(function) = tool_call.get("function").and_then(Value::as_object) else {
        return Err(Error::invalid_argument(
            &format!("{argument}.function"),
            "must contain an object",
            "provide JSON like {\"function\":{\"name\":\"...\",\"arguments\":{...}}}",
        ));
    };
    let Some(name) = function.get("name").and_then(Value::as_str) else {
        return Err(Error::invalid_argument(
            &format!("{argument}.function.name"),
            "must contain a string name",
            "provide the tool function name as a non-empty string",
        ));
    };
    let name = require_non_blank("name", name)?;
    let Some(arguments_value) = function.get("arguments") else {
        return Err(Error::invalid_argument(
            &format!("{argument}.function.arguments"),
            "must be present",
            "provide JSON arguments or a non-empty JSON string",
        ));
    };
    let arguments = match arguments_value {
        Value::String(arguments) => require_non_blank("arguments", arguments)?.to_owned(),
        other @ (Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::Array(_)
        | Value::Object(_)) => serde_json::to_string(other)?,
    };

    Ok(json!({
        "id": id,
        "type": "function",
        "function": {
            "name": name,
            "arguments": arguments,
        },
    }))
}

fn build_tool_result(tool_call_id: &str, content: &str) -> Result<Value> {
    let tool_call_id = require_non_blank("tool_call_id", tool_call_id)?;
    let content = require_non_blank("content", content)?;

    Ok(json!({
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": content,
    }))
}

fn build_function_tool(name: &str, parameters: &Value, description: Option<&str>) -> Result<Value> {
    let name = require_non_blank("name", name)?;

    if !parameters.is_object() {
        return Err(Error::invalid_argument(
            "parameters",
            "must be a JSON object",
            r#"pass a JSON Schema object like '{"type":"object","properties":{...}}'::jsonb"#,
        ));
    }

    let description = description
        .map(|description| require_non_blank("description", description).map(str::to_owned))
        .transpose()?;
    let mut function = json!({
        "name": name,
        "parameters": parameters,
    });

    if let Some(description) = description
        && let Some(object) = function.as_object_mut()
    {
        object.insert("description".to_owned(), json!(description));
    }

    Ok(json!({
        "type": "function",
        "function": function,
    }))
}

fn build_tool_choice_mode(mode: &'static str) -> Value {
    json!(mode)
}

fn build_tool_choice_function(name: &str) -> Result<Value> {
    let name = require_non_blank("name", name)?;

    Ok(json!({
        "type": "function",
        "function": {
            "name": name,
        }
    }))
}

fn validate_tools(tools: &[JsonB]) -> Result<Vec<Value>> {
    if tools.is_empty() {
        return Err(Error::invalid_argument(
            "tools",
            "must contain at least one tool definition",
            "pass a non-empty jsonb[] array built from postllm.function_tool(...)",
        ));
    }

    tools
        .iter()
        .enumerate()
        .map(|(index, tool)| validate_tool_definition(&tool.0, index))
        .collect()
}

fn validate_tool_definition(tool: &Value, index: usize) -> Result<Value> {
    let argument = format!("tools[{index}]");
    let Some(tool_type) = tool.get("type").and_then(Value::as_str) else {
        return Err(Error::invalid_argument(
            &format!("{argument}.type"),
            "must contain a string type",
            "use postllm.function_tool(...) or provide JSON like {\"type\":\"function\",\"function\":{...}}",
        ));
    };

    if tool_type != "function" {
        return Err(Error::invalid_argument(
            &format!("{argument}.type"),
            format!("must be 'function', got '{tool_type}'"),
            "use function-style tools only",
        ));
    }

    let Some(function) = tool.get("function").and_then(Value::as_object) else {
        return Err(Error::invalid_argument(
            &format!("{argument}.function"),
            "must contain an object",
            "provide JSON like {\"function\":{\"name\":\"...\",\"parameters\":{...}}}",
        ));
    };
    let Some(name) = function.get("name").and_then(Value::as_str) else {
        return Err(Error::invalid_argument(
            &format!("{argument}.function.name"),
            "must contain a string name",
            "provide the tool function name as a non-empty string",
        ));
    };
    require_non_blank(&format!("{argument}.function.name"), name)?;

    let Some(parameters) = function.get("parameters") else {
        return Err(Error::invalid_argument(
            &format!("{argument}.function.parameters"),
            "must be present",
            "provide a JSON Schema object describing the tool parameters",
        ));
    };

    if !parameters.is_object() {
        return Err(Error::invalid_argument(
            &format!("{argument}.function.parameters"),
            "must be a JSON object",
            r#"provide a JSON Schema object like '{"type":"object","properties":{...}}'::jsonb"#,
        ));
    }

    if let Some(description) = function.get("description") {
        let Some(description) = description.as_str() else {
            return Err(Error::invalid_argument(
                &format!("{argument}.function.description"),
                "must be a string when present",
                "pass a short string description or omit description",
            ));
        };
        require_non_blank(&format!("{argument}.function.description"), description)?;
    }

    if let Some(strict) = function.get("strict")
        && !strict.is_boolean()
    {
        return Err(Error::invalid_argument(
            &format!("{argument}.function.strict"),
            "must be a boolean when present",
            "pass true, false, or omit strict",
        ));
    }

    Ok(tool.clone())
}

fn validate_tool_choice(tool_choice: Option<&Value>, tools: &[Value]) -> Result<Option<Value>> {
    let Some(tool_choice) = tool_choice else {
        return Ok(None);
    };

    match tool_choice {
        Value::String(mode) if matches!(mode.as_str(), "auto" | "none" | "required") => {
            Ok(Some(tool_choice.clone()))
        }
        Value::String(mode) => Err(Error::invalid_argument(
            "tool_choice",
            format!(
                "must be 'auto', 'none', 'required', or a function-choice object, got '{mode}'"
            ),
            "use postllm.tool_choice_auto(), postllm.tool_choice_none(), postllm.tool_choice_required(), or postllm.tool_choice_function(...)",
        )),
        Value::Object(object) => {
            let Some(tool_type) = object.get("type").and_then(Value::as_str) else {
                return Err(Error::invalid_argument(
                    "tool_choice.type",
                    "must contain a string type",
                    "use postllm.tool_choice_function(...) or one of the string modes",
                ));
            };

            if tool_type != "function" {
                return Err(Error::invalid_argument(
                    "tool_choice.type",
                    format!("must be 'function', got '{tool_type}'"),
                    "use postllm.tool_choice_function(...) for named tool selection",
                ));
            }

            let Some(function) = object.get("function").and_then(Value::as_object) else {
                return Err(Error::invalid_argument(
                    "tool_choice.function",
                    "must contain an object",
                    "provide JSON like {\"type\":\"function\",\"function\":{\"name\":\"...\"}}",
                ));
            };
            let Some(name) = function.get("name").and_then(Value::as_str) else {
                return Err(Error::invalid_argument(
                    "tool_choice.function.name",
                    "must contain a string name",
                    "provide the selected tool name as a non-empty string",
                ));
            };
            let name = require_non_blank("tool_choice.function.name", name)?;
            let available_tool_names = tools
                .iter()
                .filter_map(|tool| {
                    tool.get("function")
                        .and_then(|function| function.get("name"))
                        .and_then(Value::as_str)
                })
                .collect::<Vec<_>>();

            if !available_tool_names
                .iter()
                .any(|tool_name| tool_name.eq_ignore_ascii_case(name))
            {
                return Err(Error::invalid_argument(
                    "tool_choice.function.name",
                    format!("must match one of the declared tools, got '{name}'"),
                    format!("choose one of: {}", available_tool_names.join(", ")),
                ));
            }

            Ok(Some(tool_choice.clone()))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => {
            Err(Error::invalid_argument(
                "tool_choice",
                "must be a string mode or a function-choice object",
                "use postllm.tool_choice_auto(), postllm.tool_choice_none(), postllm.tool_choice_required(), or postllm.tool_choice_function(...)",
            ))
        }
    }
}

fn build_json_schema_response_format(name: &str, schema: &Value, strict: bool) -> Result<Value> {
    let name = require_non_blank("name", name)?;

    if !schema.is_object() {
        return Err(Error::invalid_argument(
            "schema",
            "must be a JSON object",
            r#"pass a JSON Schema object like '{"type":"object","properties":{...}}'::jsonb"#,
        ));
    }

    Ok(json!({
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "strict": strict,
            "schema": schema,
        }
    }))
}

fn validate_structured_output_contract(
    response_format: &Value,
) -> Result<StructuredOutputContract> {
    let Some(response_format) = response_format.as_object() else {
        return Err(Error::invalid_argument(
            "response_format",
            "must be a JSON object",
            "build it with postllm.json_schema(...) or pass an OpenAI-style response_format object",
        ));
    };
    let Some(response_format_type) = response_format.get("type").and_then(Value::as_str) else {
        return Err(Error::invalid_argument(
            "response_format.type",
            "must contain a string type",
            "use type => 'json_schema'",
        ));
    };

    if response_format_type != "json_schema" {
        return Err(Error::invalid_argument(
            "response_format.type",
            format!("must be 'json_schema', got '{response_format_type}'"),
            "build it with postllm.json_schema(...)",
        ));
    }

    let Some(json_schema) = response_format
        .get("json_schema")
        .and_then(Value::as_object)
    else {
        return Err(Error::invalid_argument(
            "response_format.json_schema",
            "must contain an object",
            "provide json_schema.name, json_schema.schema, and optionally json_schema.strict",
        ));
    };
    let Some(name) = json_schema.get("name").and_then(Value::as_str) else {
        return Err(Error::invalid_argument(
            "response_format.json_schema.name",
            "must contain a string name",
            "provide a non-empty schema name like 'person_extraction'",
        ));
    };
    let name = require_non_blank("response_format.json_schema.name", name)?;
    let Some(schema) = json_schema.get("schema") else {
        return Err(Error::invalid_argument(
            "response_format.json_schema.schema",
            "must be present",
            "provide a JSON Schema object",
        ));
    };

    if !schema.is_object() {
        return Err(Error::invalid_argument(
            "response_format.json_schema.schema",
            "must be a JSON object",
            r#"provide a JSON Schema object like '{"type":"object",...}'::jsonb"#,
        ));
    }

    if let Some(strict) = json_schema.get("strict")
        && !strict.is_boolean()
    {
        return Err(Error::invalid_argument(
            "response_format.json_schema.strict",
            "must be a boolean when present",
            "pass true, false, or omit strict",
        ));
    }

    Ok(StructuredOutputContract {
        response_format: Value::Object(response_format.clone()),
        name: name.to_owned(),
    })
}

fn parse_structured_output_response(
    response: &Value,
    contract: &StructuredOutputContract,
) -> Result<Value> {
    if let Some(refusal) = structured_output_refusal(response) {
        return Err(Error::StructuredOutput(format!(
            "model refused schema '{}' with refusal text: {}",
            contract.name,
            preview_text(refusal, 160),
        )));
    }

    let text = backend::extract_text(response).map_err(|error| {
        Error::StructuredOutput(format!(
            "response for schema '{}' did not contain structured JSON text: {error}",
            contract.name,
        ))
    })?;

    serde_json::from_str::<Value>(&text).map_err(|error| {
        Error::StructuredOutput(format!(
            "response for schema '{}' was not valid JSON: {error}; raw text starts with '{}'",
            contract.name,
            preview_text(&text, 160),
        ))
    })
}

fn structured_output_refusal(response: &Value) -> Option<&str> {
    response
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("refusal"))
        .and_then(Value::as_str)
}

fn preview_text(text: &str, max_chars: usize) -> String {
    let mut preview = text.chars().take(max_chars).collect::<String>();
    if text.chars().count() > max_chars {
        preview.push_str("...");
    }

    preview
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

fn finish_text_result(result: Result<String>) -> String {
    match result {
        Ok(text) => text,
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_float_result(result: Result<f64>) -> f64 {
    match result {
        Ok(value) => value,
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_json_result(result: Result<Value>) -> JsonB {
    match result {
        Ok(value) => JsonB(value),
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_vector_result(result: Result<Vec<f32>>) -> Vec<f32> {
    match result {
        Ok(vector) => vector,
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_json_array_result(result: Result<Vec<Value>>) -> Vec<JsonB> {
    match result {
        Ok(values) => values.into_iter().map(JsonB).collect(),
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_text_array_result(result: Result<Vec<String>>) -> Vec<String> {
    match result {
        Ok(texts) => texts,
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_completion_rows_result(
    result: Result<Vec<(i32, String, String)>>,
) -> TableIterator<
    'static,
    (
        pgrx::name!(index, i32),
        pgrx::name!(prompt, String),
        pgrx::name!(completion, String),
    ),
> {
    match result {
        Ok(rows) => TableIterator::new(rows),
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_chunk_rows_result(
    result: Result<Vec<(i32, String, JsonB)>>,
) -> TableIterator<
    'static,
    (
        pgrx::name!(index, i32),
        pgrx::name!(chunk, String),
        pgrx::name!(metadata, JsonB),
    ),
> {
    match result {
        Ok(rows) => TableIterator::new(rows),
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_rank_rows_result(result: Result<Vec<RankRow>>) -> RankRowsIterator {
    match result {
        Ok(rows) => TableIterator::new(rows),
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_hybrid_rank_rows_result(result: Result<Vec<HybridRankRow>>) -> HybridRankRowsIterator {
    match result {
        Ok(rows) => TableIterator::new(rows),
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_embedding_document_rows_result(
    result: Result<Vec<PreparedDocumentEmbedding>>,
) -> TableIterator<
    'static,
    (
        pgrx::name!(chunk_id, String),
        pgrx::name!(doc_id, String),
        pgrx::name!(chunk_no, i32),
        pgrx::name!(content, String),
        pgrx::name!(metadata, JsonB),
        pgrx::name!(embedding, Vec<f32>),
    ),
> {
    match result {
        Ok(rows) => TableIterator::new(rows.into_iter().map(|row| {
            (
                row.chunk_id,
                row.doc_id,
                row.chunk_no,
                row.content,
                JsonB(row.metadata),
                row.embedding,
            )
        })),
        Err(error) => pgrx::error!("{error}"),
    }
}

fn finish_stream_rows_result(
    result: Result<Vec<(i32, Option<String>, JsonB)>>,
) -> TableIterator<
    'static,
    (
        pgrx::name!(index, i32),
        pgrx::name!(delta, Option<String>),
        pgrx::name!(event, JsonB),
    ),
> {
    match result {
        Ok(rows) => TableIterator::new(rows),
        Err(error) => pgrx::error!("{error}"),
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "helper tests are clearer with direct indexing and explicit expect messages"
)]
mod helper_tests {
    use super::{
        ChunkingOptions, LocalModelLaneSelector, RagContextRow, RagRetrievalStrategy,
        RankedDocument, StructuredOutputContract, TextChunk, build_assistant_tool_calls,
        build_function_tool, build_image_url_part, build_json_schema_response_format,
        build_message_template, build_rag_prompt, build_text_part, build_tool_call,
        build_tool_choice_function, build_tool_choice_mode, chunk_text_value,
        deterministic_chunk_id, fuse_hybrid_rankings, messages_require_multimodal_inputs,
        parse_local_model_evict_scope, parse_local_model_lane_selector,
        parse_structured_output_response, render_template_impl, rrf_score_impl,
        validate_aggregate_message, validate_batch_prompts, validate_chunk_metadata,
        validate_chunking_options, validate_message, validate_optional_system_prompt,
        validate_rag_retrieval, validate_rerank_documents, validate_rerank_top_n,
        validate_structured_output_contract, validate_tool_choice,
    };
    use crate::candle::LocalModelEvictionScope;
    use pgrx::JsonB;
    use serde_json::{Value, json};

    #[test]
    fn build_tool_call_should_serialize_json_arguments() {
        let tool_call = build_tool_call("call_123", "lookup_weather", &json!({"city": "Austin"}))
            .expect("tool call should build");

        assert_eq!(tool_call["id"], "call_123");
        assert_eq!(tool_call["type"], "function");
        assert_eq!(tool_call["function"]["name"], "lookup_weather");
        assert_eq!(tool_call["function"]["arguments"], r#"{"city":"Austin"}"#);
    }

    #[test]
    fn build_function_tool_should_serialize_openai_tool_definitions() {
        let tool = build_function_tool(
            "lookup_weather",
            &json!({
                "type": "object",
                "properties": {
                    "city": {"type": "string"}
                },
                "required": ["city"],
                "additionalProperties": false
            }),
            Some("Look up the current weather."),
        )
        .expect("function tool should build");

        assert_eq!(tool["type"], "function");
        assert_eq!(tool["function"]["name"], "lookup_weather");
        assert_eq!(
            tool["function"]["description"],
            "Look up the current weather."
        );
        assert_eq!(tool["function"]["parameters"]["type"], "object");
    }

    #[test]
    fn build_function_tool_should_reject_non_object_parameters() {
        let error = build_function_tool("lookup_weather", &json!(["bad"]), None)
            .expect_err("non-object parameters should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'parameters' must be a JSON object; fix: pass a JSON Schema object like '{\"type\":\"object\",\"properties\":{...}}'::jsonb"
        );
    }

    #[test]
    fn build_tool_choice_function_should_build_named_function_choice() {
        let tool_choice =
            build_tool_choice_function("lookup_weather").expect("named tool choice should build");

        assert_eq!(
            tool_choice,
            json!({
                "type": "function",
                "function": {
                    "name": "lookup_weather",
                }
            })
        );
    }

    #[test]
    fn build_tool_choice_mode_should_build_string_modes() {
        assert_eq!(build_tool_choice_mode("auto"), json!("auto"));
        assert_eq!(build_tool_choice_mode("none"), json!("none"));
        assert_eq!(build_tool_choice_mode("required"), json!("required"));
    }

    #[test]
    fn validate_tool_choice_should_accept_declared_named_tools() {
        let tools = vec![
            build_function_tool(
                "lookup_weather",
                &json!({
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"}
                    }
                }),
                None,
            )
            .expect("tool should build"),
        ];
        let tool_choice =
            build_tool_choice_function("lookup_weather").expect("tool choice should build");

        let validated = validate_tool_choice(Some(&tool_choice), &tools)
            .expect("declared named tool should validate")
            .expect("tool choice should stay present");

        assert_eq!(validated, tool_choice);
    }

    #[test]
    fn validate_tool_choice_should_reject_unknown_named_tools() {
        let tools = vec![
            build_function_tool(
                "lookup_weather",
                &json!({
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"}
                    }
                }),
                None,
            )
            .expect("tool should build"),
        ];
        let tool_choice =
            build_tool_choice_function("unknown_tool").expect("tool choice should build");
        let error = validate_tool_choice(Some(&tool_choice), &tools)
            .expect_err("unknown named tool should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'tool_choice.function.name' must match one of the declared tools, got 'unknown_tool'; fix: choose one of: lookup_weather"
        );
    }

    #[test]
    fn validate_message_should_accept_assistant_tool_calls_with_null_content() {
        let tool_call = build_tool_call("call_123", "lookup_weather", &json!({"city": "Austin"}))
            .expect("tool call should build");
        let message = build_assistant_tool_calls(&[JsonB(tool_call)], None)
            .expect("assistant tool-call message should build");

        let validated = validate_message(&message, 0)
            .expect("assistant tool-call messages with null content should validate");

        assert_eq!(validated["role"], "assistant");
        assert_eq!(validated["content"], Value::Null);
        assert_eq!(validated["tool_calls"][0]["id"], "call_123");
    }

    #[test]
    fn build_image_url_part_should_include_detail_when_present() {
        let part = build_image_url_part("https://example.com/cat.png", Some("high"))
            .expect("image_url part should build");

        assert_eq!(part["type"], "image_url");
        assert_eq!(part["image_url"]["url"], "https://example.com/cat.png");
        assert_eq!(part["image_url"]["detail"], "high");
    }

    #[test]
    fn build_text_part_should_trim_content() {
        let part = build_text_part("  describe this image  ").expect("text part should build");

        assert_eq!(part, json!({"type": "text", "text": "describe this image"}));
    }

    #[test]
    fn render_template_impl_should_replace_named_placeholders() {
        let rendered = render_template_impl(
            "Explain {{ topic }} in {{word_count}} words. Tags: {{tags}}.",
            Some(&json!({
                "topic": "MVCC",
                "word_count": 12,
                "tags": ["postgres", "storage"],
            })),
        )
        .expect("template should render");

        assert_eq!(
            rendered,
            r#"Explain MVCC in 12 words. Tags: ["postgres","storage"]."#
        );
    }

    #[test]
    fn render_template_impl_should_reject_missing_variables() {
        let error = render_template_impl("Explain {{topic}}.", Some(&json!({})))
            .expect_err("template should reject missing variables");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'variables' is missing template variable 'topic'; fix: include key 'topic' in the variables jsonb object"
        );
    }

    #[test]
    fn render_template_impl_should_reject_non_object_variables() {
        let error = render_template_impl("Explain {{topic}}.", Some(&json!(["MVCC"])))
            .expect_err("template should reject non-object variables");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'variables' must be a JSON object; fix: pass jsonb like '{\"name\":\"value\"}'::jsonb"
        );
    }

    #[test]
    fn build_message_template_should_render_then_build_message() {
        let message = build_message_template(
            "user",
            "Explain {{topic}} briefly.",
            Some(&json!({"topic": "MVCC"})),
        )
        .expect("templated message should build");

        assert_eq!(
            message,
            json!({
                "role": "user",
                "content": "Explain MVCC briefly.",
            })
        );
    }

    #[test]
    fn validate_batch_prompts_should_trim_prompts() {
        let prompts = validate_batch_prompts(&["  first  ".to_owned(), "second".to_owned()])
            .expect("prompt batch should validate");

        assert_eq!(prompts, vec!["first".to_owned(), "second".to_owned()]);
    }

    #[test]
    fn validate_rerank_documents_should_trim_documents() {
        let documents = validate_rerank_documents(&[
            "  Autovacuum removes dead tuples.  ".to_owned(),
            "Bananas are yellow.".to_owned(),
        ])
        .expect("rerank documents should validate");

        assert_eq!(
            documents,
            vec![
                "Autovacuum removes dead tuples.".to_owned(),
                "Bananas are yellow.".to_owned(),
            ]
        );
    }

    #[test]
    fn validate_rerank_top_n_should_reject_non_positive_values() {
        let error =
            validate_rerank_top_n(Some(0)).expect_err("non-positive top_n should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'top_n' must be greater than zero when present, got 0; fix: omit top_n or pass a positive integer like 5"
        );
    }

    #[test]
    fn rrf_score_impl_should_fuse_semantic_and_keyword_ranks() {
        let score =
            rrf_score_impl(Some(2), Some(1), 1.0, 2.0, 10).expect("rrf score should compute");

        assert!((score - (1.0 / 12.0 + 2.0 / 11.0)).abs() < 1e-9);
    }

    #[test]
    fn fuse_hybrid_rankings_should_prefer_documents_present_in_both_lists() {
        let rows = fuse_hybrid_rankings(
            &[
                "Bananas are yellow.".to_owned(),
                "Autovacuum controls table bloat.".to_owned(),
                "Autovacuum is a PostgreSQL worker.".to_owned(),
            ],
            &[
                RankedDocument {
                    index: 0,
                    score: 0.99,
                },
                RankedDocument {
                    index: 1,
                    score: 0.92,
                },
                RankedDocument {
                    index: 2,
                    score: 0.91,
                },
            ],
            &[
                RankedDocument {
                    index: 1,
                    score: 0.75,
                },
                RankedDocument {
                    index: 2,
                    score: 0.50,
                },
            ],
            Some(2),
            1.0,
            1.0,
            60,
        )
        .expect("hybrid fusion should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, 2);
        assert_eq!(rows[0].2, "Autovacuum controls table bloat.");
        assert_eq!(rows[0].4, Some(2));
        assert_eq!(rows[0].5, Some(1));
        assert_eq!(rows[1].1, 3);
        assert_eq!(rows[1].4, Some(3));
        assert_eq!(rows[1].5, Some(2));
    }

    #[test]
    fn validate_rag_retrieval_should_reject_unknown_modes() {
        let error = validate_rag_retrieval(Some("vector"))
            .expect_err("unknown retrieval modes should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'retrieval' must be one of 'hybrid', 'semantic', or 'keyword', got 'vector'; fix: pass retrieval => 'hybrid', 'semantic', or 'keyword'"
        );
    }

    #[test]
    fn parse_local_model_lane_selector_should_accept_supported_values() {
        assert_eq!(
            parse_local_model_lane_selector(None).expect("default lane should parse"),
            LocalModelLaneSelector::Auto
        );
        assert_eq!(
            parse_local_model_lane_selector(Some("embedding"))
                .expect("embedding lane should parse"),
            LocalModelLaneSelector::Embedding
        );
        assert_eq!(
            parse_local_model_lane_selector(Some("generation"))
                .expect("generation lane should parse"),
            LocalModelLaneSelector::Generation
        );
    }

    #[test]
    fn parse_local_model_lane_selector_should_reject_unknown_values() {
        let error = parse_local_model_lane_selector(Some("vision"))
            .expect_err("unknown lane values should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'lane' must be one of 'auto', 'embedding', or 'generation', got 'vision'; fix: omit lane for auto selection or pass lane => 'embedding' or 'generation'"
        );
    }

    #[test]
    fn parse_local_model_evict_scope_should_accept_supported_values() {
        assert_eq!(
            parse_local_model_evict_scope("memory").expect("memory scope should parse"),
            LocalModelEvictionScope::Memory
        );
        assert_eq!(
            parse_local_model_evict_scope("disk").expect("disk scope should parse"),
            LocalModelEvictionScope::Disk
        );
        assert_eq!(
            parse_local_model_evict_scope("all").expect("all scope should parse"),
            LocalModelEvictionScope::All
        );
    }

    #[test]
    fn parse_local_model_evict_scope_should_reject_unknown_values() {
        let error = parse_local_model_evict_scope("network")
            .expect_err("unknown scope values should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'scope' must be one of 'memory', 'disk', or 'all', got 'network'; fix: pass scope => 'memory', 'disk', or 'all'"
        );
    }

    #[test]
    fn build_rag_prompt_should_include_numbered_context() {
        let prompt = build_rag_prompt(
            "How does autovacuum control table bloat?",
            &[
                RagContextRow {
                    rank: 1,
                    index: 2,
                    document: "Autovacuum removes dead tuples and helps control table bloat."
                        .to_owned(),
                    score: 0.91,
                    semantic_rank: Some(1),
                    keyword_rank: Some(1),
                    semantic_score: Some(0.91),
                    keyword_score: Some(0.72),
                },
                RagContextRow {
                    rank: 2,
                    index: 3,
                    document: "VACUUM can reclaim space manually.".to_owned(),
                    score: 0.52,
                    semantic_rank: Some(2),
                    keyword_rank: Some(2),
                    semantic_score: Some(0.52),
                    keyword_score: Some(0.30),
                },
            ],
        );

        assert!(prompt.contains("Question: How does autovacuum control table bloat?"));
        assert!(
            prompt.contains("[1] Autovacuum removes dead tuples and helps control table bloat.")
        );
        assert!(prompt.contains("[2] VACUUM can reclaim space manually."));
    }

    #[test]
    fn rag_retrieval_strategy_should_report_stable_names() {
        assert_eq!(RagRetrievalStrategy::Hybrid.as_str(), "hybrid");
        assert_eq!(RagRetrievalStrategy::Semantic.as_str(), "semantic");
        assert_eq!(RagRetrievalStrategy::Keyword.as_str(), "keyword");
    }

    #[test]
    fn validate_chunking_options_should_reject_overlap_that_is_too_large() {
        let error = validate_chunking_options(100, 100)
            .expect_err("overlap equal to chunk size should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'overlap_chars' must be smaller than chunk_chars, got overlap_chars=100 and chunk_chars=100; fix: pass an overlap smaller than the chunk size, for example chunk_chars => 1000 and overlap_chars => 200"
        );
    }

    #[test]
    fn validate_chunk_metadata_should_reject_non_object_values() {
        let error = validate_chunk_metadata(Some(&json!(["bad"])))
            .expect_err("non-object metadata should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'metadata' must be a JSON object or null; fix: pass jsonb like '{\"doc_id\":\"vacuum-guide\"}'::jsonb or omit metadata"
        );
    }

    #[test]
    fn chunk_text_value_should_prefer_sentence_boundaries_when_possible() {
        let chunks = chunk_text_value(
            "Alpha sentence. Beta sentence. Gamma sentence.",
            ChunkingOptions {
                chunk_chars: 24,
                overlap_chars: 6,
            },
        );

        assert_eq!(chunks[0].text, "Alpha sentence.");
        assert_eq!(chunks[1].text, "tence. Beta sentence.");
        assert_eq!(chunks[2].text, "tence. Gamma sentence.");
    }

    #[test]
    fn chunk_text_value_should_trim_edges_and_prefer_line_breaks() {
        let chunks = chunk_text_value(
            " first line\nsecond line ",
            ChunkingOptions {
                chunk_chars: 20,
                overlap_chars: 0,
            },
        );

        assert_eq!(chunks[0].text, "first line");
        assert_eq!(chunks[1].text, "second line");
    }

    #[test]
    fn chunk_text_value_should_absorb_short_final_tails() {
        let chunks = chunk_text_value(
            "Alpha sentence. Beta sentence.",
            ChunkingOptions {
                chunk_chars: 18,
                overlap_chars: 4,
            },
        );

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "Alpha sentence.");
        assert_eq!(chunks[1].text, "nce. Beta sentence.");
    }

    #[test]
    fn deterministic_chunk_id_should_be_stable_for_same_chunk_inputs() {
        let chunk = TextChunk {
            start_char: 12,
            end_char_exclusive: 27,
            text: "Beta sentence.".to_owned(),
        };

        let first = deterministic_chunk_id("guide-1", &chunk);
        let second = deterministic_chunk_id("guide-1", &chunk);

        assert_eq!(first, second);
    }

    #[test]
    fn deterministic_chunk_id_should_change_when_chunk_bounds_change() {
        let first = deterministic_chunk_id(
            "guide-1",
            &TextChunk {
                start_char: 0,
                end_char_exclusive: 15,
                text: "Alpha sentence.".to_owned(),
            },
        );
        let second = deterministic_chunk_id(
            "guide-1",
            &TextChunk {
                start_char: 4,
                end_char_exclusive: 19,
                text: "Alpha sentence.".to_owned(),
            },
        );

        assert_ne!(first, second);
    }

    #[test]
    fn validate_optional_system_prompt_should_trim_when_present() {
        let system_prompt = validate_optional_system_prompt(Some("  You are concise.  "))
            .expect("system prompt should validate");

        assert_eq!(system_prompt, Some("You are concise.".to_owned()));
    }

    #[test]
    fn messages_require_multimodal_inputs_should_detect_image_parts() {
        let messages = vec![json!({
            "role": "user",
            "content": [
                {"type": "text", "text": "Describe this image."},
                {"type": "image_url", "image_url": {"url": "https://example.com/cat.png"}},
            ]
        })];

        assert!(messages_require_multimodal_inputs(&messages));
    }

    #[test]
    fn build_json_schema_response_format_should_wrap_schema() {
        let response_format = build_json_schema_response_format(
            "person",
            &json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"}
                },
                "required": ["name"],
                "additionalProperties": false
            }),
            true,
        )
        .expect("response format should build");

        assert_eq!(response_format["type"], "json_schema");
        assert_eq!(response_format["json_schema"]["name"], "person");
        assert_eq!(response_format["json_schema"]["strict"], true);
        assert_eq!(response_format["json_schema"]["schema"]["type"], "object");
    }

    #[test]
    fn validate_structured_output_contract_should_reject_non_object_schemas() {
        let error = validate_structured_output_contract(&json!({
            "type": "json_schema",
            "json_schema": {
                "name": "person",
                "schema": ["bad"]
            }
        }))
        .expect_err("non-object schemas should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'response_format.json_schema.schema' must be a JSON object; fix: provide a JSON Schema object like '{\"type\":\"object\",...}'::jsonb"
        );
    }

    #[test]
    fn parse_structured_output_response_should_parse_json_text() {
        let contract = StructuredOutputContract {
            response_format: json!({}),
            name: "person".to_owned(),
        };
        let parsed = parse_structured_output_response(
            &json!({
                "choices": [{
                    "message": {
                        "content": "{\"name\":\"Ada\"}"
                    }
                }]
            }),
            &contract,
        )
        .expect("structured JSON should parse");

        assert_eq!(parsed, json!({"name": "Ada"}));
    }

    #[test]
    fn parse_structured_output_response_should_report_invalid_json() {
        let contract = StructuredOutputContract {
            response_format: json!({}),
            name: "person".to_owned(),
        };
        let error = parse_structured_output_response(
            &json!({
                "choices": [{
                    "message": {
                        "content": "name=Ada"
                    }
                }]
            }),
            &contract,
        )
        .expect_err("non-JSON text should be rejected");

        assert!(
            error
                .to_string()
                .contains("response for schema 'person' was not valid JSON")
        );
    }

    #[test]
    fn parse_structured_output_response_should_report_refusals() {
        let contract = StructuredOutputContract {
            response_format: json!({}),
            name: "person".to_owned(),
        };
        let error = parse_structured_output_response(
            &json!({
                "choices": [{
                    "message": {
                        "refusal": "I can't comply."
                    }
                }]
            }),
            &contract,
        )
        .expect_err("refusals should be surfaced");

        assert!(
            error
                .to_string()
                .contains("model refused schema 'person' with refusal text")
        );
    }

    #[test]
    fn validate_aggregate_message_should_use_non_indexed_errors() {
        let error = validate_aggregate_message(&json!({"role": "user"}))
            .expect_err("aggregate validation should reject missing content");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'message.content' must be present; fix: provide JSON like {\"role\":\"user\",\"content\":\"...\"}"
        );
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::bool_assert_comparison,
    clippy::map_unwrap_or,
    clippy::too_many_arguments,
    clippy::unnecessary_option_map_or_else,
    clippy::useless_format,
    reason = "SQL-facing pg_test assertions are clearer with direct indexing and explicit expects"
)]
mod tests {
    use pgrx::{JsonB, Spi, pg_test};
    use serde_json::{Value, json};
    use std::env;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const DEFAULT_PG_TEST_CANDLE_MODEL: &str = "Qwen/Qwen2.5-0.5B-Instruct";

    fn sql_json(query: &str) -> Value {
        Spi::get_one::<JsonB>(query)
            .expect("SPI should execute")
            .expect("query should return a row")
            .0
    }

    fn sql_text(query: &str) -> String {
        Spi::get_one::<String>(query)
            .expect("SPI should execute")
            .expect("query should return a row")
    }

    fn sql_float(query: &str) -> f64 {
        Spi::get_one::<f64>(query)
            .expect("SPI should execute")
            .expect("query should return a row")
    }

    fn sql_bool(query: &str) -> bool {
        Spi::get_one::<bool>(query)
            .expect("SPI should execute")
            .expect("query should return a row")
    }

    fn sql_optional_text(query: &str) -> Option<String> {
        Spi::get_one::<String>(query).expect("SPI should execute")
    }

    fn sql_run(query: &str) {
        Spi::run(query).expect("SPI should execute");
    }

    fn psql_json(query: &str) -> Value {
        let socket_dir = sql_text("SHOW unix_socket_directories")
            .split(',')
            .next()
            .expect("socket directory should be available")
            .trim()
            .to_owned();
        let port = sql_text("SHOW port");
        let database = sql_text("SELECT current_database()::text");
        let user = sql_text("SELECT session_user::text");
        let output = Command::new(pg_test_psql_path())
            .args([
                "-X",
                "-q",
                "-t",
                "-A",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                &socket_dir,
                "-p",
                &port,
                "-U",
                &user,
                "-d",
                &database,
                "-c",
                query,
            ])
            .output()
            .expect("psql should run the async-job query");

        assert!(
            output.status.success(),
            "psql query failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8(output.stdout).expect("psql stdout should be valid UTF-8");
        let json_line = stdout
            .lines()
            .map(str::trim)
            .rfind(|line| !line.is_empty())
            .expect("psql should emit one JSON line");

        serde_json::from_str(json_line).expect("psql should emit valid JSON")
    }

    fn psql_run(query: &str) {
        let socket_dir = sql_text("SHOW unix_socket_directories")
            .split(',')
            .next()
            .expect("socket directory should be available")
            .trim()
            .to_owned();
        let port = sql_text("SHOW port");
        let database = sql_text("SELECT current_database()::text");
        let user = sql_text("SELECT session_user::text");
        let output = Command::new(pg_test_psql_path())
            .args([
                "-X",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                &socket_dir,
                "-p",
                &port,
                "-U",
                &user,
                "-d",
                &database,
                "-c",
                query,
            ])
            .output()
            .expect("psql should run the async-job command");

        assert!(
            output.status.success(),
            "psql command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn spawn_psql_listener(channel: &str, sleep_seconds: u64) -> Child {
        let socket_dir = sql_text("SHOW unix_socket_directories")
            .split(',')
            .next()
            .expect("socket directory should be available")
            .trim()
            .to_owned();
        let port = sql_text("SHOW port");
        let database = sql_text("SELECT current_database()::text");
        let user = sql_text("SELECT session_user::text");

        Command::new(pg_test_psql_path())
            .args([
                "-X",
                "-q",
                "-A",
                "-t",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                &socket_dir,
                "-p",
                &port,
                "-U",
                &user,
                "-d",
                &database,
                "-c",
                &format!("LISTEN {channel}"),
                "-c",
                &format!("SELECT pg_sleep({sleep_seconds})"),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("psql should start a LISTEN session")
    }

    fn wait_for_psql_output(child: Child) -> String {
        let output = child
            .wait_with_output()
            .expect("psql LISTEN session should exit cleanly");

        assert!(
            output.status.success(),
            "psql LISTEN session failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        format!(
            "{}\n{}",
            String::from_utf8(output.stdout).expect("psql stdout should be valid UTF-8"),
            String::from_utf8(output.stderr).expect("psql stderr should be valid UTF-8")
        )
    }

    fn decode_psql_notification_payload(encoded: &str) -> Value {
        serde_json::from_str(encoded).unwrap_or_else(|_| {
            let escaped = format!("\"{}\"", encoded.replace('\\', "\\\\").replace('"', "\\\""));
            let decoded = serde_json::from_str::<String>(&escaped)
                .expect("psql notification payload should decode as a JSON string");
            serde_json::from_str(&decoded)
                .expect("decoded psql notification payload should be valid JSON")
        })
    }

    fn parse_psql_notification_payloads(output: &str, channel: &str) -> Vec<Value> {
        output
            .lines()
            .filter_map(|line| {
                let channel_marker = format!("notification \"{channel}\"");
                if !line.contains(&channel_marker) {
                    return None;
                }

                let payload_marker = "payload \"";
                let payload_start = line.find(payload_marker)? + payload_marker.len();
                let payload_end = line[payload_start..]
                    .find("\" received")
                    .map(|offset| payload_start + offset)?;
                Some(decode_psql_notification_payload(
                    &line[payload_start..payload_end],
                ))
            })
            .collect()
    }

    fn clear_request_audit_log() {
        sql_run("TRUNCATE postllm.request_audit_log RESTART IDENTITY");
    }

    fn request_audit_row_count() -> i64 {
        Spi::get_one::<i64>("SELECT count(*)::bigint FROM postllm.request_audit_log")
            .expect("SPI should execute")
            .expect("query should return a row")
    }

    fn latest_request_audit_row() -> Value {
        sql_json(
            "SELECT to_jsonb(log_row)
             FROM (
                SELECT *
                FROM postllm.request_audit_log
                ORDER BY id DESC
                LIMIT 1
             ) AS log_row",
        )
    }

    fn wait_for_async_job_status(job_id: i64, expected: &[&str], timeout_ms: u64) -> Value {
        let started = std::time::Instant::now();

        loop {
            let row = sql_json(&format!("SELECT postllm.job_poll({job_id})"));
            let status = row["status"]
                .as_str()
                .expect("async job rows should include a string status");

            if expected.contains(&status) {
                return row;
            }

            assert!(
                started.elapsed() < Duration::from_millis(timeout_ms),
                "async job {job_id} did not reach one of {expected:?} within {timeout_ms}ms; last row: {row}"
            );

            thread::sleep(Duration::from_millis(25));
        }
    }

    fn insert_request_audit_metric_row(
        role_name: &str,
        operation: &str,
        runtime: Option<&str>,
        model: Option<&str>,
        base_url: Option<&str>,
        status: &str,
        duration_ms: i64,
        response_payload: Option<&Value>,
        error_message: Option<&str>,
    ) {
        let runtime_sql = runtime
            .map(sql_literal)
            .unwrap_or_else(|| "NULL".to_owned());
        let model_sql = model.map(sql_literal).unwrap_or_else(|| "NULL".to_owned());
        let base_url_sql = base_url
            .map(sql_literal)
            .unwrap_or_else(|| "NULL".to_owned());
        let response_payload_sql = response_payload
            .map(|payload| format!("{}::jsonb", sql_literal(&payload.to_string())))
            .unwrap_or_else(|| "NULL".to_owned());
        let error_message_sql = error_message
            .map(sql_literal)
            .unwrap_or_else(|| "NULL".to_owned());

        sql_run(&format!(
            "SELECT postllm._request_audit_insert(
                role_name => {},
                operation => {},
                runtime => {},
                model => {},
                base_url => {},
                status => {},
                duration_ms => {},
                input_redacted => true,
                output_redacted => true,
                response_payload => {},
                error_message => {}
            )",
            sql_literal(role_name),
            sql_literal(operation),
            runtime_sql,
            model_sql,
            base_url_sql,
            sql_literal(status),
            duration_ms,
            response_payload_sql,
            error_message_sql,
        ));
    }

    fn unique_role_name(label: &str) -> String {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after the unix epoch")
            .as_nanos();
        format!("postllm_{}_{}_{}", label, std::process::id(), unique)
    }

    fn create_test_role(label: &str) -> String {
        let role_name = unique_role_name(label);
        sql_run(&format!("CREATE ROLE {role_name} NOLOGIN"));
        sql_run(&format!("GRANT {role_name} TO CURRENT_USER"));

        sql_run(&format!("GRANT USAGE ON SCHEMA postllm TO {role_name}"));
        sql_run(&format!(
            "GRANT SELECT ON TABLE postllm.role_permissions TO {role_name}"
        ));
        sql_run(&format!(
            "GRANT SELECT ON TABLE postllm.model_aliases TO {role_name}"
        ));
        sql_run(&format!(
            "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA postllm TO {role_name}"
        ));
        role_name
    }

    fn sql_permission_set(
        role_name: &str,
        object_type: &str,
        target: &str,
        description: Option<&str>,
    ) -> Value {
        let description_sql = description
            .map(sql_literal)
            .map_or_else(|| "NULL".to_owned(), |value| value);

        sql_json(&format!(
            "SELECT postllm.permission_set(
                role_name => {},
                object_type => {},
                target => {},
                description => {}
            )",
            sql_literal(role_name),
            sql_literal(object_type),
            sql_literal(target),
            description_sql,
        ))
    }

    fn grant_permission(role_name: &str, object_type: &str, target: &str) {
        drop(sql_permission_set(role_name, object_type, target, None));
    }

    fn set_local_role(role_name: &str) {
        sql_run(&format!("SET LOCAL ROLE {role_name}"));
    }

    struct PsqlSessionGuard {
        child: Child,
    }

    impl Drop for PsqlSessionGuard {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    fn pg_test_psql_path() -> PathBuf {
        let version = sql_text("SHOW server_version");
        let candidate = env::var("HOME").ok().map(PathBuf::from).map(|home| {
            home.join(".pgrx")
                .join(version.trim())
                .join("pgrx-install/bin/psql")
        });

        candidate
            .filter(|path| path.is_file())
            .unwrap_or_else(|| PathBuf::from("psql"))
    }

    fn spawn_request_concurrency_holder(slot: i32, sleep_seconds: u64) -> PsqlSessionGuard {
        let socket_dir = sql_text("SHOW unix_socket_directories")
            .split(',')
            .next()
            .expect("socket directory should be available")
            .trim()
            .to_owned();
        let port = sql_text("SHOW port");
        let database = sql_text("SELECT current_database()::text");
        let user = sql_text("SELECT current_user::text");
        let mut child = Command::new(pg_test_psql_path())
            .args([
                "-X",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                &socket_dir,
                "-p",
                &port,
                "-U",
                &user,
                "-d",
                &database,
                "-c",
                &format!(
                    "SELECT pg_advisory_lock({}, {}); SELECT pg_sleep({sleep_seconds});",
                    crate::execution::REQUEST_CONCURRENCY_LOCK_NAMESPACE,
                    slot,
                ),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("psql should start the lock-holder session");

        thread::sleep(Duration::from_millis(250));
        assert!(
            child
                .try_wait()
                .expect("psql lock-holder status should be readable")
                .is_none(),
            "psql lock-holder exited before the test request ran"
        );

        PsqlSessionGuard { child }
    }

    fn start_mock_stream_server(response_body: &str) -> (String, mpsc::Receiver<Value>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = format!(
            "http://{}/v1/chat/completions",
            listener
                .local_addr()
                .expect("listener should have a local address")
        );
        let (sender, receiver) = mpsc::channel();
        let response_body = response_body.to_owned();

        thread::spawn(move || {
            let (stream, _) = listener
                .accept()
                .expect("server should accept one connection");
            let request_body = read_mock_stream_request(stream, &response_body);
            sender
                .send(request_body)
                .expect("request body should be sent back to the test");
        });

        (address, receiver)
    }

    fn start_mock_json_server(path: &str, response_body: &str) -> (String, mpsc::Receiver<Value>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = format!(
            "http://{}{}",
            listener
                .local_addr()
                .expect("listener should have a local address"),
            path
        );
        let (sender, receiver) = mpsc::channel();
        let expected_path = path.to_owned();
        let response_body = response_body.to_owned();

        thread::spawn(move || {
            let (stream, _) = listener
                .accept()
                .expect("server should accept one connection");
            let request_body = read_mock_json_request(stream, &expected_path, &response_body);
            sender
                .send(request_body)
                .expect("request body should be sent back to the test");
        });

        (address, receiver)
    }

    fn start_delayed_mock_json_server(
        path: &str,
        response_body: &str,
        response_delay: Duration,
    ) -> (String, mpsc::Receiver<Value>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = format!(
            "http://{}{}",
            listener
                .local_addr()
                .expect("listener should have a local address"),
            path
        );
        let (sender, receiver) = mpsc::channel();
        let expected_path = path.to_owned();
        let response_body = response_body.to_owned();

        thread::spawn(move || {
            let (stream, _) = listener
                .accept()
                .expect("server should accept one connection");
            let request_body = read_mock_json_request_with_delay(
                stream,
                &expected_path,
                &response_body,
                response_delay,
            );
            sender
                .send(request_body)
                .expect("request body should be sent back to the test");
        });

        (address, receiver)
    }

    fn start_mock_runtime_discovery_server(
        status_code: u16,
        response_body: &str,
    ) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = format!(
            "http://{}/v1/chat/completions",
            listener
                .local_addr()
                .expect("listener should have a local address")
        );
        let (sender, receiver) = mpsc::channel();
        let response_body = response_body.to_owned();

        thread::spawn(move || {
            let (stream, _) = listener
                .accept()
                .expect("server should accept one connection");
            let request_line =
                read_mock_runtime_discovery_request(stream, status_code, &response_body);
            sender
                .send(request_line)
                .expect("request line should be sent back to the test");
        });

        (address, receiver)
    }

    fn read_mock_stream_request(mut stream: TcpStream, response_body: &str) -> Value {
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

        loop {
            let mut header_line = String::new();
            reader
                .read_line(&mut header_line)
                .expect("header line should read");

            if header_line == "\r\n" {
                break;
            }

            if header_line
                .to_ascii_lowercase()
                .starts_with("content-length:")
            {
                let parsed = header_line
                    .split_once(':')
                    .expect("content-length header should contain a separator")
                    .1
                    .trim()
                    .parse::<usize>()
                    .expect("content-length should parse");
                content_length = Some(parsed);
            }
        }

        let body_length = content_length.expect("request should include content-length");
        let mut body = vec![0_u8; body_length];
        reader
            .read_exact(&mut body)
            .expect("request body should read");

        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        )
        .expect("response should write");
        stream.flush().expect("response should flush");

        serde_json::from_slice(&body).expect("request body should be valid JSON")
    }

    fn read_mock_json_request(
        stream: TcpStream,
        expected_path: &str,
        response_body: &str,
    ) -> Value {
        read_mock_json_request_with_delay(
            stream,
            expected_path,
            response_body,
            Duration::from_millis(0),
        )
    }

    fn read_mock_json_request_with_delay(
        mut stream: TcpStream,
        expected_path: &str,
        response_body: &str,
        response_delay: Duration,
    ) -> Value {
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

        loop {
            let mut header_line = String::new();
            reader
                .read_line(&mut header_line)
                .expect("header line should read");

            if header_line == "\r\n" {
                break;
            }

            if header_line
                .to_ascii_lowercase()
                .starts_with("content-length:")
            {
                let parsed = header_line
                    .split_once(':')
                    .expect("content-length header should contain a separator")
                    .1
                    .trim()
                    .parse::<usize>()
                    .expect("content-length should parse");
                content_length = Some(parsed);
            }
        }

        let body_length = content_length.expect("request should include content-length");
        let mut body = vec![0_u8; body_length];
        reader
            .read_exact(&mut body)
            .expect("request body should read");

        thread::sleep(response_delay);

        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        )
        .expect("response should write");
        stream.flush().expect("response should flush");

        serde_json::from_slice(&body).expect("request body should be valid JSON")
    }

    fn read_mock_runtime_discovery_request(
        mut stream: TcpStream,
        status_code: u16,
        response_body: &str,
    ) -> String {
        let mut reader = BufReader::new(stream.try_clone().expect("stream should clone"));
        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .expect("request line should read");
        assert!(
            request_line.starts_with("GET /v1/models HTTP/1.1"),
            "unexpected request line: {request_line}"
        );

        loop {
            let mut header_line = String::new();
            reader
                .read_line(&mut header_line)
                .expect("header line should read");

            if header_line == "\r\n" {
                break;
            }
        }

        write!(
            stream,
            "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            status_code,
            response_body.len(),
            response_body
        )
        .expect("response should write");
        stream.flush().expect("response should flush");

        request_line
    }

    fn candle_generation_pg_test_enabled() -> bool {
        env::var("POSTLLM_PG_TEST_CANDLE_E2E")
            .map(|value| {
                matches!(
                    value.trim(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false)
    }

    fn candle_generation_pg_test_model() -> String {
        env::var("POSTLLM_PG_TEST_CANDLE_MODEL")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_PG_TEST_CANDLE_MODEL.to_owned())
    }

    fn candle_generation_pg_test_cache_dir() -> Option<String> {
        env::var("POSTLLM_PG_TEST_CANDLE_CACHE_DIR")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    }

    fn fresh_test_cache_dir(label: &str) -> String {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after the unix epoch")
            .as_nanos();
        let path = env::temp_dir().join(format!("postllm-{label}-{}-{unique}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);

        path.display().to_string()
    }

    fn sql_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn configure_candle_generation_pg_test() -> Option<String> {
        if !candle_generation_pg_test_enabled() {
            return None;
        }

        let model = candle_generation_pg_test_model();
        let cache_dir = candle_generation_pg_test_cache_dir();
        let cache_dir_sql = cache_dir
            .as_deref()
            .map_or_else(|| "NULL".to_owned(), sql_literal);
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'candle', model => {}, candle_cache_dir => {}, candle_offline => false, candle_device => 'cpu', candle_max_input_tokens => 0, candle_max_concurrency => 0)",
            sql_literal(&model),
            cache_dir_sql,
        ));

        assert_eq!(configured["runtime"], "candle");
        assert_eq!(configured["model"].as_str(), Some(model.as_str()));
        assert_eq!(configured["candle_device"], "cpu");

        if let Some(cache_dir) = cache_dir.as_deref() {
            assert_eq!(configured["candle_cache_dir"].as_str(), Some(cache_dir));
        }

        Some(model)
    }

    fn smoke_answer_is_four(text: &str) -> bool {
        let normalized = text.trim().to_ascii_lowercase();
        normalized.contains('4') || normalized.contains("four")
    }

    #[pg_test]
    fn sql_settings_should_report_defaults() {
        let settings = sql_json("SELECT postllm.settings()");
        let capabilities = &settings["capabilities"];

        assert_eq!(settings["runtime"], "openai");
        assert_eq!(
            settings["base_url"],
            "http://127.0.0.1:11434/v1/chat/completions"
        );
        assert_eq!(settings["model"], "llama3.2");
        assert_eq!(
            settings["embedding_model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
        assert_eq!(settings["timeout_ms"], 30_000);
        assert_eq!(settings["max_retries"], 2);
        assert_eq!(settings["retry_backoff_ms"], 250);
        assert_eq!(settings["request_max_concurrency"], 0);
        assert_eq!(settings["request_token_budget"], 0);
        assert_eq!(settings["request_runtime_budget_ms"], 0);
        assert_eq!(settings["request_spend_budget_microusd"], 0);
        assert_eq!(settings["output_token_price_microusd_per_1k"], 0);
        assert_eq!(settings["http_allowed_hosts"], Value::Null);
        assert_eq!(settings["http_allowed_providers"], Value::Null);
        assert_eq!(settings["has_api_key"], false);
        assert_eq!(settings["api_key_source"], "none");
        assert_eq!(settings["api_key_secret"], Value::Null);
        assert_eq!(settings["candle_cache_dir"], Value::Null);
        assert_eq!(settings["candle_offline"], false);
        assert_eq!(settings["candle_device"], "auto");
        assert_eq!(settings["candle_max_input_tokens"], 0);
        assert_eq!(settings["candle_max_concurrency"], 0);
        assert_eq!(settings["request_logging"], false);
        assert_eq!(settings["request_log_redact_inputs"], true);
        assert_eq!(settings["request_log_redact_outputs"], true);

        assert_eq!(capabilities["runtime"], "openai");
        assert_eq!(capabilities["model"], "llama3.2");
        assert_eq!(
            capabilities["embedding_model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
        assert_eq!(capabilities["features"]["chat"]["available"], true);
        assert_eq!(capabilities["features"]["chat"]["runtime"], "openai");
        assert_eq!(capabilities["features"]["complete"]["available"], true);
        assert_eq!(capabilities["features"]["embeddings"]["runtime"], "openai");
        assert_eq!(capabilities["features"]["reranking"]["available"], true);
        assert_eq!(capabilities["features"]["tools"]["available"], true);
        assert_eq!(
            capabilities["features"]["structured_outputs"]["available"],
            true
        );
        assert_eq!(capabilities["features"]["streaming"]["available"], true);
        assert_eq!(
            capabilities["features"]["multimodal_inputs"]["available"],
            true
        );
    }

    #[pg_test]
    fn sql_configure_allow_hosted_endpoints_with_matching_allowlist() {
        sql_run("SET LOCAL postllm.http_allowed_hosts = '*.openai.com'");

        let configured = sql_json(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => 'https://api.openai.com/v1/chat/completions'
            )",
        );

        assert_eq!(
            configured["base_url"],
            "https://api.openai.com/v1/chat/completions"
        );
    }

    #[pg_test]
    fn sql_configure_reject_hosted_endpoints_outside_allowlist() {
        sql_run("SET LOCAL postllm.http_allowed_hosts = 'host.docker.internal:11434'");

        let configured = sql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => 'https://api.openai.com/v1/chat/completions'
            )"
        ));

        assert_eq!(
            configured["base_url"],
            "https://api.openai.com/v1/chat/completions"
        );
        let discovery = sql_json("SELECT postllm.runtime_discover()");
        assert_eq!(discovery["ready"], false);
        assert!(
            discovery["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("host 'api.openai.com' is not permitted"))
        );
    }

    #[pg_test]
    fn sql_runtime_discover_report_disallowed_hosted_provider_safelist() {
        sql_run("SET LOCAL postllm.http_allowed_providers = 'ollama'");
        sql_run("SET LOCAL postllm.base_url = 'https://api.openai.com/v1/chat/completions'");
        sql_run("SET LOCAL postllm.runtime = 'openai'");

        let discovery = sql_json("SELECT postllm.runtime_discover()");

        assert_eq!(discovery["ready"], false);
        assert!(
            discovery["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("provider 'openai' is not permitted"))
        );
    }

    #[pg_test]
    fn sql_profiles_and_model_aliases_should_default_to_empty_arrays() {
        assert_eq!(sql_json("SELECT postllm.profiles()"), json!([]));
        assert_eq!(sql_json("SELECT postllm.secrets()"), json!([]));
        assert_eq!(sql_json("SELECT postllm.permissions()"), json!([]));
        assert_eq!(sql_json("SELECT postllm.model_aliases()"), json!([]));
    }

    #[pg_test]
    fn sql_permissions_should_store_and_delete_rows() {
        let role_name = create_test_role("permissions");
        let stored = sql_permission_set(
            &role_name,
            "runtime",
            "openai",
            Some("Hosted runtime access"),
        );
        let fetched = sql_json(&format!(
            "SELECT postllm.permission(
                role_name => {},
                object_type => 'runtime',
                target => 'openai'
            )",
            sql_literal(&role_name),
        ));
        let listed = sql_json("SELECT postllm.permissions()");
        let deleted = sql_json(&format!(
            "SELECT postllm.permission_delete(
                role_name => {},
                object_type => 'runtime',
                target => 'openai'
            )",
            sql_literal(&role_name),
        ));

        assert_eq!(stored["role_name"], role_name);
        assert_eq!(stored["object_type"], "runtime");
        assert_eq!(stored["target"], "openai");
        assert_eq!(stored["description"], "Hosted runtime access");
        assert_eq!(fetched["role_name"], role_name);
        assert_eq!(listed.as_array().map(Vec::len), Some(1));
        assert_eq!(deleted["deleted"], true);
        assert_eq!(sql_json("SELECT postllm.permissions()"), json!([]));
    }

    #[pg_test]
    fn sql_configure_should_allow_permitted_runtime_for_outer_role() {
        let role_name = create_test_role("runtime_allowed");
        grant_permission(&role_name, "runtime", "openai");

        set_local_role(&role_name);
        let configured = sql_json("SELECT postllm.configure(runtime => 'openai')");

        assert_eq!(configured["runtime"], "openai");
    }

    #[pg_test]
    #[should_panic(expected = "postllm access denied for role '")]
    fn sql_configure_should_reject_disallowed_runtime_for_outer_role() {
        let allowed_role = create_test_role("runtime_policy");
        let caller_role = create_test_role("runtime_denied");
        grant_permission(&allowed_role, "runtime", "openai");

        set_local_role(&caller_role);
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'openai')",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "generation model 'blocked-model' is not permitted")]
    fn sql_complete_reject_disallowed_generation_model_outer_role() {
        let allowed_role = create_test_role("generation_policy");
        let caller_role = create_test_role("generation_denied");
        grant_permission(&allowed_role, "generation_model", "allowed-model");

        set_local_role(&caller_role);
        drop(Spi::get_one::<String>(
            "SELECT postllm.complete(prompt => 'hello', model => 'blocked-model')",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "embedding model 'blocked-embed' is not permitted")]
    fn sql_embed_reject_disallowed_embedding_model_outer_role() {
        let allowed_role = create_test_role("embedding_policy");
        let caller_role = create_test_role("embedding_denied");
        grant_permission(
            &allowed_role,
            "embedding_model",
            "sentence-transformers/all-MiniLM-L6-v2",
        );

        set_local_role(&caller_role);
        drop(Spi::get_one::<Vec<f32>>(
            "SELECT postllm.embed('hello from SQL', model => 'blocked-embed')",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "privileged setting 'base_url' is not permitted")]
    fn sql_configure_reject_disallowed_privileged_setting_outer_role() {
        let allowed_role = create_test_role("setting_policy");
        let caller_role = create_test_role("setting_denied");
        grant_permission(&allowed_role, "setting", "base_url");

        set_local_role(&caller_role);
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(base_url => 'http://127.0.0.1:9999/v1/chat/completions')",
        ));
    }

    #[pg_test]
    fn sql_rt_discover_reject_unauthorized_base_url() {
        let allowed_role = create_test_role("setting_direct_policy");
        let caller_role = create_test_role("setting_direct_denied");
        grant_permission(&allowed_role, "setting", "base_url");

        set_local_role(&caller_role);
        sql_run("SET LOCAL postllm.base_url = 'http://127.0.0.1:9999/v1/chat/completions'");
        let discovery = sql_json("SELECT postllm.runtime_discover()");
        assert_eq!(discovery["ready"], false);
        assert!(discovery["reason"].as_str().is_some_and(|reason| {
            reason.contains("privileged setting 'base_url' is not permitted")
        }));
    }

    #[pg_test]
    fn sql_profile_set_should_store_and_apply_named_configuration() {
        let stored = sql_json(
            "SELECT postllm.profile_set(
                name => 'hosted-staging',
                description => 'Hosted staging profile',
                runtime => 'openai',
                base_url => 'http://127.0.0.1:9090/v1/chat/completions',
                model => 'staging-chat',
                timeout_ms => 9000,
                max_retries => 1,
                retry_backoff_ms => 50,
                request_max_concurrency => 4,
                request_token_budget => 128,
                request_runtime_budget_ms => 4000,
                request_spend_budget_microusd => 750,
                output_token_price_microusd_per_1k => 250000
            )",
        );
        let fetched = sql_json("SELECT postllm.profile('hosted-staging')");
        let listed = sql_json("SELECT postllm.profiles()");
        let applied = sql_json("SELECT postllm.profile_apply('hosted-staging')");

        assert_eq!(stored["name"], "hosted-staging");
        assert_eq!(stored["description"], "Hosted staging profile");
        assert_eq!(
            stored["config"],
            json!({
                "runtime": "openai",
                "base_url": "http://127.0.0.1:9090/v1/chat/completions",
                "model": "staging-chat",
                "timeout_ms": 9000,
                "max_retries": 1,
                "retry_backoff_ms": 50,
                "request_max_concurrency": 4,
                "request_token_budget": 128,
                "request_runtime_budget_ms": 4000,
                "request_spend_budget_microusd": 750,
                "output_token_price_microusd_per_1k": 250_000,
            })
        );
        assert_eq!(fetched["name"], "hosted-staging");
        assert_eq!(listed.as_array().map(Vec::len), Some(1));
        assert_eq!(listed[0]["name"], "hosted-staging");
        assert_eq!(applied["profile"], "hosted-staging");
        assert_eq!(applied["runtime"], "openai");
        assert_eq!(
            applied["base_url"],
            "http://127.0.0.1:9090/v1/chat/completions"
        );
        assert_eq!(applied["model"], "staging-chat");
        assert_eq!(applied["timeout_ms"], 9000);
        assert_eq!(applied["max_retries"], 1);
        assert_eq!(applied["retry_backoff_ms"], 50);
        assert_eq!(applied["request_max_concurrency"], 4);
        assert_eq!(applied["request_token_budget"], 128);
        assert_eq!(applied["request_runtime_budget_ms"], 4000);
        assert_eq!(applied["request_spend_budget_microusd"], 750);
        assert_eq!(applied["output_token_price_microusd_per_1k"], 250_000);
        assert_eq!(applied["api_key_source"], "none");
        assert_eq!(applied["api_key_secret"], Value::Null);
    }

    #[pg_test]
    fn sql_profile_apply_should_reset_unspecified_settings_to_defaults() {
        drop(sql_json(
            "SELECT postllm.configure(
                runtime => 'candle',
                model => 'Qwen/Qwen2.5-0.5B-Instruct',
                request_max_concurrency => 2,
                request_token_budget => 64,
                request_runtime_budget_ms => 5000,
                request_spend_budget_microusd => 2500,
                output_token_price_microusd_per_1k => 1000,
                candle_offline => true,
                candle_device => 'cpu',
                candle_max_input_tokens => 512,
                candle_max_concurrency => 3
            )",
        ));
        drop(sql_json(
            "SELECT postllm.profile_set(
                name => 'hosted-default-reset',
                runtime => 'openai',
                base_url => 'http://127.0.0.1:8080/v1/chat/completions',
                model => 'reset-chat'
            )",
        ));

        let applied = sql_json("SELECT postllm.profile_apply('hosted-default-reset')");

        assert_eq!(applied["runtime"], "openai");
        assert_eq!(
            applied["base_url"],
            "http://127.0.0.1:8080/v1/chat/completions"
        );
        assert_eq!(applied["model"], "reset-chat");
        assert_eq!(
            applied["embedding_model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
        assert_eq!(applied["has_api_key"], false);
        assert_eq!(applied["api_key_source"], "none");
        assert_eq!(applied["api_key_secret"], Value::Null);
        assert_eq!(applied["request_max_concurrency"], 0);
        assert_eq!(applied["request_token_budget"], 0);
        assert_eq!(applied["request_runtime_budget_ms"], 0);
        assert_eq!(applied["request_spend_budget_microusd"], 0);
        assert_eq!(applied["output_token_price_microusd_per_1k"], 0);
        assert_eq!(applied["candle_offline"], false);
        assert_eq!(applied["candle_device"], "auto");
        assert_eq!(applied["candle_max_input_tokens"], 0);
        assert_eq!(applied["candle_max_concurrency"], 0);
    }

    #[pg_test]
    fn sql_profile_and_model_alias_delete_should_remove_rows() {
        drop(sql_json(
            "SELECT postllm.profile_set(name => 'to-delete', runtime => 'openai', model => 'delete-me')",
        ));
        drop(sql_json(
            "SELECT postllm.model_alias_set(alias => 'starter', lane => 'generation', model => 'Qwen/Qwen2.5-0.5B-Instruct')",
        ));

        let deleted_profile = sql_json("SELECT postllm.profile_delete('to-delete')");
        let deleted_alias =
            sql_json("SELECT postllm.model_alias_delete(alias => 'starter', lane => 'generation')");

        assert_eq!(deleted_profile["name"], "to-delete");
        assert_eq!(deleted_profile["deleted"], true);
        assert_eq!(deleted_alias["alias"], "starter");
        assert_eq!(deleted_alias["lane"], "generation");
        assert_eq!(deleted_alias["deleted"], true);
        assert_eq!(sql_json("SELECT postllm.profiles()"), json!([]));
        assert_eq!(sql_json("SELECT postllm.secrets()"), json!([]));
        assert_eq!(sql_json("SELECT postllm.model_aliases()"), json!([]));
    }

    #[pg_test]
    fn sql_secret_set_should_configure_session_when_key_is_available() {
        if env::var("POSTLLM_SECRET_KEY").is_err() {
            return;
        }

        let stored = sql_json(
            "SELECT postllm.secret_set(
                name => 'openai-prod',
                value => 'sk-test-secret',
                description => 'Production OpenAI key'
            )",
        );
        let fetched = sql_json("SELECT postllm.secret('openai-prod')");
        let configured = sql_json(
            "SELECT postllm.configure(
                runtime => 'openai',
                api_key_secret => 'openai-prod'
            )",
        );

        assert_eq!(stored["name"], "openai-prod");
        assert_eq!(stored["description"], "Production OpenAI key");
        assert_eq!(stored["algorithm"], "chacha20poly1305-v1");
        assert_eq!(fetched["name"], "openai-prod");
        assert_eq!(configured["has_api_key"], true);
        assert_eq!(configured["api_key_source"], "secret");
        assert_eq!(configured["api_key_secret"], "openai-prod");
    }

    #[pg_test]
    fn sql_profile_apply_should_resolve_stored_secret_when_available() {
        if env::var("POSTLLM_SECRET_KEY").is_err() {
            return;
        }

        drop(sql_json(
            "SELECT postllm.secret_set(
                name => 'staging-key',
                value => 'sk-staging-secret',
                description => 'Staging key'
            )",
        ));
        let stored_profile = sql_json(
            "SELECT postllm.profile_set(
                name => 'hosted-secret-profile',
                runtime => 'openai',
                base_url => 'http://127.0.0.1:9090/v1/chat/completions',
                model => 'staging-chat',
                api_key_secret => 'staging-key'
            )",
        );
        let applied = sql_json("SELECT postllm.profile_apply('hosted-secret-profile')");

        assert_eq!(stored_profile["config"]["api_key_secret"], "staging-key");
        assert_eq!(applied["profile"], "hosted-secret-profile");
        assert_eq!(applied["has_api_key"], true);
        assert_eq!(applied["api_key_source"], "secret");
        assert_eq!(applied["api_key_secret"], "staging-key");
    }

    #[pg_test]
    fn sql_model_aliases_should_resolve_for_capabilities_and_lifecycle() {
        let generation_alias = sql_json(
            "SELECT postllm.model_alias_set(
                alias => 'starter',
                lane => 'generation',
                model => 'Qwen/Qwen2.5-0.5B-Instruct',
                description => 'Starter local generation model'
            )",
        );
        let embedding_alias = sql_json(
            "SELECT postllm.model_alias_set(
                alias => 'small-embed',
                lane => 'embedding',
                model => 'sentence-transformers/all-MiniLM-L6-v2',
                description => 'Compact embedding model'
            )",
        );
        let configured = sql_json(
            "SELECT postllm.configure(runtime => 'candle', model => 'starter', embedding_model => 'small-embed')",
        );
        let capabilities = sql_json("SELECT postllm.capabilities()");
        let embedding_info = sql_json("SELECT postllm.embedding_model_info('small-embed')");
        let model_inspect =
            sql_json("SELECT postllm.model_inspect(model => 'starter', lane => 'generation')");
        let fetched_generation_alias =
            sql_json("SELECT postllm.model_alias(alias => 'starter', lane => 'generation')");

        assert_eq!(generation_alias["alias"], "starter");
        assert_eq!(generation_alias["model"], "Qwen/Qwen2.5-0.5B-Instruct");
        assert_eq!(embedding_alias["alias"], "small-embed");
        assert_eq!(
            embedding_alias["model"],
            "sentence-transformers/all-MiniLM-L6-v2"
        );
        assert_eq!(configured["model"], "starter");
        assert_eq!(configured["embedding_model"], "small-embed");
        assert_eq!(capabilities["model"], "Qwen/Qwen2.5-0.5B-Instruct");
        assert_eq!(
            capabilities["embedding_model"],
            "sentence-transformers/all-MiniLM-L6-v2"
        );
        assert_eq!(capabilities["features"]["chat"]["available"], true);
        assert_eq!(
            embedding_info["model"],
            "sentence-transformers/all-MiniLM-L6-v2"
        );
        assert_eq!(model_inspect["model"], "Qwen/Qwen2.5-0.5B-Instruct");
        assert_eq!(fetched_generation_alias["alias"], "starter");
    }

    #[pg_test]
    fn sql_capabilities_should_report_default_runtime_and_embeddings() {
        let capabilities = sql_json("SELECT postllm.capabilities()");

        assert_eq!(capabilities["features"]["chat"]["available"], true);
        assert_eq!(capabilities["features"]["chat"]["runtime"], "openai");
        assert_eq!(capabilities["features"]["tools"]["available"], true);
        assert_eq!(capabilities["features"]["tools"]["runtime"], "openai");
        assert_eq!(
            capabilities["features"]["structured_outputs"]["available"],
            true
        );
        assert_eq!(
            capabilities["features"]["structured_outputs"]["runtime"],
            "openai"
        );
        assert_eq!(capabilities["features"]["streaming"]["available"], true);
        assert_eq!(capabilities["features"]["streaming"]["runtime"], "openai");
        assert_eq!(capabilities["features"]["embeddings"]["available"], true);
        assert_eq!(capabilities["features"]["reranking"]["available"], true);
        assert_eq!(capabilities["features"]["reranking"]["runtime"], "openai");
        assert_eq!(capabilities["features"]["reranking"]["model"], "llama3.2");
        assert_eq!(
            capabilities["features"]["multimodal_inputs"]["available"],
            true
        );
        assert_eq!(
            capabilities["features"]["embeddings"]["model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
    }

    #[pg_test]
    fn sql_embedding_model_info_should_report_default_metadata() {
        let info = sql_json("SELECT postllm.embedding_model_info()");

        assert_eq!(info["runtime"], "openai");
        assert_eq!(
            info["model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
        assert_eq!(info["dimension"], Value::Null);
        assert_eq!(info["max_sequence_length"], Value::Null);
        assert_eq!(info["pooling"], Value::Null);
        assert_eq!(info["normalization"]["default"], "provider-defined");
        assert_eq!(
            info["normalization"]["supported"],
            json!(["provider-defined", "l2", "none"])
        );
        assert_eq!(info["metadata_source"], "provider-defined");
    }

    #[pg_test]
    fn sql_embedding_model_info_should_report_cls_pooling_for_bge() {
        drop(sql_json("SELECT postllm.configure(runtime => 'candle')"));
        let info = sql_json("SELECT postllm.embedding_model_info('BAAI/bge-small-en-v1.5')");

        assert_eq!(info["runtime"], "candle");
        assert_eq!(info["model"], "BAAI/bge-small-en-v1.5");
        assert_eq!(info["architecture"], "bert");
        assert_eq!(info["dimension"], 384);
        assert_eq!(info["max_sequence_length"], 512);
        assert_eq!(info["pooling"], "cls");
    }

    #[pg_test]
    fn sql_embedding_model_info_should_report_distiluse_projection() {
        drop(sql_json("SELECT postllm.configure(runtime => 'candle')"));
        let info = sql_json(
            "SELECT postllm.embedding_model_info('sentence-transformers/distiluse-base-multilingual-cased-v2')",
        );

        assert_eq!(info["runtime"], "candle");
        assert_eq!(
            info["model"],
            "sentence-transformers/distiluse-base-multilingual-cased-v2"
        );
        assert_eq!(info["architecture"], "distilbert");
        assert_eq!(info["dimension"], 512);
        assert_eq!(info["max_sequence_length"], 512);
        assert_eq!(info["pooling"], "mean");
        assert_eq!(info["projection"]["in_dimension"], 768);
        assert_eq!(info["projection"]["out_dimension"], 512);
        assert_eq!(info["projection"]["activation"], "tanh");
    }

    #[pg_test]
    fn sql_model_inspect_should_default_to_embedding_model() {
        let cache_dir = fresh_test_cache_dir("model-inspect-embedding");
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', candle_cache_dir => {}, candle_device => 'cpu')",
            sql_literal(&cache_dir)
        ));
        let inspection = sql_json("SELECT postllm.model_inspect()");

        assert_eq!(configured["runtime"], "openai");
        assert_eq!(configured["candle_device"], "cpu");
        assert_eq!(
            configured["candle_cache_dir"].as_str(),
            Some(cache_dir.as_str())
        );
        assert_eq!(inspection["runtime"], "candle");
        assert_eq!(
            inspection["model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
        assert_eq!(inspection["lane"], "embedding");
        assert_eq!(inspection["device"]["requested"], "cpu");
        assert_eq!(inspection["device"]["resolved"], "cpu");
        assert_eq!(inspection["cache_dir"].as_str(), Some(cache_dir.as_str()));
        assert_eq!(inspection["disk_cached"], false);
        assert_eq!(inspection["memory_cached"], false);
        assert_eq!(inspection["cached_file_count"], 0);
        assert_eq!(inspection["cached_bytes"], 0);
        assert_eq!(inspection["integrity"]["ok"], true);
        assert_eq!(inspection["integrity"]["status"], "unchecked");
        assert_eq!(inspection["integrity"]["verified_files"], 0);
        assert_eq!(inspection["integrity"]["mismatched_files"], 0);
        assert_eq!(inspection["metadata"]["dimension"], 384);
        assert_eq!(inspection["metadata"]["pooling"], "mean");
    }

    #[pg_test]
    fn sql_model_inspect_should_default_to_generation_model() {
        let cache_dir = fresh_test_cache_dir("model-inspect-generation");
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct', candle_cache_dir => {}, candle_device => 'cpu')",
            sql_literal(&cache_dir)
        ));
        let inspection = sql_json("SELECT postllm.model_inspect()");

        assert_eq!(configured["runtime"], "candle");
        assert_eq!(configured["candle_device"], "cpu");
        assert_eq!(inspection["runtime"], "candle");
        assert_eq!(inspection["model"], "Qwen/Qwen2.5-0.5B-Instruct");
        assert_eq!(inspection["lane"], "generation");
        assert_eq!(inspection["offline"], false);
        assert_eq!(inspection["device"]["requested"], "cpu");
        assert_eq!(inspection["device"]["resolved"], "cpu");
        assert_eq!(inspection["cache_dir"].as_str(), Some(cache_dir.as_str()));
        assert_eq!(inspection["disk_cached"], false);
        assert_eq!(inspection["memory_cached"], false);
        assert_eq!(inspection["cached_file_count"], 0);
        assert_eq!(inspection["integrity"]["ok"], true);
        assert_eq!(inspection["integrity"]["status"], "unchecked");
        assert_eq!(inspection["integrity"]["verified_files"], 0);
        assert_eq!(inspection["integrity"]["mismatched_files"], 0);
        assert_eq!(inspection["metadata"]["supported"], true);
        assert_eq!(inspection["metadata"]["chat_template"], "chatml");
    }

    #[pg_test]
    fn sql_model_evict_should_report_noop_for_empty_generation_cache() {
        let cache_dir = fresh_test_cache_dir("model-evict-generation");
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct', candle_cache_dir => {}, candle_device => 'cpu')",
            sql_literal(&cache_dir)
        ));
        let eviction = sql_json("SELECT postllm.model_evict(scope => 'all')");

        assert_eq!(configured["runtime"], "candle");
        assert_eq!(configured["candle_device"], "cpu");
        assert_eq!(eviction["runtime"], "candle");
        assert_eq!(eviction["action"], "evict");
        assert_eq!(eviction["model"], "Qwen/Qwen2.5-0.5B-Instruct");
        assert_eq!(eviction["lane"], "generation");
        assert_eq!(eviction["offline"], false);
        assert_eq!(eviction["device"]["requested"], "cpu");
        assert_eq!(eviction["device"]["resolved"], "cpu");
        assert_eq!(eviction["scope"], "all");
        assert_eq!(eviction["memory_evicted"], false);
        assert_eq!(eviction["disk_evicted"], false);
        assert_eq!(eviction["removed_files"], 0);
        assert_eq!(eviction["removed_bytes"], 0);
        assert_eq!(eviction["memory_cached"], false);
        assert_eq!(eviction["disk_cached"], false);
    }

    #[pg_test]
    fn sql_model_lifecycle_should_smoke_live_candle_generation() {
        let Some(model) = configure_candle_generation_pg_test() else {
            return;
        };

        let installed = sql_json("SELECT postllm.model_install(lane => 'generation')");
        let prewarmed = sql_json("SELECT postllm.model_prewarm(lane => 'generation')");
        let offline_configured = sql_json("SELECT postllm.configure(candle_offline => true)");
        let offline_text = sql_text(
            "SELECT trim(postllm.chat_text(ARRAY[postllm.system('You are a literal test harness. Reply with only 4.'), postllm.user('What is 2 + 2?')], temperature => 0.0, max_tokens => 8))",
        );

        assert_eq!(installed["runtime"], "candle");
        assert_eq!(installed["action"], "install");
        assert_eq!(installed["model"].as_str(), Some(model.as_str()));
        assert_eq!(installed["lane"], "generation");
        assert_eq!(installed["device"]["requested"], "cpu");
        assert_eq!(installed["device"]["resolved"], "cpu");
        assert_eq!(installed["disk_cached"], true);
        assert_eq!(installed["integrity"]["ok"], true);
        assert_eq!(installed["integrity"]["status"], "verified");
        assert!(
            installed["cached_file_count"].as_u64().unwrap_or(0) > 0,
            "expected generation install to cache files, got {installed}"
        );
        assert!(
            installed["integrity"]["verified_files"]
                .as_u64()
                .unwrap_or(0)
                > 0,
            "expected generation install to verify cached files, got {installed}"
        );
        assert!(
            installed["downloaded_files"]
                .as_array()
                .is_some_and(|files| !files.is_empty()),
            "expected install to report downloaded files, got {installed}"
        );

        assert_eq!(prewarmed["runtime"], "candle");
        assert_eq!(prewarmed["action"], "prewarm");
        assert_eq!(prewarmed["model"].as_str(), Some(model.as_str()));
        assert_eq!(prewarmed["lane"], "generation");
        assert_eq!(prewarmed["device"]["requested"], "cpu");
        assert_eq!(prewarmed["device"]["resolved"], "cpu");
        assert_eq!(prewarmed["memory_cached"], true);
        assert_eq!(prewarmed["disk_cached"], true);
        assert_eq!(prewarmed["integrity"]["ok"], true);
        assert_eq!(offline_configured["candle_offline"], true);
        assert!(
            smoke_answer_is_four(&offline_text),
            "expected cached offline Candle generation to contain 4, got {offline_text}"
        );
    }

    #[pg_test]
    #[should_panic(
        expected = "offline mode is enabled and model 'sentence-transformers/paraphrase-MiniLM-L3-v2' is missing cached artifact 'snapshot metadata'"
    )]
    fn sql_embed_should_reject_offline_cache_misses() {
        let cache_dir = fresh_test_cache_dir("offline-embed-miss");
        drop(sql_json(&format!(
            "SELECT postllm.configure(runtime => 'candle', candle_cache_dir => {}, candle_offline => true)",
            sql_literal(&cache_dir)
        )));

        drop(Spi::get_one::<Vec<f32>>(
            "SELECT postllm.embed('offline test')",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "offline mode is enabled and model 'Qwen/Qwen2.5-0.5B-Instruct' is missing cached artifact 'snapshot metadata'"
    )]
    fn sql_chat_should_reject_offline_cache_misses() {
        let cache_dir = fresh_test_cache_dir("offline-chat-miss");
        drop(sql_json(&format!(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct', candle_cache_dir => {}, candle_offline => true)",
            sql_literal(&cache_dir)
        )));

        drop(Spi::get_one::<String>(
            "SELECT postllm.chat_text(ARRAY[postllm.user('offline test')], temperature => 0.0, max_tokens => 8)",
        ));
    }

    #[pg_test]
    fn sql_chunk_text_should_split_input_into_overlapping_chunks() {
        let chunks = sql_json(
            "SELECT to_jsonb(postllm.chunk_text('Alpha sentence. Beta sentence. Gamma sentence.', chunk_chars => 24, overlap_chars => 6))",
        );

        assert_eq!(
            chunks,
            json!([
                "Alpha sentence.",
                "tence. Beta sentence.",
                "tence. Gamma sentence."
            ])
        );
    }

    #[pg_test]
    fn sql_chunk_document_should_propagate_metadata() {
        let rows = sql_json(
            r#"SELECT jsonb_agg(
                jsonb_build_object(
                    'index', index,
                    'chunk', chunk,
                    'metadata', metadata
                )
                ORDER BY index
            )
            FROM postllm.chunk_document(
                'Alpha sentence. Beta sentence.',
                '{"doc_id":"guide"}'::jsonb,
                chunk_chars => 18,
                overlap_chars => 4
            ) AS chunk"#,
        );

        assert_eq!(rows.as_array().map(Vec::len), Some(2));
        assert_eq!(rows[0]["index"], 1);
        assert_eq!(rows[0]["chunk"], "Alpha sentence.");
        assert_eq!(rows[0]["metadata"]["doc_id"], "guide");
        assert_eq!(rows[0]["metadata"]["_postllm_chunk"]["index"], 1);
        assert_eq!(rows[0]["metadata"]["_postllm_chunk"]["start_char"], 0);
        assert_eq!(rows[1]["index"], 2);
        assert_eq!(rows[1]["chunk"], "nce. Beta sentence.");
        assert_eq!(rows[1]["metadata"]["doc_id"], "guide");
        assert_eq!(rows[1]["metadata"]["_postllm_chunk"]["index"], 2);
    }

    #[pg_test]
    fn sql_embed_document_should_smoke_live_embeddings() {
        if !candle_generation_pg_test_enabled() {
            return;
        }

        let rows = sql_json(
            r#"SELECT jsonb_agg(
                jsonb_build_object(
                    'chunk_id', chunk_id,
                    'doc_id', doc_id,
                    'chunk_no', chunk_no,
                    'content', content,
                    'metadata', metadata,
                    'embedding_dims', array_length(embedding, 1)
                )
                ORDER BY chunk_no
            )
            FROM postllm.embed_document(
                'guide-1',
                'Alpha sentence. Beta sentence.',
                '{"source":"manual"}'::jsonb,
                chunk_chars => 18,
                overlap_chars => 4
            ) AS chunk"#,
        );
        let rows = rows
            .as_array()
            .expect("embed_document should aggregate to a JSON array");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["doc_id"], "guide-1");
        assert_eq!(rows[0]["chunk_no"], 1);
        assert_eq!(rows[0]["content"], "Alpha sentence.");
        assert!(
            rows[0]["chunk_id"]
                .as_str()
                .is_some_and(|value| value.starts_with("plc_"))
        );
        assert_eq!(rows[0]["metadata"]["source"], "manual");
        assert_eq!(rows[0]["metadata"]["_postllm_chunk"]["index"], 1);
        assert!(
            rows[0]["embedding_dims"]
                .as_i64()
                .is_some_and(|value| value > 0)
        );
        assert_eq!(rows[1]["chunk_no"], 2);
    }

    #[pg_test]
    fn sql_ingest_document_should_smoke_live_upserts_when_enabled() {
        if !candle_generation_pg_test_enabled() {
            return;
        }

        Spi::run(
            r"CREATE TEMP TABLE doc_chunks_ingest (
                chunk_id text PRIMARY KEY,
                doc_id text NOT NULL,
                chunk_no integer NOT NULL,
                content text NOT NULL,
                metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
                embedding real[] NOT NULL
            )",
        )
        .expect("ingest smoke test should create the temp table");

        let initial = sql_json(
            r#"SELECT postllm.ingest_document(
                'pg_temp.doc_chunks_ingest',
                'guide-1',
                'Alpha sentence. Beta sentence.',
                '{"source":"manual"}'::jsonb,
                chunk_chars => 18,
                overlap_chars => 4
            )"#,
        );
        assert_eq!(initial["chunk_count"], 2);
        assert_eq!(initial["written"], 2);
        assert_eq!(initial["unchanged"], 0);
        assert_eq!(initial["deleted"], 0);

        let second = sql_json(
            r#"SELECT postllm.ingest_document(
                'pg_temp.doc_chunks_ingest',
                'guide-1',
                'Alpha sentence. Beta sentence.',
                '{"source":"manual"}'::jsonb,
                chunk_chars => 18,
                overlap_chars => 4
            )"#,
        );
        assert_eq!(second["chunk_count"], 2);
        assert_eq!(second["written"], 0);
        assert_eq!(second["unchanged"], 2);
        assert_eq!(second["deleted"], 0);

        let third = sql_json(
            r#"SELECT postllm.ingest_document(
                'pg_temp.doc_chunks_ingest',
                'guide-1',
                'Only one chunk now.',
                '{"source":"manual"}'::jsonb,
                chunk_chars => 18,
                overlap_chars => 4
            )"#,
        );
        assert_eq!(third["chunk_count"], 1);
        assert_eq!(third["written"], 1);
        assert_eq!(third["deleted"], 2);

        let row_count = sql_text("SELECT count(*)::text FROM pg_temp.doc_chunks_ingest");
        assert_eq!(row_count.trim(), "1");
    }

    #[pg_test]
    fn sql_rrf_score_should_compute_weighted_rank_fusion() {
        let score = sql_float(
            "SELECT postllm.rrf_score(semantic_rank => 2, keyword_rank => 1, semantic_weight => 1.0, keyword_weight => 2.0, rrf_k => 10)",
        );

        assert!((score - (1.0 / 12.0 + 2.0 / 11.0)).abs() < 1e-9);
    }

    #[pg_test]
    fn sql_keyword_rank_should_order_keyword_matches() {
        let rows = sql_json(
            r"SELECT jsonb_agg(to_jsonb(ranked) ORDER BY rank)
            FROM postllm.keyword_rank(
                'autovacuum bloat',
                ARRAY[
                    'Bananas are yellow.',
                    'Autovacuum controls table bloat.',
                    'Autovacuum is a PostgreSQL worker.'
                ]
            ) AS ranked",
        );
        let rows = rows
            .as_array()
            .expect("keyword_rank should aggregate to a JSON array");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["rank"], 1);
        assert_eq!(rows[0]["index"], 2);
        assert_eq!(rows[0]["document"], "Autovacuum controls table bloat.");
        assert_eq!(rows[1]["rank"], 2);
        assert_eq!(rows[1]["index"], 3);
    }

    #[pg_test]
    fn sql_rerank_should_rank_mock_hosted_results() {
        let (base_url, receiver) = start_mock_json_server(
            "/v1/rerank",
            r#"{"results":[{"index":1,"relevance_score":0.98},{"index":0,"relevance_score":0.12}]}"#,
        );
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-reranker')",
            sql_literal(&base_url)
        ));
        let rows = sql_json(
            r"SELECT jsonb_agg(to_jsonb(ranked) ORDER BY rank)
            FROM postllm.rerank(
                'What controls table bloat?',
                ARRAY[
                    'Bananas are yellow.',
                    'Autovacuum removes dead tuples.'
                ],
                top_n => 1
            ) AS ranked",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the rerank request");

        assert_eq!(configured["runtime"], "openai");
        assert_eq!(request_body["model"], "mock-reranker");
        assert_eq!(request_body["query"], "What controls table bloat?");
        assert_eq!(request_body["documents"][0], "Bananas are yellow.");
        assert_eq!(
            request_body["documents"][1],
            "Autovacuum removes dead tuples."
        );
        assert_eq!(request_body["top_n"], 1);
        assert_eq!(
            rows,
            json!([{
                "rank": 1,
                "index": 2,
                "document": "Autovacuum removes dead tuples.",
                "score": 0.98,
            }])
        );
    }

    #[pg_test]
    fn sql_hybrid_rank_should_fuse_mock_semantic_and_keywords() {
        let (base_url, receiver) = start_mock_json_server(
            "/v1/rerank",
            r#"{"results":[
                {"index":0,"relevance_score":0.99},
                {"index":1,"relevance_score":0.92},
                {"index":2,"relevance_score":0.91}
            ]}"#,
        );
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-reranker')",
            sql_literal(&base_url)
        ));
        let rows = sql_json(
            r"SELECT jsonb_agg(to_jsonb(ranked) ORDER BY rank)
            FROM postllm.hybrid_rank(
                'autovacuum bloat',
                ARRAY[
                    'Bananas are yellow.',
                    'Autovacuum controls table bloat.',
                    'Autovacuum is a PostgreSQL worker.'
                ],
                top_n => 2
            ) AS ranked",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the hybrid semantic request");
        let rows = rows
            .as_array()
            .expect("hybrid_rank should aggregate to a JSON array");

        assert_eq!(configured["runtime"], "openai");
        assert_eq!(request_body["model"], "mock-reranker");
        assert_eq!(request_body["query"], "autovacuum bloat");
        assert_eq!(
            request_body["documents"][1],
            "Autovacuum controls table bloat."
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["rank"], 1);
        assert_eq!(rows[0]["index"], 2);
        assert_eq!(rows[0]["semantic_rank"], 2);
        assert_eq!(rows[0]["keyword_rank"], 1);
        assert_eq!(rows[1]["rank"], 2);
        assert_eq!(rows[1]["index"], 3);
        assert_eq!(rows[1]["semantic_rank"], 3);
        assert_eq!(rows[1]["keyword_rank"], 2);
    }

    #[pg_test]
    fn sql_rag_should_build_keyword_context_and_return_metadata() {
        let (base_url, receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-rag",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{
                            "role":"assistant",
                            "content":"Autovacuum removes dead tuples and VACUUM can reclaim space."
                        },
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":42,"completion_tokens":9,"total_tokens":51}
            }"#,
        );
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-answer-model')",
            sql_literal(&base_url)
        ));
        let result = sql_json(
            r"SELECT postllm.rag(
                query => 'autovacuum vacuum bloat',
                documents => ARRAY[
                    'Bananas are yellow.',
                    'Autovacuum removes dead tuples and helps control table bloat.',
                    'VACUUM can reclaim space manually.'
                ],
                retrieval => 'keyword',
                top_n => 2,
                temperature => 0.1,
                max_tokens => 64
            )",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the RAG generation request");

        assert_eq!(configured["runtime"], "openai");
        assert_eq!(request_body["model"], "mock-answer-model");
        assert_eq!(request_body["temperature"], 0.1);
        assert_eq!(request_body["max_tokens"], 64);
        assert_eq!(request_body["messages"][0]["role"], "system");
        assert!(
            request_body["messages"][0]["content"]
                .as_str()
                .expect("system prompt should be text")
                .contains("retrieved context")
        );
        assert!(
            request_body["messages"][1]["content"]
                .as_str()
                .expect("prompt should be text")
                .contains("[1] Autovacuum removes dead tuples and helps control table bloat.")
        );
        assert!(
            request_body["messages"][1]["content"]
                .as_str()
                .expect("prompt should be text")
                .contains("[2] VACUUM can reclaim space manually.")
        );
        assert!(
            !request_body["messages"][1]["content"]
                .as_str()
                .expect("prompt should be text")
                .contains("Bananas are yellow.")
        );

        assert_eq!(result["retrieval"], "keyword");
        assert_eq!(
            result["answer"],
            "Autovacuum removes dead tuples and VACUUM can reclaim space."
        );
        assert_eq!(result["documents_considered"], 3);
        assert_eq!(result["context_documents"], 2);
        assert_eq!(result["context"][0]["index"], 2);
        assert_eq!(result["context"][1]["index"], 3);
        assert_eq!(
            result["response"]["_postllm"]["provider"],
            "openai-compatible"
        );
    }

    #[pg_test]
    fn sql_rag_text_should_return_mock_answer_text() {
        let (base_url, _receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-rag-text",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"Autovacuum controls table bloat."},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":20,"completion_tokens":6,"total_tokens":26}
            }"#,
        );
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-answer-model')",
            sql_literal(&base_url)
        ));
        let answer = sql_text(
            r"SELECT postllm.rag_text(
                query => 'autovacuum bloat',
                documents => ARRAY[
                    'Bananas are yellow.',
                    'Autovacuum controls table bloat.'
                ],
                retrieval => 'keyword',
                top_n => 1
            )",
        );

        assert_eq!(configured["runtime"], "openai");
        assert_eq!(answer, "Autovacuum controls table bloat.");
    }

    #[pg_test]
    fn sql_audit_should_skip_rows_when_disabled() {
        clear_request_audit_log();

        let (base_url, receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-audit-off",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"hello from SQL"},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}
            }"#,
        );

        drop(sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-audit-model')",
            sql_literal(&base_url)
        )));

        let response = sql_json(
            "SELECT postllm.chat(
                ARRAY[postllm.user('keep this out of the audit table')],
                temperature => 0.0,
                max_tokens => 8
            )",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the audit-disabled request");

        assert_eq!(
            response["choices"][0]["message"]["content"],
            "hello from SQL"
        );
        assert_eq!(
            request_body["messages"][0]["content"],
            "keep this out of the audit table"
        );
        assert_eq!(request_audit_row_count(), 0);
    }

    #[pg_test]
    fn sql_request_metrics_should_extract_normalized_usage_fields() {
        clear_request_audit_log();

        insert_request_audit_metric_row(
            "metrics_reader",
            "chat",
            Some("openai"),
            Some("metrics-model"),
            Some("http://metrics.example.test/v1/chat/completions"),
            "ok",
            125,
            Some(&json!({
                "_postllm": {
                    "usage": {
                        "prompt_tokens": 11,
                        "completion_tokens": 4,
                        "total_tokens": 15
                    }
                }
            })),
            None,
        );

        let row = sql_json(
            "SELECT to_jsonb(metric)
             FROM (
                SELECT *
                FROM postllm.request_metrics
                ORDER BY id
                LIMIT 1
             ) AS metric",
        );

        assert_eq!(row["role_name"], "metrics_reader");
        assert_eq!(row["operation"], "chat");
        assert_eq!(row["runtime"], "openai");
        assert_eq!(row["model"], "metrics-model");
        assert_eq!(
            row["base_url"],
            "http://metrics.example.test/v1/chat/completions"
        );
        assert_eq!(row["status"], "ok");
        assert_eq!(row["duration_ms"], 125);
        assert_eq!(row["prompt_tokens"], 11);
        assert_eq!(row["completion_tokens"], 4);
        assert_eq!(row["total_tokens"], 15);
        assert!(row["error_message"].is_null());
    }

    #[pg_test]
    #[allow(
        clippy::too_many_lines,
        reason = "this SQL-facing rollup test keeps the seeded audit fixtures and assertions together"
    )]
    fn sql_request_metric_views_should_roll_up_metrics() {
        clear_request_audit_log();

        insert_request_audit_metric_row(
            "app_user",
            "chat",
            Some("openai"),
            Some("metrics-model"),
            Some("http://metrics.example.test/v1/chat/completions"),
            "ok",
            120,
            Some(&json!({
                "_postllm": {
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 5,
                        "total_tokens": 15
                    }
                }
            })),
            None,
        );
        insert_request_audit_metric_row(
            "app_user",
            "chat",
            Some("openai"),
            Some("metrics-model"),
            Some("http://metrics.example.test/v1/chat/completions"),
            "error",
            240,
            None,
            Some("provider timeout"),
        );
        insert_request_audit_metric_row(
            "app_user",
            "chat",
            Some("openai"),
            Some("metrics-model"),
            Some("http://metrics.example.test/v1/chat/completions"),
            "ok",
            60,
            Some(&json!({
                "_postllm": {
                    "usage": {
                        "prompt_tokens": 8,
                        "completion_tokens": 2,
                        "total_tokens": 10
                    }
                }
            })),
            None,
        );
        insert_request_audit_metric_row(
            "embed_user",
            "embed",
            Some("candle"),
            Some("embed-model"),
            None,
            "ok",
            80,
            Some(&json!({
                "vector_count": 1,
                "dimension": 384,
                "normalize": true
            })),
            None,
        );

        let count_row = sql_json(
            "SELECT to_jsonb(metric)
             FROM (
                SELECT *
                FROM postllm.request_count_metrics
                WHERE role_name = 'app_user'
                  AND operation = 'chat'
                  AND runtime = 'openai'
                  AND model = 'metrics-model'
                LIMIT 1
             ) AS metric",
        );
        let error_row = sql_json(
            "SELECT jsonb_build_object(
                'request_count', request_count,
                'error_count', error_count,
                'error_rate', round(error_rate::numeric, 4),
                'last_error_message', last_error_message
            )
            FROM postllm.request_error_metrics
            WHERE role_name = 'app_user'
              AND operation = 'chat'
              AND runtime = 'openai'
              AND model = 'metrics-model'",
        );
        let latency_row = sql_json(
            "SELECT jsonb_build_object(
                'request_count', request_count,
                'avg_duration_ms', avg_duration_ms,
                'p50_duration_ms', p50_duration_ms,
                'p95_duration_ms', round(p95_duration_ms::numeric, 2),
                'max_duration_ms', max_duration_ms
            )
            FROM postllm.request_latency_metrics
            WHERE role_name = 'app_user'
              AND operation = 'chat'
              AND runtime = 'openai'
              AND model = 'metrics-model'",
        );
        let token_row = sql_json(
            "SELECT jsonb_build_object(
                'request_count', request_count,
                'requests_with_usage', requests_with_usage,
                'prompt_tokens', prompt_tokens,
                'completion_tokens', completion_tokens,
                'total_tokens', total_tokens,
                'avg_total_tokens', avg_total_tokens,
                'max_total_tokens', max_total_tokens
            )
            FROM postllm.request_token_usage_metrics
            WHERE role_name = 'app_user'
              AND operation = 'chat'
              AND runtime = 'openai'
              AND model = 'metrics-model'",
        );
        let embed_token_row = sql_json(
            "SELECT jsonb_build_object(
                'request_count', request_count,
                'requests_with_usage', requests_with_usage,
                'prompt_tokens', prompt_tokens,
                'completion_tokens', completion_tokens,
                'total_tokens', total_tokens,
                'avg_total_tokens', avg_total_tokens
            )
            FROM postllm.request_token_usage_metrics
            WHERE role_name = 'embed_user'
              AND operation = 'embed'
              AND runtime = 'candle'
              AND model = 'embed-model'",
        );

        assert_eq!(count_row["request_count"], 3);
        assert_eq!(count_row["ok_count"], 2);
        assert_eq!(count_row["error_count"], 1);

        assert_eq!(error_row["request_count"], 3);
        assert_eq!(error_row["error_count"], 1);
        assert_eq!(error_row["error_rate"].as_f64(), Some(0.3333));
        assert_eq!(error_row["last_error_message"], "provider timeout");

        assert_eq!(latency_row["request_count"], 3);
        assert_eq!(latency_row["avg_duration_ms"].as_f64(), Some(140.0));
        assert_eq!(latency_row["p50_duration_ms"].as_f64(), Some(120.0));
        assert_eq!(latency_row["p95_duration_ms"].as_f64(), Some(228.0));
        assert_eq!(latency_row["max_duration_ms"], 240);

        assert_eq!(token_row["request_count"], 3);
        assert_eq!(token_row["requests_with_usage"], 2);
        assert_eq!(token_row["prompt_tokens"], 18);
        assert_eq!(token_row["completion_tokens"], 7);
        assert_eq!(token_row["total_tokens"], 25);
        assert_eq!(token_row["avg_total_tokens"].as_f64(), Some(12.5));
        assert_eq!(token_row["max_total_tokens"], 15);

        assert_eq!(embed_token_row["request_count"], 1);
        assert_eq!(embed_token_row["requests_with_usage"], 0);
        assert_eq!(embed_token_row["prompt_tokens"], 0);
        assert_eq!(embed_token_row["completion_tokens"], 0);
        assert_eq!(embed_token_row["total_tokens"], 0);
        assert!(embed_token_row["avg_total_tokens"].is_null());
    }

    #[pg_test]
    fn sql_audit_should_redact_chat_rows() {
        clear_request_audit_log();
        sql_run("SET LOCAL postllm.request_logging = on");

        let (base_url, receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-audit-redacted",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"sensitive response"},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":6,"completion_tokens":2,"total_tokens":8}
            }"#,
        );

        drop(sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-audit-model')",
            sql_literal(&base_url)
        )));

        let text = sql_text(
            "SELECT postllm.chat_text(
                ARRAY[postllm.user('sensitive prompt')],
                temperature => 0.0,
                max_tokens => 8
            )",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the redacted audit request");
        let audit_row = latest_request_audit_row();

        assert_eq!(text, "sensitive response");
        assert_eq!(request_body["messages"][0]["content"], "sensitive prompt");
        assert_eq!(request_audit_row_count(), 1);
        assert_eq!(audit_row["operation"], "chat");
        assert_eq!(audit_row["status"], "ok");
        assert_eq!(audit_row["runtime"], "openai");
        assert_eq!(audit_row["model"], "mock-audit-model");
        assert_eq!(audit_row["input_redacted"], true);
        assert_eq!(audit_row["output_redacted"], true);
        assert_eq!(
            audit_row["request_payload"]["messages"][0]["content"],
            "[redacted]"
        );
        assert_eq!(
            audit_row["response_payload"]["choices"][0]["message"]["content"],
            "[redacted]"
        );
    }

    #[pg_test]
    fn sql_audit_should_store_unredacted_complete_rows() {
        clear_request_audit_log();
        sql_run("SET LOCAL postllm.request_logging = on");
        sql_run("SET LOCAL postllm.request_log_redact_inputs = off");
        sql_run("SET LOCAL postllm.request_log_redact_outputs = off");

        let (base_url, receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-audit-unredacted",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"visible response"},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":9,"completion_tokens":2,"total_tokens":11}
            }"#,
        );

        drop(sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-audit-model')",
            sql_literal(&base_url)
        )));

        let text = sql_text(
            "SELECT postllm.complete(
                prompt => 'visible prompt',
                system_prompt => 'visible system prompt',
                temperature => 0.0,
                max_tokens => 8
            )",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the unredacted audit request");
        let audit_row = latest_request_audit_row();

        assert_eq!(text, "visible response");
        assert_eq!(
            request_body["messages"][0]["content"],
            "visible system prompt"
        );
        assert_eq!(request_body["messages"][1]["content"], "visible prompt");
        assert_eq!(request_audit_row_count(), 1);
        assert_eq!(audit_row["operation"], "complete");
        assert_eq!(audit_row["input_redacted"], false);
        assert_eq!(audit_row["output_redacted"], false);
        assert_eq!(
            audit_row["request_payload"]["messages"][0]["content"],
            "visible system prompt"
        );
        assert_eq!(
            audit_row["request_payload"]["messages"][1]["content"],
            "visible prompt"
        );
        assert_eq!(
            audit_row["response_payload"]["choices"][0]["message"]["content"],
            "visible response"
        );
    }

    #[pg_test]
    fn sql_audit_should_redact_chat_stream_rows() {
        clear_request_audit_log();
        sql_run("SET LOCAL postllm.request_logging = on");

        let (base_url, receiver) = start_mock_stream_server(concat!(
            "data: {\"id\":\"chatcmpl-audit-stream\",\"object\":\"chat.completion.chunk\",\"model\":\"mock-stream-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-audit-stream\",\"object\":\"chat.completion.chunk\",\"model\":\"mock-stream-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hel\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-audit-stream\",\"object\":\"chat.completion.chunk\",\"model\":\"mock-stream-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        ));

        drop(sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-stream-model')",
            sql_literal(&base_url)
        )));

        let rows = sql_json(
            "SELECT jsonb_agg(to_jsonb(chunk) ORDER BY index)
             FROM postllm.chat_stream(
                ARRAY[postllm.user('stream this sensitive prompt')],
                temperature => 0.0,
                max_tokens => 8
             ) AS chunk",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the streamed audit request");
        let audit_row = latest_request_audit_row();

        assert_eq!(
            rows.as_array()
                .expect("chat_stream rows should aggregate to a JSON array")
                .len(),
            3
        );
        assert_eq!(request_body["stream"], true);
        assert_eq!(
            request_body["messages"][0]["content"],
            "stream this sensitive prompt"
        );
        assert_eq!(request_audit_row_count(), 1);
        assert_eq!(audit_row["operation"], "chat_stream");
        assert_eq!(audit_row["status"], "ok");
        assert_eq!(audit_row["runtime"], "openai");
        assert_eq!(audit_row["model"], "mock-stream-model");
        assert_eq!(audit_row["input_redacted"], true);
        assert_eq!(audit_row["output_redacted"], true);
        assert_eq!(
            audit_row["request_payload"]["messages"][0]["content"],
            "[redacted]"
        );
        assert_eq!(audit_row["response_payload"]["event_count"], 3);
        assert_eq!(audit_row["response_payload"]["text"], "[redacted]");
    }

    #[pg_test]
    fn sql_audit_should_redact_rerank_rows() {
        clear_request_audit_log();
        sql_run("SET LOCAL postllm.request_logging = on");

        let (base_url, receiver) = start_mock_json_server(
            "/v1/rerank",
            r#"{"results":[{"index":1,"relevance_score":0.98},{"index":0,"relevance_score":0.12}]}"#,
        );

        drop(sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-reranker')",
            sql_literal(&base_url)
        )));

        let rows = sql_json(
            r"SELECT jsonb_agg(to_jsonb(ranked) ORDER BY rank)
            FROM postllm.rerank(
                'What controls table bloat?',
                ARRAY[
                    'Bananas are yellow.',
                    'Autovacuum removes dead tuples.'
                ],
                top_n => 1
            ) AS ranked",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the rerank audit request");
        let audit_row = latest_request_audit_row();

        assert_eq!(
            rows,
            json!([{
                "rank": 1,
                "index": 2,
                "document": "Autovacuum removes dead tuples.",
                "score": 0.98,
            }])
        );
        assert_eq!(request_body["query"], "What controls table bloat?");
        assert_eq!(request_audit_row_count(), 1);
        assert_eq!(audit_row["operation"], "rerank");
        assert_eq!(audit_row["status"], "ok");
        assert_eq!(audit_row["runtime"], "openai");
        assert_eq!(audit_row["model"], "mock-reranker");
        assert_eq!(audit_row["input_redacted"], true);
        assert_eq!(audit_row["output_redacted"], true);
        assert_eq!(audit_row["request_payload"]["query"], "[redacted]");
        assert_eq!(audit_row["request_payload"]["documents"], "[redacted]");
        assert_eq!(audit_row["request_payload"]["top_n"], 1);
        assert_eq!(audit_row["response_payload"][0]["rank"], 1);
        assert_eq!(audit_row["response_payload"][0]["index"], 2);
        assert_eq!(
            audit_row["response_payload"][0]["score"].as_f64(),
            Some(0.98)
        );
        assert_eq!(audit_row["response_payload"][0]["document"], "[redacted]");
    }

    #[pg_test]
    fn sql_audit_should_record_embed_success_rows() {
        clear_request_audit_log();
        sql_run("SET LOCAL postllm.request_logging = on");

        drop(sql_json(&format!(
            "SELECT postllm.configure(runtime => 'candle', embedding_model => {})",
            sql_literal(crate::guc::DEFAULT_EMBEDDING_MODEL)
        )));

        let vectors = super::execution::ExecutionContext::new("embed", || {
            super::embed_request_audit_payload(&["offline-safe input".to_owned()], true)
        })
        .run_embedding(
            None,
            || Ok::<Vec<String>, crate::error::Error>(vec!["offline-safe input".to_owned()]),
            |_settings, validated_inputs| {
                assert_eq!(validated_inputs, vec!["offline-safe input".to_owned()]);
                Ok::<Vec<Vec<f32>>, crate::error::Error>(vec![vec![0.1, 0.2, 0.3]])
            },
            |vectors| super::embed_response_audit_payload(vectors, true),
        )
        .expect("embed audit success path should complete");
        let audit_row = latest_request_audit_row();

        assert_eq!(vectors, vec![vec![0.1, 0.2, 0.3]]);
        assert_eq!(request_audit_row_count(), 1);
        assert_eq!(audit_row["operation"], "embed");
        assert_eq!(audit_row["status"], "ok");
        assert_eq!(audit_row["runtime"], "candle");
        assert_eq!(audit_row["model"], crate::guc::DEFAULT_EMBEDDING_MODEL);
        assert_eq!(audit_row["input_redacted"], true);
        assert_eq!(audit_row["output_redacted"], true);
        assert_eq!(audit_row["request_payload"]["inputs"], "[redacted]");
        assert_eq!(audit_row["request_payload"]["normalize"], true);
        assert_eq!(audit_row["response_payload"]["vector_count"], 1);
        assert_eq!(audit_row["response_payload"]["dimension"], 3);
        assert_eq!(audit_row["response_payload"]["normalize"], true);
        assert!(audit_row["error_message"].is_null());
    }

    #[pg_test]
    fn sql_audit_should_record_embed_error_rows() {
        clear_request_audit_log();
        sql_run("SET LOCAL postllm.request_logging = on");

        let cache_dir = fresh_test_cache_dir("audit-embed-miss");
        drop(sql_json(&format!(
            "SELECT postllm.configure(candle_cache_dir => {}, candle_offline => true)",
            sql_literal(&cache_dir)
        )));

        let error = super::embed_many_impl(&["offline test".to_owned()], None, true)
            .expect_err("offline embed request should fail");
        let audit_row = latest_request_audit_row();

        assert!(
            error
                .to_string()
                .contains("offline mode is enabled and model")
        );
        assert_eq!(request_audit_row_count(), 1);
        assert_eq!(audit_row["operation"], "embed");
        assert_eq!(audit_row["status"], "error");
        assert_eq!(audit_row["runtime"], "candle");
        assert_eq!(audit_row["model"], crate::guc::DEFAULT_EMBEDDING_MODEL);
        assert_eq!(audit_row["input_redacted"], true);
        assert_eq!(audit_row["output_redacted"], true);
        assert_eq!(audit_row["request_payload"]["inputs"], "[redacted]");
        assert_eq!(audit_row["request_payload"]["normalize"], true);
        assert!(audit_row["response_payload"].is_null());
        assert!(
            audit_row["error_message"]
                .as_str()
                .is_some_and(|message| message.contains("offline mode is enabled and model"))
        );
    }

    #[pg_test]
    fn sql_rerank_should_smoke_live_candle_when_enabled() {
        let Some(_) = configure_candle_generation_pg_test() else {
            return;
        };

        let rows = sql_json(
            r"SELECT jsonb_agg(to_jsonb(ranked) ORDER BY rank)
            FROM postllm.rerank(
                'How does PostgreSQL remove dead tuples?',
                ARRAY[
                    'Autovacuum removes dead tuples and helps control table bloat.',
                    'Bananas are yellow and grow in bunches.'
                ],
                top_n => 1
            ) AS ranked",
        );
        let rows = rows
            .as_array()
            .expect("rerank should aggregate to a JSON array");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["rank"], 1);
        assert_eq!(rows[0]["index"], 1);
        assert_eq!(
            rows[0]["document"],
            "Autovacuum removes dead tuples and helps control table bloat."
        );
        assert!(
            rows[0]["score"].as_f64().is_some_and(|score| score > 0.0),
            "expected a positive rerank score, got {rows:?}"
        );
    }

    #[pg_test]
    fn sql_message_helpers_should_build_json() {
        let message = sql_json("SELECT postllm.user('Hello from SQL')");

        assert_eq!(
            message,
            json!({
                "role": "user",
                "content": "Hello from SQL",
            })
        );
    }

    #[pg_test]
    fn sql_json_schema_should_build_response_format_json() {
        let response_format = sql_json(
            r#"SELECT postllm.json_schema(
                'person',
                '{
                    "type":"object",
                    "properties":{"name":{"type":"string"}},
                    "required":["name"],
                    "additionalProperties":false
                }'::jsonb
            )"#,
        );

        assert_eq!(response_format["type"], "json_schema");
        assert_eq!(response_format["json_schema"]["name"], "person");
        assert_eq!(response_format["json_schema"]["strict"], true);
        assert_eq!(response_format["json_schema"]["schema"]["type"], "object");
    }

    #[pg_test]
    fn sql_template_helpers_should_render_text_and_messages() {
        let rendered = sql_text(
            r#"SELECT postllm.render_template(
                'Hello {{name}}. Limit={{limit}}.',
                '{"name":"SQL","limit":4}'::jsonb
            )"#,
        );
        let message = sql_json(
            r#"SELECT postllm.user_template(
                'Explain {{topic}} for a {{audience}} audience.',
                '{"topic":"MVCC","audience":"beginner"}'::jsonb
            )"#,
        );

        assert_eq!(rendered, "Hello SQL. Limit=4.");
        assert_eq!(
            message,
            json!({
                "role": "user",
                "content": "Explain MVCC for a beginner audience.",
            })
        );
    }

    #[pg_test]
    fn sql_multimodal_message_helpers_should_build_json() {
        let message = sql_json(
            "SELECT postllm.user_parts(ARRAY[postllm.text_part('Describe this image.'), postllm.image_url_part('https://example.com/cat.png', detail => 'low')])",
        );

        assert_eq!(
            message,
            json!({
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Describe this image.",
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": "https://example.com/cat.png",
                            "detail": "low",
                        }
                    }
                ],
            })
        );
    }

    #[pg_test]
    fn sql_tool_message_helpers_should_build_json() {
        let assistant_message = sql_json(
            r#"SELECT postllm.assistant_tool_calls(
                ARRAY[
                    postllm.tool_call(
                        'call_123',
                        'lookup_weather',
                        '{"city":"Austin"}'::jsonb
                    )
                ]
            )"#,
        );
        let tool_result =
            sql_json("SELECT postllm.tool_result('call_123', '{\"temperature\":72}')");

        assert_eq!(
            assistant_message,
            json!({
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_123",
                    "type": "function",
                    "function": {
                        "name": "lookup_weather",
                        "arguments": "{\"city\":\"Austin\"}",
                    }
                }]
            })
        );
        assert_eq!(
            tool_result,
            json!({
                "role": "tool",
                "tool_call_id": "call_123",
                "content": "{\"temperature\":72}",
            })
        );
    }

    #[pg_test]
    fn sql_tool_definition_helpers_should_build_json() {
        let tool = sql_json(
            r#"SELECT postllm.function_tool(
                'lookup_weather',
                '{
                    "type":"object",
                    "properties":{"city":{"type":"string"}},
                    "required":["city"],
                    "additionalProperties":false
                }'::jsonb,
                description => 'Look up the current weather.'
            )"#,
        );
        let auto_choice = sql_json("SELECT postllm.tool_choice_auto()");
        let none_choice = sql_json("SELECT postllm.tool_choice_none()");
        let required_choice = sql_json("SELECT postllm.tool_choice_required()");
        let named_choice = sql_json("SELECT postllm.tool_choice_function('lookup_weather')");

        assert_eq!(
            tool,
            json!({
                "type": "function",
                "function": {
                    "name": "lookup_weather",
                    "description": "Look up the current weather.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "city": {
                                "type": "string",
                            }
                        },
                        "required": ["city"],
                        "additionalProperties": false,
                    }
                }
            })
        );
        assert_eq!(auto_choice, json!("auto"));
        assert_eq!(none_choice, json!("none"));
        assert_eq!(required_choice, json!("required"));
        assert_eq!(
            named_choice,
            json!({
                "type": "function",
                "function": {
                    "name": "lookup_weather",
                }
            })
        );
    }

    #[pg_test]
    fn sql_messages_agg_should_build_rowset_conversation() {
        let messages = sql_json(
            r"SELECT to_jsonb(postllm.messages_agg(
                postllm.message(role, content)
                ORDER BY ord
            ))
            FROM (
                VALUES
                    (2, 'user', 'Explain MVCC in one sentence.'),
                    (1, 'system', 'You are concise.')
            ) AS conversation(ord, role, content)",
        );

        assert_eq!(
            messages,
            json!([
                {
                    "role": "system",
                    "content": "You are concise.",
                },
                {
                    "role": "user",
                    "content": "Explain MVCC in one sentence.",
                }
            ])
        );
    }

    #[pg_test]
    fn sql_conversation_primitives_should_store_and_return_history() {
        sql_run(
            "TRUNCATE postllm.conversation_messages, postllm.conversations RESTART IDENTITY CASCADE",
        );

        let created = sql_json(
            r#"SELECT postllm.conversation_create(
                title => 'Support thread',
                metadata => '{"ticket":"INC-42"}'::jsonb
            )"#,
        );
        let conversation_id = created["id"]
            .as_i64()
            .expect("created conversation should include an integer id");
        let first = sql_json(&format!(
            "SELECT postllm.conversation_append(
                conversation_id => {conversation_id},
                message => postllm.system('You are a careful support assistant.')
            )"
        ));
        let second = sql_json(&format!(
            "SELECT postllm.conversation_append(
                conversation_id => {conversation_id},
                message => postllm.user('Summarize the latest outage update.')
            )"
        ));
        let transcript = sql_json(&format!(
            "SELECT to_jsonb(postllm.conversation_history({conversation_id}))"
        ));
        let conversation = sql_json(&format!("SELECT postllm.conversation({conversation_id})"));
        let listed = sql_json("SELECT postllm.conversations()");

        assert_eq!(created["title"], "Support thread");
        assert_eq!(created["metadata"]["ticket"], "INC-42");
        assert_eq!(first["message_no"], 1);
        assert_eq!(second["message_no"], 2);
        assert_eq!(transcript[0]["role"], "system");
        assert_eq!(transcript[1]["role"], "user");
        assert_eq!(conversation["message_count"], 2);
        assert_eq!(conversation["messages"][0]["message"]["role"], "system");
        assert_eq!(
            conversation["messages"][1]["message"]["content"],
            "Summarize the latest outage update."
        );
        assert_eq!(listed.as_array().map(Vec::len), Some(1));
    }

    #[pg_test]
    fn sql_conversation_reply_should_append_assistant_messages() {
        sql_run(
            "TRUNCATE postllm.conversation_messages, postllm.conversations RESTART IDENTITY CASCADE",
        );
        let (base_url, receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-conversation-reply",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"Here is the latest outage summary."},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":6,"completion_tokens":7,"total_tokens":13}
            }"#,
        );

        drop(sql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                model => 'mock-conversation-model'
            )",
            sql_literal(&base_url)
        )));

        let created = sql_json("SELECT postllm.conversation_create(title => 'Reply thread')");
        let conversation_id = created["id"]
            .as_i64()
            .expect("created conversation should include an integer id");
        let reply = sql_json(&format!(
            "SELECT postllm.conversation_reply(
                conversation_id => {conversation_id},
                message => postllm.user('What changed in the outage status?'),
                temperature => 0.0,
                max_tokens => 16
            )"
        ));
        let transcript = sql_json(&format!(
            "SELECT to_jsonb(postllm.conversation_history({conversation_id}))"
        ));
        let request_body = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("mock server should capture the conversation reply request");

        assert_eq!(reply["assistant_message"]["message_no"], 2);
        assert_eq!(
            reply["assistant_message"]["message"]["content"],
            "Here is the latest outage summary."
        );
        assert_eq!(transcript.as_array().map(Vec::len), Some(2));
        assert_eq!(
            transcript[0]["content"],
            "What changed in the outage status?"
        );
        assert_eq!(transcript[1]["role"], "assistant");
        assert_eq!(
            request_body["messages"][0]["content"],
            "What changed in the outage status?"
        );
    }

    #[pg_test]
    fn sql_prompt_registry_should_version_render_and_delete() {
        sql_run(
            "TRUNCATE postllm.prompt_versions, postllm.prompt_registries RESTART IDENTITY CASCADE",
        );

        let first = sql_json(
            r#"SELECT postllm.prompt_set(
                name => 'incident_summary',
                template => 'Summarize incident {{ticket}} for {{team}}.',
                role => 'system',
                title => 'Incident summary',
                description => 'Initial wording',
                metadata => '{"team":"ops"}'::jsonb
            )"#,
        );
        let second = sql_json(
            r#"SELECT postllm.prompt_set(
                name => 'incident_summary',
                template => 'Summarize incident {{ticket}} for {{team}} in one paragraph.',
                role => 'system',
                description => 'Short paragraph wording',
                metadata => '{"team":"ops","style":"short"}'::jsonb
            )"#,
        );
        let listed = sql_json("SELECT postllm.prompts()");
        let current = sql_json("SELECT postllm.prompt('incident_summary')");
        let first_version =
            sql_json("SELECT postllm.prompt(name => 'incident_summary', version => 1)");
        let rendered = sql_text(
            r#"SELECT postllm.prompt_render(
                name => 'incident_summary',
                variables => '{"ticket":"INC-42","team":"operations"}'::jsonb
            )"#,
        );
        let message = sql_json(
            r#"SELECT postllm.prompt_message(
                name => 'incident_summary',
                variables => '{"ticket":"INC-42","team":"operations"}'::jsonb
            )"#,
        );
        let deleted = sql_json("SELECT postllm.prompt_delete('incident_summary')");

        assert_eq!(first["active_version"], 1);
        assert_eq!(second["active_version"], 2);
        assert_eq!(listed.as_array().map(Vec::len), Some(1));
        assert_eq!(current["title"], "Incident summary");
        assert_eq!(current["active_version"], 2);
        assert_eq!(current["current"]["description"], "Short paragraph wording");
        assert_eq!(current["versions"].as_array().map(Vec::len), Some(2));
        assert_eq!(first_version["current"]["version"], 1);
        assert_eq!(first_version["current"]["description"], "Initial wording");
        assert_eq!(
            rendered,
            "Summarize incident INC-42 for operations in one paragraph."
        );
        assert_eq!(message["role"], "system");
        assert_eq!(
            message["content"],
            "Summarize incident INC-42 for operations in one paragraph."
        );
        assert_eq!(deleted["deleted"], true);
        assert_eq!(sql_json("SELECT postllm.prompts()"), json!([]));
    }

    #[pg_test]
    fn sql_chat_structured_should_support_responses_api_base_url() {
        let (base_url, receiver) = start_mock_json_server(
            "/v1/responses",
            r#"{
                "id":"resp-structured",
                "model":"gpt-4.1-mini",
                "status":"completed",
                "output_text":"{\"name\":\"Ada\",\"country\":\"UK\"}",
                "usage":{"input_tokens":8,"output_tokens":6,"total_tokens":14},
                "output":[
                    {
                        "type":"message",
                        "role":"assistant",
                        "content":[
                            {"type":"output_text","text":"{\"name\":\"Ada\",\"country\":\"UK\"}"}
                        ]
                    }
                ]
            }"#,
        );

        drop(sql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                model => 'gpt-4.1-mini'
            )",
            sql_literal(&base_url)
        )));

        let response = sql_json(
            r#"SELECT postllm.chat_structured(
                ARRAY[
                    postllm.system('Return JSON only.'),
                    postllm.user('Return name and country for Ada.')
                ],
                postllm.json_schema(
                    'person',
                    '{
                        "type":"object",
                        "properties":{
                            "name":{"type":"string"},
                            "country":{"type":"string"}
                        },
                        "required":["name","country"],
                        "additionalProperties":false
                    }'::jsonb
                ),
                temperature => 0.0,
                max_tokens => 32
            )"#,
        );
        let request_body = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("mock server should capture the responses-api structured request");

        assert_eq!(response["name"], "Ada");
        assert_eq!(response["country"], "UK");
        assert_eq!(request_body["input"][0]["role"], "system");
        assert_eq!(request_body["input"][1]["role"], "user");
        assert_eq!(request_body["input"][1]["content"][0]["type"], "input_text");
        assert_eq!(
            request_body["text"]["format"]["json_schema"]["name"],
            "person"
        );
        assert_eq!(request_body["max_output_tokens"], 32);
    }

    #[pg_test]
    fn sql_embed_should_support_openai_runtime_embeddings() {
        let (embeddings_url, receiver) = start_mock_json_server(
            "/v1/embeddings",
            r#"{
                "data":[
                    {"index":0,"embedding":[3.0,4.0]}
                ]
            }"#,
        );
        let base_url = embeddings_url.replace("/v1/embeddings", "/v1/chat/completions");

        drop(sql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                embedding_model => 'text-embedding-3-small'
            )",
            sql_literal(&base_url)
        )));

        let vector = Spi::get_one::<Vec<f32>>("SELECT postllm.embed('hello from SQL')")
            .expect("SPI should execute")
            .expect("query should return a row");
        let request_body = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("mock server should capture the embedding request");

        assert_eq!(request_body["model"], "text-embedding-3-small");
        assert_eq!(request_body["input"], json!(["hello from SQL"]));
        assert!((vector[0] - 0.6).abs() < 1.0e-6);
        assert!((vector[1] - 0.8).abs() < 1.0e-6);
    }

    #[pg_test]
    fn sql_embed_many_should_support_openai_runtime_embeddings() {
        let (embeddings_url, receiver) = start_mock_json_server(
            "/v1/embeddings",
            r#"{
                "data":[
                    {"index":1,"embedding":[1.0,2.0]},
                    {"index":0,"embedding":[5.0,0.0]}
                ]
            }"#,
        );
        let base_url = embeddings_url.replace("/v1/embeddings", "/v1/responses");

        drop(sql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                embedding_model => 'text-embedding-3-small'
            )",
            sql_literal(&base_url)
        )));

        let vectors = sql_json(
            "SELECT postllm.embed_many(ARRAY['hello from SQL', 'world from SQL'], NULL, false)",
        );
        let request_body = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("mock server should capture the embedding request");

        assert_eq!(request_body["model"], "text-embedding-3-small");
        assert_eq!(
            request_body["input"],
            json!(["hello from SQL", "world from SQL"])
        );
        assert_eq!(vectors, json!([[5.0, 0.0], [1.0, 2.0]]));
    }

    #[pg_test]
    fn sql_eval_registry_should_store_score_and_delete_cases() {
        sql_run("TRUNCATE postllm.eval_cases, postllm.eval_datasets RESTART IDENTITY CASCADE");

        let dataset = sql_json(
            r#"SELECT postllm.eval_dataset_set(
                name => 'incident_regressions',
                description => 'Checks for outage summary wording',
                metadata => '{"team":"ops"}'::jsonb
            )"#,
        );
        let eval_case = sql_json(
            r#"SELECT postllm.eval_case_set(
                dataset_name => 'incident_regressions',
                case_name => 'summary_mentions_failover',
                input_payload => '{"ticket":"INC-42","severity":"sev1"}'::jsonb,
                expected_payload => '"failover"'::jsonb,
                scorer => 'contains_text',
                threshold => 1.0,
                metadata => '{"owner":"release"}'::jsonb
            )"#,
        );
        let listed = sql_json("SELECT postllm.eval_datasets()");
        let current_dataset = sql_json("SELECT postllm.eval_dataset('incident_regressions')");
        let current_case = sql_json(
            "SELECT postllm.eval_case('incident_regressions', 'summary_mentions_failover')",
        );
        let scored = sql_json(
            r#"SELECT postllm.eval_case_score(
                'incident_regressions',
                'summary_mentions_failover',
                '{"choices":[{"message":{"role":"assistant","content":"The incident stabilized after failover to the replica."}}]}'::jsonb
            )"#,
        );
        let deleted_case = sql_json(
            "SELECT postllm.eval_case_delete('incident_regressions', 'summary_mentions_failover')",
        );
        let deleted_dataset =
            sql_json("SELECT postllm.eval_dataset_delete('incident_regressions')");

        assert_eq!(dataset["name"], "incident_regressions");
        assert_eq!(dataset["metadata"]["team"], "ops");
        assert_eq!(eval_case["dataset_name"], "incident_regressions");
        assert_eq!(eval_case["scorer"], "contains_text");
        assert_eq!(listed.as_array().map(Vec::len), Some(1));
        assert_eq!(current_dataset["case_count"], 1);
        assert_eq!(
            current_dataset["cases"][0]["name"],
            "summary_mentions_failover"
        );
        assert_eq!(current_case["metadata"]["owner"], "release");
        assert_eq!(scored["passed"], true);
        assert_eq!(scored["score"], 1.0);
        assert_eq!(deleted_case["deleted"], true);
        assert_eq!(deleted_dataset["deleted"], true);
        assert_eq!(sql_json("SELECT postllm.eval_datasets()"), json!([]));
    }

    #[pg_test]
    fn sql_eval_score_should_support_text_and_json_scorers() {
        let exact_text = sql_json(
            r#"SELECT postllm.eval_score(
                actual => '"ready"'::jsonb,
                expected => '"ready"'::jsonb
            )"#,
        );
        let contains_text = sql_json(
            r#"SELECT postllm.eval_score(
                actual => '{"choices":[{"message":{"role":"assistant","content":"Database recovered after failover."}}]}'::jsonb,
                expected => '"failover"'::jsonb,
                scorer => 'contains_text'
            )"#,
        );
        let exact_json = sql_json(
            r#"SELECT postllm.eval_score(
                actual => '{"status":"ok","count":2}'::jsonb,
                expected => '{"status":"ok","count":2}'::jsonb,
                scorer => 'exact_json'
            )"#,
        );
        let subset_json = sql_json(
            r#"SELECT postllm.eval_score(
                actual => '{"status":"ok","nested":{"count":2,"details":"stable"}}'::jsonb,
                expected => '{"nested":{"count":2}}'::jsonb,
                scorer => 'json_subset'
            )"#,
        );
        let failed_subset = sql_json(
            r#"SELECT postllm.eval_score(
                actual => '{"status":"ok","nested":{"count":2}}'::jsonb,
                expected => '{"nested":{"count":3}}'::jsonb,
                scorer => 'json_subset'
            )"#,
        );

        assert_eq!(exact_text["passed"], true);
        assert_eq!(contains_text["passed"], true);
        assert_eq!(contains_text["scorer"], "contains_text");
        assert_eq!(exact_json["passed"], true);
        assert_eq!(subset_json["passed"], true);
        assert_eq!(failed_subset["passed"], false);
        assert_eq!(failed_subset["score"], 0.0);
    }

    #[pg_test]
    fn sql_configure_should_update_the_current_session() {
        let configured = sql_json(
            "SELECT postllm.configure(model => 'pg-test-model', embedding_model => 'sentence-transformers/all-MiniLM-L6-v2', timeout_ms => 5000, max_retries => 4, retry_backoff_ms => 750, request_max_concurrency => 6, request_token_budget => 256, request_runtime_budget_ms => 4000, request_spend_budget_microusd => 1500, output_token_price_microusd_per_1k => 500000, candle_offline => true, candle_device => 'cpu', candle_max_input_tokens => 2048, candle_max_concurrency => 2)",
        );

        assert_eq!(configured["model"], "pg-test-model");
        assert_eq!(
            configured["embedding_model"],
            "sentence-transformers/all-MiniLM-L6-v2"
        );
        assert_eq!(configured["timeout_ms"], 5_000);
        assert_eq!(configured["max_retries"], 4);
        assert_eq!(configured["retry_backoff_ms"], 750);
        assert_eq!(configured["request_max_concurrency"], 6);
        assert_eq!(configured["request_token_budget"], 256);
        assert_eq!(configured["request_runtime_budget_ms"], 4_000);
        assert_eq!(configured["request_spend_budget_microusd"], 1_500);
        assert_eq!(configured["output_token_price_microusd_per_1k"], 500_000);
        assert_eq!(configured["candle_offline"], true);
        assert_eq!(configured["candle_device"], "cpu");
        assert_eq!(configured["candle_max_input_tokens"], 2_048);
        assert_eq!(configured["candle_max_concurrency"], 2);
    }

    #[pg_test]
    fn sql_chat_should_inject_max_tokens_from_request_token_budget() {
        let (base_url, receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-budget",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"guardrail applied"},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}
            }"#,
        );

        drop(sql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                model => 'mock-budget-model',
                request_token_budget => 32
            )",
            sql_literal(&base_url)
        )));

        let response = sql_json(
            "SELECT postllm.chat(ARRAY[postllm.user('apply the guardrail automatically')])",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the token-budget request");

        assert_eq!(
            response["choices"][0]["message"]["content"],
            "guardrail applied"
        );
        assert_eq!(request_body["model"], "mock-budget-model");
        assert_eq!(request_body["max_tokens"], 32);
    }

    #[pg_test]
    fn sql_chat_should_inject_max_tokens_from_spend_budget() {
        let (base_url, receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-spend-budget",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"spend guardrail applied"},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}
            }"#,
        );

        drop(sql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                model => 'mock-spend-model',
                request_spend_budget_microusd => 750,
                output_token_price_microusd_per_1k => 250000
            )",
            sql_literal(&base_url)
        )));

        let response =
            sql_json("SELECT postllm.chat(ARRAY[postllm.user('derive max_tokens from spend')])");
        let request_body = receiver
            .recv()
            .expect("mock server should capture the spend-budget request");

        assert_eq!(
            response["choices"][0]["message"]["content"],
            "spend guardrail applied"
        );
        assert_eq!(request_body["max_tokens"], 3);
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm request backpressure blocked execution: request chat could not start because postllm.request_max_concurrency=1 stayed saturated until postllm.timeout_ms=100ms elapsed"
    )]
    fn sql_chat_should_reject_saturated_global_request_concurrency() {
        let (base_url, _receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-backpressure",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"should not be reached"},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}
            }"#,
        );

        drop(sql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                model => 'mock-backpressure-model',
                timeout_ms => 100,
                request_max_concurrency => 1
            )",
            sql_literal(&base_url)
        )));

        let _holder = spawn_request_concurrency_holder(0, 5);

        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.chat(ARRAY[postllm.user('blocked by backpressure')])",
        ));
    }

    #[pg_test]
    fn sql_job_should_submit_poll_and_fetch_complete_results() {
        psql_run("TRUNCATE postllm.async_jobs RESTART IDENTITY");
        let (base_url, receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-async-complete",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"hello from async complete"},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":5,"completion_tokens":4,"total_tokens":9}
            }"#,
        );

        let submitted = psql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                model => 'mock-async-model'
            );
            SELECT postllm.job_submit(
                kind => 'complete',
                request => jsonb_build_object(
                    'prompt', 'Say hi from async complete',
                    'temperature', 0.0,
                    'max_tokens', 12
                )
            )",
            sql_literal(&base_url)
        ));
        let job_id = submitted["id"]
            .as_i64()
            .expect("submitted async job should include an integer id");
        let finished =
            wait_for_async_job_status(job_id, &["succeeded", "failed", "cancelled"], 5_000);
        let result = sql_json(&format!("SELECT postllm.job_result({job_id})"));
        let request_body = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("mock server should capture the async request");

        assert_eq!(submitted["kind"], "complete");
        assert_eq!(submitted["status"], "queued");
        assert_eq!(finished["status"], "succeeded");
        assert_eq!(result["text"], "hello from async complete");
        assert_eq!(
            request_body["messages"][0]["content"],
            "Say hi from async complete"
        );
        assert_eq!(request_body["temperature"], 0.0);
        assert_eq!(request_body["max_tokens"], 12);
    }

    #[pg_test]
    fn sql_job_cancel_should_mark_running_jobs_cancelled() {
        psql_run("TRUNCATE postllm.async_jobs RESTART IDENTITY");
        let (base_url, _receiver) = start_delayed_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-async-cancel",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"too late"},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}
            }"#,
            Duration::from_secs(2),
        );

        let submitted = psql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                model => 'mock-async-cancel-model',
                timeout_ms => 10000
            );
            SELECT postllm.job_submit(
                kind => 'complete',
                request => jsonb_build_object(
                    'prompt', 'cancel me asynchronously',
                    'temperature', 0.0,
                    'max_tokens', 12
                )
            )",
            sql_literal(&base_url)
        ));
        let job_id = submitted["id"]
            .as_i64()
            .expect("submitted async job should include an integer id");
        let running = wait_for_async_job_status(job_id, &["running"], 5_000);
        let cancelled = sql_json(&format!("SELECT postllm.job_cancel({job_id})"));
        let finished = wait_for_async_job_status(job_id, &["cancelled"], 2_000);

        assert_eq!(running["status"], "running");
        assert_eq!(cancelled["status"], "cancelled");
        assert_eq!(finished["status"], "cancelled");
        assert_eq!(finished["error_message"], "job was cancelled");
    }

    #[pg_test]
    fn sql_job_should_emit_async_notifications() {
        psql_run("TRUNCATE postllm.async_jobs RESTART IDENTITY");
        let (base_url, _receiver) = start_mock_json_server(
            "/v1/chat/completions",
            r#"{
                "id":"chatcmpl-async-notify",
                "object":"chat.completion",
                "choices":[
                    {
                        "index":0,
                        "message":{"role":"assistant","content":"hello from notify"},
                        "finish_reason":"stop"
                    }
                ],
                "usage":{"prompt_tokens":4,"completion_tokens":3,"total_tokens":7}
            }"#,
        );
        let listener = spawn_psql_listener(crate::jobs::JOB_NOTIFY_CHANNEL, 2);
        thread::sleep(Duration::from_millis(150));

        let submitted = psql_json(&format!(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => {},
                model => 'mock-async-notify-model'
            );
            SELECT postllm.job_submit(
                kind => 'complete',
                request => jsonb_build_object(
                    'prompt', 'emit lifecycle events',
                    'temperature', 0.0,
                    'max_tokens', 12
                )
            )",
            sql_literal(&base_url)
        ));
        let job_id = submitted["id"]
            .as_i64()
            .expect("submitted async job should include an integer id");
        let finished = wait_for_async_job_status(job_id, &["succeeded"], 5_000);
        let listener_output = wait_for_psql_output(listener);
        let notifications =
            parse_psql_notification_payloads(&listener_output, crate::jobs::JOB_NOTIFY_CHANNEL);

        assert_eq!(finished["status"], "succeeded");
        assert!(
            notifications.len() >= 3,
            "expected at least three async job notifications, got {notifications:?} from output: {listener_output}"
        );

        let lifecycle = notifications
            .into_iter()
            .filter(|payload| payload["job_id"].as_i64() == Some(job_id))
            .collect::<Vec<_>>();

        assert_eq!(lifecycle.len(), 3);
        assert_eq!(lifecycle[0]["event"], "submitted");
        assert_eq!(lifecycle[0]["status"], "queued");
        assert_eq!(lifecycle[0]["kind"], "complete");
        assert_eq!(lifecycle[1]["event"], "started");
        assert_eq!(lifecycle[1]["status"], "running");
        assert_eq!(lifecycle[2]["event"], "finished");
        assert_eq!(lifecycle[2]["status"], "succeeded");
        assert_eq!(lifecycle[2]["has_result"], true);
    }

    #[pg_test]
    #[should_panic(expected = "async jobs do not support direct postllm.api_key session secrets")]
    fn sql_job_submit_should_reject_direct_api_key_sessions() {
        psql_run("TRUNCATE postllm.async_jobs RESTART IDENTITY");

        drop(sql_json(
            "SELECT postllm.configure(
                runtime => 'openai',
                base_url => 'https://example.invalid/v1/chat/completions',
                model => 'mock-direct-key-model',
                api_key => 'sk-direct-async'
            )",
        ));

        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.job_submit(
                kind => 'complete',
                request => jsonb_build_object('prompt', 'should fail')
            )",
        ));
    }

    #[pg_test]
    fn sql_configure_should_accept_runtime_metadata() {
        let configured = sql_json(
            "SELECT postllm.configure(runtime => 'candle', candle_cache_dir => '/tmp/postllm-candle', candle_offline => true, candle_device => 'cpu', candle_max_input_tokens => 1024, candle_max_concurrency => 3)",
        );

        assert_eq!(configured["runtime"], "candle");
        assert_eq!(configured["candle_cache_dir"], "/tmp/postllm-candle");
        assert_eq!(configured["candle_offline"], true);
        assert_eq!(configured["candle_device"], "cpu");
        assert_eq!(configured["candle_max_input_tokens"], 1_024);
        assert_eq!(configured["candle_max_concurrency"], 3);
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.chat is not available for runtime 'candle' and model 'llama3.2': model 'llama3.2' is not in the local Candle generation starter set"
    )]
    fn sql_chat_should_reject_candle_runtime_generation() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle')",
        ));

        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.chat(ARRAY[postllm.user('hello')])",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.chat is not available for runtime 'candle' and model 'llama3.2': model 'llama3.2' is not in the local Candle generation starter set"
    )]
    fn sql_chat_text_should_reject_candle_runtime_generation() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle')",
        ));

        drop(Spi::get_one::<String>(
            "SELECT postllm.chat_text(ARRAY[postllm.user('hello')])",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.complete is not available for runtime 'candle' and model 'llama3.2': model 'llama3.2' is not in the local Candle generation starter set"
    )]
    fn sql_complete_should_reject_candle_runtime_generation() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle')",
        ));

        drop(Spi::get_one::<String>(
            "SELECT postllm.complete(prompt => 'hello')",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.chat_structured/postllm.complete_structured is not available for runtime 'candle' and model 'Qwen/Qwen2.5-0.5B-Instruct': structured outputs are not implemented by the local Candle runtime"
    )]
    fn sql_chat_structured_should_reject_candle_runtime() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct')",
        ));

        drop(Spi::get_one::<JsonB>(
            r#"SELECT postllm.chat_structured(
                ARRAY[postllm.user('Return a name.')],
                postllm.json_schema(
                    'person',
                    '{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}'::jsonb
                )
            )"#,
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.chat_structured/postllm.complete_structured is not available for runtime 'candle' and model 'Qwen/Qwen2.5-0.5B-Instruct': structured outputs are not implemented by the local Candle runtime"
    )]
    fn sql_complete_structured_should_reject_candle_runtime() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct')",
        ));

        drop(Spi::get_one::<JsonB>(
            r#"SELECT postllm.complete_structured(
                prompt => 'Return a name.',
                response_format => postllm.json_schema(
                    'person',
                    '{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}'::jsonb
                )
            )"#,
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.chat_tools/postllm.complete_tools is not available for runtime 'candle' and model 'Qwen/Qwen2.5-0.5B-Instruct': tool-calling requests are not implemented by the local Candle runtime"
    )]
    fn sql_chat_tools_should_reject_candle_runtime() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct')",
        ));

        drop(Spi::get_one::<JsonB>(
            r#"SELECT postllm.chat_tools(
                ARRAY[postllm.user('Call the tool.')],
                ARRAY[
                    postllm.function_tool(
                        'lookup_weather',
                        '{"type":"object","properties":{"city":{"type":"string"}},"required":["city"],"additionalProperties":false}'::jsonb
                    )
                ],
                tool_choice => postllm.tool_choice_function('lookup_weather')
            )"#,
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.chat_tools/postllm.complete_tools is not available for runtime 'candle' and model 'Qwen/Qwen2.5-0.5B-Instruct': tool-calling requests are not implemented by the local Candle runtime"
    )]
    fn sql_complete_tools_should_reject_candle_runtime() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct')",
        ));

        drop(Spi::get_one::<JsonB>(
            r#"SELECT postllm.complete_tools(
                prompt => 'Call the tool.',
                tools => ARRAY[
                    postllm.function_tool(
                        'lookup_weather',
                        '{"type":"object","properties":{"city":{"type":"string"}},"required":["city"],"additionalProperties":false}'::jsonb
                    )
                ],
                tool_choice => postllm.tool_choice_function('lookup_weather')
            )"#,
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.chat_stream/postllm.complete_stream is not available for runtime 'candle' and model 'Qwen/Qwen2.5-0.5B-Instruct': streaming is not implemented by the local Candle runtime"
    )]
    fn sql_chat_stream_should_reject_candle_runtime() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct')",
        ));

        drop(Spi::get_one::<JsonB>(
            "SELECT to_jsonb(t) FROM postllm.chat_stream(ARRAY[postllm.user('stream hello')]) AS t LIMIT 1",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.chat_stream/postllm.complete_stream is not available for runtime 'candle' and model 'Qwen/Qwen2.5-0.5B-Instruct': streaming is not implemented by the local Candle runtime"
    )]
    fn sql_complete_stream_should_reject_candle_runtime() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct')",
        ));

        drop(Spi::get_one::<JsonB>(
            "SELECT to_jsonb(t) FROM postllm.complete_stream(prompt => 'stream hello') AS t LIMIT 1",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "multimodal inputs is not available for runtime 'candle' and model 'Qwen/Qwen2.5-0.5B-Instruct': multimodal inputs are not implemented by the local Candle runtime"
    )]
    fn sql_chat_should_reject_multimodal_inputs_for_candle_runtime() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct')",
        ));

        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.chat(ARRAY[postllm.user_parts(ARRAY[postllm.text_part('Describe this image.'), postllm.image_url_part('https://example.com/cat.png')])])",
        ));
    }

    #[pg_test]
    fn sql_capabilities_should_surface_candle_generation_limits() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle')",
        ));

        let capabilities = sql_json("SELECT postllm.capabilities()");

        assert_eq!(capabilities["features"]["chat"]["available"], false);
        assert_eq!(capabilities["features"]["chat"]["runtime"], "candle");
        assert_eq!(
            capabilities["features"]["chat"]["reason"],
            "model 'llama3.2' is not in the local Candle generation starter set; supported starter models are Qwen/Qwen2.5-0.5B-Instruct, Qwen/Qwen2.5-1.5B-Instruct"
        );
        assert_eq!(
            capabilities["features"]["chat"]["supported_models"],
            json!(["Qwen/Qwen2.5-0.5B-Instruct", "Qwen/Qwen2.5-1.5B-Instruct"])
        );
        assert_eq!(capabilities["features"]["tools"]["available"], false);
        assert_eq!(
            capabilities["features"]["tools"]["reason"],
            "tool-calling requests are not implemented by the local Candle runtime"
        );
        assert_eq!(capabilities["features"]["streaming"]["available"], false);
        assert_eq!(
            capabilities["features"]["streaming"]["reason"],
            "streaming is not implemented by the local Candle runtime"
        );
        assert_eq!(
            capabilities["features"]["structured_outputs"]["available"],
            false
        );
        assert_eq!(
            capabilities["features"]["structured_outputs"]["reason"],
            "structured outputs are not implemented by the local Candle runtime"
        );
        assert_eq!(capabilities["features"]["embeddings"]["available"], true);
        assert_eq!(capabilities["features"]["reranking"]["available"], true);
        assert_eq!(capabilities["features"]["reranking"]["runtime"], "candle");
        assert_eq!(
            capabilities["features"]["reranking"]["model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
    }

    #[pg_test]
    fn sql_capabilities_should_report_registered_candle_starter_models() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct')",
        ));

        let capabilities = sql_json("SELECT postllm.capabilities()");

        assert_eq!(capabilities["features"]["chat"]["available"], true);
        assert_eq!(capabilities["features"]["chat"]["runtime"], "candle");
        assert_eq!(
            capabilities["features"]["chat"]["model"],
            "Qwen/Qwen2.5-0.5B-Instruct"
        );
        assert_eq!(capabilities["features"]["chat"].get("reason"), None);
        assert_eq!(capabilities["features"]["complete"]["available"], true);
        assert_eq!(capabilities["features"]["complete"]["runtime"], "candle");
        assert_eq!(
            capabilities["features"]["complete"]["model"],
            "Qwen/Qwen2.5-0.5B-Instruct"
        );
        assert_eq!(capabilities["features"]["complete"].get("reason"), None);
        assert_eq!(capabilities["features"]["reranking"]["available"], true);
        assert_eq!(capabilities["features"]["reranking"]["runtime"], "candle");
        assert_eq!(
            capabilities["features"]["reranking"]["model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
        assert_eq!(capabilities["features"]["reranking"].get("reason"), None);
        assert_eq!(capabilities["features"]["streaming"]["available"], false);
        assert_eq!(
            capabilities["features"]["streaming"]["reason"],
            "streaming is not implemented by the local Candle runtime"
        );
    }

    #[pg_test]
    fn sql_runtime_discover_should_probe_openai_models_endpoint() {
        let (base_url, receiver) = start_mock_runtime_discovery_server(
            200,
            r#"{"data":[{"id":"llama3.2"},{"id":"other-model"}]}"#,
        );
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'llama3.2')",
            sql_literal(&base_url)
        ));
        let discovery = sql_json("SELECT postllm.runtime_discover()");
        let request_line = receiver
            .recv()
            .expect("runtime discovery probe should hit the mock server");

        assert_eq!(configured["runtime"], "openai");
        assert!(request_line.starts_with("GET /v1/models HTTP/1.1"));
        assert_eq!(discovery["runtime"], "openai");
        assert_eq!(discovery["ready"], true);
        assert_eq!(discovery["provider"], "openai-compatible");
        assert_eq!(discovery["model"], "llama3.2");
        assert_eq!(discovery["model_listed"], true);
        assert_eq!(discovery["status_code"], 200);
        assert_eq!(discovery["base_url_kind"], "loopback");
        assert_eq!(discovery["execution_environment"], "local");
    }

    #[pg_test]
    fn sql_runtime_discover_should_flag_missing_openai_model() {
        let (base_url, receiver) =
            start_mock_runtime_discovery_server(200, r#"{"data":[{"id":"different-model"}]}"#);
        drop(Spi::get_one::<JsonB>(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'llama3.2')",
            sql_literal(&base_url)
        )));

        let discovery = sql_json("SELECT postllm.runtime_discover()");
        let ready = sql_bool("SELECT postllm.runtime_ready()");

        receiver
            .recv()
            .expect("runtime discovery probe should hit the mock server");

        assert_eq!(discovery["runtime"], "openai");
        assert_eq!(discovery["ready"], false);
        assert_eq!(discovery["reachable"], true);
        assert_eq!(discovery["model_listed"], false);
        assert!(
            discovery["reason"]
                .as_str()
                .expect("missing-model discovery should expose a reason")
                .contains("was not listed by the discovery endpoint")
        );
        assert_eq!(ready, false);
    }

    #[pg_test]
    fn sql_runtime_discover_should_report_candle_generation_readiness() {
        let cache_dir = fresh_test_cache_dir("runtime-discover-candle");
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct', candle_cache_dir => {}, candle_offline => false, candle_device => 'cpu')",
            sql_literal(&cache_dir)
        ));
        let discovery = sql_json("SELECT postllm.runtime_discover()");
        let ready = sql_bool("SELECT postllm.runtime_ready()");

        assert_eq!(configured["runtime"], "candle");
        assert_eq!(discovery["runtime"], "candle");
        assert_eq!(discovery["provider"], "candle");
        assert_eq!(discovery["ready"], true);
        assert_eq!(discovery["cold_start"], true);
        assert_eq!(discovery["model"], "Qwen/Qwen2.5-0.5B-Instruct");
        assert_eq!(
            discovery["embedding_model"],
            "sentence-transformers/paraphrase-MiniLM-L3-v2"
        );
        assert_eq!(discovery["generation"]["metadata"]["supported"], true);
        assert_eq!(discovery["generation"]["device"]["resolved"], "cpu");
        assert_eq!(discovery["embedding"]["lane"], "embedding");
        assert_eq!(discovery["execution_environment"], "local");
        assert_eq!(ready, true);
    }

    #[pg_test]
    fn sql_runtime_discover_should_report_offline_candle_cache_misses() {
        let cache_dir = fresh_test_cache_dir("runtime-discover-candle-offline");
        drop(Spi::get_one::<JsonB>(&format!(
            "SELECT postllm.configure(runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct', candle_cache_dir => {}, candle_offline => true, candle_device => 'cpu')",
            sql_literal(&cache_dir)
        )));

        let discovery = sql_json("SELECT postllm.runtime_discover()");
        let ready = sql_bool("SELECT postllm.runtime_ready()");

        assert_eq!(discovery["runtime"], "candle");
        assert_eq!(discovery["ready"], false);
        assert_eq!(discovery["offline"], true);
        assert_eq!(discovery["generation"]["disk_cached"], false);
        assert_eq!(discovery["generation"]["memory_cached"], false);
        assert!(
            discovery["reason"]
                .as_str()
                .expect("offline cache miss should expose a reason")
                .contains(
                    "is enabled and model 'Qwen/Qwen2.5-0.5B-Instruct' is not cached locally"
                )
        );
        assert_eq!(ready, false);
    }

    #[pg_test]
    fn sql_complete_stream_should_return_ordered_mock_sse_chunks() {
        let (base_url, receiver) = start_mock_stream_server(concat!(
            "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"model\":\"mock-stream-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"model\":\"mock-stream-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hel\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"model\":\"mock-stream-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        ));
        let configured = sql_json(&format!(
            "SELECT postllm.configure(runtime => 'openai', base_url => {}, model => 'mock-stream-model')",
            sql_literal(&base_url)
        ));
        let rows = sql_json(
            r"WITH chunks AS (
                SELECT index, delta, event
                FROM postllm.complete_stream(
                    prompt => 'Say hello.',
                    system_prompt => 'You are terse.',
                    temperature => 0.0,
                    max_tokens => 8
                )
            )
            SELECT jsonb_agg(
                jsonb_build_object(
                    'index', index,
                    'delta', delta,
                    'event', event
                )
                ORDER BY index
            )
            FROM chunks",
        );
        let request_body = receiver
            .recv()
            .expect("mock server should capture the streaming request");

        assert_eq!(configured["runtime"], "openai");
        assert_eq!(request_body["stream"], true);
        assert_eq!(request_body["messages"][0]["role"], "system");
        assert_eq!(request_body["messages"][1]["role"], "user");
        assert_eq!(
            rows.as_array()
                .expect("stream rows should be aggregated")
                .len(),
            3
        );
        assert_eq!(rows[0]["delta"], Value::Null);
        assert_eq!(rows[1]["delta"], "hel");
        assert_eq!(rows[2]["delta"], "lo");
        assert_eq!(rows[2]["event"]["_postllm"]["finish_reason"], "stop");
        assert_eq!(rows[1]["event"]["_postllm"]["content_delta"], "hel");
    }

    #[pg_test]
    fn sql_chat_should_smoke_live_candle_generation_when_enabled() {
        let Some(model) = configure_candle_generation_pg_test() else {
            return;
        };

        let response = sql_json(
            "SELECT postllm.chat(ARRAY[postllm.system('You are a literal test harness. Reply with only 4.'), postllm.user('What is 2 + 2?')], temperature => 0.0, max_tokens => 8)",
        );
        let text = response["choices"][0]["message"]["content"]
            .as_str()
            .expect("Candle chat response should contain string content");
        let finish_reason = response["_postllm"]["finish_reason"]
            .as_str()
            .expect("normalized metadata should include a finish_reason");
        let prompt_tokens = response["_postllm"]["usage"]["prompt_tokens"]
            .as_i64()
            .expect("normalized metadata should include prompt_tokens");
        let completion_tokens = response["_postllm"]["usage"]["completion_tokens"]
            .as_i64()
            .expect("normalized metadata should include completion_tokens");

        assert!(
            smoke_answer_is_four(text),
            "expected a local Candle answer containing 4, got {text}"
        );
        assert_eq!(response["_postllm"]["runtime"], "candle");
        assert_eq!(response["_postllm"]["provider"], "candle");
        assert_eq!(response["_postllm"]["model"].as_str(), Some(model.as_str()));
        assert!(matches!(finish_reason, "stop" | "length"));
        assert!(prompt_tokens > 0);
        assert!(completion_tokens > 0);
    }

    #[pg_test]
    fn sql_chat_text_should_smoke_live_candle_generation_when_enabled() {
        let Some(_) = configure_candle_generation_pg_test() else {
            return;
        };

        let text = sql_text(
            "SELECT trim(postllm.chat_text(ARRAY[postllm.system('You are a literal test harness. Reply with only 4.'), postllm.user('What is 2 + 2?')], temperature => 0.0, max_tokens => 8))",
        );

        assert!(
            smoke_answer_is_four(&text),
            "expected a local Candle chat_text answer containing 4, got {text}"
        );
    }

    #[pg_test]
    fn sql_complete_should_smoke_live_candle_generation_when_enabled() {
        let Some(_) = configure_candle_generation_pg_test() else {
            return;
        };

        let text = sql_text(
            "SELECT trim(postllm.complete(prompt => '2 + 2 =', system_prompt => 'You are a literal test harness. Reply with only 4.', temperature => 0.0, max_tokens => 8))",
        );

        assert!(
            smoke_answer_is_four(&text),
            "expected a local Candle completion containing 4, got {text}"
        );
    }

    #[pg_test]
    fn sql_complete_many_should_smoke_live_candle() {
        let Some(_) = configure_candle_generation_pg_test() else {
            return;
        };

        let results = sql_json(
            "SELECT to_jsonb(postllm.complete_many(ARRAY['2 + 2 =', '2 + 2 ='], system_prompt => 'You are a literal test harness. Reply with only 4.', temperature => 0.0, max_tokens => 8))",
        );
        let batch = results
            .as_array()
            .expect("complete_many should return a JSON array");

        assert_eq!(batch.len(), 2);
        assert!(
            batch
                .iter()
                .all(|value| value.as_str().is_some_and(smoke_answer_is_four)),
            "expected all batch completions to contain 4, got {results}"
        );
    }

    #[pg_test]
    fn sql_complete_many_rows_should_smoke_live_candle() {
        let Some(_) = configure_candle_generation_pg_test() else {
            return;
        };

        let rows = sql_json(
            "SELECT jsonb_agg(to_jsonb(batch) ORDER BY index) FROM postllm.complete_many_rows(ARRAY['2 + 2 =', '2 + 2 ='], system_prompt => 'You are a literal test harness. Reply with only 4.', temperature => 0.0, max_tokens => 8) AS batch",
        );
        let batch = rows
            .as_array()
            .expect("complete_many_rows should aggregate to a JSON array");

        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0]["index"], 1);
        assert_eq!(batch[1]["index"], 2);
        assert!(
            batch
                .iter()
                .all(|row| row["completion"].as_str().is_some_and(smoke_answer_is_four)),
            "expected all batch completion rows to contain 4, got {rows}"
        );
    }

    #[pg_test]
    fn sql_message_should_trim_role_and_content() {
        let message = sql_json("SELECT postllm.message(' user ', '  Hello from SQL  ')");

        assert_eq!(
            message,
            json!({
                "role": "user",
                "content": "Hello from SQL",
            })
        );
    }

    #[pg_test]
    fn sql_configure_should_treat_blank_api_key_as_unset() {
        let configured = sql_json("SELECT postllm.configure(api_key => '   ')");

        assert_eq!(configured["has_api_key"], false);
        assert_eq!(configured["api_key_source"], "none");
        assert_eq!(configured["api_key_secret"], Value::Null);
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'api_key_secret' must not be combined with api_key in the same configure(...) call; fix: pass either api_key => '...' for a direct session secret or api_key_secret => '...' for a named stored secret"
    )]
    fn sql_configure_should_reject_api_key_plus_secret() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(api_key => 'sk-inline', api_key_secret => 'named-secret')",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'max_retries' must be greater than or equal to zero, got -1; fix: pass zero to disable retries or a positive retry count"
    )]
    fn sql_configure_should_reject_negative_max_retries() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(max_retries => -1)",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'request_max_concurrency' must be greater than or equal to zero, got -1; fix: pass zero to disable the global request concurrency cap or a positive integer slot count"
    )]
    fn sql_configure_should_reject_negative_request_max_concurrency() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(request_max_concurrency => -1)",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'candle_max_input_tokens' must be greater than or equal to zero, got -1; fix: pass zero to disable the local Candle token cap or a positive integer token limit"
    )]
    fn sql_configure_should_reject_negative_candle_max_input_tokens() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(candle_max_input_tokens => -1)",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'candle_max_concurrency' must be greater than or equal to zero, got -1; fix: pass zero to disable the local Candle concurrency cap or a positive integer slot count"
    )]
    fn sql_configure_should_reject_negative_candle_max_concurrency() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(candle_max_concurrency => -1)",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'candle_device' must be one of 'auto', 'cpu', 'cuda', or 'metal', got 'tpu'; fix: pass candle_device => 'auto', 'cpu', 'cuda', or 'metal'"
    )]
    fn sql_configure_should_reject_unknown_candle_device() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.configure(candle_device => 'tpu')",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'profile' must include at least one setting override; fix: pass one or more settings such as runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct', or base_url => 'http://127.0.0.1:11434/v1/chat/completions'"
    )]
    fn sql_profile_set_should_reject_empty_profiles() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.profile_set(name => 'empty-profile')",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'name' refers to unknown profile 'missing-profile'; fix: create it with postllm.profile_set(...) or choose one from postllm.profiles()"
    )]
    fn sql_profile_apply_should_reject_unknown_profiles() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.profile_apply('missing-profile')",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'lane' must be one of 'generation' or 'embedding', got 'vision'; fix: pass lane => 'generation' or 'embedding'"
    )]
    fn sql_model_alias_set_should_reject_unknown_lanes() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.model_alias_set(alias => 'bad', lane => 'vision', model => 'example')",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'lane' must be one of 'auto', 'embedding', or 'generation', got 'vision'; fix: omit lane for auto selection or pass lane => 'embedding' or 'generation'"
    )]
    fn sql_model_inspect_should_reject_unknown_lanes() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.model_inspect(lane => 'vision')",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'scope' must be one of 'memory', 'disk', or 'all', got 'network'; fix: pass scope => 'memory', 'disk', or 'all'"
    )]
    fn sql_model_evict_should_reject_unknown_scopes() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.model_evict(scope => 'network')",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'overlap_chars' must be smaller than chunk_chars, got overlap_chars=32 and chunk_chars=32; fix: pass an overlap smaller than the chunk size, for example chunk_chars => 1000 and overlap_chars => 200"
    )]
    fn sql_chunk_text_should_reject_overlap_that_is_too_large() {
        drop(Spi::get_one::<Vec<String>>(
            "SELECT postllm.chunk_text('hello world', chunk_chars => 32, overlap_chars => 32)",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'metadata' must be a JSON object or null; fix: pass jsonb like '{\"doc_id\":\"vacuum-guide\"}'::jsonb or omit metadata"
    )]
    fn sql_chunk_document_should_reject_non_object_metadata() {
        drop(Spi::get_one::<JsonB>(
            "SELECT to_jsonb(chunk) FROM postllm.chunk_document('hello world', '[]'::jsonb) AS chunk LIMIT 1",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'doc_id' must not be empty or whitespace-only; fix: pass a non-empty value for 'doc_id'"
    )]
    fn sql_embed_document_should_reject_blank_doc_id() {
        drop(Spi::get_one::<JsonB>(
            "SELECT to_jsonb(chunk) FROM postllm.embed_document('   ', 'hello world') AS chunk LIMIT 1",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm received an invalid argument: argument 'target_table' must expose canonical ingestion columns chunk_id, doc_id, chunk_no, content, metadata, and embedding; missing chunk_id, doc_id, chunk_no, content, metadata, embedding; fix: create a table with those columns or use postllm.embed_document(...) and write the INSERT yourself"
    )]
    fn sql_ingest_document_should_reject_noncanonical_tables() {
        Spi::run("CREATE TEMP TABLE bad_ingest_table (id bigint PRIMARY KEY)")
            .expect("invalid-ingest test should create the temp table");

        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.ingest_document('pg_temp.bad_ingest_table', 'guide-1', 'hello world')",
        ));
    }

    #[pg_test]
    fn sql_extract_text_should_return_string_message_content() {
        let text = sql_text(
            r#"SELECT postllm.extract_text(
                '{"choices":[{"message":{"content":"hello from SQL"}}]}'::jsonb
            )"#,
        );

        assert_eq!(text, "hello from SQL");
    }

    #[pg_test]
    fn sql_usage_should_return_normalized_usage_objects() {
        let usage = sql_json(
            r#"SELECT postllm.usage(
                '{"usage":{"prompt_tokens":9,"completion_tokens":4}}'::jsonb
            )"#,
        );

        assert_eq!(
            usage,
            json!({
                "prompt_tokens": 9,
                "completion_tokens": 4,
                "total_tokens": 13,
            })
        );
    }

    #[pg_test]
    fn sql_choice_should_return_the_requested_choice() {
        let choice = sql_json(
            r#"SELECT postllm.choice(
                '{"choices":[
                    {"index":0,"message":{"content":"first"},"finish_reason":"stop"},
                    {"index":1,"message":{"content":"second"},"finish_reason":"length"}
                ]}'::jsonb,
                1
            )"#,
        );

        assert_eq!(
            choice,
            json!({
                "index": 1,
                "message": {
                    "content": "second",
                },
                "finish_reason": "length",
            })
        );
    }

    #[pg_test]
    fn sql_finish_reason_should_prefer_postllm_metadata() {
        let finish_reason = sql_text(
            r#"SELECT postllm.finish_reason(
                '{
                    "choices":[{"message":{"content":"hello from SQL"},"finish_reason":"length"}],
                    "_postllm":{"finish_reason":"stop"}
                }'::jsonb
            )"#,
        );

        assert_eq!(finish_reason, "stop");
    }

    #[pg_test]
    fn sql_finish_reason_should_return_null_when_unavailable() {
        let finish_reason = sql_optional_text(
            r#"SELECT postllm.finish_reason(
                '{"choices":[{"message":{"content":"hello from SQL"}}]}'::jsonb
            )"#,
        );

        assert_eq!(finish_reason, None);
    }

    #[pg_test]
    fn sql_extract_text_should_ignore_postllm_metadata() {
        let text = sql_text(
            r#"SELECT postllm.extract_text(
                '{
                    "choices":[{"message":{"content":"hello from SQL"},"finish_reason":"stop"}],
                    "_postllm":{
                        "runtime":"candle",
                        "provider":"candle",
                        "model":"Qwen/Qwen2.5-0.5B-Instruct",
                        "finish_reason":"stop",
                        "usage":{"prompt_tokens":9,"completion_tokens":4,"total_tokens":13}
                    }
                }'::jsonb
            )"#,
        );

        assert_eq!(text, "hello from SQL");
    }

    #[pg_test]
    fn sql_extract_text_should_join_text_parts() {
        let text = sql_text(
            r#"SELECT postllm.extract_text(
                '{"choices":[{"message":{"content":[{"type":"text","text":"hello"},{"type":"text","text":" world"}]}}]}'::jsonb
            )"#,
        );

        assert_eq!(text, "hello world");
    }

    #[pg_test]
    #[should_panic(expected = "argument 'content' must not be empty or whitespace-only")]
    fn sql_message_should_reject_blank_content() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.message('user', '   ')",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'parts' must contain at least one content part")]
    fn sql_message_parts_should_reject_empty_arrays() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.user_parts(ARRAY[]::jsonb[])",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'tool_calls' must contain at least one tool call")]
    fn sql_assistant_tool_calls_should_reject_empty_arrays() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.assistant_tool_calls(ARRAY[]::jsonb[])",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'messages' must contain at least one chat message")]
    fn sql_chat_should_reject_empty_message_arrays() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.chat(ARRAY[]::jsonb[])",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'variables' is missing template variable 'name'")]
    fn sql_render_template_should_reject_missing_variables() {
        drop(Spi::get_one::<String>(
            "SELECT postllm.render_template('Hello {{name}}', '{}'::jsonb)",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'variables' must be a JSON object")]
    fn sql_render_template_should_reject_non_object_variables() {
        drop(Spi::get_one::<String>(
            "SELECT postllm.render_template('Hello {{name}}', '[]'::jsonb)",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'message' must not be null in postllm.messages_agg")]
    fn sql_messages_agg_should_reject_null_rows() {
        drop(Spi::get_one::<JsonB>(
            r"SELECT to_jsonb(postllm.messages_agg(message ORDER BY ord))
            FROM (
                VALUES
                    (1, NULL::jsonb)
            ) AS conversation(ord, message)",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'message.content' must be present")]
    fn sql_messages_agg_should_reject_invalid_rows() {
        drop(Spi::get_one::<JsonB>(
            r#"SELECT to_jsonb(postllm.messages_agg(message ORDER BY ord))
            FROM (
                VALUES
                    (1, '{"role":"user"}'::jsonb)
            ) AS conversation(ord, message)"#,
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'prompts' must contain at least one text value")]
    fn sql_complete_many_should_reject_empty_prompt_batches() {
        drop(Spi::get_one::<JsonB>(
            "SELECT to_jsonb(postllm.complete_many(ARRAY[]::text[]))",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'prompts' must contain at least one text value")]
    fn sql_complete_many_rows_should_reject_empty_prompt_batches() {
        drop(Spi::get_one::<i64>(
            "SELECT count(*) FROM postllm.complete_many_rows(ARRAY[]::text[])",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'query' must not be empty or whitespace-only")]
    fn sql_rerank_should_reject_blank_queries() {
        drop(Spi::get_one::<JsonB>(
            "SELECT to_jsonb(ranked) FROM postllm.rerank('   ', ARRAY['hello']) AS ranked LIMIT 1",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'documents' must contain at least one text value")]
    fn sql_rerank_should_reject_empty_document_arrays() {
        drop(Spi::get_one::<JsonB>(
            "SELECT to_jsonb(ranked) FROM postllm.rerank('hello', ARRAY[]::text[]) AS ranked LIMIT 1",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'top_n' must be greater than zero when present")]
    fn sql_rerank_should_reject_non_positive_top_n() {
        drop(Spi::get_one::<JsonB>(
            "SELECT to_jsonb(ranked) FROM postllm.rerank('hello', ARRAY['world'], top_n => 0) AS ranked LIMIT 1",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "argument 'text_search_config' text search configuration 'missing_config' was not found"
    )]
    fn sql_keyword_rank_should_reject_unknown_text_search_configs() {
        drop(Spi::get_one::<JsonB>(
            "SELECT to_jsonb(ranked) FROM postllm.keyword_rank('hello', ARRAY['world'], text_search_config => 'missing_config') AS ranked LIMIT 1",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "argument 'retrieval' must be one of 'hybrid', 'semantic', or 'keyword', got 'vector'"
    )]
    fn sql_rag_should_reject_unknown_retrieval_modes() {
        drop(Spi::get_one::<String>(
            "SELECT postllm.rag_text('hello', ARRAY['world'], retrieval => 'vector')",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'top_n' must be greater than zero when present")]
    fn sql_rag_should_reject_non_positive_top_n() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.rag('hello', ARRAY['world'], top_n => 0)",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'semantic_rank' must be present when keyword_rank is null")]
    fn sql_rrf_score_should_require_at_least_one_rank() {
        drop(Spi::get_one::<f64>("SELECT postllm.rrf_score(NULL, NULL)"));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'temperature' must be between 0.0 and 2.0")]
    fn sql_chat_should_reject_out_of_range_temperature() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.chat(ARRAY[postllm.user('hello')], temperature => 2.1)",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'max_tokens' must be greater than zero")]
    fn sql_chat_should_reject_non_positive_max_tokens() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.chat(ARRAY[postllm.user('hello')], max_tokens => 0)",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "argument 'max_tokens' must be less than or equal to 8 because postllm request guardrails are active, got 16"
    )]
    fn sql_chat_should_reject_max_tokens_above_request_token_budget() {
        drop(sql_json(
            "SELECT postllm.configure(request_token_budget => 8)",
        ));

        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.chat(ARRAY[postllm.user('hello')], max_tokens => 16)",
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "postllm.output_token_price_microusd_per_1k must be greater than zero when postllm.request_spend_budget_microusd is enabled"
    )]
    fn sql_chat_should_reject_spend_budget_without_output_token_price() {
        drop(sql_json(
            "SELECT postllm.configure(request_spend_budget_microusd => 500)",
        ));

        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.chat(ARRAY[postllm.user('hello')])",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "LLM response was missing choices[0].message.content")]
    fn sql_extract_text_should_reject_malformed_responses() {
        drop(Spi::get_one::<String>(
            r#"SELECT postllm.extract_text('{"choices":[]}'::jsonb)"#,
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'index' must be greater than or equal to zero")]
    fn sql_choice_should_reject_negative_indexes() {
        drop(Spi::get_one::<JsonB>(
            r#"SELECT postllm.choice('{"choices":[{"index":0}]}'::jsonb, -1)"#,
        ));
    }

    #[pg_test]
    #[should_panic(
        expected = "argument 'index' with value 1 is out of range for 1 available choices"
    )]
    fn sql_choice_should_reject_out_of_range_indexes() {
        drop(Spi::get_one::<JsonB>(
            r#"SELECT postllm.choice('{"choices":[{"index":0}]}'::jsonb, 1)"#,
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'input' must not be empty or whitespace-only")]
    fn sql_embed_should_reject_blank_inputs() {
        drop(Spi::get_one::<Vec<f32>>("SELECT postllm.embed('   ')"));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'inputs' must contain at least one text value")]
    fn sql_embed_many_should_reject_empty_arrays() {
        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.embed_many(ARRAY[]::text[])",
        ));
    }

    #[pg_test]
    #[should_panic(expected = "argument 'input' must not be empty or whitespace-only")]
    fn sql_embed_many_should_validate_blank_inputs_before_settings() {
        sql_run("SET LOCAL postllm.embedding_model = ''");

        drop(Spi::get_one::<JsonB>(
            "SELECT postllm.embed_many(ARRAY['   '])",
        ));
    }
}

/// `cargo pgrx test` looks for this module at the crate root.
#[cfg(test)]
pub mod pg_test {
    /// Performs one-time test initialization.
    pub fn setup(_options: Vec<&str>) {
        // No one-off setup is required.
    }

    /// Returns extra `postgresql.conf` options required for tests.
    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
