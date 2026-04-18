#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::error::{Error, Result};
use pgrx::JsonB;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::datum::DatumWithOid;
use pgrx::spi::Spi;
use serde_json::{Value, json};
use std::env;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const JOB_KIND_CHAT: &str = "chat";
const JOB_KIND_COMPLETE: &str = "complete";
const JOB_KIND_EMBED: &str = "embed";
const JOB_KIND_RERANK: &str = "rerank";
const JOB_STATUS_QUEUED: &str = "queued";
const JOB_STATUS_RUNNING: &str = "running";
const JOB_STATUS_SUCCEEDED: &str = "succeeded";
const JOB_STATUS_FAILED: &str = "failed";
const JOB_STATUS_CANCELLED: &str = "cancelled";
const WORKER_LIBRARY: &str = "postllm";
const WORKER_FUNCTION: &str = "postllm_async_job_worker_main";
const WORKER_NAME: &str = "postllm async job";
const SUBPROCESS_FUNCTION: &str = "postllm._async_job_run";
const WORKER_WAIT_FOR_ROW_MS: u64 = 2_000;
const WORKER_WAIT_POLL_MS: u64 = 25;

#[derive(Debug, Clone, PartialEq)]
struct ChatJobRequest {
    messages: Vec<Value>,
    model: Option<String>,
    temperature: f64,
    max_tokens: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
struct CompleteJobRequest {
    prompt: String,
    system_prompt: Option<String>,
    model: Option<String>,
    temperature: f64,
    max_tokens: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
struct EmbedJobRequest {
    input: String,
    model: Option<String>,
    normalize: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct RerankJobRequest {
    query: String,
    documents: Vec<String>,
    top_n: Option<i32>,
    model: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
enum AsyncJobRequest {
    Chat(ChatJobRequest),
    Complete(CompleteJobRequest),
    Embed(EmbedJobRequest),
    Rerank(RerankJobRequest),
}

impl AsyncJobRequest {
    fn parse(kind: &str, payload: &Value) -> Result<Self> {
        let kind = normalize_kind(kind)?;
        let object = payload.as_object().ok_or_else(|| {
            Error::invalid_argument(
                "request",
                "must be a JSON object",
                "pass request => jsonb_build_object(...) for the selected async job kind",
            )
        })?;

        match kind.as_str() {
            JOB_KIND_CHAT => Ok(Self::Chat(ChatJobRequest {
                messages: required_json_array(object, "messages")?,
                model: optional_string(object, "model"),
                temperature: optional_f64(object, "temperature")?.unwrap_or(0.2),
                max_tokens: optional_i32(object, "max_tokens")?,
            })),
            JOB_KIND_COMPLETE => Ok(Self::Complete(CompleteJobRequest {
                prompt: required_string(object, "prompt")?,
                system_prompt: optional_string(object, "system_prompt"),
                model: optional_string(object, "model"),
                temperature: optional_f64(object, "temperature")?.unwrap_or(0.2),
                max_tokens: optional_i32(object, "max_tokens")?,
            })),
            JOB_KIND_EMBED => Ok(Self::Embed(EmbedJobRequest {
                input: required_string(object, "input")?,
                model: optional_string(object, "model"),
                normalize: optional_bool(object, "normalize")?.unwrap_or(true),
            })),
            JOB_KIND_RERANK => Ok(Self::Rerank(RerankJobRequest {
                query: required_string(object, "query")?,
                documents: required_string_array(object, "documents")?,
                top_n: optional_i32(object, "top_n")?,
                model: optional_string(object, "model"),
            })),
            _ => Err(unknown_job_kind(&kind)),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Chat(_) => JOB_KIND_CHAT,
            Self::Complete(_) => JOB_KIND_COMPLETE,
            Self::Embed(_) => JOB_KIND_EMBED,
            Self::Rerank(_) => JOB_KIND_RERANK,
        }
    }

    fn payload(&self) -> Value {
        match self {
            Self::Chat(request) => json!({
                "messages": request.messages,
                "model": request.model,
                "temperature": request.temperature,
                "max_tokens": request.max_tokens,
            }),
            Self::Complete(request) => json!({
                "prompt": request.prompt,
                "system_prompt": request.system_prompt,
                "model": request.model,
                "temperature": request.temperature,
                "max_tokens": request.max_tokens,
            }),
            Self::Embed(request) => json!({
                "input": request.input,
                "model": request.model,
                "normalize": request.normalize,
            }),
            Self::Rerank(request) => json!({
                "query": request.query,
                "documents": request.documents,
                "top_n": request.top_n,
                "model": request.model,
            }),
        }
    }

    fn execute(&self) -> Result<Value> {
        match self {
            Self::Chat(request) => crate::chat_impl_from_values(
                &request.messages,
                request.model.as_deref(),
                request.temperature,
                request.max_tokens,
                crate::backend::Feature::Chat,
                crate::ChatRequestExtensions::default(),
            ),
            Self::Complete(request) => crate::complete_impl(
                &request.prompt,
                request.system_prompt.as_deref(),
                request.model.as_deref(),
                request.temperature,
                request.max_tokens,
            )
            .map(|text| json!({ "text": text })),
            Self::Embed(request) => {
                crate::embed_impl(&request.input, request.model.as_deref(), request.normalize).map(
                    |embedding| {
                        json!({
                            "embedding": embedding,
                            "dimensions": embedding.len(),
                            "normalize": request.normalize,
                        })
                    },
                )
            }
            Self::Rerank(request) => {
                let rows = crate::rerank_impl(
                    &request.query,
                    &request.documents,
                    request.top_n,
                    request.model.as_deref(),
                )?;
                let results = rows
                    .into_iter()
                    .map(|(rank, index, document, score)| {
                        json!({
                            "rank": rank,
                            "index": index,
                            "document": document,
                            "score": score,
                        })
                    })
                    .collect::<Vec<_>>();

                Ok(json!({ "results": results }))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ClaimedJob {
    submitted_by: String,
    kind: String,
    request_payload: Value,
    settings_snapshot: Value,
}

impl ClaimedJob {
    fn from_value(value: &Value) -> Result<Self> {
        let object = value.as_object().ok_or_else(|| {
            Error::Internal("async job claim payload was not a JSON object".to_owned())
        })?;

        Ok(Self {
            submitted_by: required_string(object, "submitted_by")?,
            kind: required_string(object, "kind")?,
            request_payload: object.get("request_payload").cloned().ok_or_else(|| {
                Error::Internal(
                    "async job claim payload did not include request_payload".to_owned(),
                )
            })?,
            settings_snapshot: object.get("settings_snapshot").cloned().ok_or_else(|| {
                Error::Internal(
                    "async job claim payload did not include settings_snapshot".to_owned(),
                )
            })?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerContext {
    job_id: i64,
    database_oid: pgrx::pg_sys::Oid,
    role_oid: pgrx::pg_sys::Oid,
}

impl WorkerContext {
    fn encode(self) -> String {
        format!("{}:{}:{}", self.job_id, self.database_oid, self.role_oid)
    }

    fn decode(value: &str) -> Result<Self> {
        let mut parts = value.split(':');
        let job_id = parts
            .next()
            .ok_or_else(|| {
                Error::Internal("async job worker context was missing the job id".to_owned())
            })?
            .parse::<i64>()
            .map_err(|error| {
                Error::Internal(format!(
                    "async job worker context contained an invalid job id: {error}"
                ))
            })?;
        let database_oid = parts
            .next()
            .ok_or_else(|| {
                Error::Internal("async job worker context was missing the database oid".to_owned())
            })?
            .parse::<u32>()
            .map_err(|error| {
                Error::Internal(format!(
                    "async job worker context contained an invalid database oid: {error}"
                ))
            })?
            .into();
        let role_oid = parts
            .next()
            .ok_or_else(|| {
                Error::Internal("async job worker context was missing the role oid".to_owned())
            })?
            .parse::<u32>()
            .map_err(|error| {
                Error::Internal(format!(
                    "async job worker context contained an invalid role oid: {error}"
                ))
            })?
            .into();

        if parts.next().is_some() {
            return Err(Error::Internal(
                "async job worker context contained unexpected trailing fields".to_owned(),
            ));
        }

        Ok(Self {
            job_id,
            database_oid,
            role_oid,
        })
    }
}

pub(crate) fn submit(kind: &str, request: &Value) -> Result<Value> {
    let parsed = AsyncJobRequest::parse(kind, request)?;
    let settings_snapshot = crate::guc::async_job_settings_snapshot()?;
    let submitted_by = crate::operator_policy::caller_role_name();
    let row = insert_job_row(
        &submitted_by,
        parsed.kind(),
        &parsed.payload(),
        &settings_snapshot,
    )?;
    let job_id = row.get("id").and_then(Value::as_i64).ok_or_else(|| {
        Error::Internal("inserted async job row did not include an id".to_owned())
    })?;

    if let Err(error) = spawn_job_runner(job_id, &submitted_by) {
        mark_job_launch_failure(job_id, &error.to_string())?;
        return fetch_job_row(job_id, &submitted_by);
    }

    Ok(row)
}

pub(crate) fn poll(job_id: i64) -> Result<Value> {
    reconcile_stale_running_jobs()?;
    fetch_job_row(job_id, &crate::operator_policy::caller_role_name())
}

pub(crate) fn result(job_id: i64) -> Result<Value> {
    reconcile_stale_running_jobs()?;
    let row = fetch_job_row(job_id, &crate::operator_policy::caller_role_name())?;
    let status = row
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Internal("async job row did not include a status".to_owned()))?;

    match status {
        JOB_STATUS_SUCCEEDED => row.get("result_payload").cloned().ok_or_else(|| {
            Error::Internal("completed async job row did not include result_payload".to_owned())
        }),
        JOB_STATUS_QUEUED | JOB_STATUS_RUNNING => Err(Error::invalid_argument(
            "job_id",
            format!("refers to async job {job_id} which is still {status}"),
            format!("poll it with postllm.job_poll({job_id}) until status = 'succeeded'"),
        )),
        JOB_STATUS_FAILED | JOB_STATUS_CANCELLED => {
            let error_message = row
                .get("error_message")
                .and_then(Value::as_str)
                .unwrap_or("the job did not record an error message");
            Err(Error::invalid_argument(
                "job_id",
                format!(
                    "refers to async job {job_id} which ended with status '{status}': {error_message}"
                ),
                format!("inspect it with postllm.job_poll({job_id})"),
            ))
        }
        other => Err(Error::Internal(format!(
            "async job {job_id} had an unknown status '{other}'"
        ))),
    }
}

pub(crate) fn cancel(job_id: i64) -> Result<Value> {
    let submitted_by = crate::operator_policy::caller_role_name();
    let row = fetch_job_row(job_id, &submitted_by)?;
    let status = row
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Internal("async job row did not include a status".to_owned()))?;

    if !matches!(status, JOB_STATUS_QUEUED | JOB_STATUS_RUNNING) {
        return Ok(row);
    }

    let worker_pid = row
        .get("worker_pid")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok());
    let cancelled_message = "job was cancelled";
    update_job_cancelled(job_id, &submitted_by, cancelled_message)?;

    if let Some(worker_pid) = worker_pid {
        let _ = Spi::get_one_with_args::<bool>(
            "SELECT pg_terminate_backend($1)",
            &[DatumWithOid::from(worker_pid)],
        )?;
    }

    fetch_job_row(job_id, &submitted_by)
}

pub(crate) fn worker_main() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);

    let context = match WorkerContext::decode(BackgroundWorker::get_extra()) {
        Ok(context) => context,
        Err(error) => {
            pgrx::warning!("postllm async worker could not decode its context: {error}");
            return;
        }
    };

    BackgroundWorker::connect_worker_to_spi_by_oid(Some(context.database_oid), None);

    let claimed_job = match claim_job_with_retry(context.job_id) {
        Ok(Some(job)) => job,
        Ok(None) => {
            let message = format!(
                "async worker could not see job {} before the startup wait elapsed",
                context.job_id
            );
            let _ = mark_job_failed_via_helper(context.job_id, &message);
            pgrx::warning!("{message}");
            return;
        }
        Err(error) => {
            let message = format!(
                "async worker failed before claiming job {}: {error}",
                context.job_id
            );
            let _ = mark_job_failed_via_helper(context.job_id, &message);
            pgrx::warning!("{message}");
            return;
        }
    };

    let result = BackgroundWorker::transaction(|| -> Result<Value> {
        set_local_role_for_worker(&claimed_job.submitted_by)?;
        crate::guc::apply_async_job_settings_snapshot(&claimed_job.settings_snapshot)?;
        let request = AsyncJobRequest::parse(&claimed_job.kind, &claimed_job.request_payload)?;
        request.execute()
    });

    match result {
        Ok(result_payload) => {
            let _ = finish_claimed_job(
                context.job_id,
                JOB_STATUS_SUCCEEDED,
                Some(&result_payload),
                None,
            );
        }
        Err(error) => {
            pgrx::warning!(
                "postllm async worker failed job {}: {error}",
                context.job_id
            );
            let _ = finish_claimed_job(context.job_id, JOB_STATUS_FAILED, None, Some(&error));
        }
    }
}

pub(crate) fn run_spawned_job(job_id: i64) -> Result<bool> {
    let Some(claimed_job) = claim_job_by_polling(job_id)? else {
        mark_job_launch_failure(
            job_id,
            "async runner could not see the queued job before the startup wait elapsed",
        )?;
        return Ok(false);
    };

    set_local_role_for_worker(&claimed_job.submitted_by)?;
    crate::guc::apply_async_job_settings_snapshot(&claimed_job.settings_snapshot)?;
    let request = AsyncJobRequest::parse(&claimed_job.kind, &claimed_job.request_payload)?;

    match request.execute() {
        Ok(result_payload) => {
            worker_finish(job_id, JOB_STATUS_SUCCEEDED, Some(&result_payload), None)
        }
        Err(error) => worker_finish(job_id, JOB_STATUS_FAILED, None, Some(&error.to_string())),
    }
}

pub(crate) fn worker_claim(job_id: i64) -> Result<Option<Value>> {
    let claimed = Spi::get_one_with_args::<JsonB>(
        r"
        WITH claimed AS (
            UPDATE postllm.async_jobs
            SET status = 'running',
                started_at = COALESCE(started_at, clock_timestamp()),
                updated_at = clock_timestamp(),
                worker_pid = pg_backend_pid()
            WHERE id = $1
              AND status = 'queued'
            RETURNING submitted_by, kind, request_payload, settings_snapshot
        )
        SELECT jsonb_build_object(
            'submitted_by', submitted_by,
            'kind', kind,
            'request_payload', request_payload,
            'settings_snapshot', settings_snapshot
        )
        FROM claimed
        ",
        &[DatumWithOid::from(job_id)],
    )?;

    Ok(claimed.map(|value| value.0))
}

pub(crate) fn worker_finish(
    job_id: i64,
    status: &str,
    result_payload: Option<&Value>,
    error_message: Option<&str>,
) -> Result<bool> {
    validate_finish_status(status)?;
    let result_payload = result_payload.cloned().map(JsonB);

    Spi::get_one_with_args::<bool>(
        r"
        UPDATE postllm.async_jobs
        SET status = $2,
            result_payload = $3,
            error_message = $4,
            finished_at = clock_timestamp(),
            updated_at = clock_timestamp(),
            worker_pid = NULL
        WHERE id = $1
          AND status = 'running'
        RETURNING true
        ",
        &[
            DatumWithOid::from(job_id),
            DatumWithOid::from(status),
            DatumWithOid::from(result_payload),
            DatumWithOid::from(error_message),
        ],
    )
    .map(|updated| updated.is_some())
    .map_err(Error::from)
}

pub(crate) fn worker_mark_failed(job_id: i64, error_message: &str) -> Result<bool> {
    Spi::get_one_with_args::<bool>(
        r"
        UPDATE postllm.async_jobs
        SET status = 'failed',
            error_message = $2,
            finished_at = clock_timestamp(),
            updated_at = clock_timestamp(),
            worker_pid = NULL
        WHERE id = $1
          AND status IN ('queued', 'running')
        RETURNING true
        ",
        &[
            DatumWithOid::from(job_id),
            DatumWithOid::from(error_message),
        ],
    )
    .map(|updated| updated.is_some())
    .map_err(Error::from)
}

fn claim_job_with_retry(job_id: i64) -> Result<Option<ClaimedJob>> {
    let started = Instant::now();

    loop {
        if BackgroundWorker::sigterm_received() {
            return Ok(None);
        }

        let claimed = BackgroundWorker::transaction(|| worker_claim_via_helper(job_id));
        match claimed {
            Ok(Some(value)) => return ClaimedJob::from_value(&value).map(Some),
            Ok(None) => {
                if started.elapsed() >= Duration::from_millis(WORKER_WAIT_FOR_ROW_MS) {
                    return Ok(None);
                }
            }
            Err(error) => return Err(error),
        }

        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(WORKER_WAIT_POLL_MS))) {
            return Ok(None);
        }
    }
}

fn finish_claimed_job(
    job_id: i64,
    status: &str,
    result_payload: Option<&Value>,
    error: Option<&Error>,
) -> Result<bool> {
    worker_finish_via_helper(
        job_id,
        status,
        result_payload,
        error.map(ToString::to_string).as_deref(),
    )
}

fn launch_worker(context: WorkerContext) -> Result<()> {
    BackgroundWorkerBuilder::new(WORKER_NAME)
        .set_type(WORKER_NAME)
        .set_library(WORKER_LIBRARY)
        .set_function(WORKER_FUNCTION)
        .enable_spi_access()
        .set_extra(&context.encode())
        .load_dynamic()
        .map(|_| ())
        .map_err(|_| {
            Error::Config(
                "unable to start an async background worker; fix: raise max_worker_processes or reduce concurrent async jobs"
                    .to_owned(),
            )
        })
}

fn spawn_job_runner(job_id: i64, execute_role: &str) -> Result<()> {
    let socket_dir = current_socket_dir()?;
    let port = current_port()?;
    let database = current_database_name()?;
    let session_user = current_session_user()?;
    let quoted_role = quote_identifier(execute_role)?;

    let _child = Command::new(psql_path())
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
            &session_user,
            "-d",
            &database,
            "-c",
            &format!("SET ROLE {quoted_role}; SELECT {SUBPROCESS_FUNCTION}({job_id});"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            Error::Config(format!(
                "unable to start an async job runner process: {error}; fix: install psql locally, ensure the server's unix socket is reachable, or reduce concurrent async jobs"
            ))
        })?;

    Ok(())
}

fn current_database_oid() -> Result<pgrx::pg_sys::Oid> {
    Spi::get_one::<i64>("SELECT oid::bigint FROM pg_database WHERE datname = current_database()")
        .map_err(Error::from)?
        .ok_or_else(|| Error::Internal("current database oid was not available".to_owned()))
        .and_then(|value| {
            u32::try_from(value)
                .map_err(|_| {
                    Error::Internal(format!(
                        "current database oid {value} could not be represented as u32"
                    ))
                })
                .map(Into::into)
        })
}

fn insert_job_row(
    submitted_by: &str,
    kind: &str,
    request_payload: &Value,
    settings_snapshot: &Value,
) -> Result<Value> {
    Spi::get_one_with_args::<JsonB>(
        r"
        INSERT INTO postllm.async_jobs AS jobs (
            submitted_by,
            kind,
            request_payload,
            settings_snapshot
        )
        VALUES ($1, $2, $3, $4)
        RETURNING to_jsonb(jobs) - 'settings_snapshot'
        ",
        &[
            DatumWithOid::from(submitted_by),
            DatumWithOid::from(kind),
            DatumWithOid::from(JsonB(request_payload.clone())),
            DatumWithOid::from(JsonB(settings_snapshot.clone())),
        ],
    )?
    .map(|row| row.0)
    .ok_or_else(|| Error::Internal("async job insert returned no row".to_owned()))
}

fn worker_claim_via_helper(job_id: i64) -> Result<Option<Value>> {
    Spi::get_one_with_args::<JsonB>(
        "SELECT postllm._async_job_claim($1)",
        &[DatumWithOid::from(job_id)],
    )
    .map(|row| row.map(|value| value.0))
    .map_err(Error::from)
}

fn worker_finish_via_helper(
    job_id: i64,
    status: &str,
    result_payload: Option<&Value>,
    error_message: Option<&str>,
) -> Result<bool> {
    let result_payload = result_payload.cloned().map(JsonB);

    Spi::get_one_with_args::<bool>(
        "SELECT postllm._async_job_finish($1, $2, $3, $4)",
        &[
            DatumWithOid::from(job_id),
            DatumWithOid::from(status),
            DatumWithOid::from(result_payload),
            DatumWithOid::from(error_message),
        ],
    )
    .map(|updated| updated.unwrap_or(false))
    .map_err(Error::from)
}

fn mark_job_failed_via_helper(job_id: i64, error_message: &str) -> Result<bool> {
    Spi::get_one_with_args::<bool>(
        "SELECT postllm._async_job_mark_failed($1, $2)",
        &[
            DatumWithOid::from(job_id),
            DatumWithOid::from(error_message),
        ],
    )
    .map(|updated| updated.unwrap_or(false))
    .map_err(Error::from)
}

fn set_local_role_for_worker(role_name: &str) -> Result<()> {
    let quoted_role = quote_identifier(role_name)?;

    let _ = Spi::run(&format!("SET LOCAL ROLE {quoted_role}"))?;
    Ok(())
}

fn quote_identifier(value: &str) -> Result<String> {
    Spi::get_one_with_args::<String>("SELECT quote_ident($1)", &[DatumWithOid::from(value)])?
        .ok_or_else(|| Error::Internal("quote_ident returned no value".to_owned()))
}

fn claim_job_by_polling(job_id: i64) -> Result<Option<ClaimedJob>> {
    let started = Instant::now();

    loop {
        if let Some(value) = worker_claim(job_id)? {
            return ClaimedJob::from_value(&value).map(Some);
        }

        if started.elapsed() >= Duration::from_millis(WORKER_WAIT_FOR_ROW_MS) {
            return Ok(None);
        }

        thread::sleep(Duration::from_millis(WORKER_WAIT_POLL_MS));
    }
}

fn current_socket_dir() -> Result<String> {
    sql_text("SHOW unix_socket_directories").map(|value| {
        value
            .split(',')
            .next()
            .expect("socket directory should have at least one value")
            .trim()
            .to_owned()
    })
}

fn current_port() -> Result<String> {
    sql_text("SHOW port")
}

fn current_database_name() -> Result<String> {
    sql_text("SELECT current_database()::text")
}

fn current_session_user() -> Result<String> {
    sql_text("SELECT session_user::text")
}

fn sql_text(query: &str) -> Result<String> {
    Spi::get_one::<String>(query)
        .map_err(Error::from)?
        .ok_or_else(|| Error::Internal(format!("query returned no row: {query}")))
}

fn psql_path() -> PathBuf {
    let version = sql_text("SHOW server_version").unwrap_or_else(|_| "17".to_owned());
    let candidate = env::var("HOME").ok().map(PathBuf::from).map(|home| {
        home.join(".pgrx")
            .join(version.trim())
            .join("pgrx-install/bin/psql")
    });

    candidate
        .filter(|path| path.is_file())
        .unwrap_or_else(|| PathBuf::from("psql"))
}

fn fetch_job_row(job_id: i64, submitted_by: &str) -> Result<Value> {
    Spi::get_one_with_args::<JsonB>(
        r"
        SELECT to_jsonb(job_row) - 'settings_snapshot'
        FROM (
            SELECT *
            FROM postllm.async_jobs
            WHERE id = $1
              AND submitted_by = $2
        ) AS job_row
        ",
        &[
            DatumWithOid::from(job_id),
            DatumWithOid::from(submitted_by),
        ],
    )?
    .map(|row| row.0)
    .ok_or_else(|| {
        Error::invalid_argument(
            "job_id",
            format!("refers to unknown async job {job_id}"),
            "submit a job with postllm.job_submit(...) or use a job id returned from your own session",
        )
    })
}

fn update_job_cancelled(job_id: i64, submitted_by: &str, message: &str) -> Result<()> {
    let _ = Spi::run_with_args(
        r"
        UPDATE postllm.async_jobs
        SET status = 'cancelled',
            finished_at = clock_timestamp(),
            updated_at = clock_timestamp(),
            error_message = $3
        WHERE id = $1
          AND submitted_by = $2
          AND status IN ('queued', 'running')
        ",
        &[
            DatumWithOid::from(job_id),
            DatumWithOid::from(submitted_by),
            DatumWithOid::from(message),
        ],
    )?;

    Ok(())
}

fn mark_job_launch_failure(job_id: i64, error_message: &str) -> Result<()> {
    let _ = Spi::run_with_args(
        r"
        UPDATE postllm.async_jobs
        SET status = 'failed',
            finished_at = clock_timestamp(),
            updated_at = clock_timestamp(),
            worker_pid = NULL,
            error_message = $2
        WHERE id = $1
          AND status = 'queued'
        ",
        &[
            DatumWithOid::from(job_id),
            DatumWithOid::from(error_message),
        ],
    )?;

    Ok(())
}

fn reconcile_stale_running_jobs() -> Result<()> {
    let _ = Spi::run(
        r"
        UPDATE postllm.async_jobs AS jobs
        SET status = 'failed',
            finished_at = clock_timestamp(),
            updated_at = clock_timestamp(),
            worker_pid = NULL,
            error_message = COALESCE(
                jobs.error_message,
                'async worker exited before recording a result'
            )
        WHERE jobs.status = 'running'
          AND jobs.worker_pid IS NOT NULL
          AND NOT EXISTS (
                SELECT 1
                FROM pg_stat_activity AS activity
                WHERE activity.pid = jobs.worker_pid
          )
        ",
    )?;

    Ok(())
}

fn validate_finish_status(status: &str) -> Result<()> {
    if matches!(
        status,
        JOB_STATUS_SUCCEEDED | JOB_STATUS_FAILED | JOB_STATUS_CANCELLED
    ) {
        Ok(())
    } else {
        Err(Error::Internal(format!(
            "async job worker attempted to finish with unsupported status '{status}'"
        )))
    }
}

fn normalize_kind(kind: &str) -> Result<String> {
    let normalized = kind.trim().to_ascii_lowercase();

    match normalized.as_str() {
        JOB_KIND_CHAT | JOB_KIND_COMPLETE | JOB_KIND_EMBED | JOB_KIND_RERANK => Ok(normalized),
        _ => Err(unknown_job_kind(&normalized)),
    }
}

fn unknown_job_kind(kind: &str) -> Error {
    Error::invalid_argument(
        "kind",
        format!(
            "must be one of '{JOB_KIND_CHAT}', '{JOB_KIND_COMPLETE}', '{JOB_KIND_EMBED}', or '{JOB_KIND_RERANK}', got '{kind}'"
        ),
        "pass kind => 'chat', 'complete', 'embed', or 'rerank'",
    )
}

fn required_string(object: &serde_json::Map<String, Value>, key: &str) -> Result<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            Error::invalid_argument(
                "request",
                format!("field '{key}' must be a JSON string"),
                format!("pass request => jsonb_build_object('{key}', '...')"),
            )
        })
}

fn optional_string(object: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn optional_i32(object: &serde_json::Map<String, Value>, key: &str) -> Result<Option<i32>> {
    object
        .get(key)
        .map(|value| {
            value.as_i64().ok_or_else(|| {
                Error::invalid_argument(
                    "request",
                    format!("field '{key}' must be a JSON integer when present"),
                    format!("pass request => jsonb_build_object('{key}', 123)"),
                )
            })
        })
        .transpose()?
        .map(|value| {
            i32::try_from(value).map_err(|_| {
                Error::invalid_argument(
                    "request",
                    format!("field '{key}' must fit into a 32-bit integer"),
                    format!("pass a smaller integer for request.{key}"),
                )
            })
        })
        .transpose()
}

fn optional_f64(object: &serde_json::Map<String, Value>, key: &str) -> Result<Option<f64>> {
    object
        .get(key)
        .map(|value| {
            value.as_f64().ok_or_else(|| {
                Error::invalid_argument(
                    "request",
                    format!("field '{key}' must be a JSON number when present"),
                    format!("pass request => jsonb_build_object('{key}', 0.2)"),
                )
            })
        })
        .transpose()
}

fn optional_bool(object: &serde_json::Map<String, Value>, key: &str) -> Result<Option<bool>> {
    object
        .get(key)
        .map(|value| {
            value.as_bool().ok_or_else(|| {
                Error::invalid_argument(
                    "request",
                    format!("field '{key}' must be a JSON boolean when present"),
                    format!("pass request => jsonb_build_object('{key}', true)"),
                )
            })
        })
        .transpose()
}

fn required_json_array(object: &serde_json::Map<String, Value>, key: &str) -> Result<Vec<Value>> {
    object
        .get(key)
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| {
            Error::invalid_argument(
                "request",
                format!("field '{key}' must be a JSON array"),
                format!("pass request => jsonb_build_object('{key}', jsonb_build_array(...))"),
            )
        })
}

fn required_string_array(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Vec<String>> {
    let values = object.get(key).and_then(Value::as_array).ok_or_else(|| {
        Error::invalid_argument(
            "request",
            format!("field '{key}' must be a JSON array of strings"),
            format!("pass request => jsonb_build_object('{key}', jsonb_build_array('...'))"),
        )
    })?;

    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                Error::invalid_argument(
                    "request",
                    format!("field '{key}[{}]' must be a JSON string", index + 1),
                    format!(
                        "pass request => jsonb_build_object('{key}', jsonb_build_array('...'))"
                    ),
                )
            })
        })
        .collect()
}
