#![allow(
    clippy::needless_pass_by_value,
    clippy::redundant_pub_crate,
    reason = "pgrx materializes SQL-facing values as owned Rust types and these wrappers are crate-visible by design"
)]

use pgrx::JsonB;

// SQL-facing async-job entrypoints.
//
// These wrappers keep the exported SQL signatures close to the durable job
// model while leaving lifecycle, validation, and worker orchestration in the
// internal jobs module.

pub(crate) fn submit(kind: &str, request: JsonB) -> JsonB {
    crate::finish_json_result(crate::jobs::submit(kind, &request.0))
}

pub(crate) fn poll(job_id: i64) -> JsonB {
    crate::finish_json_result(crate::jobs::poll(job_id))
}

pub(crate) fn result(job_id: i64) -> JsonB {
    crate::finish_json_result(crate::jobs::result(job_id))
}

pub(crate) fn cancel(job_id: i64) -> JsonB {
    crate::finish_json_result(crate::jobs::cancel(job_id))
}
