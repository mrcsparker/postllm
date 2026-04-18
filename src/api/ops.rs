#![allow(
    clippy::needless_pass_by_value,
    clippy::redundant_pub_crate,
    reason = "pgrx materializes SQL-facing values as owned Rust types and these wrappers are crate-visible by design"
)]

use pgrx::JsonB;

// SQL-facing model-lifecycle entrypoints.
//
// The module keeps lifecycle wrappers narrow and aligned with the SQL API naming
// while routing all operational behavior to internal `model_*_impl` functions.

pub(crate) fn model_install(
    model: pgrx::default!(Option<&str>, "NULL"),
    lane: pgrx::default!(Option<&str>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::model_install_impl(model, lane))
}

pub(crate) fn model_prewarm(
    model: pgrx::default!(Option<&str>, "NULL"),
    lane: pgrx::default!(Option<&str>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::model_prewarm_impl(model, lane))
}

pub(crate) fn model_inspect(
    model: pgrx::default!(Option<&str>, "NULL"),
    lane: pgrx::default!(Option<&str>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::model_inspect_impl(model, lane))
}

pub(crate) fn model_evict(
    model: pgrx::default!(Option<&str>, "NULL"),
    lane: pgrx::default!(Option<&str>, "NULL"),
    scope: pgrx::default!(&str, "'all'"),
) -> JsonB {
    crate::finish_json_result(crate::model_evict_impl(model, lane, scope))
}
