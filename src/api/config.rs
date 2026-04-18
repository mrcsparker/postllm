use pgrx::JsonB;

// SQL-facing configuration and governance entrypoints.
//
// These wrappers keep `lib.rs` signatures close to the SQL contract and route the
// actual behavior to internal `*_impl` functions.

pub(crate) fn settings() -> JsonB {
    JsonB(crate::guc::snapshot())
}

pub(crate) fn capabilities() -> JsonB {
    JsonB(crate::guc::capabilities_snapshot())
}

pub(crate) fn runtime_discover() -> JsonB {
    JsonB(crate::runtime_discover_impl())
}

pub(crate) fn runtime_ready() -> bool {
    crate::runtime_ready_impl()
}

#[expect(
    clippy::too_many_arguments,
    reason = "the SQL surface intentionally exposes a flat configure(...) API instead of forcing callers through JSON"
)]
pub(crate) fn configure(
    base_url: pgrx::default!(Option<&str>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    embedding_model: pgrx::default!(Option<&str>, "NULL"),
    api_key: pgrx::default!(Option<&str>, "NULL"),
    api_key_secret: pgrx::default!(Option<&str>, "NULL"),
    timeout_ms: pgrx::default!(Option<i32>, "NULL"),
    max_retries: pgrx::default!(Option<i32>, "NULL"),
    retry_backoff_ms: pgrx::default!(Option<i32>, "NULL"),
    request_token_budget: pgrx::default!(Option<i32>, "NULL"),
    request_runtime_budget_ms: pgrx::default!(Option<i32>, "NULL"),
    request_spend_budget_microusd: pgrx::default!(Option<i32>, "NULL"),
    output_token_price_microusd_per_1k: pgrx::default!(Option<i32>, "NULL"),
    runtime: pgrx::default!(Option<&str>, "NULL"),
    candle_cache_dir: pgrx::default!(Option<&str>, "NULL"),
    candle_offline: pgrx::default!(Option<bool>, "NULL"),
    candle_device: pgrx::default!(Option<&str>, "NULL"),
    candle_max_input_tokens: pgrx::default!(Option<i32>, "NULL"),
    candle_max_concurrency: pgrx::default!(Option<i32>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::guc::configure_session(
        base_url,
        model,
        embedding_model,
        api_key,
        api_key_secret,
        timeout_ms,
        max_retries,
        retry_backoff_ms,
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
    ))
}

pub(crate) fn profiles() -> JsonB {
    crate::finish_json_result(crate::profiles_impl())
}

pub(crate) fn profile(name: &str) -> JsonB {
    crate::finish_json_result(crate::profile_impl(name))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the SQL surface intentionally exposes a flat profile_set(...) API aligned with configure(...)"
)]
pub(crate) fn profile_set(
    name: &str,
    description: pgrx::default!(Option<&str>, "NULL"),
    base_url: pgrx::default!(Option<&str>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    embedding_model: pgrx::default!(Option<&str>, "NULL"),
    api_key_secret: pgrx::default!(Option<&str>, "NULL"),
    timeout_ms: pgrx::default!(Option<i32>, "NULL"),
    max_retries: pgrx::default!(Option<i32>, "NULL"),
    retry_backoff_ms: pgrx::default!(Option<i32>, "NULL"),
    request_token_budget: pgrx::default!(Option<i32>, "NULL"),
    request_runtime_budget_ms: pgrx::default!(Option<i32>, "NULL"),
    request_spend_budget_microusd: pgrx::default!(Option<i32>, "NULL"),
    output_token_price_microusd_per_1k: pgrx::default!(Option<i32>, "NULL"),
    runtime: pgrx::default!(Option<&str>, "NULL"),
    candle_cache_dir: pgrx::default!(Option<&str>, "NULL"),
    candle_offline: pgrx::default!(Option<bool>, "NULL"),
    candle_device: pgrx::default!(Option<&str>, "NULL"),
    candle_max_input_tokens: pgrx::default!(Option<i32>, "NULL"),
    candle_max_concurrency: pgrx::default!(Option<i32>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::profile_set_impl(
        name,
        description,
        base_url,
        model,
        embedding_model,
        api_key_secret,
        timeout_ms,
        max_retries,
        retry_backoff_ms,
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
    ))
}

pub(crate) fn profile_apply(name: &str) -> JsonB {
    crate::finish_json_result(crate::profile_apply_impl(name))
}

pub(crate) fn profile_delete(name: &str) -> JsonB {
    crate::finish_json_result(crate::profile_delete_impl(name))
}

pub(crate) fn secrets() -> JsonB {
    crate::finish_json_result(crate::secrets_impl())
}

pub(crate) fn secret(name: &str) -> JsonB {
    crate::finish_json_result(crate::secret_impl(name))
}

pub(crate) fn secret_set(
    name: &str,
    value: &str,
    description: pgrx::default!(Option<&str>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::secret_set_impl(name, value, description))
}

pub(crate) fn secret_delete(name: &str) -> JsonB {
    crate::finish_json_result(crate::secret_delete_impl(name))
}

pub(crate) fn permissions() -> JsonB {
    crate::finish_json_result(crate::permissions_impl())
}

pub(crate) fn permission(role_name: &str, object_type: &str, target: &str) -> JsonB {
    crate::finish_json_result(crate::permission_impl(role_name, object_type, target))
}

pub(crate) fn permission_set(
    role_name: &str,
    object_type: &str,
    target: &str,
    description: pgrx::default!(Option<&str>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::permission_set_impl(
        role_name,
        object_type,
        target,
        description,
    ))
}

pub(crate) fn permission_delete(role_name: &str, object_type: &str, target: &str) -> JsonB {
    crate::finish_json_result(crate::permission_delete_impl(
        role_name,
        object_type,
        target,
    ))
}

pub(crate) fn model_aliases() -> JsonB {
    crate::finish_json_result(crate::model_aliases_impl())
}

pub(crate) fn model_alias(alias: &str, lane: &str) -> JsonB {
    crate::finish_json_result(crate::model_alias_impl(alias, lane))
}

pub(crate) fn model_alias_set(
    alias: &str,
    lane: &str,
    model: &str,
    description: pgrx::default!(Option<&str>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::model_alias_set_impl(alias, lane, model, description))
}

pub(crate) fn model_alias_delete(alias: &str, lane: &str) -> JsonB {
    crate::finish_json_result(crate::model_alias_delete_impl(alias, lane))
}
