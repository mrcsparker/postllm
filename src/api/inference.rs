#![allow(
    clippy::needless_pass_by_value,
    clippy::redundant_pub_crate,
    reason = "pgrx materializes SQL-facing values as owned Rust types and these wrappers are crate-visible by design"
)]

use pgrx::JsonB;
use pgrx::iter::TableIterator;

// SQL-facing inference entrypoints.
//
// This module keeps chat/complete wrappers focused on argument normalization and
// result formatting before delegating to implementation code in the internal
// inference path.

pub(crate) fn chat(
    messages: Vec<JsonB>,
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::chat_impl(&messages, model, temperature, max_tokens))
}

pub(crate) fn chat_text(
    messages: Vec<JsonB>,
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> String {
    crate::finish_text_result(crate::chat_text_impl(
        &messages,
        model,
        temperature,
        max_tokens,
    ))
}

pub(crate) fn chat_stream(
    messages: Vec<JsonB>,
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> TableIterator<
    'static,
    (
        pgrx::name!(index, i32),
        pgrx::name!(delta, Option<String>),
        pgrx::name!(event, JsonB),
    ),
> {
    crate::finish_stream_rows_result(crate::chat_stream_impl(
        &messages,
        model,
        temperature,
        max_tokens,
    ))
}

pub(crate) fn chat_structured(
    messages: Vec<JsonB>,
    response_format: JsonB,
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::chat_structured_impl(
        &messages,
        &response_format.0,
        model,
        temperature,
        max_tokens,
    ))
}

pub(crate) fn chat_tools(
    messages: Vec<JsonB>,
    tools: Vec<JsonB>,
    tool_choice: pgrx::default!(Option<JsonB>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::chat_tools_impl(
        &messages,
        &tools,
        tool_choice.as_ref().map(|tool_choice| &tool_choice.0),
        model,
        temperature,
        max_tokens,
    ))
}

pub(crate) fn usage(response: JsonB) -> JsonB {
    JsonB(crate::usage_impl(&response.0))
}

pub(crate) fn choice(response: JsonB, index: i32) -> JsonB {
    crate::finish_json_result(crate::choice_impl(&response.0, index))
}

pub(crate) fn finish_reason(response: JsonB) -> Option<String> {
    crate::backend::finish_reason(&response.0)
}

pub(crate) fn extract_text(response: JsonB) -> String {
    crate::finish_text_result(crate::client::extract_text(&response.0))
}

pub(crate) fn complete(
    prompt: &str,
    system_prompt: pgrx::default!(Option<&str>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> String {
    crate::finish_text_result(crate::complete_impl(
        prompt,
        system_prompt,
        model,
        temperature,
        max_tokens,
    ))
}

pub(crate) fn complete_structured(
    prompt: &str,
    response_format: JsonB,
    system_prompt: pgrx::default!(Option<&str>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::complete_structured_impl(
        prompt,
        &response_format.0,
        system_prompt,
        model,
        temperature,
        max_tokens,
    ))
}

pub(crate) fn complete_stream(
    prompt: &str,
    system_prompt: pgrx::default!(Option<&str>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> TableIterator<
    'static,
    (
        pgrx::name!(index, i32),
        pgrx::name!(delta, Option<String>),
        pgrx::name!(event, JsonB),
    ),
> {
    crate::finish_stream_rows_result(crate::complete_stream_impl(
        prompt,
        system_prompt,
        model,
        temperature,
        max_tokens,
    ))
}

pub(crate) fn complete_tools(
    prompt: &str,
    tools: Vec<JsonB>,
    system_prompt: pgrx::default!(Option<&str>, "NULL"),
    tool_choice: pgrx::default!(Option<JsonB>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::complete_tools_impl(
        prompt,
        &tools,
        system_prompt,
        tool_choice.as_ref().map(|tool_choice| &tool_choice.0),
        model,
        temperature,
        max_tokens,
    ))
}

pub(crate) fn complete_many(
    prompts: Vec<String>,
    system_prompt: pgrx::default!(Option<&str>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> Vec<String> {
    crate::finish_text_array_result(crate::complete_many_impl(
        &prompts,
        system_prompt,
        model,
        temperature,
        max_tokens,
    ))
}

pub(crate) fn complete_many_rows(
    prompts: Vec<String>,
    system_prompt: pgrx::default!(Option<&str>, "NULL"),
    model: pgrx::default!(Option<&str>, "NULL"),
    temperature: f64,
    max_tokens: pgrx::default!(Option<i32>, "NULL"),
) -> TableIterator<
    'static,
    (
        pgrx::name!(index, i32),
        pgrx::name!(prompt, String),
        pgrx::name!(completion, String),
    ),
> {
    crate::finish_completion_rows_result(crate::complete_many_rows_impl(
        &prompts,
        system_prompt,
        model,
        temperature,
        max_tokens,
    ))
}
