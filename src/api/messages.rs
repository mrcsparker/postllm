use pgrx::JsonB;

// SQL-facing message and tool helper constructors.
//
// Keeping these helpers together lets `lib.rs` remain a thin router for the SQL
// API without repeating argument-shaping or JSON wrapping logic.

pub(crate) fn message(role: &str, content: &str) -> JsonB {
    crate::finish_json_result(crate::build_message(role, content))
}

pub(crate) fn system(content: &str) -> JsonB {
    crate::finish_json_result(crate::build_message("system", content))
}

pub(crate) fn user(content: &str) -> JsonB {
    crate::finish_json_result(crate::build_message("user", content))
}

pub(crate) fn assistant(content: &str) -> JsonB {
    crate::finish_json_result(crate::build_message("assistant", content))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL jsonb arguments as owned Rust values"
)]
pub(crate) fn render_template(
    template: &str,
    variables: pgrx::default!(Option<JsonB>, "NULL"),
) -> String {
    crate::finish_text_result(crate::render_template_impl(
        template,
        variables.as_ref().map(|variables| &variables.0),
    ))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL jsonb arguments as owned Rust values"
)]
pub(crate) fn message_template(
    role: &str,
    template: &str,
    variables: pgrx::default!(Option<JsonB>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::build_message_template(
        role,
        template,
        variables.as_ref().map(|variables| &variables.0),
    ))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL jsonb arguments as owned Rust values"
)]
pub(crate) fn system_template(
    template: &str,
    variables: pgrx::default!(Option<JsonB>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::build_message_template(
        "system",
        template,
        variables.as_ref().map(|variables| &variables.0),
    ))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL jsonb arguments as owned Rust values"
)]
pub(crate) fn user_template(
    template: &str,
    variables: pgrx::default!(Option<JsonB>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::build_message_template(
        "user",
        template,
        variables.as_ref().map(|variables| &variables.0),
    ))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL jsonb arguments as owned Rust values"
)]
pub(crate) fn assistant_template(
    template: &str,
    variables: pgrx::default!(Option<JsonB>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::build_message_template(
        "assistant",
        template,
        variables.as_ref().map(|variables| &variables.0),
    ))
}

pub(crate) fn text_part(text: &str) -> JsonB {
    crate::finish_json_result(crate::build_text_part(text))
}

pub(crate) fn image_url_part(url: &str, detail: pgrx::default!(Option<&str>, "NULL")) -> JsonB {
    crate::finish_json_result(crate::build_image_url_part(url, detail))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL array arguments as owned Rust values"
)]
pub(crate) fn message_parts(role: &str, parts: Vec<JsonB>) -> JsonB {
    crate::finish_json_result(crate::build_parts_message(role, &parts))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL array arguments as owned Rust values"
)]
pub(crate) fn system_parts(parts: Vec<JsonB>) -> JsonB {
    crate::finish_json_result(crate::build_parts_message("system", &parts))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL array arguments as owned Rust values"
)]
pub(crate) fn user_parts(parts: Vec<JsonB>) -> JsonB {
    crate::finish_json_result(crate::build_parts_message("user", &parts))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL array arguments as owned Rust values"
)]
pub(crate) fn assistant_parts(parts: Vec<JsonB>) -> JsonB {
    crate::finish_json_result(crate::build_parts_message("assistant", &parts))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL jsonb arguments as owned Rust values"
)]
pub(crate) fn tool_call(id: &str, name: &str, arguments: JsonB) -> JsonB {
    crate::finish_json_result(crate::build_tool_call(id, name, &arguments.0))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL array arguments as owned Rust values"
)]
pub(crate) fn assistant_tool_calls(
    tool_calls: Vec<JsonB>,
    content: pgrx::default!(Option<&str>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::build_assistant_tool_calls(&tool_calls, content))
}

pub(crate) fn tool_result(tool_call_id: &str, content: &str) -> JsonB {
    crate::finish_json_result(crate::build_tool_result(tool_call_id, content))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL jsonb arguments as owned Rust values"
)]
pub(crate) fn function_tool(
    name: &str,
    parameters: JsonB,
    description: pgrx::default!(Option<&str>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::build_function_tool(name, &parameters.0, description))
}

pub(crate) fn tool_choice_auto() -> JsonB {
    JsonB(crate::build_tool_choice_mode("auto"))
}

pub(crate) fn tool_choice_none() -> JsonB {
    JsonB(crate::build_tool_choice_mode("none"))
}

pub(crate) fn tool_choice_required() -> JsonB {
    JsonB(crate::build_tool_choice_mode("required"))
}

pub(crate) fn tool_choice_function(name: &str) -> JsonB {
    crate::finish_json_result(crate::build_tool_choice_function(name))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pgrx materializes SQL jsonb arguments as owned Rust values"
)]
pub(crate) fn json_schema(name: &str, schema: JsonB, strict: pgrx::default!(bool, true)) -> JsonB {
    crate::finish_json_result(crate::build_json_schema_response_format(
        name, &schema.0, strict,
    ))
}
