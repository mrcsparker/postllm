#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::error::{Error, Result};
use crate::operator_policy;
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::spi::Spi;
use serde_json::{Value, json};

pub(crate) fn prompts() -> Result<Value> {
    let created_by = operator_policy::caller_role_name();

    json_query(
        r"
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'id', registry.id,
                    'created_at', to_jsonb(registry.created_at),
                    'updated_at', to_jsonb(registry.updated_at),
                    'created_by', registry.created_by,
                    'name', registry.name,
                    'title', registry.title,
                    'active_version', registry.active_version,
                    'current', jsonb_build_object(
                        'version', version.version,
                        'created_at', to_jsonb(version.created_at),
                        'role', version.role,
                        'template', version.template,
                        'description', version.description,
                        'metadata', version.metadata
                    )
                )
                ORDER BY registry.updated_at DESC, registry.id DESC
            ),
            '[]'::jsonb
        )
        FROM postllm.prompt_registries AS registry
        JOIN postllm.prompt_versions AS version
          ON version.prompt_id = registry.id
         AND version.version = registry.active_version
        WHERE registry.created_by = $1
        ",
        &[DatumWithOid::from(created_by.as_str())],
    )
}

pub(crate) fn prompt(name: &str, version: Option<i32>) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let name = require_non_blank("name", name)?;
    let version = version.map(validate_version).transpose()?;

    json_query(
        r"
        WITH registry AS (
            SELECT *
            FROM postllm.prompt_registries
            WHERE created_by = $1
              AND name = $2
        ),
        selected AS (
            SELECT version.*
            FROM postllm.prompt_versions AS version
            JOIN registry
              ON registry.id = version.prompt_id
            WHERE version.version = COALESCE($3, registry.active_version)
        )
        SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'id', registry.id,
                    'created_at', to_jsonb(registry.created_at),
                    'updated_at', to_jsonb(registry.updated_at),
                    'created_by', registry.created_by,
                    'name', registry.name,
                    'title', registry.title,
                    'active_version', registry.active_version,
                    'current', (
                        SELECT jsonb_build_object(
                            'version', selected.version,
                            'created_at', to_jsonb(selected.created_at),
                            'role', selected.role,
                            'template', selected.template,
                            'description', selected.description,
                            'metadata', selected.metadata
                        )
                        FROM selected
                    ),
                    'versions', COALESCE(
                        (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'version', version.version,
                                    'created_at', to_jsonb(version.created_at),
                                    'role', version.role,
                                    'template', version.template,
                                    'description', version.description,
                                    'metadata', version.metadata
                                )
                                ORDER BY version.version DESC
                            )
                            FROM postllm.prompt_versions AS version
                            WHERE version.prompt_id = registry.id
                        ),
                        '[]'::jsonb
                    )
                )
                FROM registry
                WHERE EXISTS (SELECT 1 FROM selected)
            ),
            'null'::jsonb
        )
        ",
        &[
            DatumWithOid::from(created_by.as_str()),
            DatumWithOid::from(name),
            DatumWithOid::from(version),
        ],
    )
    .and_then(|prompt| {
        if prompt.is_null() {
            Err(unknown_prompt(name))
        } else {
            Ok(prompt)
        }
    })
}

pub(crate) fn set(
    name: &str,
    template: &str,
    role: Option<&str>,
    title: Option<&str>,
    description: Option<&str>,
    metadata: Option<&Value>,
) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let name = require_non_blank("name", name)?;
    let template = require_non_blank("template", template)?;
    let role = role
        .map(|role| require_non_blank("role", role))
        .transpose()?;
    let title = trimmed_or_none(title);
    let description = trimmed_or_none(description);
    let metadata = validate_metadata(metadata, "metadata")?;

    let prompt_id = Spi::get_one_with_args::<i64>(
        r"
        INSERT INTO postllm.prompt_registries (
            created_by,
            name,
            title,
            active_version
        )
        VALUES ($1, $2, $3, 1)
        ON CONFLICT (created_by, name) DO UPDATE
        SET updated_at = clock_timestamp(),
            title = COALESCE(EXCLUDED.title, postllm.prompt_registries.title)
        RETURNING id
        ",
        &[
            DatumWithOid::from(created_by.as_str()),
            DatumWithOid::from(name),
            DatumWithOid::from(title),
        ],
    )?
    .ok_or_else(|| Error::Internal("prompt registry upsert returned no id".to_owned()))?;

    let version = Spi::get_one_with_args::<i32>(
        r"
        SELECT COALESCE(MAX(version), 0)::integer + 1
        FROM postllm.prompt_versions
        WHERE prompt_id = $1
        ",
        &[DatumWithOid::from(prompt_id)],
    )?
    .ok_or_else(|| Error::Internal("prompt version query returned no row".to_owned()))?;

    Spi::run_with_args(
        r"
        INSERT INTO postllm.prompt_versions (
            prompt_id,
            version,
            role,
            template,
            description,
            metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ",
        &[
            DatumWithOid::from(prompt_id),
            DatumWithOid::from(version),
            DatumWithOid::from(role),
            DatumWithOid::from(template),
            DatumWithOid::from(description),
            DatumWithOid::from(JsonB(metadata)),
        ],
    )?;

    Spi::run_with_args(
        r"
        UPDATE postllm.prompt_registries
        SET updated_at = clock_timestamp(),
            active_version = $2
        WHERE id = $1
        ",
        &[DatumWithOid::from(prompt_id), DatumWithOid::from(version)],
    )?;

    prompt(name, Some(version))
}

pub(crate) fn render(
    name: &str,
    variables: Option<&Value>,
    version: Option<i32>,
) -> Result<String> {
    let prompt = prompt(name, version)?;
    let template = current_field_as_str(&prompt, "template")?;

    crate::render_template_impl(template, variables)
}

pub(crate) fn message(
    name: &str,
    variables: Option<&Value>,
    version: Option<i32>,
) -> Result<Value> {
    let prompt = prompt(name, version)?;
    let role = current_field_as_optional_str(&prompt, "role");
    let Some(role) = role else {
        return Err(Error::invalid_argument(
            "name",
            format!("prompt '{name}' does not declare a message role"),
            "set role => 'system', 'user', or 'assistant' when storing the prompt, or call postllm.prompt_render(...) instead",
        ));
    };
    let template = current_field_as_str(&prompt, "template")?;

    crate::build_message_template(role, template, variables)
}

pub(crate) fn delete(name: &str) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let name = require_non_blank("name", name)?;
    let deleted = prompt(name, None)?;

    let removed = Spi::get_one_with_args::<bool>(
        r"
        DELETE FROM postllm.prompt_registries
        WHERE created_by = $1
          AND name = $2
        RETURNING true
        ",
        &[
            DatumWithOid::from(created_by.as_str()),
            DatumWithOid::from(name),
        ],
    )?
    .unwrap_or(false);

    if !removed {
        return Err(unknown_prompt(name));
    }

    let mut deleted = deleted;
    if let Some(object) = deleted.as_object_mut() {
        object.insert("deleted".to_owned(), json!(true));
    }
    Ok(deleted)
}

fn current_field_as_str<'a>(prompt: &'a Value, field: &str) -> Result<&'a str> {
    prompt
        .get("current")
        .and_then(|current| current.get(field))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::Internal(format!(
                "prompt registry payload did not include current.{field}"
            ))
        })
}

fn current_field_as_optional_str<'a>(prompt: &'a Value, field: &str) -> Option<&'a str> {
    prompt
        .get("current")
        .and_then(|current| current.get(field))
        .and_then(Value::as_str)
}

fn validate_metadata(metadata: Option<&Value>, argument: &str) -> Result<Value> {
    match metadata {
        None | Some(Value::Null) => Ok(json!({})),
        Some(Value::Object(object)) => Ok(Value::Object(object.clone())),
        Some(_) => Err(Error::invalid_argument(
            argument,
            "must be a JSON object or null",
            r#"pass jsonb like '{"team":"ops"}'::jsonb or omit it"#,
        )),
    }
}

fn validate_version(version: i32) -> Result<i32> {
    if version <= 0 {
        Err(Error::invalid_argument(
            "version",
            format!("must be greater than zero, got {version}"),
            "pass version => 1 or another positive integer",
        ))
    } else {
        Ok(version)
    }
}

fn require_non_blank<'value>(argument: &str, value: &'value str) -> Result<&'value str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(Error::invalid_argument(
            argument,
            "must not be empty or whitespace-only",
            format!("pass a non-empty value for '{argument}'"),
        ))
    } else {
        Ok(trimmed)
    }
}

fn trimmed_or_none(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn unknown_prompt(name: &str) -> Error {
    Error::invalid_argument(
        "name",
        format!("refers to unknown prompt registry '{name}' for the current role"),
        "create it with postllm.prompt_set(...) or choose one from postllm.prompts()",
    )
}

fn json_query(query: &str, args: &[DatumWithOid<'_>]) -> Result<Value> {
    Spi::get_one_with_args::<JsonB>(query, args)
        .map(|value| value.map_or(Value::Null, |value| value.0))
        .map_err(Into::into)
}
