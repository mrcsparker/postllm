#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::backend::Runtime;
use crate::error::{Error, Result};
use crate::operator_policy;
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys;
use pgrx::spi::Spi;
use serde_json::Value;

const WILDCARD_TARGET: &str = "*";
const PRIVILEGED_SETTINGS: [&str; 11] = [
    "base_url",
    "api_key",
    "api_key_secret",
    "timeout_ms",
    "max_retries",
    "retry_backoff_ms",
    "candle_cache_dir",
    "candle_offline",
    "candle_device",
    "candle_max_input_tokens",
    "candle_max_concurrency",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PermissionObjectType {
    Runtime,
    GenerationModel,
    EmbeddingModel,
    Setting,
}

impl PermissionObjectType {
    pub(crate) const RUNTIME: &'static str = "runtime";
    pub(crate) const GENERATION_MODEL: &'static str = "generation_model";
    pub(crate) const EMBEDDING_MODEL: &'static str = "embedding_model";
    pub(crate) const SETTING: &'static str = "setting";
    pub(crate) const ACCEPTED_VALUES: &'static str =
        "'runtime', 'generation_model', 'embedding_model', or 'setting'";

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => Self::RUNTIME,
            Self::GenerationModel => Self::GENERATION_MODEL,
            Self::EmbeddingModel => Self::EMBEDDING_MODEL,
            Self::Setting => Self::SETTING,
        }
    }

    pub(crate) fn parse(argument: &str, value: &str) -> Result<Self> {
        match require_non_blank(argument, value)?
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            Self::RUNTIME => Ok(Self::Runtime),
            Self::GENERATION_MODEL => Ok(Self::GenerationModel),
            Self::EMBEDDING_MODEL => Ok(Self::EmbeddingModel),
            Self::SETTING => Ok(Self::Setting),
            unknown => Err(Error::invalid_argument(
                argument,
                format!("must be one of {}, got '{unknown}'", Self::ACCEPTED_VALUES),
                format!("pass {argument} => {}", Self::ACCEPTED_VALUES),
            )),
        }
    }
}

pub(crate) fn permissions() -> Result<Value> {
    json_query(
        "SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'role_name', role_name,
                    'object_type', object_type,
                    'target', target,
                    'description', description,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at)
                )
                ORDER BY object_type, role_name, target
            ),
            '[]'::jsonb
        )
        FROM postllm.role_permissions",
        &[],
    )
}

pub(crate) fn permission(
    role_name: &str,
    object_type: PermissionObjectType,
    target: &str,
) -> Result<Value> {
    let role_name = validated_role_name(role_name)?;
    let target = normalize_target("target", object_type, target)?;
    let permission = json_query(
        "SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'role_name', role_name,
                    'object_type', object_type,
                    'target', target,
                    'description', description,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at)
                )
                FROM postllm.role_permissions
                WHERE role_name = $1 AND object_type = $2 AND target = $3
            ),
            'null'::jsonb
        )",
        &[
            DatumWithOid::from(role_name.as_str()),
            DatumWithOid::from(object_type.as_str()),
            DatumWithOid::from(target.as_str()),
        ],
    )?;

    if permission.is_null() {
        Err(unknown_permission(&role_name, object_type, target.as_str()))
    } else {
        Ok(permission)
    }
}

pub(crate) fn permission_set(
    role_name: &str,
    object_type: PermissionObjectType,
    target: &str,
    description: Option<&str>,
) -> Result<Value> {
    let role_name = validated_role_name(role_name)?;
    let target = normalize_target("target", object_type, target)?;
    let description = trimmed_or_none_option(description);

    json_query(
        "WITH saved AS (
            INSERT INTO postllm.role_permissions (role_name, object_type, target, description)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (role_name, object_type, target) DO UPDATE
            SET description = EXCLUDED.description,
                updated_at = clock_timestamp()
            RETURNING role_name, object_type, target, description, created_at, updated_at
        )
        SELECT jsonb_build_object(
            'role_name', role_name,
            'object_type', object_type,
            'target', target,
            'description', description,
            'created_at', to_jsonb(created_at),
            'updated_at', to_jsonb(updated_at)
        )
        FROM saved",
        &[
            DatumWithOid::from(role_name.as_str()),
            DatumWithOid::from(object_type.as_str()),
            DatumWithOid::from(target.as_str()),
            DatumWithOid::from(description.as_deref()),
        ],
    )
}

pub(crate) fn permission_delete(
    role_name: &str,
    object_type: PermissionObjectType,
    target: &str,
) -> Result<Value> {
    let role_name = validated_role_name(role_name)?;
    let target = normalize_target("target", object_type, target)?;
    let deleted = json_query(
        "WITH deleted AS (
            DELETE FROM postllm.role_permissions
            WHERE role_name = $1 AND object_type = $2 AND target = $3
            RETURNING role_name, object_type, target, description, created_at, updated_at
        )
        SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'role_name', role_name,
                    'object_type', object_type,
                    'target', target,
                    'description', description,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at),
                    'deleted', true
                )
                FROM deleted
            ),
            'null'::jsonb
        )",
        &[
            DatumWithOid::from(role_name.as_str()),
            DatumWithOid::from(object_type.as_str()),
            DatumWithOid::from(target.as_str()),
        ],
    )?;

    if deleted.is_null() {
        Err(unknown_permission(&role_name, object_type, target.as_str()))
    } else {
        Ok(deleted)
    }
}

pub(crate) fn ensure_runtime_allowed(runtime: Runtime) -> Result<()> {
    ensure_allowed(
        PermissionObjectType::Runtime,
        runtime.as_str(),
        &format!("runtime '{}'", runtime.as_str()),
    )
}

pub(crate) fn ensure_generation_model_allowed(model: &str) -> Result<()> {
    ensure_allowed(
        PermissionObjectType::GenerationModel,
        model,
        &format!("generation model '{model}'"),
    )
}

pub(crate) fn ensure_embedding_model_allowed(model: &str) -> Result<()> {
    ensure_allowed(
        PermissionObjectType::EmbeddingModel,
        model,
        &format!("embedding model '{model}'"),
    )
}

pub(crate) fn ensure_setting_change_allowed(setting_name: &str) -> Result<()> {
    let target = normalize_target("setting", PermissionObjectType::Setting, setting_name)?;
    ensure_allowed(
        PermissionObjectType::Setting,
        &target,
        &format!("privileged setting '{target}'"),
    )
}

#[must_use]
pub(crate) fn caller_is_superuser() -> bool {
    operator_policy::caller_is_superuser()
}

pub(crate) fn caller_role_name() -> String {
    operator_policy::caller_role_name()
}

fn ensure_allowed(
    object_type: PermissionObjectType,
    target: &str,
    object_label: &str,
) -> Result<()> {
    let normalized_target = normalize_target("target", object_type, target)?;
    if !permission_scope_active(object_type, &normalized_target)? {
        return Ok(());
    }

    if caller_has_permission(object_type, &normalized_target)? {
        Ok(())
    } else {
        let role_name = caller_role_name();
        Err(Error::Config(format!(
            "postllm access denied for role '{role_name}': {object_label} is not permitted; fix: grant it with postllm.permission_set(role_name => '{role_name}', object_type => '{}', target => '{}') or switch to an allowed {}",
            object_type.as_str(),
            normalized_target,
            denied_fix_suffix(object_type),
        )))
    }
}

fn permission_scope_active(object_type: PermissionObjectType, target: &str) -> Result<bool> {
    let sql = match object_type {
        PermissionObjectType::Setting => {
            "SELECT EXISTS(
                SELECT 1
                FROM postllm.role_permissions
                WHERE object_type = $1
                  AND target IN ($2, $3)
            )"
        }
        _ => {
            "SELECT EXISTS(
                SELECT 1
                FROM postllm.role_permissions
                WHERE object_type = $1
            )"
        }
    };

    let args = match object_type {
        PermissionObjectType::Setting => vec![
            DatumWithOid::from(object_type.as_str()),
            DatumWithOid::from(target),
            DatumWithOid::from(WILDCARD_TARGET),
        ],
        _ => vec![DatumWithOid::from(object_type.as_str())],
    };

    Spi::get_one_with_args::<bool>(sql, &args)
        .map(|value| value.unwrap_or(false))
        .map_err(Into::into)
}

fn caller_has_permission(object_type: PermissionObjectType, target: &str) -> Result<bool> {
    Spi::get_one_with_args::<bool>(
        "SELECT COALESCE(
            bool_or(p.target = $3 OR p.target = $4),
            false
        )
        FROM postllm.role_permissions p
        JOIN pg_roles r
          ON r.rolname = p.role_name
        WHERE p.object_type = $2
          AND pg_has_role($1, r.oid, 'member')",
        &[
            DatumWithOid::from(caller_role_oid()),
            DatumWithOid::from(object_type.as_str()),
            DatumWithOid::from(target),
            DatumWithOid::from(WILDCARD_TARGET),
        ],
    )
    .map(|value| value.unwrap_or(false))
    .map_err(Into::into)
}

fn normalize_target(
    argument: &str,
    object_type: PermissionObjectType,
    target: &str,
) -> Result<String> {
    let target = require_non_blank(argument, target)?;
    if target == WILDCARD_TARGET {
        return Ok(WILDCARD_TARGET.to_owned());
    }

    match object_type {
        PermissionObjectType::Runtime => Ok(Runtime::parse(target)?.as_str().to_owned()),
        PermissionObjectType::Setting => {
            let normalized = target.trim().to_ascii_lowercase();
            if PRIVILEGED_SETTINGS.contains(&normalized.as_str()) {
                Ok(normalized)
            } else {
                Err(Error::invalid_argument(
                    argument,
                    format!(
                        "must be '*' or one of {}, got '{normalized}'",
                        privileged_settings_list()
                    ),
                    format!(
                        "pass {argument} => '*' or one of {}",
                        privileged_settings_list()
                    ),
                ))
            }
        }
        PermissionObjectType::GenerationModel | PermissionObjectType::EmbeddingModel => {
            Ok(target.to_owned())
        }
    }
}

fn validated_role_name(role_name: &str) -> Result<String> {
    let role_name = require_non_blank("role_name", role_name)?.to_owned();
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)",
        &[DatumWithOid::from(role_name.as_str())],
    )?
    .unwrap_or(false);

    if exists {
        Ok(role_name)
    } else {
        Err(Error::invalid_argument(
            "role_name",
            format!("refers to unknown PostgreSQL role '{role_name}'"),
            "create the role first or choose one from pg_roles",
        ))
    }
}

fn unknown_permission(role_name: &str, object_type: PermissionObjectType, target: &str) -> Error {
    Error::invalid_argument(
        "permission",
        format!(
            "refers to unknown permission for role '{role_name}', object_type '{}', and target '{target}'",
            object_type.as_str()
        ),
        format!(
            "create it with postllm.permission_set(role_name => '{role_name}', object_type => '{}', target => '{target}') or choose one from postllm.permissions()",
            object_type.as_str()
        ),
    )
}

fn json_query(query: &str, args: &[DatumWithOid]) -> Result<Value> {
    Spi::get_one_with_args::<JsonB>(query, args)
        .map(|value| value.map_or(Value::Null, |value| value.0))
        .map_err(Into::into)
}

fn require_non_blank<'value>(name: &str, value: &'value str) -> Result<&'value str> {
    trimmed_or_none(value).ok_or_else(|| {
        Error::invalid_argument(
            name,
            "must not be empty or whitespace-only",
            format!("pass a non-empty value for '{name}'"),
        )
    })
}

fn trimmed_or_none(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn trimmed_or_none_option(value: Option<&str>) -> Option<String> {
    value.and_then(trimmed_or_none).map(str::to_owned)
}

fn privileged_settings_list() -> String {
    PRIVILEGED_SETTINGS
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn denied_fix_suffix(object_type: PermissionObjectType) -> &'static str {
    match object_type {
        PermissionObjectType::Runtime => "runtime",
        PermissionObjectType::GenerationModel => "generation model",
        PermissionObjectType::EmbeddingModel => "embedding model",
        PermissionObjectType::Setting => "setting",
    }
}

fn caller_role_oid() -> pg_sys::Oid {
    operator_policy::caller_role_oid()
}
