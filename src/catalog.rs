#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::enum_parser;
use crate::error::{Error, Result};
use crate::guc::SessionOverrides;
use crate::secrets::StoredSecret;
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::spi::Spi;
use serde_json::{Value, json};

/// Lane-aware model alias selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ModelAliasLane {
    /// Alias resolves to a chat/complete model.
    Generation,
    /// Alias resolves to an embedding/rerank model.
    Embedding,
}

impl ModelAliasLane {
    pub(crate) const GENERATION: &'static str = "generation";
    pub(crate) const EMBEDDING: &'static str = "embedding";
    const VARIANTS: [(&'static str, Self); 2] = [
        (Self::GENERATION, Self::Generation),
        (Self::EMBEDDING, Self::Embedding),
    ];

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Generation => Self::GENERATION,
            Self::Embedding => Self::EMBEDDING,
        }
    }

    pub(crate) fn parse(argument: &str, value: &str) -> Result<Self> {
        enum_parser::parse_case_insensitive_required(argument, value, &Self::VARIANTS)
    }
}

/// Resolves a model alias for the requested lane, if one exists.
pub(crate) fn resolve_model_alias(alias: &str, lane: ModelAliasLane) -> Result<Option<String>> {
    let alias = alias.trim();
    if alias.is_empty() {
        return Ok(None);
    }

    Spi::get_one_with_args::<String>(
        "SELECT COALESCE(
            (SELECT model FROM postllm.model_aliases WHERE alias = $1 AND lane = $2),
            ''
        )",
        &[DatumWithOid::from(alias), DatumWithOid::from(lane.as_str())],
    )
    .map(|value| value.and_then(|value| trimmed_or_none(&value).map(str::to_owned)))
    .map_err(Into::into)
}

/// Returns all named config profiles as a JSON array.
pub(crate) fn profiles() -> Result<Value> {
    json_query(
        "SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'name', name,
                    'description', description,
                    'config', config,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at)
                )
                ORDER BY name
            ),
            '[]'::jsonb
        )
        FROM postllm.config_profiles",
        &[],
    )
}

/// Returns one named config profile.
pub(crate) fn profile(name: &str) -> Result<Value> {
    let name = require_non_blank("name", name)?;
    fetch_profile_record(name)
}

/// Stores or updates a named config profile.
pub(crate) fn profile_set(
    name: &str,
    description: Option<&str>,
    overrides: &SessionOverrides,
) -> Result<Value> {
    let name = require_non_blank("name", name)?;
    if overrides.profile_is_empty() {
        return Err(Error::invalid_argument(
            "profile",
            "must include at least one setting override",
            "pass one or more settings such as runtime => 'candle', model => 'Qwen/Qwen2.5-0.5B-Instruct', or base_url => 'http://127.0.0.1:11434/v1/chat/completions'",
        ));
    }

    let description = trimmed_or_none_option(description);
    let config = JsonB(overrides.to_profile_json());

    json_query(
        "WITH saved AS (
            INSERT INTO postllm.config_profiles (name, description, config)
            VALUES ($1, $2, $3)
            ON CONFLICT (name) DO UPDATE
            SET description = EXCLUDED.description,
                config = EXCLUDED.config,
                updated_at = clock_timestamp()
            RETURNING name, description, config, created_at, updated_at
        )
        SELECT jsonb_build_object(
            'name', name,
            'description', description,
            'config', config,
            'created_at', to_jsonb(created_at),
            'updated_at', to_jsonb(updated_at)
        )
        FROM saved",
        &[
            DatumWithOid::from(name),
            DatumWithOid::from(description.as_deref()),
            DatumWithOid::from(config),
        ],
    )
}

/// Applies a named config profile to the current session.
pub(crate) fn profile_apply(name: &str) -> Result<Value> {
    let profile = profile(name)?;
    let config = profile
        .get("config")
        .ok_or_else(|| Error::Config("stored profile is missing its config payload".to_owned()))?;
    let overrides = SessionOverrides::from_profile_json(config)?;
    let mut snapshot = crate::guc::apply_profile_overrides(&overrides)?;
    if let Some(object) = snapshot.as_object_mut() {
        object.insert("profile".to_owned(), json!(name.trim()));
    }
    Ok(snapshot)
}

/// Deletes a named config profile.
pub(crate) fn profile_delete(name: &str) -> Result<Value> {
    let name = require_non_blank("name", name)?;

    let deleted = json_query(
        "WITH deleted AS (
            DELETE FROM postllm.config_profiles
            WHERE name = $1
            RETURNING name, description, config, created_at, updated_at
        )
        SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'name', name,
                    'description', description,
                    'config', config,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at),
                    'deleted', true
                )
                FROM deleted
            ),
            'null'::jsonb
        )",
        &[DatumWithOid::from(name)],
    )?;

    if deleted.is_null() {
        Err(unknown_profile(name))
    } else {
        Ok(deleted)
    }
}

/// Returns all stored provider secret metadata as a JSON array.
pub(crate) fn secrets() -> Result<Value> {
    json_query(
        "SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'name', name,
                    'description', description,
                    'algorithm', algorithm,
                    'key_fingerprint', key_fingerprint,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at)
                )
                ORDER BY name
            ),
            '[]'::jsonb
        )
        FROM postllm.provider_secrets",
        &[],
    )
}

/// Returns one stored provider secret metadata record.
pub(crate) fn secret(name: &str) -> Result<Value> {
    let name = require_non_blank("name", name)?;
    fetch_secret_record(name)
}

/// Stores or updates an encrypted provider secret.
pub(crate) fn secret_set(name: &str, value: &str, description: Option<&str>) -> Result<Value> {
    let name = require_non_blank("name", name)?;
    let value = require_non_blank("value", value)?;
    let description = trimmed_or_none_option(description);
    let encrypted = crate::secrets::encrypt_secret(name, value)?;

    json_query(
        "WITH saved AS (
            INSERT INTO postllm.provider_secrets (
                name,
                description,
                algorithm,
                nonce,
                ciphertext,
                key_fingerprint
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (name) DO UPDATE
            SET description = EXCLUDED.description,
                algorithm = EXCLUDED.algorithm,
                nonce = EXCLUDED.nonce,
                ciphertext = EXCLUDED.ciphertext,
                key_fingerprint = EXCLUDED.key_fingerprint,
                updated_at = clock_timestamp()
            RETURNING name, description, algorithm, key_fingerprint, created_at, updated_at
        )
        SELECT jsonb_build_object(
            'name', name,
            'description', description,
            'algorithm', algorithm,
            'key_fingerprint', key_fingerprint,
            'created_at', to_jsonb(created_at),
            'updated_at', to_jsonb(updated_at)
        )
        FROM saved",
        &[
            DatumWithOid::from(name),
            DatumWithOid::from(description.as_deref()),
            DatumWithOid::from(encrypted.algorithm.as_str()),
            DatumWithOid::from(encrypted.nonce.as_str()),
            DatumWithOid::from(encrypted.ciphertext.as_str()),
            DatumWithOid::from(encrypted.key_fingerprint.as_str()),
        ],
    )
}

/// Deletes a stored provider secret.
pub(crate) fn secret_delete(name: &str) -> Result<Value> {
    let name = require_non_blank("name", name)?;

    let deleted = json_query(
        "WITH deleted AS (
            DELETE FROM postllm.provider_secrets
            WHERE name = $1
            RETURNING name, description, algorithm, key_fingerprint, created_at, updated_at
        )
        SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'name', name,
                    'description', description,
                    'algorithm', algorithm,
                    'key_fingerprint', key_fingerprint,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at),
                    'deleted', true
                )
                FROM deleted
            ),
            'null'::jsonb
        )",
        &[DatumWithOid::from(name)],
    )?;

    if deleted.is_null() {
        Err(unknown_secret("name", name))
    } else {
        Ok(deleted)
    }
}

/// Resolves and decrypts a stored provider secret by name.
pub(crate) fn secret_value(name: &str) -> Result<String> {
    let name = require_non_blank("api_key_secret", name)?;
    let stored = fetch_secret_payload(name)?;
    crate::secrets::decrypt_secret(name, &stored)
}

/// Returns all model aliases as a JSON array.
pub(crate) fn model_aliases() -> Result<Value> {
    json_query(
        "SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'alias', alias,
                    'lane', lane,
                    'model', model,
                    'description', description,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at)
                )
                ORDER BY lane, alias
            ),
            '[]'::jsonb
        )
        FROM postllm.model_aliases",
        &[],
    )
}

/// Returns one lane-aware model alias.
pub(crate) fn model_alias(alias: &str, lane: ModelAliasLane) -> Result<Value> {
    let alias = require_non_blank("alias", alias)?;
    fetch_model_alias_record(alias, lane)
}

/// Stores or updates a lane-aware model alias.
pub(crate) fn model_alias_set(
    alias: &str,
    lane: ModelAliasLane,
    model: &str,
    description: Option<&str>,
) -> Result<Value> {
    let alias = require_non_blank("alias", alias)?;
    let model = require_non_blank("model", model)?;
    let description = trimmed_or_none_option(description);

    json_query(
        "WITH saved AS (
            INSERT INTO postllm.model_aliases (alias, lane, model, description)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (alias, lane) DO UPDATE
            SET model = EXCLUDED.model,
                description = EXCLUDED.description,
                updated_at = clock_timestamp()
            RETURNING alias, lane, model, description, created_at, updated_at
        )
        SELECT jsonb_build_object(
            'alias', alias,
            'lane', lane,
            'model', model,
            'description', description,
            'created_at', to_jsonb(created_at),
            'updated_at', to_jsonb(updated_at)
        )
        FROM saved",
        &[
            DatumWithOid::from(alias),
            DatumWithOid::from(lane.as_str()),
            DatumWithOid::from(model),
            DatumWithOid::from(description.as_deref()),
        ],
    )
}

/// Deletes a lane-aware model alias.
pub(crate) fn model_alias_delete(alias: &str, lane: ModelAliasLane) -> Result<Value> {
    let alias = require_non_blank("alias", alias)?;

    let deleted = json_query(
        "WITH deleted AS (
            DELETE FROM postllm.model_aliases
            WHERE alias = $1 AND lane = $2
            RETURNING alias, lane, model, description, created_at, updated_at
        )
        SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'alias', alias,
                    'lane', lane,
                    'model', model,
                    'description', description,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at),
                    'deleted', true
                )
                FROM deleted
            ),
            'null'::jsonb
        )",
        &[DatumWithOid::from(alias), DatumWithOid::from(lane.as_str())],
    )?;

    if deleted.is_null() {
        Err(unknown_model_alias(alias, lane))
    } else {
        Ok(deleted)
    }
}

fn fetch_profile_record(name: &str) -> Result<Value> {
    let profile = json_query(
        "SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'name', name,
                    'description', description,
                    'config', config,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at)
                )
                FROM postllm.config_profiles
                WHERE name = $1
            ),
            'null'::jsonb
        )",
        &[DatumWithOid::from(name)],
    )?;

    if profile.is_null() {
        Err(unknown_profile(name))
    } else {
        Ok(profile)
    }
}

fn fetch_secret_record(name: &str) -> Result<Value> {
    let secret = json_query(
        "SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'name', name,
                    'description', description,
                    'algorithm', algorithm,
                    'key_fingerprint', key_fingerprint,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at)
                )
                FROM postllm.provider_secrets
                WHERE name = $1
            ),
            'null'::jsonb
        )",
        &[DatumWithOid::from(name)],
    )?;

    if secret.is_null() {
        Err(unknown_secret("name", name))
    } else {
        Ok(secret)
    }
}

fn fetch_secret_payload(name: &str) -> Result<StoredSecret> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT algorithm, nonce, ciphertext, key_fingerprint
             FROM postllm.provider_secrets
             WHERE name = $1",
            None,
            &[DatumWithOid::from(name)],
        )?;
        if table.is_empty() {
            return Err(unknown_secret("api_key_secret", name));
        }
        let row = table.first();

        let algorithm = row.get_by_name::<String, _>("algorithm")?.ok_or_else(|| {
            Error::Config(format!("stored secret '{name}' is missing its algorithm"))
        })?;
        let nonce = row
            .get_by_name::<String, _>("nonce")?
            .ok_or_else(|| Error::Config(format!("stored secret '{name}' is missing its nonce")))?;
        let ciphertext = row.get_by_name::<String, _>("ciphertext")?.ok_or_else(|| {
            Error::Config(format!("stored secret '{name}' is missing its ciphertext"))
        })?;
        let key_fingerprint = row
            .get_by_name::<String, _>("key_fingerprint")?
            .ok_or_else(|| {
                Error::Config(format!(
                    "stored secret '{name}' is missing its key fingerprint"
                ))
            })?;

        Ok(StoredSecret {
            algorithm,
            nonce,
            ciphertext,
            key_fingerprint,
        })
    })
}

fn fetch_model_alias_record(alias: &str, lane: ModelAliasLane) -> Result<Value> {
    let alias_record = json_query(
        "SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'alias', alias,
                    'lane', lane,
                    'model', model,
                    'description', description,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at)
                )
                FROM postllm.model_aliases
                WHERE alias = $1 AND lane = $2
            ),
            'null'::jsonb
        )",
        &[DatumWithOid::from(alias), DatumWithOid::from(lane.as_str())],
    )?;

    if alias_record.is_null() {
        Err(unknown_model_alias(alias, lane))
    } else {
        Ok(alias_record)
    }
}

fn json_query(query: &str, args: &[DatumWithOid<'_>]) -> Result<Value> {
    Spi::get_one_with_args::<JsonB>(query, args)?
        .map(|value| value.0)
        .ok_or_else(|| Error::Config(format!("catalog query did not return a row: {query}")))
}

fn unknown_profile(name: &str) -> Error {
    Error::invalid_argument(
        "name",
        format!("refers to unknown profile '{name}'"),
        "create it with postllm.profile_set(...) or choose one from postllm.profiles()",
    )
}

fn unknown_secret(argument: &str, name: &str) -> Error {
    Error::invalid_argument(
        argument,
        format!("refers to unknown provider secret '{name}'"),
        "create it with postllm.secret_set(...) or choose one from postllm.secrets()",
    )
}

fn unknown_model_alias(alias: &str, lane: ModelAliasLane) -> Error {
    Error::invalid_argument(
        "alias",
        format!(
            "refers to unknown {} model alias '{}'",
            lane.as_str(),
            alias
        ),
        format!(
            "create it with postllm.model_alias_set(alias => '{alias}', lane => '{}', model => '...') or choose one from postllm.model_aliases()",
            lane.as_str()
        ),
    )
}

fn require_non_blank<'value>(argument: &str, value: &'value str) -> Result<&'value str> {
    trimmed_or_none(value).ok_or_else(|| {
        Error::invalid_argument(
            argument,
            "must not be empty or whitespace-only",
            format!("pass a non-empty value for '{argument}'"),
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

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "unit tests use expect-style assertions for clearer failure context"
)]
mod test {
    use super::ModelAliasLane;

    #[test]
    fn parse_model_alias_lane_should_accept_supported_values() {
        assert_eq!(
            ModelAliasLane::parse("lane", "generation").expect("generation lane should parse"),
            ModelAliasLane::Generation
        );
        assert_eq!(
            ModelAliasLane::parse("lane", "Embedding").expect("embedding lane should parse"),
            ModelAliasLane::Embedding
        );
    }

    #[test]
    fn parse_model_alias_lane_should_reject_unknown_values() {
        let error = ModelAliasLane::parse("lane", "vision")
            .expect_err("unknown lane values should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'lane' must be one of 'generation' or 'embedding', got 'vision'; fix: pass lane => 'generation' or 'embedding'"
        );
    }
}
