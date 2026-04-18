#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::backend::Feature;
use crate::error::{Error, Result};
use crate::operator_policy;
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::spi::Spi;
use serde_json::{Value, json};

pub(crate) fn conversations() -> Result<Value> {
    let created_by = operator_policy::caller_role_name();

    json_query(
        r"
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'id', id,
                    'created_at', to_jsonb(created_at),
                    'updated_at', to_jsonb(updated_at),
                    'created_by', created_by,
                    'title', title,
                    'metadata', metadata,
                    'message_count', message_count,
                    'last_message_at', to_jsonb(last_message_at)
                )
                ORDER BY updated_at DESC, id DESC
            ),
            '[]'::jsonb
        )
        FROM postllm.conversations
        WHERE created_by = $1
        ",
        &[DatumWithOid::from(created_by.as_str())],
    )
}

pub(crate) fn conversation(conversation_id: i64) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();

    json_query(
        r"
        WITH conversation_row AS (
            SELECT *
            FROM postllm.conversations
            WHERE id = $1
              AND created_by = $2
        )
        SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'id', conversation_row.id,
                    'created_at', to_jsonb(conversation_row.created_at),
                    'updated_at', to_jsonb(conversation_row.updated_at),
                    'created_by', conversation_row.created_by,
                    'title', conversation_row.title,
                    'metadata', conversation_row.metadata,
                    'message_count', conversation_row.message_count,
                    'last_message_at', to_jsonb(conversation_row.last_message_at),
                    'messages', COALESCE(
                        (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'conversation_id', message.conversation_id,
                                    'message_no', message.message_no,
                                    'created_at', to_jsonb(message.created_at),
                                    'role', message.role,
                                    'message', message.message,
                                    'metadata', message.metadata
                                )
                                ORDER BY message.message_no
                            )
                            FROM postllm.conversation_messages AS message
                            WHERE message.conversation_id = conversation_row.id
                        ),
                        '[]'::jsonb
                    )
                )
                FROM conversation_row
            ),
            'null'::jsonb
        )
        ",
        &[
            DatumWithOid::from(conversation_id),
            DatumWithOid::from(created_by.as_str()),
        ],
    )
    .and_then(|conversation| {
        if conversation.is_null() {
            Err(unknown_conversation(conversation_id))
        } else {
            Ok(conversation)
        }
    })
}

pub(crate) fn create(title: Option<&str>, metadata: Option<&Value>) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let title = trimmed_or_none(title);
    let metadata = validate_metadata(metadata, "metadata")?;

    Spi::get_one_with_args::<JsonB>(
        r"
        INSERT INTO postllm.conversations (
            created_by,
            title,
            metadata
        )
        VALUES ($1, $2, $3)
        RETURNING jsonb_build_object(
            'id', id,
            'created_at', to_jsonb(created_at),
            'updated_at', to_jsonb(updated_at),
            'created_by', created_by,
            'title', title,
            'metadata', metadata,
            'message_count', message_count,
            'last_message_at', to_jsonb(last_message_at)
        )
        ",
        &[
            DatumWithOid::from(created_by.as_str()),
            DatumWithOid::from(title),
            DatumWithOid::from(JsonB(metadata)),
        ],
    )?
    .map(|row| row.0)
    .ok_or_else(|| Error::Internal("conversation insert returned no row".to_owned()))
}

pub(crate) fn append(
    conversation_id: i64,
    message: &Value,
    metadata: Option<&Value>,
) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let message = crate::validate_message_with_argument(message, "message")?;
    let role = message.get("role").and_then(Value::as_str).ok_or_else(|| {
        Error::Internal("validated conversation message lost its role".to_owned())
    })?;
    let metadata = validate_metadata(metadata, "metadata")?;

    Spi::get_one_with_args::<JsonB>(
        r"
        WITH conversation_row AS (
            SELECT id
            FROM postllm.conversations
            WHERE id = $1
              AND created_by = $5
            FOR UPDATE
        ),
        next_message AS (
            SELECT COALESCE(MAX(message_no), 0) + 1 AS message_no
            FROM postllm.conversation_messages
            WHERE conversation_id = $1
        ),
        inserted AS (
            INSERT INTO postllm.conversation_messages (
                conversation_id,
                message_no,
                role,
                message,
                metadata
            )
            SELECT
                $1,
                next_message.message_no,
                $2,
                $3,
                $4
            FROM conversation_row, next_message
            RETURNING conversation_id, message_no, created_at, role, message, metadata
        ),
        updated AS (
            UPDATE postllm.conversations AS conversation
            SET updated_at = inserted.created_at,
                last_message_at = inserted.created_at,
                message_count = inserted.message_no
            FROM inserted
            WHERE conversation.id = inserted.conversation_id
            RETURNING conversation.id
        )
        SELECT jsonb_build_object(
            'conversation_id', inserted.conversation_id,
            'message_no', inserted.message_no,
            'created_at', to_jsonb(inserted.created_at),
            'role', inserted.role,
            'message', inserted.message,
            'metadata', inserted.metadata
        )
        FROM inserted
        ",
        &[
            DatumWithOid::from(conversation_id),
            DatumWithOid::from(role),
            DatumWithOid::from(JsonB(message)),
            DatumWithOid::from(JsonB(metadata)),
            DatumWithOid::from(created_by.as_str()),
        ],
    )?
    .map(|row| row.0)
    .ok_or_else(|| unknown_conversation(conversation_id))
}

pub(crate) fn history(conversation_id: i64) -> Result<Vec<Value>> {
    let created_by = operator_policy::caller_role_name();
    ensure_conversation_exists(conversation_id, &created_by)?;
    let history = json_query(
        r"
        SELECT COALESCE(
            (
                SELECT jsonb_agg(message ORDER BY message_no)
                FROM postllm.conversation_messages
                WHERE conversation_id = $1
            ),
            '[]'::jsonb
        )
        ",
        &[DatumWithOid::from(conversation_id)],
    )?;

    history.as_array().cloned().ok_or_else(|| {
        Error::Internal("conversation history query did not return a JSON array".to_owned())
    })
}

pub(crate) fn reply(
    conversation_id: i64,
    message: Option<&Value>,
    model: Option<&str>,
    temperature: f64,
    max_tokens: Option<i32>,
) -> Result<Value> {
    if let Some(message) = message {
        append(conversation_id, message, None)?;
    } else {
        let created_by = operator_policy::caller_role_name();
        ensure_conversation_exists(conversation_id, &created_by)?;
    }

    let messages = history(conversation_id)?;
    let response = crate::chat_impl_from_values(
        &messages,
        model,
        temperature,
        max_tokens,
        Feature::Chat,
        crate::ChatRequestExtensions::default(),
    )?;
    let assistant_message = assistant_message_from_response(&response)?;
    let stored_message = append(conversation_id, &assistant_message, None)?;

    Ok(json!({
        "conversation_id": conversation_id,
        "assistant_message": stored_message,
        "response": response,
    }))
}

fn ensure_conversation_exists(conversation_id: i64, created_by: &str) -> Result<()> {
    let exists = Spi::get_one_with_args::<bool>(
        r"
        SELECT EXISTS(
            SELECT 1
            FROM postllm.conversations
            WHERE id = $1
              AND created_by = $2
        )
        ",
        &[
            DatumWithOid::from(conversation_id),
            DatumWithOid::from(created_by),
        ],
    )?
    .unwrap_or(false);

    if exists {
        Ok(())
    } else {
        Err(unknown_conversation(conversation_id))
    }
}

fn assistant_message_from_response(response: &Value) -> Result<Value> {
    let Some(message) = response
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
    else {
        return Err(Error::MalformedResponse);
    };

    crate::validate_message_with_argument(message, "response.choices[0].message")
}

fn validate_metadata(metadata: Option<&Value>, argument: &str) -> Result<Value> {
    match metadata {
        None | Some(Value::Null) => Ok(json!({})),
        Some(Value::Object(object)) => Ok(Value::Object(object.clone())),
        Some(_) => Err(Error::invalid_argument(
            argument,
            "must be a JSON object or null",
            r#"pass jsonb like '{"topic":"support"}'::jsonb or omit it"#,
        )),
    }
}

fn trimmed_or_none(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn unknown_conversation(conversation_id: i64) -> Error {
    Error::invalid_argument(
        "conversation_id",
        format!("refers to unknown conversation {conversation_id} for the current role"),
        "create it with postllm.conversation_create(...) or choose one from postllm.conversations()",
    )
}

fn json_query(query: &str, args: &[DatumWithOid<'_>]) -> Result<Value> {
    Spi::get_one_with_args::<JsonB>(query, args)
        .map(|value| value.map_or(Value::Null, |value| value.0))
        .map_err(Into::into)
}
