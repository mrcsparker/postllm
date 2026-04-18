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

const SCORER_EXACT_TEXT: &str = "exact_text";
const SCORER_CONTAINS_TEXT: &str = "contains_text";
const SCORER_EXACT_JSON: &str = "exact_json";
const SCORER_JSON_SUBSET: &str = "json_subset";

#[derive(Clone, Copy)]
enum EvalScorer {
    ExactText,
    ContainsText,
    ExactJson,
    JsonSubset,
}

impl EvalScorer {
    fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            SCORER_EXACT_TEXT => Ok(Self::ExactText),
            SCORER_CONTAINS_TEXT => Ok(Self::ContainsText),
            SCORER_EXACT_JSON => Ok(Self::ExactJson),
            SCORER_JSON_SUBSET => Ok(Self::JsonSubset),
            _ => Err(Error::invalid_argument(
                "scorer",
                format!(
                    "must be one of '{SCORER_EXACT_TEXT}', '{SCORER_CONTAINS_TEXT}', '{SCORER_EXACT_JSON}', or '{SCORER_JSON_SUBSET}'"
                ),
                "choose one of the documented built-in scorer names",
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::ExactText => SCORER_EXACT_TEXT,
            Self::ContainsText => SCORER_CONTAINS_TEXT,
            Self::ExactJson => SCORER_EXACT_JSON,
            Self::JsonSubset => SCORER_JSON_SUBSET,
        }
    }
}

pub(crate) fn datasets() -> Result<Value> {
    let created_by = operator_policy::caller_role_name();

    json_query(
        r"
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'id', dataset.id,
                    'created_at', to_jsonb(dataset.created_at),
                    'updated_at', to_jsonb(dataset.updated_at),
                    'created_by', dataset.created_by,
                    'name', dataset.name,
                    'description', dataset.description,
                    'metadata', dataset.metadata,
                    'case_count', COALESCE(case_counts.case_count, 0)
                )
                ORDER BY dataset.updated_at DESC, dataset.id DESC
            ),
            '[]'::jsonb
        )
        FROM postllm.eval_datasets AS dataset
        LEFT JOIN (
            SELECT dataset_id, COUNT(*)::integer AS case_count
            FROM postllm.eval_cases
            GROUP BY dataset_id
        ) AS case_counts
          ON case_counts.dataset_id = dataset.id
        WHERE dataset.created_by = $1
        ",
        &[DatumWithOid::from(created_by.as_str())],
    )
}

pub(crate) fn dataset(name: &str) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let name = require_non_blank("name", name)?;

    json_query(
        r"
        WITH dataset_row AS (
            SELECT *
            FROM postllm.eval_datasets
            WHERE created_by = $1
              AND name = $2
        )
        SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'id', dataset_row.id,
                    'created_at', to_jsonb(dataset_row.created_at),
                    'updated_at', to_jsonb(dataset_row.updated_at),
                    'created_by', dataset_row.created_by,
                    'name', dataset_row.name,
                    'description', dataset_row.description,
                    'metadata', dataset_row.metadata,
                    'case_count', (
                        SELECT COUNT(*)::integer
                        FROM postllm.eval_cases
                        WHERE dataset_id = dataset_row.id
                    ),
                    'cases', COALESCE(
                        (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'dataset_id', eval_case.dataset_id,
                                    'name', eval_case.case_name,
                                    'created_at', to_jsonb(eval_case.created_at),
                                    'updated_at', to_jsonb(eval_case.updated_at),
                                    'input', eval_case.input_payload,
                                    'expected', eval_case.expected_payload,
                                    'scorer', eval_case.scorer,
                                    'threshold', eval_case.threshold,
                                    'metadata', eval_case.metadata
                                )
                                ORDER BY eval_case.updated_at DESC, eval_case.case_name
                            )
                            FROM postllm.eval_cases AS eval_case
                            WHERE eval_case.dataset_id = dataset_row.id
                        ),
                        '[]'::jsonb
                    )
                )
                FROM dataset_row
            ),
            'null'::jsonb
        )
        ",
        &[
            DatumWithOid::from(created_by.as_str()),
            DatumWithOid::from(name),
        ],
    )
    .and_then(|dataset| {
        if dataset.is_null() {
            Err(unknown_dataset(name))
        } else {
            Ok(dataset)
        }
    })
}

pub(crate) fn dataset_set(
    name: &str,
    description: Option<&str>,
    metadata: Option<&Value>,
) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let name = require_non_blank("name", name)?;
    let description = trimmed_or_none(description);
    let metadata = validate_metadata(metadata, "metadata")?;

    Spi::run_with_args(
        r"
        INSERT INTO postllm.eval_datasets (
            created_by,
            name,
            description,
            metadata
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (created_by, name) DO UPDATE
        SET updated_at = clock_timestamp(),
            description = EXCLUDED.description,
            metadata = EXCLUDED.metadata
        ",
        &[
            DatumWithOid::from(created_by.as_str()),
            DatumWithOid::from(name),
            DatumWithOid::from(description),
            DatumWithOid::from(JsonB(metadata)),
        ],
    )?;

    dataset(name)
}

pub(crate) fn dataset_delete(name: &str) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let name = require_non_blank("name", name)?;
    let deleted = dataset(name)?;

    let removed = Spi::get_one_with_args::<bool>(
        r"
        DELETE FROM postllm.eval_datasets
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
        return Err(unknown_dataset(name));
    }

    mark_deleted(deleted)
}

pub(crate) fn eval_case(dataset_name: &str, case_name: &str) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let dataset_name = require_non_blank("dataset_name", dataset_name)?;
    let case_name = require_non_blank("case_name", case_name)?;

    json_query(
        r"
        WITH dataset_row AS (
            SELECT id, name
            FROM postllm.eval_datasets
            WHERE created_by = $1
              AND name = $2
        )
        SELECT COALESCE(
            (
                SELECT jsonb_build_object(
                    'dataset_id', eval_case.dataset_id,
                    'dataset_name', dataset_row.name,
                    'name', eval_case.case_name,
                    'created_at', to_jsonb(eval_case.created_at),
                    'updated_at', to_jsonb(eval_case.updated_at),
                    'input', eval_case.input_payload,
                    'expected', eval_case.expected_payload,
                    'scorer', eval_case.scorer,
                    'threshold', eval_case.threshold,
                    'metadata', eval_case.metadata
                )
                FROM postllm.eval_cases AS eval_case
                JOIN dataset_row
                  ON dataset_row.id = eval_case.dataset_id
                WHERE eval_case.case_name = $3
            ),
            'null'::jsonb
        )
        ",
        &[
            DatumWithOid::from(created_by.as_str()),
            DatumWithOid::from(dataset_name),
            DatumWithOid::from(case_name),
        ],
    )
    .and_then(|eval_case| {
        if eval_case.is_null() {
            Err(unknown_case(dataset_name, case_name))
        } else {
            Ok(eval_case)
        }
    })
}

pub(crate) fn case_set(
    dataset_name: &str,
    case_name: &str,
    input_payload: &Value,
    expected_payload: &Value,
    scorer: &str,
    threshold: f64,
    metadata: Option<&Value>,
) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let dataset_name = require_non_blank("dataset_name", dataset_name)?;
    let case_name = require_non_blank("case_name", case_name)?;
    let scorer = EvalScorer::parse(scorer)?;
    let threshold = validate_threshold(threshold)?;
    let metadata = validate_metadata(metadata, "metadata")?;
    let dataset_id = require_dataset_id(dataset_name, &created_by)?;

    Spi::run_with_args(
        r"
        INSERT INTO postllm.eval_cases (
            dataset_id,
            case_name,
            input_payload,
            expected_payload,
            scorer,
            threshold,
            metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (dataset_id, case_name) DO UPDATE
        SET updated_at = clock_timestamp(),
            input_payload = EXCLUDED.input_payload,
            expected_payload = EXCLUDED.expected_payload,
            scorer = EXCLUDED.scorer,
            threshold = EXCLUDED.threshold,
            metadata = EXCLUDED.metadata
        ",
        &[
            DatumWithOid::from(dataset_id),
            DatumWithOid::from(case_name),
            DatumWithOid::from(JsonB(input_payload.clone())),
            DatumWithOid::from(JsonB(expected_payload.clone())),
            DatumWithOid::from(scorer.as_str()),
            DatumWithOid::from(threshold),
            DatumWithOid::from(JsonB(metadata)),
        ],
    )?;

    touch_dataset(dataset_id)?;

    eval_case(dataset_name, case_name)
}

pub(crate) fn case_delete(dataset_name: &str, case_name: &str) -> Result<Value> {
    let created_by = operator_policy::caller_role_name();
    let dataset_name = require_non_blank("dataset_name", dataset_name)?;
    let case_name = require_non_blank("case_name", case_name)?;
    let dataset_id = require_dataset_id(dataset_name, &created_by)?;
    let deleted = eval_case(dataset_name, case_name)?;

    let removed = Spi::get_one_with_args::<bool>(
        r"
        DELETE FROM postllm.eval_cases
        WHERE dataset_id = $1
          AND case_name = $2
        RETURNING true
        ",
        &[
            DatumWithOid::from(dataset_id),
            DatumWithOid::from(case_name),
        ],
    )?
    .unwrap_or(false);

    if !removed {
        return Err(unknown_case(dataset_name, case_name));
    }

    touch_dataset(dataset_id)?;
    mark_deleted(deleted)
}

pub(crate) fn score(
    actual: &Value,
    expected: &Value,
    scorer: &str,
    threshold: f64,
) -> Result<Value> {
    let scorer = EvalScorer::parse(scorer)?;
    let threshold = validate_threshold(threshold)?;
    let score = compute_score(actual, expected, scorer)?;

    Ok(json!({
        "scorer": scorer.as_str(),
        "threshold": threshold,
        "score": score,
        "passed": score >= threshold,
    }))
}

pub(crate) fn case_score(dataset_name: &str, case_name: &str, actual: &Value) -> Result<Value> {
    let eval_case = eval_case(dataset_name, case_name)?;
    let expected = eval_case.get("expected").ok_or_else(|| {
        Error::Internal("stored eval case payload is missing expected".to_owned())
    })?;
    let scorer_name = eval_case
        .get("scorer")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Internal("stored eval case payload is missing scorer".to_owned()))?;
    let threshold = eval_case
        .get("threshold")
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            Error::Internal("stored eval case payload is missing threshold".to_owned())
        })?;
    let score_payload = score(actual, expected, scorer_name, threshold)?;
    let input = required_field(&eval_case, "input")?;
    let metadata = required_field(&eval_case, "metadata")?;
    let scorer = required_field(&score_payload, "scorer")?;
    let threshold = required_field(&score_payload, "threshold")?;
    let score = required_field(&score_payload, "score")?;
    let passed = required_field(&score_payload, "passed")?;

    Ok(json!({
        "dataset_name": dataset_name,
        "case_name": case_name,
        "input": input,
        "expected": expected.clone(),
        "actual": actual.clone(),
        "scorer": scorer,
        "threshold": threshold,
        "score": score,
        "passed": passed,
        "metadata": metadata,
    }))
}

fn compute_score(actual: &Value, expected: &Value, scorer: EvalScorer) -> Result<f64> {
    let score = match scorer {
        EvalScorer::ExactText => {
            let actual = normalized_text(actual, "actual")?;
            let expected = normalized_text(expected, "expected")?;
            if actual == expected { 1.0 } else { 0.0 }
        }
        EvalScorer::ContainsText => {
            let actual = normalized_text(actual, "actual")?;
            let expected = normalized_text(expected, "expected")?;
            if expected.trim().is_empty() {
                return Err(Error::invalid_argument(
                    "expected",
                    "must not be empty or whitespace-only for scorer 'contains_text'",
                    "pass a non-empty expected substring",
                ));
            }
            if actual.contains(&expected) { 1.0 } else { 0.0 }
        }
        EvalScorer::ExactJson => {
            if actual == expected {
                1.0
            } else {
                0.0
            }
        }
        EvalScorer::JsonSubset => {
            if json_subset_matches(actual, expected) {
                1.0
            } else {
                0.0
            }
        }
    };

    Ok(score)
}

fn normalized_text(value: &Value, argument: &str) -> Result<String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Object(_) => {
            if let Some(content) = value.get("content").and_then(Value::as_str) {
                return Ok(content.to_owned());
            }

            crate::client::extract_text(value).map_err(|_| {
                Error::invalid_argument(
                    argument,
                    "must be a JSON string, a message-like object with string content, or a chat/completion response payload",
                    "pass a JSON string like '\"ok\"'::jsonb or the raw response from postllm.chat(...)",
                )
            })
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => {
            Err(Error::invalid_argument(
                argument,
                "must be a JSON string, a message-like object with string content, or a chat/completion response payload",
                "pass a JSON string like '\"ok\"'::jsonb or the raw response from postllm.chat(...)",
            ))
        }
    }
}

fn json_subset_matches(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Object(actual), Value::Object(expected)) => {
            expected.iter().all(|(key, expected_value)| {
                actual
                    .get(key)
                    .is_some_and(|actual_value| json_subset_matches(actual_value, expected_value))
            })
        }
        (Value::Array(actual), Value::Array(expected)) => {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected)
                    .all(|(actual_value, expected_value)| {
                        json_subset_matches(actual_value, expected_value)
                    })
        }
        _ => actual == expected,
    }
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

fn validate_threshold(threshold: f64) -> Result<f64> {
    if !threshold.is_finite() || !(0.0..=1.0).contains(&threshold) {
        Err(Error::invalid_argument(
            "threshold",
            format!("must be a finite value between 0.0 and 1.0, got {threshold}"),
            "pass threshold => 1.0 for exact pass/fail matching",
        ))
    } else {
        Ok(threshold)
    }
}

fn require_dataset_id(dataset_name: &str, created_by: &str) -> Result<i64> {
    Spi::get_one_with_args::<i64>(
        r"
        SELECT id
        FROM postllm.eval_datasets
        WHERE created_by = $1
          AND name = $2
        ",
        &[
            DatumWithOid::from(created_by),
            DatumWithOid::from(dataset_name),
        ],
    )?
    .ok_or_else(|| unknown_dataset(dataset_name))
}

fn touch_dataset(dataset_id: i64) -> Result<()> {
    Spi::run_with_args(
        r"
        UPDATE postllm.eval_datasets
        SET updated_at = clock_timestamp()
        WHERE id = $1
        ",
        &[DatumWithOid::from(dataset_id)],
    )?;
    Ok(())
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

fn mark_deleted(value: Value) -> Result<Value> {
    match value {
        Value::Object(mut object) => {
            object.insert("deleted".to_owned(), json!(true));
            Ok(Value::Object(object))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Err(Error::Internal(
                "deleted eval payload was not a JSON object".to_owned(),
            ))
        }
    }
}

fn required_field(value: &Value, field: &str) -> Result<Value> {
    value.get(field).cloned().ok_or_else(|| {
        Error::Internal(format!(
            "stored eval payload is missing required field '{field}'"
        ))
    })
}

fn unknown_dataset(name: &str) -> Error {
    Error::invalid_argument(
        "name",
        format!("refers to unknown evaluation dataset '{name}' for the current role"),
        "create it with postllm.eval_dataset_set(...) or choose one from postllm.eval_datasets()",
    )
}

fn unknown_case(dataset_name: &str, case_name: &str) -> Error {
    Error::invalid_argument(
        "case_name",
        format!("refers to unknown evaluation case '{case_name}' in dataset '{dataset_name}'"),
        "create it with postllm.eval_case_set(...) or choose one from postllm.eval_dataset(...)",
    )
}

fn json_query(query: &str, args: &[DatumWithOid<'_>]) -> Result<Value> {
    Spi::get_one_with_args::<JsonB>(query, args)
        .map(|value| value.map_or(Value::Null, |value| value.0))
        .map_err(Into::into)
}
