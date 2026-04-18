#![allow(
    clippy::needless_pass_by_value,
    clippy::redundant_pub_crate,
    reason = "pgrx materializes SQL-facing values as owned Rust types and these wrappers are crate-visible by design"
)]

use pgrx::JsonB;

pub(crate) fn eval_datasets() -> JsonB {
    crate::finish_json_result(crate::evals::datasets())
}

pub(crate) fn eval_dataset(name: &str) -> JsonB {
    crate::finish_json_result(crate::evals::dataset(name))
}

pub(crate) fn eval_dataset_set(
    name: &str,
    description: pgrx::default!(Option<&str>, "NULL"),
    metadata: pgrx::default!(Option<JsonB>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::evals::dataset_set(
        name,
        description,
        metadata.as_ref().map(|metadata| &metadata.0),
    ))
}

pub(crate) fn eval_dataset_delete(name: &str) -> JsonB {
    crate::finish_json_result(crate::evals::dataset_delete(name))
}

pub(crate) fn eval_case(dataset_name: &str, case_name: &str) -> JsonB {
    crate::finish_json_result(crate::evals::eval_case(dataset_name, case_name))
}

pub(crate) fn eval_case_set(
    dataset_name: &str,
    case_name: &str,
    input_payload: JsonB,
    expected_payload: JsonB,
    scorer: pgrx::default!(&str, "exact_text"),
    threshold: pgrx::default!(f64, 1.0),
    metadata: pgrx::default!(Option<JsonB>, "NULL"),
) -> JsonB {
    crate::finish_json_result(crate::evals::case_set(
        dataset_name,
        case_name,
        &input_payload.0,
        &expected_payload.0,
        scorer,
        threshold,
        metadata.as_ref().map(|metadata| &metadata.0),
    ))
}

pub(crate) fn eval_case_delete(dataset_name: &str, case_name: &str) -> JsonB {
    crate::finish_json_result(crate::evals::case_delete(dataset_name, case_name))
}

pub(crate) fn eval_score(
    actual: JsonB,
    expected: JsonB,
    scorer: pgrx::default!(&str, "exact_text"),
    threshold: pgrx::default!(f64, 1.0),
) -> JsonB {
    crate::finish_json_result(crate::evals::score(
        &actual.0,
        &expected.0,
        scorer,
        threshold,
    ))
}

pub(crate) fn eval_case_score(dataset_name: &str, case_name: &str, actual: JsonB) -> JsonB {
    crate::finish_json_result(crate::evals::case_score(dataset_name, case_name, &actual.0))
}
