#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::error::{Error, Result};
use pgrx::JsonB;
use pgrx::datum::DatumWithOid;
use pgrx::spi::Spi;
use serde_json::Value;

pub(crate) fn json_value(context: &str, query: &str, args: &[DatumWithOid<'_>]) -> Result<Value> {
    Spi::get_one_with_args::<JsonB>(query, args)?
        .map(|value| value.0)
        .ok_or_else(|| Error::Config(format!("{context} query did not return a row: {query}")))
}

pub(crate) fn bool_or_false(query: &str, args: &[DatumWithOid<'_>]) -> Result<bool> {
    Spi::get_one_with_args::<bool>(query, args)
        .map(|value| value.unwrap_or(false))
        .map_err(Into::into)
}

pub(crate) fn optional_trimmed_string(
    query: &str,
    args: &[DatumWithOid<'_>],
) -> Result<Option<String>> {
    Spi::get_one_with_args::<String>(query, args)
        .map(|value| value.and_then(|value| trimmed_to_owned(&value)))
        .map_err(Into::into)
}

pub(crate) fn set_session_config(name: &str, value: &str) -> Result<()> {
    drop(Spi::get_one_with_args::<String>(
        "SELECT set_config($1, $2, false)",
        &[DatumWithOid::from(name), DatumWithOid::from(value)],
    )?);

    Ok(())
}

fn trimmed_to_owned(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "unit tests use expect-style assertions for clearer failure context"
)]
mod test {
    use super::trimmed_to_owned;

    #[test]
    fn trimmed_to_owned_should_return_none_for_blank_strings() {
        assert_eq!(trimmed_to_owned("   "), None);
    }

    #[test]
    fn trimmed_to_owned_should_return_trimmed_owned_text() {
        assert_eq!(
            trimmed_to_owned("  llama3.2  ").expect("trimmed value should be present"),
            "llama3.2"
        );
    }
}
