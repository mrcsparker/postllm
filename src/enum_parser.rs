#![allow(
    clippy::redundant_pub_crate,
    reason = "crate-private helper modules use pub(crate) to satisfy unreachable_pub without exporting APIs"
)]

use crate::error::{Error, Result};

macro_rules! canonical_string_enum {
    ($type:ty { $($label:expr => $variant:path),+ $(,)? }) => {
        impl $type {
            const VARIANTS: &'static [(&'static str, Self)] = &[$(($label, $variant)),+];

            /// Returns the canonical configuration string for this value.
            #[must_use]
            pub(crate) const fn as_str(self) -> &'static str {
                match self {
                    $($variant => $label),+
                }
            }
        }

        impl core::fmt::Display for $type {
            fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

pub(crate) use canonical_string_enum;

macro_rules! display_string_enum {
    ($visibility:vis $type:ty { $($label:expr => $variant:path),+ $(,)? }) => {
        impl $type {
            /// Returns the canonical string for this value.
            #[must_use]
            $visibility const fn as_str(self) -> &'static str {
                match self {
                    $($variant => $label),+
                }
            }
        }

        impl core::fmt::Display for $type {
            fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

pub(crate) use display_string_enum;

/// Returns the normalized, canonical representation used for enum matching.
#[must_use]
pub(crate) fn normalize_input(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

/// Parses a case-insensitive value against a canonical `("value", variant)` map.
///
/// Returns the normalized value when no match exists so callers can produce
/// context-specific diagnostics.
pub(crate) fn parse_case_insensitive<T: Copy>(
    value: &str,
    variants: &'static [(&'static str, T)],
) -> core::result::Result<T, String> {
    let normalized = normalize_input(value);

    variants
        .iter()
        .find_map(|(label, parsed)| (normalized == *label).then_some(*parsed))
        .ok_or(normalized)
}

/// Parses a case-insensitive value against a canonical variant map.
/// Returns `Some(variant)` when the value matches, otherwise `None`.
pub(crate) fn parse_case_insensitive_optional<T: Copy>(
    value: &str,
    variants: &'static [(&'static str, T)],
) -> Option<T> {
    parse_case_insensitive(value, variants).ok()
}

/// Parses a required enum value and returns a standardized invalid-argument error.
///
/// Blank input is rejected before variant matching so callers get an actionable
/// "missing value" diagnostic instead of a generic "unknown variant" response.
pub(crate) fn parse_case_insensitive_required<T: Copy>(
    argument: &str,
    value: &str,
    variants: &'static [(&'static str, T)],
) -> Result<T> {
    let normalized = normalize_input(value);
    if normalized.is_empty() {
        return Err(Error::invalid_argument(
            argument,
            "must not be empty or whitespace-only",
            format!("pass a non-empty value for '{argument}'"),
        ));
    }

    parse_case_insensitive(value, variants).map_err(|normalized| {
        let allowed = format_variant_values(variants);
        Error::invalid_argument(
            argument,
            format!("must be one of {allowed}, got '{normalized}'"),
            format!("pass {argument} => {allowed}"),
        )
    })
}

/// Formats canonical enum variant names as an English conjunction list for errors.
#[must_use]
pub(crate) fn format_variant_values<T>(variants: &'static [(&'static str, T)]) -> String {
    format_enumeration(&variants.iter().map(|(label, _)| *label).collect::<Vec<_>>())
}

fn format_enumeration(values: &[&str]) -> String {
    match values {
        [] => String::new(),
        [only] => format!("'{only}'"),
        [first, second] => format!("'{first}' or '{second}'"),
        _ => {
            let Some((last, leading)) = values.split_last() else {
                return String::new();
            };
            let mut rendered = String::new();
            for value in leading {
                rendered.push('\'');
                rendered.push_str(value);
                rendered.push_str("', ");
            }

            rendered.push_str("or '");
            rendered.push_str(last);
            rendered.push('\'');
            rendered
        }
    }
}

/// Parses a value using `parse_case_insensitive` and returns a standard
/// one-of diagnostic on mismatch.
pub(crate) fn parse_case_insensitive_with_default_error<T: Copy>(
    argument: &str,
    value: &str,
    variants: &'static [(&'static str, T)],
) -> Result<T> {
    parse_case_insensitive(value, variants).map_err(|normalized| {
        let allowed = format_variant_values(variants);
        Error::invalid_argument(
            argument,
            format!("must be one of {allowed}, got '{normalized}'"),
            format!("pass {argument} => {allowed}"),
        )
    })
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "unit tests use expect-style assertions for clearer failure context"
)]
mod tests {
    use super::format_variant_values;
    use super::parse_case_insensitive;
    use super::parse_case_insensitive_optional;
    use super::parse_case_insensitive_required;
    use super::parse_case_insensitive_with_default_error;

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum TestMode {
        Alpha,
        Beta,
        Gamma,
    }

    const TEST_VARIANTS: [(&str, TestMode); 3] = [
        ("alpha", TestMode::Alpha),
        ("beta", TestMode::Beta),
        ("gamma", TestMode::Gamma),
    ];

    #[test]
    fn parse_case_insensitive_should_match_expected_values() {
        let parsed = parse_case_insensitive(" Beta ", &TEST_VARIANTS)
            .expect("test enum variant should parse");
        assert_eq!(parsed, TestMode::Beta);
    }

    #[test]
    fn parse_case_insensitive_should_reject_unknown_values() {
        let error = parse_case_insensitive::<TestMode>(" delta ", &TEST_VARIANTS)
            .expect_err("unknown value should be rejected");
        assert_eq!(error, "delta");
    }

    #[test]
    fn parse_case_insensitive_with_default_error_should_return_consistent_message() {
        let error =
            parse_case_insensitive_with_default_error::<TestMode>("mode", "delta", &TEST_VARIANTS)
                .expect_err("unknown value should fail with invalid_argument");
        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'mode' must be one of 'alpha', 'beta', or 'gamma', got 'delta'; fix: pass mode => 'alpha', 'beta', or 'gamma'"
        );
    }

    #[test]
    fn parse_case_insensitive_optional_should_map_unknown_values_to_none() {
        let parsed = parse_case_insensitive_optional("gamma", &TEST_VARIANTS)
            .expect("a known value should parse");
        assert_eq!(parsed, TestMode::Gamma);

        let unknown = parse_case_insensitive_optional("unknown", &TEST_VARIANTS);
        assert!(unknown.is_none());
    }

    #[test]
    fn parse_case_insensitive_required_should_reject_whitespace_only_input() {
        let error = parse_case_insensitive_required::<TestMode>("mode", "   ", &TEST_VARIANTS)
            .expect_err("blank values should be rejected");

        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'mode' must not be empty or whitespace-only; fix: pass a non-empty value for 'mode'"
        );
    }

    #[test]
    fn format_variant_values_should_readably_join_values() {
        assert_eq!(
            format_variant_values(&TEST_VARIANTS),
            "'alpha', 'beta', or 'gamma'"
        );
    }
}
