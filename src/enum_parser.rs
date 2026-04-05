use crate::error::{Error, Result};

/// Returns the normalized, canonical representation used for enum matching.
#[must_use]
pub(crate) fn normalize_input(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

/// Parses a case-insensitive value against a canonical `("value", variant)` map.
///
/// Returns the normalized value when no match exists so callers can produce
/// context-specific diagnostics.
#[must_use]
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
        values => {
            let mut rendered = String::new();
            for value in &values[..values.len() - 1] {
                rendered.push('\'');
                rendered.push_str(value);
                rendered.push_str("', ");
            }

            rendered.push_str("or '");
            rendered.push_str(values[values.len() - 1]);
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
mod test {
    use super::format_variant_values;
    use super::parse_case_insensitive;
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
        let error = parse_case_insensitive_with_default_error::<TestMode>(
            "mode",
            "delta",
            &TEST_VARIANTS,
        )
        .expect_err("unknown value should fail with invalid_argument");
        assert_eq!(
            error.to_string(),
            "postllm received an invalid argument: argument 'mode' must be one of 'alpha', 'beta', or 'gamma', got 'delta'; fix: pass mode => 'alpha', 'beta', or 'gamma'"
        );
    }

    #[test]
    fn format_variant_values_should_readably_join_values() {
        assert_eq!(format_variant_values(&TEST_VARIANTS), "'alpha', 'beta', or 'gamma'");
    }
}
