#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::backend::{Runtime, Settings};
use crate::error::{Error, Result};
use reqwest::Url;

const PROVIDER_OPENAI: &str = "openai";
const PROVIDER_OLLAMA: &str = "ollama";
pub(crate) const PROVIDER_ANTHROPIC: &str = "anthropic";
const PROVIDER_OPENAI_COMPATIBLE: &str = "openai-compatible";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostedEndpointSummary {
    pub(crate) provider: String,
    pub(crate) base_url_host: Option<String>,
    pub(crate) base_url_kind: Option<&'static str>,
    pub(crate) discovery_url: Option<String>,
}

pub(crate) fn summarize(settings: &Settings) -> HostedEndpointSummary {
    let parsed_url = settings
        .base_url
        .as_deref()
        .and_then(|base_url| Url::parse(base_url).ok());

    HostedEndpointSummary {
        provider: provider_identity(settings),
        base_url_host: parsed_url
            .as_ref()
            .and_then(|url| url.host_str().map(str::to_owned)),
        base_url_kind: parsed_url.as_ref().and_then(classify_url_kind),
        discovery_url: parsed_url.as_ref().and_then(derive_models_url),
    }
}

pub(crate) fn provider_identity(settings: &Settings) -> String {
    match settings.runtime {
        Runtime::Candle => Runtime::CANDLE.to_owned(),
        Runtime::OpenAi => infer_hosted_provider(settings.base_url.as_deref()),
    }
}

pub(crate) fn infer_hosted_provider(base_url: Option<&str>) -> String {
    infer_openai_provider(base_url)
}

pub(crate) fn enforce_settings(settings: &Settings) -> Result<()> {
    if settings.runtime != Runtime::OpenAi {
        return Ok(());
    }

    let Some(base_url) = settings.base_url.as_deref() else {
        return Ok(());
    };

    if !host_is_allowed(base_url, &settings.http_allowed_hosts)? {
        let host = summarize(settings)
            .base_url_host
            .unwrap_or_else(|| "<unknown>".to_owned());
        return Err(Error::Config(format!(
            "postllm.base_url host '{host}' is not permitted by postllm.http_allowed_hosts; fix: point postllm.base_url at an allowed host or SET postllm.http_allowed_hosts = '{host}'"
        )));
    }

    let provider = provider_identity(settings);
    if !provider_is_allowed(&provider, &settings.http_allowed_providers) {
        return Err(Error::Config(format!(
            "provider '{provider}' is not permitted by postllm.http_allowed_providers; fix: point postllm.base_url at an allowed provider or SET postllm.http_allowed_providers = '{provider}'"
        )));
    }

    Ok(())
}

pub(crate) fn parse_allowed_hosts(raw: Option<&str>, setting_name: &str) -> Result<Vec<String>> {
    parse_csv_setting(raw)
        .into_iter()
        .map(|entry| {
            if entry == "*" || entry.starts_with("*.") || is_valid_exact_host_entry(&entry) {
                Ok(entry)
            } else {
                Err(Error::invalid_setting(
                    setting_name,
                    format!("contains invalid host entry '{entry}'"),
                    format!(
                        "SET {setting_name} = 'api.openai.com,host.docker.internal:11434,*.openai.com' or leave it empty"
                    ),
                ))
            }
        })
        .collect()
}

pub(crate) fn parse_allowed_providers(
    raw: Option<&str>,
    setting_name: &str,
) -> Result<Vec<String>> {
    parse_csv_setting(raw)
        .into_iter()
        .map(|entry| match entry.as_str() {
            "*"
            | PROVIDER_OPENAI
            | PROVIDER_OLLAMA
            | PROVIDER_ANTHROPIC
            | PROVIDER_OPENAI_COMPATIBLE => Ok(entry),
            _ => Err(Error::invalid_setting(
                setting_name,
                format!("contains unsupported provider '{entry}'"),
                format!(
                    "SET {setting_name} = '{PROVIDER_OPENAI},{PROVIDER_OLLAMA},{PROVIDER_ANTHROPIC},{PROVIDER_OPENAI_COMPATIBLE}' or leave it empty"
                ),
            )),
        })
        .collect()
}

fn infer_openai_provider(base_url: Option<&str>) -> String {
    let Some(base_url) = base_url else {
        return PROVIDER_OPENAI_COMPATIBLE.to_owned();
    };
    let Ok(url) = Url::parse(base_url) else {
        return PROVIDER_OPENAI_COMPATIBLE.to_owned();
    };
    if url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        == Some("messages")
    {
        return PROVIDER_ANTHROPIC.to_owned();
    }
    let Some(host) = url.host_str() else {
        return PROVIDER_OPENAI_COMPATIBLE.to_owned();
    };

    match (host, url.port_or_known_default()) {
        ("api.openai.com", _) => PROVIDER_OPENAI.to_owned(),
        ("api.anthropic.com", _) => PROVIDER_ANTHROPIC.to_owned(),
        ("127.0.0.1" | "localhost" | "host.docker.internal", Some(11_434)) => {
            PROVIDER_OLLAMA.to_owned()
        }
        _ => PROVIDER_OPENAI_COMPATIBLE.to_owned(),
    }
}

fn derive_models_url(base_url: &Url) -> Option<String> {
    let mut url = base_url.clone();
    let mut segments = url
        .path_segments()?
        .map(str::to_owned)
        .collect::<Vec<String>>();

    if matches!(segments.last().map(String::as_str), Some("completions")) {
        segments.pop();
    }
    if matches!(segments.last().map(String::as_str), Some("chat")) {
        segments.pop();
    }
    if matches!(
        segments.last().map(String::as_str),
        Some("responses" | "embeddings" | "rerank" | "messages")
    ) {
        segments.pop();
    }
    if segments.is_empty() {
        segments.push("v1".to_owned());
    }
    if !matches!(segments.last().map(String::as_str), Some("models")) {
        segments.push("models".to_owned());
    }

    url.set_path(&format!("/{}", segments.join("/")));
    Some(url.to_string())
}

fn classify_url_kind(url: &Url) -> Option<&'static str> {
    let host = url.host_str()?;

    Some(match host {
        "host.docker.internal" => "docker-host",
        "127.0.0.1" | "localhost" => "loopback",
        _ => "remote",
    })
}

fn host_is_allowed(base_url: &str, allowed_hosts: &[String]) -> Result<bool> {
    if allowed_hosts.is_empty() || allowed_hosts.iter().any(|entry| entry == "*") {
        return Ok(true);
    }

    let url = parse_absolute_base_url(base_url)?;
    let host = url.host_str().ok_or_else(|| {
        Error::invalid_setting(
            "postllm.base_url",
            format!("must include a hostname, got '{base_url}'"),
            "SET postllm.base_url = 'http://127.0.0.1:11434/v1/chat/completions' or another URL with a host",
        )
    })?;
    let port = url.port_or_known_default();
    let host_and_port = port.map(|port| format!("{host}:{port}"));

    Ok(allowed_hosts.iter().any(|entry| {
        entry.strip_prefix("*.").map_or_else(
            || {
                host_and_port
                    .as_deref()
                    .is_some_and(|host_and_port| entry == host || entry == host_and_port)
                    || entry == host
            },
            |suffix| host == suffix || host.ends_with(&format!(".{suffix}")),
        )
    }))
}

fn provider_is_allowed(provider: &str, allowed_providers: &[String]) -> bool {
    allowed_providers.is_empty()
        || allowed_providers
            .iter()
            .any(|entry| entry == "*" || entry == provider)
}

fn parse_absolute_base_url(base_url: &str) -> Result<Url> {
    Url::parse(base_url).map_err(|_| {
        Error::invalid_setting(
            "postllm.base_url",
            format!("must be a valid absolute URL, got '{base_url}'"),
            "SET postllm.base_url = 'http://127.0.0.1:11434/v1/chat/completions' or another valid absolute URL",
        )
    })
}

fn parse_csv_setting(raw: Option<&str>) -> Vec<String> {
    raw.into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn is_valid_exact_host_entry(entry: &str) -> bool {
    let candidate = if entry.contains(':') {
        format!("http://{entry}")
    } else {
        format!("http://{entry}/")
    };

    Url::parse(&candidate)
        .ok()
        .is_some_and(|url| url.host_str().is_some())
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "tests should fail loudly with focused messages when endpoint-policy fixtures break"
)]
mod tests {
    use super::{enforce_settings, parse_allowed_hosts, parse_allowed_providers, summarize};
    use crate::backend::test_support::SettingsBuilder;
    use crate::backend::{Runtime, Settings};

    fn settings(base_url: Option<&str>) -> Settings {
        let builder = SettingsBuilder::new().runtime(Runtime::OpenAi);

        match base_url {
            Some(base_url) => builder.base_url(base_url).build(),
            None => builder.no_base_url().build(),
        }
    }

    #[test]
    fn summarize_should_derive_provider_and_discovery_metadata() {
        let summary = summarize(&settings(Some(
            "http://127.0.0.1:11434/v1/chat/completions",
        )));

        assert_eq!(summary.provider, "ollama");
        assert_eq!(summary.base_url_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(summary.base_url_kind, Some("loopback"));
        assert_eq!(
            summary.discovery_url.as_deref(),
            Some("http://127.0.0.1:11434/v1/models")
        );
    }

    #[test]
    fn summarize_should_derive_anthropic_provider_and_models_url() {
        let summary = summarize(&settings(Some("https://api.anthropic.com/v1/messages")));

        assert_eq!(summary.provider, "anthropic");
        assert_eq!(summary.base_url_host.as_deref(), Some("api.anthropic.com"));
        assert_eq!(summary.base_url_kind, Some("remote"));
        assert_eq!(
            summary.discovery_url.as_deref(),
            Some("https://api.anthropic.com/v1/models")
        );
    }

    #[test]
    fn parse_allowed_hosts_should_accept_suffix_and_host_port_entries() {
        let parsed = parse_allowed_hosts(
            Some("*.openai.com,host.docker.internal:11434"),
            "postllm.http_allowed_hosts",
        )
        .expect("host policy should parse");

        assert_eq!(
            parsed,
            vec![
                "*.openai.com".to_owned(),
                "host.docker.internal:11434".to_owned()
            ]
        );
    }

    #[test]
    fn parse_allowed_providers_should_accept_anthropic() {
        let parsed =
            parse_allowed_providers(Some("openai,anthropic"), "postllm.http_allowed_providers")
                .expect("provider policy should parse");

        assert_eq!(parsed, vec!["openai".to_owned(), "anthropic".to_owned()]);
    }

    #[test]
    fn parse_allowed_providers_should_reject_unknown_entries() {
        let error = parse_allowed_providers(Some("bedrock"), "postllm.http_allowed_providers")
            .expect_err("provider policy should reject unsupported providers");

        assert!(
            error
                .to_string()
                .contains("contains unsupported provider 'bedrock'")
        );
    }

    #[test]
    fn enforce_settings_should_allow_matching_host_and_provider_rules() {
        let mut settings = settings(Some("https://api.openai.com/v1/chat/completions"));
        settings.http_allowed_hosts = vec!["api.openai.com".to_owned()];
        settings.http_allowed_providers = vec!["openai".to_owned()];

        enforce_settings(&settings).expect("policy should allow matching endpoint");
    }

    #[test]
    fn enforce_settings_should_reject_disallowed_hosts() {
        let mut settings = settings(Some("https://api.openai.com/v1/chat/completions"));
        settings.http_allowed_hosts = vec!["host.docker.internal:11434".to_owned()];

        let error = enforce_settings(&settings).expect_err("host policy should reject");

        assert!(
            error
                .to_string()
                .contains("postllm.base_url host 'api.openai.com' is not permitted")
        );
    }

    #[test]
    fn enforce_settings_should_reject_disallowed_providers() {
        let mut settings = settings(Some("https://api.openai.com/v1/chat/completions"));
        settings.http_allowed_providers = vec!["ollama".to_owned()];

        let error = enforce_settings(&settings).expect_err("provider policy should reject");

        assert!(
            error
                .to_string()
                .contains("provider 'openai' is not permitted")
        );
    }
}
