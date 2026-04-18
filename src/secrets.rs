#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private APIs across sibling modules"
)]

use crate::error::{Error, Result};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::aead::{Aad, CHACHA20_POLY1305, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

const SECRET_ALGORITHM: &str = "chacha20poly1305-v1";
const SECRET_KEY_ENV: &str = "POSTLLM_SECRET_KEY";
const SECRET_FINGERPRINT_LEN: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredSecret {
    pub(crate) algorithm: String,
    pub(crate) nonce: String,
    pub(crate) ciphertext: String,
    pub(crate) key_fingerprint: String,
}

pub(crate) fn encrypt_secret(name: &str, value: &str) -> Result<StoredSecret> {
    let material = master_key_material()?;
    let key = build_key(&material.derived_key)?;
    let mut nonce_bytes = [0_u8; NONCE_LEN];
    SystemRandom::new().fill(&mut nonce_bytes).map_err(|_| {
        Error::Internal("failed to generate a random nonce for secret encryption".to_owned())
    })?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);

    let mut buffer = value.as_bytes().to_vec();
    key.seal_in_place_append_tag(nonce, Aad::from(name.as_bytes()), &mut buffer)
        .map_err(|_| Error::Internal("failed to encrypt provider secret".to_owned()))?;

    Ok(StoredSecret {
        algorithm: SECRET_ALGORITHM.to_owned(),
        nonce: BASE64.encode(nonce_bytes),
        ciphertext: BASE64.encode(buffer),
        key_fingerprint: material.key_fingerprint,
    })
}

pub(crate) fn decrypt_secret(name: &str, secret: &StoredSecret) -> Result<String> {
    if secret.algorithm != SECRET_ALGORITHM {
        return Err(Error::Config(format!(
            "stored secret '{name}' uses unsupported algorithm '{}'; fix: rewrite it with postllm.secret_set(...) on this postllm version",
            secret.algorithm
        )));
    }

    let material = master_key_material()?;
    if secret.key_fingerprint != material.key_fingerprint {
        return Err(Error::Config(format!(
            "stored secret '{name}' was encrypted under a different POSTLLM_SECRET_KEY fingerprint; fix: restore the original POSTLLM_SECRET_KEY or rewrite the secret with postllm.secret_set(...)"
        )));
    }

    let nonce_bytes = decode_nonce(&secret.nonce, name)?;
    let key = build_key(&material.derived_key)?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut ciphertext = BASE64.decode(secret.ciphertext.as_bytes()).map_err(|error| {
        Error::Config(format!(
            "stored secret '{name}' ciphertext was not valid base64 ({error}); fix: rewrite it with postllm.secret_set(...)"
        ))
    })?;

    let plaintext = key
        .open_in_place(nonce, Aad::from(name.as_bytes()), &mut ciphertext)
        .map_err(|_| {
            Error::Config(format!(
                "stored secret '{name}' could not be decrypted with the current POSTLLM_SECRET_KEY; fix: restore the original POSTLLM_SECRET_KEY or rewrite the secret with postllm.secret_set(...)"
            ))
        })?;

    String::from_utf8(plaintext.to_vec()).map_err(|error| {
        Error::Config(format!(
            "stored secret '{name}' decrypted to invalid UTF-8 ({error}); fix: rewrite it with postllm.secret_set(...)"
        ))
    })
}

fn decode_nonce(encoded: &str, name: &str) -> Result<[u8; NONCE_LEN]> {
    let decoded = BASE64.decode(encoded.as_bytes()).map_err(|error| {
        Error::Config(format!(
            "stored secret '{name}' nonce was not valid base64 ({error}); fix: rewrite it with postllm.secret_set(...)"
        ))
    })?;

    <[u8; NONCE_LEN]>::try_from(decoded.as_slice()).map_err(|_| {
        Error::Config(format!(
            "stored secret '{name}' nonce had the wrong length; fix: rewrite it with postllm.secret_set(...)"
        ))
    })
}

fn build_key(derived_key: &[u8; 32]) -> Result<LessSafeKey> {
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, derived_key).map_err(|_| {
        Error::Internal("failed to construct the symmetric key for secret storage".to_owned())
    })?;
    Ok(LessSafeKey::new(unbound))
}

fn master_key_material() -> Result<MasterKeyMaterial> {
    let raw = std::env::var(SECRET_KEY_ENV).map_err(|_| {
        Error::missing_setting(
            SECRET_KEY_ENV,
            "set the POSTLLM_SECRET_KEY server environment variable before using postllm.secret_set(...) or postllm.configure(api_key_secret => ...)",
        )
    })?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::invalid_setting(
            SECRET_KEY_ENV,
            "must not be empty or whitespace-only",
            "set POSTLLM_SECRET_KEY to a long random string before using postllm secret helpers",
        ));
    }

    let digest = Sha256::digest(trimmed.as_bytes());
    let mut derived_key = [0_u8; 32];
    derived_key.copy_from_slice(&digest);

    Ok(MasterKeyMaterial {
        derived_key,
        key_fingerprint: hex_prefix(&digest, SECRET_FINGERPRINT_LEN),
    })
}

fn hex_prefix(bytes: &[u8], chars: usize) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded.truncate(chars.min(encoded.len()));
    encoded
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MasterKeyMaterial {
    derived_key: [u8; 32],
    key_fingerprint: String,
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "unit tests use expect-style assertions for clearer failure context"
)]
mod tests {
    use super::{StoredSecret, decrypt_secret, encrypt_secret};

    #[test]
    fn stored_secrets_should_round_trip() {
        // This test depends on a stable local key when the developer opts in.
        let Ok(_) = std::env::var("POSTLLM_SECRET_KEY") else {
            return;
        };

        let encrypted = encrypt_secret("openai-prod", "sk-test-secret")
            .expect("secret encryption should succeed");
        let decrypted =
            decrypt_secret("openai-prod", &encrypted).expect("secret decryption should succeed");

        assert_eq!(decrypted, "sk-test-secret");
        assert_eq!(encrypted.algorithm, "chacha20poly1305-v1");
        assert!(!encrypted.key_fingerprint.is_empty());
    }

    #[test]
    fn decrypt_should_reject_mismatched_algorithm() {
        let Ok(_) = std::env::var("POSTLLM_SECRET_KEY") else {
            return;
        };

        let error = decrypt_secret(
            "openai-prod",
            &StoredSecret {
                algorithm: "legacy".to_owned(),
                nonce: "AA==".to_owned(),
                ciphertext: "AA==".to_owned(),
                key_fingerprint: "deadbeef".to_owned(),
            },
        )
        .expect_err("unsupported algorithms should be rejected");

        assert!(
            error
                .to_string()
                .contains("uses unsupported algorithm 'legacy'")
        );
    }
}
