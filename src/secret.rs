use anyhow::Context;
use iroh::SecretKey;
use std::str::FromStr;

/// Get the secret key or generate a new one.
pub fn get_or_create_secret() -> anyhow::Result<SecretKey> {
    match std::env::var("IROH_SECRET") {
        Ok(secret) => SecretKey::from_str(&secret).context("invalid secret"),
        Err(_) => Ok(SecretKey::generate()),
    }
}
