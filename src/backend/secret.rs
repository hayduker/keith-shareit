use anyhow::{Context, Result};
use iroh::SecretKey;
use std::{fs, path::Path};

/// Get the secret key from disk or generate a new one if it doesn't exist.
pub fn get_or_create_secret() -> Result<SecretKey> {
    let key_path = Path::new("endpoint.key");

    if key_path.exists() {
        read_existing_key(key_path)
    } else {
        create_and_write_new_key(key_path)
    }
}

pub fn read_existing_key(path: &Path) -> Result<SecretKey> {
    let bytes = fs::read(path).context("Failed to read existing secret key file")?;

    // Ensure the file contains exactly 32 bytes
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("Saved secret key must be exactly 32 bytes"))?;

    Ok(SecretKey::from_bytes(&bytes))
}

fn create_and_write_new_key(path: &Path) -> Result<SecretKey> {
    let key = SecretKey::generate();

    // Write the raw 32 bytes directly to the file
    fs::write(path, key.to_bytes())
        .context("Failed to write newly generated secret key to disk")?;

    Ok(key)
}
