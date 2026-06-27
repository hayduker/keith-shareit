use anyhow::{Context, Result};
use iroh::SecretKey;
use std::{fs, path::Path};

/// Get the secret key from disk or generate a new one if it doesn't exist.
pub fn get_or_create_secret() -> Result<SecretKey> {
    let key_path = Path::new("endpoint.key");

    if key_path.exists() {
        let bytes = fs::read(key_path).context("Failed to read existing secret key file")?;

        // Ensure the file contains exactly 32 bytes
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("Saved secret key must be exactly 32 bytes"))?;

        Ok(SecretKey::from_bytes(&bytes))
    } else {
        let key = SecretKey::generate();

        // Write the raw 32 bytes directly to the file
        fs::write(key_path, key.to_bytes())
            .context("Failed to write newly generated secret key to disk")?;

        Ok(key)
    }
}
