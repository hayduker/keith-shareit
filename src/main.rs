use anyhow::Result;
use iroh::{Endpoint, SecretKey, endpoint::presets};

#[tokio::main]
async fn main() -> Result<()> {
    let secret_key = find_or_create_secret_key("endpoint.key")?;

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .bind()
        .await?;

    endpoint.online().await;

    let endpoint_id = endpoint.id();

    println!("My endpoint ID = {}", endpoint_id);

    Ok(())
}

fn find_or_create_secret_key(filename: &str) -> Result<SecretKey> {
    match std::fs::read(filename) {
        Ok(bytes) => Ok(SecretKey::from_bytes(&bytes.as_slice().try_into()?)),
        Err(_) => {
            let key = SecretKey::generate();
            std::fs::write(filename, key.to_bytes())?;
            Ok(key)
        }
    }
}
