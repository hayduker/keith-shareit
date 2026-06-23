use anyhow::Result;
use iroh::{
    Endpoint, EndpointId,
    endpoint::{Incoming, presets},
};
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::secret::get_or_create_secret;

// The unique ALPN identifier for your custom notification channel
pub const NOTIFY_ALPN: &[u8] = b"music-sync/notify/0.1.0";

#[derive(Serialize, Deserialize, Debug)]
pub enum SyncCommand {
    /// Tell the phone to fetch a specific hash (could be a single blob or a HashSeq/Collection)
    FetchBlob { hash: Hash, path: PathBuf },
}

pub async fn notify_receiver_to_download(blob_hash: Hash, blob_path: PathBuf) -> Result<()> {
    let secret_key = get_or_create_secret()?;
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
        .secret_key(secret_key)
        .bind()
        .await?;

    let receiver_id = EndpointId::from_bytes(&[0 as u8; 32]);

    let conn = endpoint.connect(receiver_id, NOTIFY_ALPN).await?;
    println!("Connected to receiver successfull.");

    let mut send_stream = conn.open_uni().await?;

    let command = SyncCommand::FetchBlob {
        hash: blob_hash,
        path: blob_path,
    };
    let payload = postcard::to_vec(&command)?;

    send_stream.write_all(&payload).await?;
    send_stream.finish().await?;

    println!("Notification sent to phone.");
    Ok(())
}

pub async fn start_reciever_listener() -> Result<()> {
    let secret = get_or_create_secret()?;
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![NOTIFY_ALPN.to_vec()])
        .secret_key(secret)
        .bind()
        .await?;

    println!(
        "Receiver listening loop active. EndpointID: {}",
        endpoint.id()
    );

    // Main connection acceptance loop
    while let Some(incoming) = endpoint.accept().await {
        tokio::spawn(async move {
            if let Err(e) = handle_incoming_connection(incoming).await {
                eprintln!("Error handling incoming sync request: {:?}", e);
            }
        });
    }

    Ok(())
}

async fn handle_incoming_connection(incoming: Incoming) -> Result<()> {
    let conn = incoming.await?;
    println!("Accepted connection from: {}", conn.remote_node_id()?);

    let mut recv_stream = conn.accept_uni().await?;
    let buffer = recv_stream.read_to_end(100000).await?;

    let command: SyncCommand = bincode::deserialize(&buffer)?;

    match command {
        SyncCommand::FetchBlob { hash, path } => {
            println!("Received request to download: '{}'", collection_name);
            println!("Target Hash: {}", hash);

            // TODO: Pass this hash to your local iroh-blobs download client loop
            // e.g., blobs_client.download(hash, laptop_node_id).await;
        }
    }

    Ok(())
}
