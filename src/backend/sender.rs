use anyhow::{Context, Result};
use iroh::endpoint::Connection;
use iroh_blobs::{HashAndFormat, api::TempTag};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, str::FromStr};
use tokio::io::AsyncBufReadExt;

use crate::store::KeithStore;

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncCommand {
    pub hash_and_format: HashAndFormat,
    pub path: PathBuf,
}

pub async fn run_loop(connection: &Connection, store: &KeithStore) -> Result<()> {
    let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    let mut line = String::new();

    let mut active_tags = vec![];

    loop {
        println!("\nPress any key to trigger a test SyncCommand transmission...");
        tokio::select! {
            _ = connection.closed() => {
                println!("Receiver disconnected. Exiting sender loop.");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down.");
                connection.close(1u8.into(), b"done");
            }
            read_result = reader.read_line(&mut line) => {
                read_result.context("Failed to read stdin")?;

                println!("User triggered sync action!");

                let path_to_send = PathBuf::from_str(line.trim())?;

                println!("User entered >{}<, path = {:?}", line, path_to_send);

                match send_download_notification(connection, path_to_send, store).await {
                    Ok(tag) => {
                        active_tags.push(tag);
                    }
                    Err(e) => eprintln!("Error sending notification: {:?}", e)
                }

                line.clear();
            }
        }
    }

    println!("Cleaning up...");

    active_tags.clear();
    store.cleanup().await?;
    connection.closed().await;

    println!("Shutting down.");

    Ok(())
}

async fn send_download_notification(
    connection: &Connection,
    blob_path: PathBuf,
    store: &KeithStore,
) -> Result<TempTag> {
    println!("Going to send notification");

    let mut send_stream = connection
        .open_uni()
        .await
        .context("failed to open unidirectional connection")?;

    println!("Got SendStream {}", send_stream.id());

    println!("Importing...");

    let tag = store.import(blob_path.clone()).await?;

    println!(
        "Sending SyncCommand with hash {} and path {:?}",
        tag.hash(),
        blob_path
    );

    let command = SyncCommand {
        hash_and_format: tag.hash_and_format(),
        path: blob_path,
    };
    let payload = postcard::to_stdvec(&command)?;
    send_stream.write_all(&payload).await?;
    send_stream
        .finish()
        .context("failed to finish send stream")?;

    println!("Sync command sent");

    Ok(tag)
}
