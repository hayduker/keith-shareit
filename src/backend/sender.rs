use anyhow::{Context, Result};
use iroh::endpoint::Connection;
use iroh_blobs::{HashAndFormat, api::TempTag};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::sync::mpsc;

use crate::{
    backend::{BackendEvent, TuiCommand},
    store::KeithStore,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncCommand {
    pub hash_and_format: HashAndFormat,
    pub path: PathBuf,
}

pub async fn run_loop(
    connection: Connection,
    store: KeithStore,
    mut command_rx: mpsc::Receiver<TuiCommand>,
    event_tx: mpsc::Sender<BackendEvent>,
) -> Result<()> {
    let mut active_tags = vec![];
    event_tx
        .send(BackendEvent::StatusUpdate(
            "Sender active. Ready for UI inputs.".into(),
        ))
        .await
        .ok();

    loop {
        tokio::select! {
            _ = connection.closed() => {
                event_tx.send(BackendEvent::StatusUpdate("Receiver disconnected.".into())).await.ok();
                println!("Receiver disconnected. Exiting sender loop.");
                break;
            }
            cmd = command_rx.recv() => {
                match cmd {
                    Some(TuiCommand::SyncPath(path)) => {
                        event_tx.send(BackendEvent::StatusUpdate(format!("Importing: {:?}", path))).await.ok();
                        match send_notification(&connection, &store, path).await {
                            Ok(tag) => {
                                active_tags.push(tag);
                                event_tx.send(BackendEvent::StatusUpdate("Sync metadata broadcast complete.".into())).await.ok();
                            }
                            Err(e) => {
                                event_tx.send(BackendEvent::StatusUpdate(format!("Error: {}", e))).await.ok();
                            }
                        }
                    }
                    Some(TuiCommand::Shutdown) | None => {
                        event_tx.send(BackendEvent::StatusUpdate("Backend shutting down...".into())).await.ok();
                        break;
                    }
                }
            }
        }
    }

    println!("Cleaning up...");

    active_tags.clear();
    store.cleanup().await?;
    connection.close(0u8.into(), b"shutdown");

    println!("Shutting down.");

    Ok(())
}

async fn send_notification(
    connection: &Connection,
    store: &KeithStore,
    path: PathBuf,
) -> Result<TempTag> {
    println!("Going to send notification");

    let mut send_stream = connection
        .open_uni()
        .await
        .context("Failed to open stream")?;

    println!("Got SendStream {}", send_stream.id());

    println!("Importing...");

    let tag = store.import(path.clone()).await?;

    println!(
        "Sending SyncCommand with hash {} and path {:?}",
        tag.hash(),
        path
    );

    let command = SyncCommand {
        hash_and_format: tag.hash_and_format(),
        path,
    };
    let payload = postcard::to_stdvec(&command)?;
    send_stream.write_all(&payload).await?;
    send_stream
        .finish()
        .context("Failed to close send stream")?;

    println!("Sync command sent");

    Ok(tag)
}
