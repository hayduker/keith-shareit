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
                break;
            }
            cmd = command_rx.recv() => {
                match cmd {
                    Some(TuiCommand::SyncPath(full_path, root_path)) => {
                        event_tx.send(BackendEvent::StatusUpdate(format!("Importing: {:?}", full_path))).await.ok();
                        match send_notification(&connection, &store, full_path, root_path, &event_tx).await {
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

    event_tx
        .send(BackendEvent::StatusUpdate("Cleaning up...".into()))
        .await
        .ok();

    active_tags.clear();
    connection.close(0u8.into(), b"shutdown");

    event_tx
        .send(BackendEvent::StatusUpdate("Shutting down.".into()))
        .await
        .ok();

    Ok(())
}

async fn send_notification(
    connection: &Connection,
    store: &KeithStore,
    full_path: PathBuf,
    root_path: PathBuf,
    event_tx: &mpsc::Sender<BackendEvent>,
) -> Result<TempTag> {
    event_tx
        .send(BackendEvent::StatusUpdate(
            "Going to send notification".into(),
        ))
        .await
        .ok();

    let mut send_stream = connection
        .open_uni()
        .await
        .context("Failed to open stream")?;

    event_tx
        .send(BackendEvent::StatusUpdate(format!(
            "Got SendStream {}",
            send_stream.id()
        )))
        .await
        .ok();

    event_tx
        .send(BackendEvent::StatusUpdate("Importing...".into()))
        .await
        .ok();

    let tag = store.import(full_path.clone()).await?;

    let relative_path = full_path.strip_prefix(root_path)?;

    event_tx
        .send(BackendEvent::StatusUpdate(format!(
            "Sending SyncCommand with hash {} and path {:?}",
            tag.hash(),
            relative_path
        )))
        .await
        .ok();

    let command = SyncCommand {
        hash_and_format: tag.hash_and_format(),
        path: relative_path.to_path_buf(),
    };
    let payload = postcard::to_stdvec(&command)?;
    send_stream.write_all(&payload).await?;
    send_stream
        .finish()
        .context("Failed to close send stream")?;

    event_tx
        .send(BackendEvent::StatusUpdate("Sync command sent".into()))
        .await
        .ok();

    Ok(tag)
}
