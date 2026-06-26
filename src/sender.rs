use anyhow::{Context, Result};
use iroh::endpoint::Connection;
use iroh_blobs::{Hash, HashAndFormat, api::TempTag};
use serde::{Deserialize, Serialize};
use std::{ffi::OsStr, path::PathBuf};
use tokio::sync::mpsc;

use crate::{
    event::{BackendEvent, TuiCommand},
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
    loop {
        event_tx
            .send(BackendEvent::StatusUpdate(String::new()))
            .await
            .ok();

        event_tx
            .send(BackendEvent::StatusUpdate(
                "Sender ready for next user selection".into(),
            ))
            .await
            .ok();

        tokio::select! {
            _ = connection.closed() => {
                event_tx.send(BackendEvent::StatusUpdate("Receiver disconnected, shutting down".into())).await.ok();
                break;
            }
            cmd = command_rx.recv() => {
                match cmd {
                    Some(TuiCommand::SyncPath(full_path, root_path)) => {
                        match send_notification(&connection, &store, full_path, root_path, &event_tx).await {
                            Ok(tag) => {
                                active_tags.push(tag);
                            }
                            Err(e) => {
                                event_tx.send(BackendEvent::StatusUpdate(format!("Error: {}", e))).await.ok();
                            }
                        }
                    }
                    Some(TuiCommand::Shutdown) | None => {
                        event_tx.send(BackendEvent::StatusUpdate("Backend shutting down".into())).await.ok();
                        break;
                    }
                }
            }
        }
    }

    active_tags.clear();
    connection.close(0u8.into(), b"shutdown");

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
        .send(BackendEvent::StatusUpdate(format!(
            "Sharing: {:?}",
            full_path.file_name().unwrap_or(OsStr::new("None"))
        )))
        .await
        .ok();

    let mut send_stream = connection
        .open_uni()
        .await
        .context("Failed to open stream")?;

    event_tx
        .send(BackendEvent::StatusUpdate("Opened send stream".into()))
        .await
        .ok();

    event_tx
        .send(BackendEvent::StatusUpdate("Importing blob".into()))
        .await
        .ok();

    let tag = store.import(full_path.clone()).await?;

    let relative_path = full_path.strip_prefix(root_path)?;

    event_tx
        .send(BackendEvent::StatusUpdate(format!(
            "Sending sync command with hash: {}",
            shortened_hash(&tag.hash()),
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
        .send(BackendEvent::StatusUpdate(
            "Sync command sent, blob ready for transfer".into(),
        ))
        .await
        .ok();

    Ok(tag)
}

pub fn shortened_hash(id: &Hash) -> String {
    id.to_string()
        .get(0..8)
        .expect("Couldn't shorten hash")
        .to_string()
        + "..."
}
