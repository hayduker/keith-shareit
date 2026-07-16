//! This module contains the logic for the sender side of the media sharing application.
//! It manages the connection to a receiver, imports files into the blob store,
//! and sends synchronization commands to initiate file transfers.

use anyhow::{Context, Result};
use iroh::endpoint::Connection;
use iroh_blobs::{Hash, HashAndFormat, api::TempTag};
use serde::{Deserialize, Serialize};
use std::{ffi::OsStr, path::PathBuf};
use tokio::sync::mpsc;

use crate::{
    backend::{BackendEvent, store::KeithStore},
    frontend::TuiCommand,
};

/// Represents a command sent from the sender to the receiver to initiate a file synchronization.
/// It contains the hash and format of the blob to be transferred, and the relative path
/// where the receiver should export the file.
#[derive(Serialize, Deserialize, Debug)]
pub struct SyncCommand {
    /// The [`HashAndFormat`] of the blob to be synchronized.
    pub hash_and_format: HashAndFormat,
    /// The relative path where the receiver should store the file.
    pub path: PathBuf,
}

/// The main event loop for the sender. It continuously listens for commands from the
/// frontend to initiate file transfers, and handles the connection to the receiver.
///
/// This loop handles:
/// - Sending status updates to the frontend.
/// - Detecting when the receiver disconnects and breaking the loop.
/// - Processing [`TuiCommand::SyncPath`] to import and send files.
/// - Handling [`TuiCommand::Shutdown`] to gracefully terminate the sender.
///
/// # Arguments
///
/// * `connection` - The established Iroh [`Connection`] with the receiver.
/// * `store` - A [`KeithStore`] instance for managing blob data.
/// * `command_rx` - A mutable receiver for [`TuiCommand`]s from the frontend.
/// * `event_tx` - A sender for [`BackendEvent`]s, used to report status updates to the frontend.
///
/// # Returns
///
/// A `Result` indicating success or failure of the sender loop. Returns `Ok(())`
/// on graceful shutdown or receiver disconnection, and an `Err` on unrecoverable errors.
///
/// # Errors
///
/// - Propagates errors from `send_notification` if a file transfer fails.
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
                event_tx.send(BackendEvent::StatusUpdate("Receiver disconnected, feel free to quit".into())).await.ok();
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
                    _ => {}
                }
            }
        }
    }

    active_tags.clear();
    connection.close(0u8.into(), b"graceful shutdown");

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

/// Returns a shortened, human-readable string representation of an Iroh [`Hash`].
/// This is primarily used for logging and displaying concise identifiers in the TUI.
///
/// # Arguments
///
/// * `id` - A reference to the Iroh [`Hash`] to shorten.
///
/// # Returns
///
/// A `String` containing the first 8 characters of the hash, followed by "...".
///
/// # Panics
///
/// Panics if the hash string representation is less than 8 characters long (which should not happen with valid Iroh hashes).
///
/// # Examples
///
/// ```
/// # use keith_shareit::sender::shortened_hash;
/// # use iroh_blobs::Hash;
/// let hash_str = "1234567890abcdefedcba0987654321";
/// let hash = Hash::new(hex::decode(hash_str).unwrap().try_into().unwrap());
/// assert_eq!(shortened_hash(&hash), "12345678...");
/// ```
pub fn shortened_hash(id: &Hash) -> String {
    id.to_string()
        .get(0..8)
        .expect("Couldn't shorten hash")
        .to_string()
        + "..."
}
