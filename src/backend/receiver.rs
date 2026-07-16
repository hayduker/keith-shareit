//! This module contains the logic for the receiver side of the media sharing application.
//! It handles accepting incoming connections, receiving synchronization commands, downloading
//! blobs (files or collections) from a sender, and exporting them to a destination directory.
//!

use std::{
    io::{self, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, ConnectionError, RecvStream},
};
use iroh_blobs::{
    api::remote::GetProgressItem, format::collection::Collection,
    get::request::get_hash_seq_and_sizes,
};
use n0_future::StreamExt;
use tokio::sync::mpsc::Receiver;

use crate::{
    backend::store::KeithStore,
    frontend::TuiCommand,
    sender::{SyncCommand, shortened_hash},
};

/// The main event loop for the receiver. It continuously listens for incoming
/// synchronization commands from a sender, processes them, and downloads the
/// requested content.
///
/// This loop handles:
/// - Closing the connection if the sender disconnects.
/// - Shutting down gracefully upon receiving a [`TuiCommand::Shutdown`].
/// - Accepting unidirectional streams from the sender, parsing them into [`SyncCommand`]s.
/// - Initiating blob downloads based on the received commands.
///
/// # Arguments
///
/// * `connection` - The established Iroh [`Connection`] with the sender.
/// * `endpoint` - The Iroh [`Endpoint`] used for communication.
/// * `target_addr` - The [`EndpointAddr`] of the sender.
/// * `store` - A [`KeithStore`] instance for managing blob data.
/// * `dst_dir` - The destination directory where received files will be exported.
/// * `command_rx` - A receiver for [`TuiCommand`]s from the frontend, mainly for shutdown.
///
/// # Returns
///
/// A `Result` indicating success or failure of the receiver loop. Returns `Ok(())`
/// on graceful shutdown or sender disconnection, and an `Err` on unrecoverable errors.
///
/// # Errors
///
/// - [`anyhow::Error`] if an unsupported [`TuiCommand`] is received.
/// - [`anyhow::Error`] if there's an error accepting a unidirectional stream that isn't a graceful shutdown.
/// - [`anyhow::Error`] propagated from `read_command_from_stream` or `download_blob`.
pub async fn run_loop(
    connection: Connection,
    endpoint: Endpoint,
    target_addr: EndpointAddr,
    store: KeithStore,
    dst_dir: PathBuf,
    mut command_rx: Receiver<TuiCommand>,
) -> Result<()> {
    loop {
        println!("\nReceiver awaiting next incoming sync commands");

        tokio::select! {
            _ = connection.closed() => {
                println!("Sender disconnected, feel free to quit");
                return Ok(());
            }
            cmd = command_rx.recv() => {
                println!("Got command");

                match cmd {
                    Some(TuiCommand::Shutdown) | None => {
                        println!("Got Shutdown command from ui");
                        break
                    }
                    _ => {
                        anyhow::bail!("Receiver got unsupported TuiCommand: {:?}", cmd);
                    }
                }
            }
            stream_result = connection.accept_uni() => {
                match stream_result {
                    Ok(mut recv_stream) => {
                        match read_command_from_stream(&mut recv_stream).await {
                            Ok(command) => {
                                println!("Received sync command for hash: {}", shortened_hash(&command.hash_and_format.hash));
                                download_blob(&endpoint, &store, &target_addr, command, dst_dir.clone()).await?;
                            }
                            Err(e) => eprintln!("Failed to parse incoming stream data: {:?}", e),
                        }
                    }
                    Err(e) => {
                        if let ConnectionError::ApplicationClosed(close) = e.clone() &&
                            close.error_code.into_inner() == 0 &&
                            close.reason == Bytes::from_static(b"graceful shutdown") {
                            println!("Sender disconnected, feel free to quit");
                            break;
                        }
                        anyhow::bail!("Error accepting unidirectional stream: {:?}", e);
                    }
                }
            }
        }
    }

    connection.close(0u8.into(), b"shutdown");

    Ok(())
}

/// Downloads a blob (file or collection) from the sender.
///
/// This function first checks if the blob is already complete locally. If not, it establishes
/// a connection back to the sender using the blobs ALPN, requests the hash sequence and sizes,
/// and then executes the get operation to download missing parts of the blob. Progress updates
/// are printed to stdout. After download, if it's a collection, it's loaded and exported
/// to the specified destination directory.
///
/// # Arguments
///
/// * `endpoint` - The Iroh [`Endpoint`] used for communication.
/// * `store` - A reference to the [`KeithStore`] for blob storage and management.
/// * `target_addr` - The [`EndpointAddr`] of the sender.
/// * `command` - The [`SyncCommand`] containing information about the blob to download.
/// * `dst_dir` - The destination directory where the blob will be exported.
///
/// # Returns
///
/// A `Result` indicating the success or failure of the download and export process.
///
/// # Errors
///
/// - [`anyhow::Error`] if the local store query fails.
/// - [`anyhow::Error`] if connecting back to the sender fails.
/// - [`anyhow::Error`] if fetching hash sequence and sizes fails.
/// - [`anyhow::Error`] if an Iroh fetch error occurs during download.
/// - [`anyhow::Error`] if loading the collection fails.
/// - [`anyhow::Error`] if exporting the collection fails.
///
/// # Examples
///
/// ```no_run
/// # use keith_shareit::backend::receiver::download_blob;
/// # use keith_shareit::backend::store::KeithStore;
/// # use keith_shareit::sender::SyncCommand;
/// # use iroh::{Endpoint, EndpointAddr, PeerId};
/// # use std::path::PathBuf;
/// # use iroh_blobs::Hash;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// # let store = KeithStore::new_mem().await?;
/// # let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal).bind().await?;
/// # let target_addr = EndpointAddr::new(PeerId::from_bytes([0; 32]), 1234.into());
/// # let dummy_hash = Hash::new([0; 32]);
/// # let command = SyncCommand { hash_and_format: iroh_blobs::HashAndFormat::raw(dummy_hash), path: "test.txt".into() };
/// // In a real scenario, you would have an actual connection and a valid command.
/// // This example just shows the function signature and expected parameters.
/// // download_blob(&endpoint, &store, &target_addr, command, PathBuf::from("/tmp")).await?;
/// # Ok(())
/// # }
/// ```
pub async fn download_blob(
    endpoint: &Endpoint,
    store: &KeithStore,
    target_addr: &EndpointAddr,
    command: SyncCommand,
    dst_dir: PathBuf,
) -> Result<()> {
    let local = store.db.remote().local(command.hash_and_format).await?;
    if !local.is_complete() {
        let connection = endpoint
            .connect(target_addr.clone(), iroh_blobs::protocol::ALPN)
            .await?;

        println!("Made connection back to sender");

        let (_, sizes) = get_hash_seq_and_sizes(
            &connection,
            &command.hash_and_format.hash,
            1024 * 1024 * 32,
            None,
        )
        .await?;
        let total_size = sizes.iter().copied().sum::<u64>();

        let get = store.db.remote().execute_get(connection, local.missing());
        let mut stream = get.stream();

        let mut next_checkpoint = 0;

        while let Some(item) = stream.next().await {
            match item {
                GetProgressItem::Progress(offset) => {
                    let percentage = 100 * offset / total_size;
                    if percentage > next_checkpoint && next_checkpoint < 100 {
                        print!("\rDownloading blob: {percentage}%");
                        io::stdout().flush()?;

                        next_checkpoint += 1;
                    }
                }
                GetProgressItem::Done(_) => {
                    println!("\nDownload complete");
                }
                GetProgressItem::Error(cause) => {
                    anyhow::bail!("Iroh fetch error: {:?}", cause);
                }
            }
        }
    };

    let collection = Collection::load(command.hash_and_format.hash, store.db.as_ref()).await?;

    if let Some((name, _)) = collection.iter().next()
        && let Some(first) = name.split('/').next()
    {
        println!("Exporting to: '{first}'");
    }
    store.export(collection, command.path, dst_dir).await?;
    println!("Export complete");

    Ok(())
}

/// Reads a [`SyncCommand`] from an incoming unidirectional Iroh receive stream.
///
/// This function reads bytes from the provided `RecvStream`, attempts to deserialize
/// them into a [`SyncCommand`] using `postcard`.
///
/// # Arguments
///
/// * `recv_stream` - A mutable reference to the [`RecvStream`] from which to read the command.
///
/// # Returns
///
/// A `Result` containing the deserialized [`SyncCommand`] on success.
///
/// # Errors
///
/// - [`anyhow::Error`] if reading from the stream fails (e.g., buffer issues or stream closure).
/// - [`postcard::Error`] if deserialization of the bytes into `SyncCommand` fails.
async fn read_command_from_stream(recv_stream: &mut RecvStream) -> Result<SyncCommand> {
    let bytes = recv_stream
        .read_to_end(10000)
        .await
        .context("Failed reading from incoming stream buffer")?;

    let command: SyncCommand = postcard::from_bytes(&bytes)?;
    Ok(command)
}
