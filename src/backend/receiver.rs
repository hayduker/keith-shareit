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

async fn read_command_from_stream(recv_stream: &mut RecvStream) -> Result<SyncCommand> {
    let bytes = recv_stream
        .read_to_end(10000)
        .await
        .context("Failed reading from incoming stream buffer")?;

    let command: SyncCommand = postcard::from_bytes(&bytes)?;
    Ok(command)
}
