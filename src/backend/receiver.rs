use std::path::PathBuf;

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, RecvStream},
};
use iroh_blobs::{
    api::remote::GetProgressItem, format::collection::Collection,
    get::request::get_hash_seq_and_sizes,
};
use n0_future::StreamExt;
use tokio::sync::mpsc;

use crate::{
    backend::{
        TuiCommand,
        sender::{SyncCommand, shortened_hash},
    },
    store::KeithStore,
};

pub async fn run_loop(
    connection: Connection,
    endpoint: Endpoint,
    target_addr: EndpointAddr,
    store: KeithStore,
    dst_dir: PathBuf,
    mut command_rx: mpsc::Receiver<TuiCommand>,
) -> Result<()> {
    loop {
        println!("\nReceiver awaiting next incoming sync commands");

        tokio::select! {
            _ = connection.closed() => {
                println!("Sender disconnected, shutting down");
                return Ok(());
            }
            command = command_rx.recv() => {
                if let Some(TuiCommand::Shutdown) = command {
                    return Ok(());
                }
            }
            stream_result = connection.accept_uni() => {
                match stream_result {
                    Ok(mut recv_stream) => {
                        match read_command_from_stream(&mut recv_stream).await {
                            Ok(command) => {
                                println!("Received sync command for hash: {}", shortened_hash(&command.hash_and_format.hash));
                                // println!("  HashAndFormat: {}", command.hash_and_format);
                                // println!("  Path: {:?}", command.path);

                                download_blob(&endpoint, &store, &target_addr, command, dst_dir.clone()).await?;
                            }
                            Err(e) => eprintln!("Failed to parse incoming stream data: {:?}", e),
                        }
                    }
                    Err(e) => {
                        eprintln!("Error accepting unidirectional stream: {:?}", e);
                        break;
                    }
                }
            }
        }
    }

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
        println!("Downloading blob");
        get_hash_seq_and_sizes(
            &connection,
            &command.hash_and_format.hash,
            1024 * 1024 * 32,
            None,
        )
        .await?;

        let get = store.db.remote().execute_get(connection, local.missing());
        let mut stream = get.stream();

        while let Some(item) = stream.next().await {
            if let GetProgressItem::Error(cause) = item {
                anyhow::bail!("Iroh fetch error: {:?}", cause);
            }
        }
    };

    let collection = Collection::load(command.hash_and_format.hash, store.db.as_ref()).await?;

    if let Some((name, _)) = collection.iter().next()
        && let Some(first) = name.split('/').next()
    {
        println!("Download complete, exporting to: '{first}'...");
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
