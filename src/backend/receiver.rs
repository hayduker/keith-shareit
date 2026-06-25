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

use crate::{backend::sender::SyncCommand, store::KeithStore};

pub async fn run_loop(
    connection: Connection,
    endpoint: Endpoint,
    target_addr: EndpointAddr,
    store: KeithStore,
    dst_dir: PathBuf,
) -> Result<()> {
    // let connection = connection.clone();
    loop {
        println!("\nReceiver is listening for incoming SyncCommands...");
        tokio::select! {
            _ = connection.closed() => {
                println!("Sender disconnected. Exiting receiver loop. {:?}", connection.close_reason());
                break;
            }
            stream_result = connection.accept_uni() => {
                match stream_result {
                    Ok(mut recv_stream) => {
                        match read_command_from_stream(&mut recv_stream).await {
                            Ok(command) => {
                                println!("Received a new target hash");
                                println!("  HashAndFormat: {}", command.hash_and_format);
                                println!("  Path: {:?}", command.path);

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
    println!("Downloading blob...");
    let local = store.db.remote().local(command.hash_and_format).await?;
    if !local.is_complete() {
        let connection = endpoint
            .connect(target_addr.clone(), iroh_blobs::protocol::ALPN)
            .await?;

        println!("Made blob connection back to sender");
        println!("Downloading...");

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

    println!("Download complete.");

    let collection = Collection::load(command.hash_and_format.hash, store.db.as_ref()).await?;

    if let Some((name, _)) = collection.iter().next()
        && let Some(first) = name.split('/').next()
    {
        println!("Exporting to {first}...");
    }
    store.export(collection, command.path, dst_dir).await?;
    println!("Done.");

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
