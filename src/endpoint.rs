use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, RecvStream, presets},
    protocol::Router,
};
use iroh_blobs::{BlobsProtocol, HashAndFormat, api::TempTag, store::fs::FsStore};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::io::AsyncBufReadExt;

use crate::{
    provider::{create_store, import},
    requester::receive,
    secret::get_or_create_secret,
};

const SYNC_ALPN: &[u8] = b"keith-shareit/1";

pub async fn create_endpoint(
    sender: bool,
) -> Result<(
    Endpoint,
    MdnsAddressLookup,
    FsStore,
    PathBuf,
    Option<Router>,
)> {
    let secret_key = get_or_create_secret()?;

    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .alpns(vec![
            SYNC_ALPN.to_vec(),
            iroh_blobs::protocol::ALPN.to_vec(),
        ])
        .bind()
        .await?;

    println!("Endpoint created with id: {}", endpoint.id());

    let mdns = MdnsAddressLookup::builder().build(endpoint.id()).unwrap();
    endpoint.address_lookup().unwrap().add(mdns.clone());

    println!("Creating store");

    let (store, store_dir) = create_store().await?;
    let blobs = BlobsProtocol::new(&store, None);

    println!("Creating router");

    if sender {
        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .spawn();

        Ok((endpoint, mdns, store, store_dir, Some(router)))
    } else {
        Ok((endpoint, mdns, store, store_dir, None))
    }
}

pub async fn make_connection(
    endpoint: &Endpoint,
    mdns: MdnsAddressLookup,
    store: &FsStore,
    sender: bool,
) -> Result<()> {
    let mut events = mdns.subscribe().await;
    let mut connection: Option<Connection> = None;
    let mut target_address: Option<EndpointAddr> = None;

    println!("Starting discovery phase...");

    while connection.is_none() {
        if let Some(event) = events.next().await {
            match event {
                DiscoveryEvent::Discovered { endpoint_info, .. } => {
                    let target_addr = endpoint_info.into_endpoint_addr();
                    println!("MDNS discovered: {}", target_addr.id);

                    if sender {
                        if let Ok(conn) = connect(endpoint, target_addr.clone()).await {
                            connection = Some(conn);
                            target_address = Some(target_addr);
                        }
                    } else {
                        if let Ok(conn) = accept(endpoint).await {
                            connection = Some(conn);
                            target_address = Some(target_addr);
                        }
                    }
                }
                DiscoveryEvent::Expired { endpoint_id } => {
                    println!("MDNS expired: {endpoint_id}");
                }
                _ => {}
            }
        }
    }

    let connection = connection.unwrap();
    let target_addr = target_address.unwrap();
    println!("Connection secured, moving to sync loop");

    if sender {
        run_sender_loop(&connection, store).await?;
    } else {
        run_receiver_loop(&connection, endpoint, target_addr).await?;
    }

    Ok(())
}

async fn connect(endpoint: &Endpoint, addr: EndpointAddr) -> Result<Connection> {
    println!("Trying to connect to {}", addr.id);

    let connection = endpoint.connect(addr, SYNC_ALPN).await?;

    println!("Connection established");

    Ok(connection)
}

async fn accept(endpoint: &Endpoint) -> Result<Connection> {
    println!("Waiting to accept connection");

    let connection = endpoint
        .accept()
        .await
        .context("no incoming connection")?
        .await
        .context("accept connection")?;

    println!("Connection accepted");

    Ok(connection)
}

async fn run_sender_loop(connection: &Connection, store: &FsStore) -> Result<()> {
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
                line.clear();

                println!("User triggered sync action!");

                let path_to_send = PathBuf::from_str("/home/derek/programming/keith-shareit/a/payload")?;

                match send_download_notification(connection, path_to_send, store).await {
                    Ok(tag) => {
                        active_tags.push(tag);
                    }
                    Err(e) => eprintln!("Error sending notification: {:?}", e)
                }
            }
        }
    }

    println!("Cleaning up...");

    // for (_tag, store, store_dir, router) in active_tags {
    //     verify_safe_to_delete(&store_dir, ".send")?;
    //     tokio::fs::remove_dir_all(store_dir).await?;
    //     let _ = tokio::time::timeout(Duration::from_secs(2), router.shutdown())
    //         .await
    //         .context("Failed to shutdown router");
    //     let _ = tokio::time::timeout(Duration::from_secs(2), store.shutdown())
    //         .await
    //         .context("Failed to shutown store");
    // }
    connection.closed().await;

    println!("Shutting down.");

    Ok(())
}

async fn send_download_notification(
    connection: &Connection,
    blob_path: PathBuf,
    store: &FsStore,
) -> Result<TempTag> {
    println!("Going to send notification");

    let mut send_stream = connection
        .open_uni()
        .await
        .context("failed to open unidirectional connection")?;

    println!("Got SendStream {}", send_stream.id());

    let tag = import(blob_path.clone(), store).await?;

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

async fn run_receiver_loop(
    connection: &Connection,
    endpoint: &Endpoint,
    target_addr: EndpointAddr,
) -> Result<()> {
    let connection = connection.clone();
    loop {
        println!("\nReceiver is listening for incoming SyncCommands...");
        tokio::select! {
            _ = connection.closed() => {
                println!("Sender disconnected. Exiting receiver loop. {:?}", connection.close_reason());
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down.");
                connection.close(1u8.into(), b"done");
            }
            stream_result = connection.accept_uni() => {
                match stream_result {
                    Ok(mut recv_stream) => {
                        match read_command_from_stream(&mut recv_stream).await {
                            Ok(command) => {
                                println!("Received a new target hash");
                                println!("  HashAndFormat: {}", command.hash_and_format);
                                println!("  Path: {:?}", command.path);

                                receive(endpoint, command.hash_and_format, target_addr.clone()).await?;
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

async fn read_command_from_stream(recv_stream: &mut RecvStream) -> Result<SyncCommand> {
    let bytes = recv_stream
        .read_to_end(10000)
        .await
        .context("Failed reading from incoming stream buffer")?;

    let command: SyncCommand = postcard::from_bytes(&bytes)?;
    Ok(command)
}

fn verify_safe_to_delete(path: &PathBuf, expected_prefix: &str) -> Result<()> {
    let canonical_path = path
        .canonicalize()
        .map_err(|e| anyhow::anyhow!("Failed to resolve absolute path: {e}"))?;

    if canonical_path == Path::new("/") {
        anyhow::bail!("Safety violation: Attempted to delete the root directory!");
    }

    if let Some(file_name) = canonical_path.file_name().and_then(|s| s.to_str()) {
        if !file_name.starts_with(expected_prefix) {
            anyhow::bail!(
                "Safety violation: Path '{:?}' does not start with the expected prefix '{expected_prefix}'",
                canonical_path
            );
        }
    } else {
        anyhow::bail!("Safety violation: Could not extract a valid directory name.");
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncCommand {
    /// Tell the blob receiver to fetch a specific hash (could be a single blob or a HashSeq/Collection)
    hash_and_format: HashAndFormat,
    path: PathBuf,
}
