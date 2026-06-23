use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, RecvStream, presets},
    endpoint_info::EndpointInfo,
};
use iroh_blobs::{BlobsProtocol, Hash};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, str::FromStr};
use tokio::io::AsyncBufReadExt;

use crate::{
    provider::{create_store, import},
    secret::get_or_create_secret,
};

const NOTIFY_ALPN: &[u8] = b"keith-shareit/1";

pub async fn create_endpoint() -> Result<(Endpoint, MdnsAddressLookup)> {
    let secret_key = get_or_create_secret()?;

    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .alpns(vec![NOTIFY_ALPN.to_vec()])
        .bind()
        .await?;

    let mdns = MdnsAddressLookup::builder().build(endpoint.id()).unwrap();
    endpoint.address_lookup().unwrap().add(mdns.clone());

    println!("Endpoint created with id: {}", endpoint.id());

    Ok((endpoint, mdns))
}

pub async fn make_connection(
    endpoint: &Endpoint,
    mdns: MdnsAddressLookup,
    sender: bool,
) -> Result<()> {
    let mut events = mdns.subscribe().await;
    let mut connection: Option<Connection> = None;

    println!("Starting discovery phase...");

    while connection.is_none() {
        if let Some(event) = events.next().await {
            match event {
                DiscoveryEvent::Discovered { endpoint_info, .. } => {
                    let target_addr = endpoint_info.into_endpoint_addr();
                    println!("MDNS discovered: {}", target_addr.id);

                    if sender {
                        if let Ok(conn) = connect(endpoint, target_addr).await {
                            connection = Some(conn);
                        }
                    } else {
                        if let Ok(conn) = accept(endpoint).await {
                            connection = Some(conn);
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
    println!("Connection secured, moving to sync loop");

    if sender {
        run_sender_loop(connection).await?;
    } else {
        run_receiver_loop(connection).await?;
    }

    Ok(())
}

async fn connect(endpoint: &Endpoint, addr: EndpointAddr) -> Result<Connection> {
    println!("Trying to connect to {}", addr.id);

    let connection = endpoint.connect(addr, NOTIFY_ALPN).await?;

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

async fn run_sender_loop(connection: Connection) -> Result<()> {
    let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    let mut line = String::new();

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

                let path_to_send = PathBuf::from_str("/home/derek/smallmusic")?;

                if let Err(e) = send_download_notification(&connection, path_to_send).await {
                    eprintln!("Error sending notification: {:?}", e);
                }

                println!("Sync command sent");
            }
        }
    }

    Ok(())
}

async fn send_download_notification(connection: &Connection, blob_path: PathBuf) -> Result<()> {
    println!("Going to send notification");

    let mut send_stream = connection
        .open_uni()
        .await
        .context("failed to open unidirectional connection")?;

    println!("Got SendStream {}", send_stream.id());

    let (store, _store_dir) = create_store(&blob_path).await?;
    let blobs = BlobsProtocol::new(&store, None);
    let tag = import(blob_path.clone(), blobs.store()).await?;

    println!(
        "Sending SyncCommand with hash {} and path {:?}",
        tag.hash(),
        blob_path
    );

    let command = SyncCommand {
        hash: tag.hash(),
        path: blob_path,
    };
    let payload = postcard::to_stdvec(&command)?;
    send_stream.write_all(&payload).await?;
    send_stream
        .finish()
        .context("failed to finish send stream")?;

    Ok(())
}

async fn run_receiver_loop(connection: Connection) -> Result<()> {
    loop {
        println!("\nReceiver is listening for incoming SyncCommands...");
        tokio::select! {
            _ = connection.closed() => {
                println!("Sender disconnected. Exiting receiver loop.");
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
                                println!("  Hash: {}", command.hash);
                                println!("  Path: {:?}", command.path);
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

async fn _close_connection(connection: Connection) -> Result<()> {
    connection.closed().await;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncCommand {
    /// Tell the blob receiver to fetch a specific hash (could be a single blob or a HashSeq/Collection)
    hash: Hash,
    path: PathBuf,
}
