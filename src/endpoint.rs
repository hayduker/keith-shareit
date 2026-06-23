use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, presets},
    endpoint_info::EndpointInfo,
};
use iroh_blobs::{BlobsProtocol, Hash};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, str::FromStr};
use std::{thread, time};

use crate::{
    provider::{create_store, import},
    secret::get_or_create_secret,
};

const NOTIFY_ALPN: &[u8] = b"keith-shareit/1";

pub async fn create_endpoint(sender: bool) -> Result<Endpoint> {
    let secret_key = get_or_create_secret()?;

    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .alpns(vec![NOTIFY_ALPN.to_vec()])
        .bind()
        .await?;

    let mdns = MdnsAddressLookup::builder().build(endpoint.id()).unwrap();
    endpoint.address_lookup().unwrap().add(mdns.clone());

    println!("Endpoint created with id: {}", endpoint.id());

    let mut connected_addr: Option<EndpointInfo> = None;
    let mut events = mdns.subscribe().await;
    while let Some(event) = events.next().await {
        match event {
            DiscoveryEvent::Discovered { endpoint_info, .. } => {
                println!(
                    "MDNS discovered: {}",
                    endpoint_info.clone().into_endpoint_addr().id
                );

                if sender && connected_addr.is_none() {
                    let connection =
                        connect(&endpoint, endpoint_info.clone().into_endpoint_addr()).await?;

                    connected_addr = Some(endpoint_info);

                    send_download_notification(
                        &connection,
                        PathBuf::from_str("/home/derek/smallmusic")?,
                    )
                    .await?;

                    thread::sleep(time::Duration::from_secs(5));

                    println!("End of sender block");
                } else if !sender && connected_addr.is_none() {
                    let connection = accept(&endpoint).await?;

                    println!("Accepted connection to sender");

                    receive_download_notification(&connection).await?;

                    println!("End of receiver block");
                }
            }
            DiscoveryEvent::Expired { endpoint_id } => {
                println!("MDNS expired: {endpoint_id}");
            }
            _ => {}
        }
    }

    Ok(endpoint)
}

async fn connect(endpoint: &Endpoint, addr: EndpointAddr) -> Result<Connection> {
    println!("Trying to connect to {}", addr.id);

    let connection = endpoint.connect(addr, NOTIFY_ALPN).await?;

    println!("Connection established");

    Ok(connection)
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

async fn receive_download_notification(connection: &Connection) -> Result<SyncCommand> {
    println!("Going to receive notification");

    let mut recv_stream = connection
        .accept_uni()
        .await
        .context("failed to accept stream")?;

    let bytes = recv_stream
        .read_to_end(10000)
        .await
        .context("read from stream")?;

    println!("Got {} bytes from sender", bytes.len());

    let command: SyncCommand = postcard::from_bytes(&bytes)?;
    println!(
        "Got SyncCommand command with hash {} and path {:?}",
        command.hash, command.path
    );

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
