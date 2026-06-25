use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, RecvStream, presets},
    protocol::Router,
};
use iroh_blobs::{
    BlobsProtocol, HashAndFormat,
    api::{TempTag, remote::GetProgressItem},
    format::collection::Collection,
    get::request::get_hash_seq_and_sizes,
};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, str::FromStr};
use tokio::io::AsyncBufReadExt;

use crate::{backend::receiver, backend::sender, secret::get_or_create_secret, store::KeithStore};

const SYNC_ALPN: &[u8] = b"keith-shareit/1";

pub async fn create_endpoint(
    sender: bool,
) -> Result<(Endpoint, MdnsAddressLookup, KeithStore, Option<Router>)> {
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

    let store = KeithStore::new().await?;

    let blobs = BlobsProtocol::new(&store.db, None);

    println!("Creating router");

    if sender {
        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .spawn();

        Ok((endpoint, mdns, store, Some(router)))
    } else {
        Ok((endpoint, mdns, store, None))
    }
}

pub async fn establish_connection(
    endpoint: &Endpoint,
    mdns: MdnsAddressLookup,
    store: &KeithStore,
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
        sender::run_loop(&connection, store).await?;
    } else {
        receiver::run_loop(&connection, endpoint, target_addr, store).await?;
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
