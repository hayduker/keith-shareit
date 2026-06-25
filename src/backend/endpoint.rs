use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, presets},
    protocol::Router,
};
use iroh_blobs::BlobsProtocol;
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use tokio::sync::mpsc;

use crate::{backend::BackendEvent, secret::get_or_create_secret, store::KeithStore};

const SYNC_ALPN: &[u8] = b"keith-shareit/1";

pub async fn create_endpoint(
    sender: bool,
    store: &KeithStore,
) -> Result<(Endpoint, MdnsAddressLookup, Option<Router>)> {
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

    let blobs = BlobsProtocol::new(&store.db, None);

    println!("Creating router");

    let router = if sender {
        Some(
            Router::builder(endpoint.clone())
                .accept(iroh_blobs::ALPN, blobs)
                .spawn(),
        )
    } else {
        None
    };

    Ok((endpoint, mdns, router))
}

pub async fn establish_connection(
    endpoint: &Endpoint,
    mdns: MdnsAddressLookup,
    sender: bool,
    event_tx: &mpsc::Sender<BackendEvent>,
) -> Result<(Connection, EndpointAddr)> {
    let mut events = mdns.subscribe().await;

    event_tx
        .send(BackendEvent::StatusUpdate(
            "Searching for peers via mDNS...".into(),
        ))
        .await
        .ok();

    println!("Starting discovery phase...");

    while let Some(event) = events.next().await {
        if let DiscoveryEvent::Discovered { endpoint_info, .. } = event {
            let target_addr = endpoint_info.into_endpoint_addr();
            println!("MDNS discovered: {}", target_addr.id);
            event_tx
                .send(BackendEvent::PeerDiscovered(target_addr.id))
                .await
                .ok();

            let connection = if sender {
                endpoint.connect(target_addr.clone(), SYNC_ALPN).await?
            } else {
                endpoint
                    .accept()
                    .await
                    .context("no incoming connection")?
                    .await
                    .context("accept connection")?
            };

            event_tx.send(BackendEvent::ConnectionSecured).await.ok();
            return Ok((connection, target_addr));
        }
    }
    anyhow::bail!("mDNS discovery stream ended without finding a peer");
}
