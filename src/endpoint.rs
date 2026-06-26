use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{Connection, presets},
    protocol::Router,
};
use iroh_blobs::BlobsProtocol;
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use tokio::sync::mpsc;

use crate::{event::BackendEvent, secret::get_or_create_secret, store::KeithStore};

const SYNC_ALPN: &[u8] = b"keith-shareit/1";

pub async fn create_endpoint(
    is_sender: bool,
    store: &KeithStore,
    event_tx: &mpsc::Sender<BackendEvent>,
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

    log_message(
        &format!("Endpoint created with id: {}", shortened_id(&endpoint.id())),
        is_sender,
        event_tx,
    )
    .await;

    let mdns = MdnsAddressLookup::builder().build(endpoint.id()).unwrap();
    endpoint.address_lookup().unwrap().add(mdns.clone());

    let blobs = BlobsProtocol::new(&store.db, None);
    let router = if is_sender {
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
    is_sender: bool,
    event_tx: &mpsc::Sender<BackendEvent>,
) -> Result<(Connection, EndpointAddr)> {
    let mut events = mdns.subscribe().await;

    log_message("Searching for peers", is_sender, event_tx).await;

    while let Some(event) = events.next().await {
        if let DiscoveryEvent::Discovered { endpoint_info, .. } = event {
            let target_addr = endpoint_info.into_endpoint_addr();

            log_message(
                &format!(
                    "Discovered endpoint with id: {}",
                    shortened_id(&target_addr.id)
                ),
                is_sender,
                event_tx,
            )
            .await;

            let connection = if is_sender {
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

async fn log_message(msg: &str, is_sender: bool, event_tx: &mpsc::Sender<BackendEvent>) {
    if is_sender {
        event_tx
            .send(BackendEvent::StatusUpdate(msg.into()))
            .await
            .ok();
    } else {
        println!("{}", msg);
    }
}

fn shortened_id(id: &EndpointId) -> String {
    id.to_string()
        .get(0..8)
        .expect("Couldn't shorten endpoint ID")
        .to_string()
        + "..."
}
