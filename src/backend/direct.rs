use std::{
    fs,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr, EndpointId, TransportAddr, address_lookup,
    endpoint::{Connection, presets},
    protocol::Router,
};
use iroh_blobs::BlobsProtocol;
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use tokio::sync::mpsc::{self, Receiver};

use crate::{
    backend::{BackendEvent, secret::get_or_create_secret, store::KeithStore},
    frontend::TuiCommand,
};

const SYNC_ALPN: &[u8] = b"keith-shareit/1";

pub async fn create_direct_endpoint(
    is_sender: bool,
    store: &KeithStore,
    event_tx: &mpsc::Sender<BackendEvent>,
) -> Result<(Endpoint, Option<Router>)> {
    let secret_key = get_or_create_secret()?;
    let addr = fs::read_to_string("endpoint.addr")?;

    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(secret_key)
        .alpns(vec![
            SYNC_ALPN.to_vec(),
            iroh_blobs::protocol::ALPN.to_vec(),
        ])
        .bind_addr(addr)?
        .bind()
        .await?;

    log_message(
        &format!("Endpoint created with id: {}", shortened_id(&endpoint.id())),
        is_sender,
        event_tx,
    )
    .await;

    log_message(
        &format!(
            "Bound to address: {:?}",
            endpoint.addr().addrs.iter().next().unwrap()
        ),
        is_sender,
        event_tx,
    )
    .await;

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

    log_message("Created router", is_sender, event_tx).await;

    Ok((endpoint, router))
}

pub async fn establish_direct_connection(
    endpoint: &Endpoint,
    is_sender: bool,
    event_tx: &mpsc::Sender<BackendEvent>,
    command_rx: &mut Receiver<TuiCommand>,
) -> Result<Option<(Connection, EndpointAddr)>> {
    let target_addr = {
        let target_id = {
            let bytes = fs::read("other.id")?;
            EndpointId::from_bytes(bytes.as_array::<32>().unwrap())?
        };

        let target_ip = {
            let addr_string = fs::read_to_string("other.addr")?;
            let with_port: SocketAddrV4 = addr_string.parse()?; //.expect("failed to parse socket addr v4");
            TransportAddr::Ip(SocketAddr::V4(with_port))
        };

        EndpointAddr::from_parts(target_id, vec![target_ip])
    };

    log_message(
        &format!("Got target id: {:?}", shortened_id(&target_addr.id)),
        is_sender,
        event_tx,
    )
    .await;

    log_message(
        &format!("Got target addr: {:?}", target_addr.addrs),
        is_sender,
        event_tx,
    )
    .await;

    let connection = if is_sender {
        log_message("Attempting connection to target", is_sender, event_tx).await;
        endpoint.connect(target_addr.clone(), SYNC_ALPN).await?
    } else {
        println!("Attempting to accept connection");
        endpoint
            .accept()
            .await
            .context("no incoming connection")?
            .await
            .context("accept connection")?
    };

    log_message("Connection established", is_sender, event_tx).await;

    event_tx.send(BackendEvent::ConnectionSecured).await.ok();

    Ok(Some((connection, target_addr)))
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
