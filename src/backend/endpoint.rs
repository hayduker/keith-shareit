//! This module provides functionalities for creating and managing Iroh endpoints,
//! and establishing secure connections between peers for file transfer.

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
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

/// Creates and configures an Iroh [`Endpoint`] for peer-to-peer communication.
///
/// This function initializes an Iroh endpoint, sets up its secret key, and registers
/// application-layer protocol negotiations (ALPNs) for both `keith-shareit`'s
/// synchronization protocol and Iroh's blob transfer protocol.
/// If `is_sender` is true, a [`Router`] for blob transfers will also be spawned.
///
/// # Arguments
///
/// * `is_sender` - A boolean indicating whether this endpoint will primarily be used for sending files.
/// * `store` - A reference to the [`KeithStore`] which manages data storage.
/// * `event_tx` - A sender for [`BackendEvent`]s, used to report status updates and other events.
///
/// # Returns
///
/// A `Result` containing a tuple of the initialized [`Endpoint`] and an optional [`Router`].
/// The `Router` is present only if `is_sender` is true.
///
/// # Errors
///
/// Returns an error if the secret key cannot be retrieved or created, or if the endpoint
/// fails to bind.
///
/// # Examples
///
/// ```no_run
/// # use keith_shareit::backend::endpoint::create_endpoint;
/// # use keith_shareit::backend::store::KeithStore;
/// # use keith_shareit::backend::BackendEvent;
/// # use tokio::sync::mpsc;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// # let (event_tx, mut event_rx) = mpsc::channel(10);
/// # let store = KeithStore::new_mem().await?;
/// let (endpoint, router) = create_endpoint(true, &store, &event_tx).await?;
/// println!("Endpoint ID: {}", endpoint.id());
/// # Ok(())
/// # }
/// ```
pub async fn create_endpoint(
    is_sender: bool,
    store: &KeithStore,
    event_tx: &mpsc::Sender<BackendEvent>,
) -> Result<(Endpoint, Option<Router>)> {
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

    Ok((endpoint, router))
}

/// Establishes a secure connection with a peer using mDNS for discovery.
///
/// This function sets up mDNS advertising and subscribes to discovery events to find other peers.
/// Once a peer is discovered, it attempts to establish a connection. If `is_sender` is true,
/// it initiates a connection; otherwise, it waits to accept an incoming connection.
/// The function also listens for [`TuiCommand::Shutdown`] to gracefully terminate.
///
/// # Arguments
///
/// * `endpoint` - A reference to the initialized Iroh [`Endpoint`].
/// * `is_sender` - A boolean indicating whether this endpoint is acting as the sender.
/// * `event_tx` - A sender for [`BackendEvent`]s, used to report connection status.
/// * `command_rx` - A mutable receiver for [`TuiCommand`]s, used to listen for shutdown signals.
///
/// # Returns
///
/// A `Result` containing an `Option` of a tuple `(Connection, EndpointAddr)`. Returns `Some`
/// with the connection and peer address if a connection is successfully established, or `None`
/// if a shutdown command is received.
///
/// # Errors
///
/// Returns an error if mDNS setup fails, if the discovery stream ends without finding a peer,
/// if connection establishment fails, or if an unsupported [`TuiCommand`] is received.
///
/// # Examples
///
/// ```no_run
/// # use keith_shareit::backend::endpoint::{create_endpoint, establish_connection};
/// # use keith_shareit::backend::store::KeithStore;
/// # use keith_shareit::backend::BackendEvent;
/// # use keith_shareit::frontend::TuiCommand;
/// # use tokio::sync::mpsc;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// # let (event_tx, mut event_rx) = mpsc::channel(10);
/// # let (command_tx, mut command_rx) = mpsc::channel(10);
/// # let store = KeithStore::new_mem().await?;
/// # let (endpoint, _router) = create_endpoint(false, &store, &event_tx).await?;
/// // In a real application, you would run this in a separate task or await it directly.
/// // For demonstration, we'll simulate a shutdown.
/// let connection_task = tokio::spawn(async move {
///     establish_connection(&endpoint, false, &event_tx, &mut command_rx).await
/// });
///
/// // Simulate a shutdown command after some time
/// // tokio::time::sleep(std::time::Duration::from_secs(1)).await;
/// // command_tx.send(TuiCommand::Shutdown).await.unwrap();
///
/// let result = connection_task.await?;
/// match result {
///     Some((_conn, addr)) => println!("Connected to: {}", addr.id),
///     None => println!("Connection process shut down."),
/// }
/// # Ok(())
/// # }
/// ```
pub async fn establish_connection(
    endpoint: &Endpoint,
    is_sender: bool,
    event_tx: &mpsc::Sender<BackendEvent>,
    command_rx: &mut Receiver<TuiCommand>,
) -> Result<Option<(Connection, EndpointAddr)>> {
    let mdns = MdnsAddressLookup::builder()
        .advertise(true)
        .build(endpoint.id())?;

    endpoint.address_lookup().unwrap().add(mdns.clone());

    log_message(
        &format!(
            "Configured mDNS for endpoint with id: {}",
            shortened_id(&endpoint.id())
        ),
        is_sender,
        event_tx,
    )
    .await;

    let mut events = mdns.subscribe().await;

    log_message("Searching for peers", is_sender, event_tx).await;

    let discovery_loop = async {
        let mut result: Result<Option<(Connection, EndpointAddr)>> = Ok(None);

        while let Some(event) = events.next().await {
            match event {
                DiscoveryEvent::Discovered { endpoint_info, .. } => {
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

                    result = Ok(Some((connection, target_addr)));

                    break;
                }
                DiscoveryEvent::Expired { endpoint_id } => {
                    log_message(
                        &format!("Endpoint id expired: {}", shortened_id(&endpoint_id)),
                        is_sender,
                        event_tx,
                    )
                    .await;
                }
                _ => {}
            }
        }

        if let Ok(None) = result {
            anyhow::bail!("mDNS discovery stream ended without finding a peer");
        }

        result
    };

    tokio::select! {
        result = discovery_loop => result,
        cmd = command_rx.recv() => {
            match cmd {
                Some(TuiCommand::Shutdown) | None => {
                    Ok(None)
                }
                _ => {
                    anyhow::bail!("Got unsupported TuiCommand during peer discovery: {:?}", cmd);
                }
            }
        }
    }
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
