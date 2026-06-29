use std::str::FromStr;

use anyhow::{Context, Result};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{Connection, presets},
    protocol::Router,
};
use iroh_blobs::BlobsProtocol;
use iroh_tickets::endpoint::EndpointTicket;
use tokio::sync::mpsc::{self, Receiver};

use crate::{
    backend::{BackendEvent, secret::get_or_create_secret, store::KeithStore},
    frontend::TuiCommand,
};

const SYNC_ALPN: &[u8] = b"keith-shareit/1";

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

pub async fn establish_ticket_connection(
    endpoint: &Endpoint,
    is_sender: bool,
    event_tx: &mpsc::Sender<BackendEvent>,
    command_rx: &mut Receiver<TuiCommand>,
) -> Result<Option<(Connection, EndpointAddr)>> {
    let ticket = EndpointTicket::new(endpoint.addr());
    log_message(&format!("Ticket: {ticket}"), is_sender, event_tx).await;

    let ticket_str = if is_sender {
        event_tx.send(BackendEvent::TicketRequest).await.ok();

        match command_rx.recv().await {
            Some(TuiCommand::TicketInput(t)) => t,
            Some(TuiCommand::Shutdown) => {
                log_message("Got Shutdown waiting for user input", is_sender, event_tx).await;
                return Ok(None);
            }
            _ => {
                log_message(
                    "Got invalid command waiting for user input",
                    is_sender,
                    event_tx,
                )
                .await;
                return Ok(None);
            }
        }
    } else {
        let mut buffer = String::new();
        log_message("Enter peer's ticket: ", is_sender, event_tx).await;
        std::io::stdin().read_line(&mut buffer)?;

        buffer
    };

    log_message(
        &format!("Got ticket from user input: {}", ticket_str),
        is_sender,
        event_tx,
    )
    .await;

    let target_ticket = EndpointTicket::from_str(&ticket_str)?;

    log_message(
        &format!("Parsed ticket: {}", target_ticket),
        is_sender,
        event_tx,
    )
    .await;

    let target_addr = target_ticket.endpoint_addr().clone();

    log_message(
        &format!("Got address out of ticket: {:?}", target_addr),
        is_sender,
        event_tx,
    )
    .await;

    let connection = if is_sender {
        log_message("Initiating connection", is_sender, event_tx).await;
        endpoint.connect(target_addr.clone(), SYNC_ALPN).await?
    } else {
        log_message("Trying to accept connection", is_sender, event_tx).await;
        endpoint
            .accept()
            .await
            .context("no incoming connection")?
            .await
            .context("accept connection")?
    };

    log_message("Connection successful!", is_sender, event_tx).await;

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
