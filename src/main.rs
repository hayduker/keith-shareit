use std::path::PathBuf;

use clap::Parser;
use iroh::{Endpoint, EndpointId, endpoint::presets};
use iroh_blobs::Hash;
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;

use crate::{
    app::App,
    cli::{Args, Commands},
    endpoint::{create_endpoint, make_connection},
    provider::send,
    requester::receive,
};

mod app;
mod cli;
mod endpoint;
mod provider;
mod requester;
mod secret;
mod ui;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ratatui::run(|terminal| App::new().run(terminal))

    let args = Args::parse();

    let (endpoint, mdns) = create_endpoint().await?;

    let result = match args.command {
        // Commands::Send(args) => send(args.path).await,
        // Commands::Receive(args) => receive(args.ticket).await,
        Commands::Send(_) => make_connection(&endpoint, mdns, true).await,
        Commands::Receive(_) => make_connection(&endpoint, mdns, false).await,
    };

    if let Err(e) = &result {
        eprintln!("{e}");
    }

    match result {
        Ok(_) => std::process::exit(0),
        Err(_) => std::process::exit(1),
    }
}
