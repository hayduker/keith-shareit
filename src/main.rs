use std::path::PathBuf;

use clap::Parser;
use iroh::EndpointId;
use iroh_blobs::Hash;

use crate::{
    app::App,
    cli::{Args, Commands},
    // notify::{notify_receiver_to_download, start_reciever_listener},
    provider::send,
    requester::receive,
};

mod app;
mod cli;
// mod notify;
mod provider;
mod requester;
mod secret;
mod ui;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ratatui::run(|terminal| App::new().run(terminal))

    let args = Args::parse();

    let result = match args.command {
        // Commands::Receive(_) => start_reciever_listener().await,
        // Commands::Send(_) => {
        //     notify_receiver_to_download(Hash::new([0, 1, 2]), PathBuf::new()).await
        // }
        Commands::Send(args) => send(args.path).await,
        Commands::Receive(args) => receive(args.ticket).await,
    };

    if let Err(e) = &result {
        eprintln!("{e}");
    }

    match result {
        Ok(()) => std::process::exit(0),
        Err(_) => std::process::exit(1),
    }
}
