use clap::Parser;

use crate::{
    app::App,
    cli::{Args, Commands},
    endpoint::{create_endpoint, make_connection},
};

mod app;
mod cli;
mod endpoint;
mod provider;
mod requester;
mod secret;
mod store;
mod ui;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ratatui::run(|terminal| App::new().run(terminal))

    let args = Args::parse();

    let result = match args.command {
        // Commands::Send(args) => send(args.path).await,
        // Commands::Receive(args) => receive(args.ticket).await,
        Commands::Send(_) => {
            let (endpoint, mdns, store, store_dir, router) = create_endpoint(true).await?;
            make_connection(&endpoint, mdns, &store, true).await
        }
        Commands::Receive(_) => {
            let (endpoint, mdns, store, store_dir, _) = create_endpoint(false).await?;
            make_connection(&endpoint, mdns, &store, false).await
        }
    };

    if let Err(e) = &result {
        eprintln!("{e}");
    }

    match result {
        Ok(_) => std::process::exit(0),
        Err(_) => std::process::exit(1),
    }
}
