use clap::Parser;

use crate::{
    app::App,
    backend::endpoint::{create_endpoint, establish_connection},
    cli::{Args, Commands},
};

mod app;
mod backend;
mod cli;
mod requester;
mod secret;
mod store;
mod ui;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ratatui::run(|terminal| App::new().run(terminal))

    let args = Args::parse();

    let result = match args.command {
        Commands::Send => {
            let (endpoint, mdns, store, router) = create_endpoint(true).await?;
            establish_connection(&endpoint, mdns, &store, true).await
        }
        Commands::Receive => {
            let (endpoint, mdns, store, _) = create_endpoint(false).await?;
            establish_connection(&endpoint, mdns, &store, false).await
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
