use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use crate::{
    backend::{
        endpoint::{create_endpoint, establish_connection},
        receiver, sender,
        store::KeithStore,
    },
    ui::{
        app::App,
        cli::{Args, Commands},
    },
};

mod backend;
mod ui;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Args::parse().command {
        Commands::Send(args) => run_sender(args.src_dir).await,
        Commands::Receive(args) => run_receiver(args.dst_dir).await,
    }
}

async fn run_sender(src_dir: PathBuf) -> Result<()> {
    let (tui_cmd_tx, tui_cmd_rx) = tokio::sync::mpsc::channel(100);
    let (backend_event_tx, backend_event_rx) = tokio::sync::mpsc::channel(100);

    tokio::spawn(async move {
        let store = KeithStore::new().await?;
        let (endpoint, mdns, _router) = create_endpoint(true, &store, &backend_event_tx).await?;
        let (connection, _) =
            establish_connection(&endpoint, mdns, true, &backend_event_tx).await?;

        sender::run_loop(connection, store, tui_cmd_rx, backend_event_tx).await
    });

    ratatui::run(|terminal| {
        let mut app = App::new(src_dir, tui_cmd_tx, backend_event_rx);
        app.run(terminal)
    })?;

    Ok(())
}

async fn run_receiver(dst_dir: PathBuf) -> Result<()> {
    let (backend_event_tx, _) = tokio::sync::mpsc::channel(100);

    let run_backend = async move {
        let store = KeithStore::new().await?;
        let (endpoint, mdns, _router) = create_endpoint(false, &store, &backend_event_tx).await?;
        let (connection, target_addr) =
            establish_connection(&endpoint, mdns, false, &backend_event_tx).await?;

        receiver::run_loop(connection, endpoint, target_addr, store, dst_dir).await
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("User interrupt, shutting down");
        }
        _ = run_backend => {
            println!("Connection terminated, shutting down")
        }
    }

    Ok(())
}
