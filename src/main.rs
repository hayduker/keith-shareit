use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use crate::{
    app::App,
    backend::{
        endpoint::{create_endpoint, establish_connection},
        receiver, sender,
    },
    cli::{Args, Commands},
    store::KeithStore,
};

mod app;
mod backend;
mod cli;
mod secret;
mod store;
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

    let store = KeithStore::new().await?;

    tokio::spawn(async move {
        if let Ok((endpoint, mdns, _router)) =
            create_endpoint(true, &store, &backend_event_tx).await
            && let Ok((connection, _)) =
                establish_connection(&endpoint, mdns, true, &backend_event_tx).await
        {
            sender::run_loop(connection, store, tui_cmd_rx, backend_event_tx).await
        } else {
            anyhow::bail!("Failed to establish endpoint or connection.");
        }
    });

    ratatui::run(|terminal| {
        let mut app = App::new(src_dir, tui_cmd_tx, backend_event_rx);
        app.run(terminal)
    })?;

    Ok(())
}

async fn run_receiver(dst_dir: PathBuf) -> Result<()> {
    let (tui_cmd_tx, tui_cmd_rx) = tokio::sync::mpsc::channel(100);
    let (backend_event_tx, mut _backend_event_rx) = tokio::sync::mpsc::channel(100);

    let store = KeithStore::new().await?;

    let backend_handle = tokio::spawn(async move {
        if let Ok((endpoint, mdns, _router)) =
            create_endpoint(false, &store, &backend_event_tx).await
            && let Ok((connection, target_addr)) =
                establish_connection(&endpoint, mdns, false, &backend_event_tx).await
        {
            receiver::run_loop(
                connection,
                endpoint,
                target_addr,
                store,
                dst_dir,
                tui_cmd_rx,
            )
            .await
        } else {
            anyhow::bail!("Failed to establish endpoint or connection.");
        }
    });

    tokio::signal::ctrl_c().await?;
    println!("Shutting down receiver.");
    tui_cmd_tx.send(backend::TuiCommand::Shutdown).await?;
    println!("Send shutdown msg to backend, waiting for it to finish");
    let _ = backend_handle.await?;
    println!("Done!");

    Ok(())
}
