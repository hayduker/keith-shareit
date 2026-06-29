use anyhow::Result;
use clap::Parser;
use std::{fs::File, path::PathBuf};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use crate::{
    backend::{
        endpoint::{create_endpoint, establish_connection},
        receiver, sender,
        store::KeithStore,
    },
    frontend::{
        TuiCommand,
        app::App,
        cli::{Args, Commands},
    },
};

mod backend;
mod frontend;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.with_trace {
        init_log()?;
    }

    match args.command {
        Commands::Send(args) => run_sender(args.src_dir).await,
        Commands::Receive(args) => run_receiver(args.dst_dir).await,
    }
}

async fn run_sender(src_dir: PathBuf) -> Result<()> {
    let (tui_cmd_tx, mut tui_cmd_rx) = tokio::sync::mpsc::channel(100);
    let (backend_event_tx, backend_event_rx) = tokio::sync::mpsc::channel(100);

    let backend_handle = tokio::spawn(async move {
        let store = KeithStore::new().await?;
        let (endpoint, _router) = create_endpoint(true, &store, &backend_event_tx).await?;
        if let Some((connection, _)) =
            establish_connection(&endpoint, true, &backend_event_tx, &mut tui_cmd_rx).await?
        {
            sender::run_loop(connection, store, tui_cmd_rx, backend_event_tx).await?;
        }

        endpoint.close().await;

        anyhow::Ok(())
    });

    ratatui::run(|terminal| {
        let mut app = App::new(src_dir, tui_cmd_tx.clone(), backend_event_rx);
        app.run(terminal)
    })?;

    if !tui_cmd_tx.is_closed() {
        tui_cmd_tx.send(TuiCommand::Shutdown).await?;
    }

    let _ = backend_handle.await;

    Ok(())
}

async fn run_receiver(dst_dir: PathBuf) -> Result<()> {
    let (tui_cmd_tx, mut tui_cmd_rx) = tokio::sync::mpsc::channel(100);
    let (backend_event_tx, _) = tokio::sync::mpsc::channel(100);

    let backend_handle = tokio::spawn(async move {
        let store = KeithStore::new().await?;
        let (endpoint, _router) = create_endpoint(false, &store, &backend_event_tx).await?;
        if let Some((connection, target_addr)) =
            establish_connection(&endpoint, false, &backend_event_tx, &mut tui_cmd_rx).await?
        {
            receiver::run_loop(
                connection,
                endpoint.clone(),
                target_addr,
                store,
                dst_dir,
                tui_cmd_rx,
            )
            .await?;
        }

        endpoint.close().await;

        anyhow::Ok(())
    });

    tokio::signal::ctrl_c().await?;

    if !tui_cmd_tx.is_closed() {
        tui_cmd_tx.send(TuiCommand::Shutdown).await?;
    }

    let _ = backend_handle.await?;

    Ok(())
}

fn init_log() -> Result<()> {
    let log_file = File::create("keith.log")?;
    let file_layer = fmt::layer().with_writer(log_file).with_ansi(false);
    let filter_layer =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("iroh=trace,info"));
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(file_layer)
        .init();

    Ok(())
}
