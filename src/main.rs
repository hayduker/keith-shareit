use clap::Parser;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::io::AsyncBufReadExt;

use crate::{
    app::App,
    backend::{
        BackendEvent, TuiCommand,
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
    let args = Args::parse();
    let is_sender = match args.command {
        Commands::Send => true,
        Commands::Receive => false,
    };

    let (tui_cmd_tx, tui_cmd_rx) = tokio::sync::mpsc::channel(100);
    let (backend_event_tx, mut backend_event_rx) = tokio::sync::mpsc::channel(100);

    let store = KeithStore::new().await?;

    println!("Starting file sharing app, is_sender = {is_sender}");

    let backend_handle = tokio::spawn(async move {
        if let Ok((endpoint, mdns, _router)) =
            create_endpoint(is_sender, &store, &backend_event_tx).await
            && let Ok((connection, target_addr)) =
                establish_connection(&endpoint, mdns, is_sender, &backend_event_tx).await
        {
            if is_sender {
                sender::run_loop(connection, store, tui_cmd_rx, backend_event_tx)
                    .await
                    .ok();
            } else {
                receiver::run_loop(connection, endpoint, target_addr, store)
                    .await
                    .ok();
            }
        }
    });

    println!("Spawned backend task");

    if is_sender {
        ratatui::run(|terminal| {
            let mut app = App::new(tui_cmd_tx, backend_event_rx);
            app.run(terminal)
        })?;
    } else {
        tokio::signal::ctrl_c().await?;
        println!("Shutting down receiver.");
    }

    // if is_sender {
    //     // tokio::spawn(async move {
    //     //     while let Some(event) = backend_event_rx.recv().await {
    //     //         match event {
    //     //             BackendEvent::StatusUpdate(msg) => println!("[Engine Status]: {}", msg),
    //     //             BackendEvent::PeerDiscovered(id) => {
    //     //                 println!("[Engine Event]: Found peer node {}", id)
    //     //             }
    //     //             BackendEvent::ConnectionSecured => {
    //     //                 println!("[Engine Event]: Cryptographic link established!")
    //     //             }
    //     //             BackendEvent::DownloadStarted => {
    //     //                 println!("[Engine Event]: Transfer initiated...")
    //     //             }
    //     //             BackendEvent::DownloadComplete => {
    //     //                 println!("[Engine Event]: Transfer successfully written to disk.")
    //     //             }
    //     //         }
    //     //     }
    //     // });
    //     //
    //     // println!("[UI Simulator]: Spawned listener for backend events");

    //     let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    //     let mut line = String::new();

    //     println!("\n[UI Simulator]: Ready to send. Paste or type a file path and press Enter");

    //     loop {
    //         line.clear();
    //         tokio::select! {
    //             _ = tokio::signal::ctrl_c() => {
    //                 println!("\nUser aborted interface. Sending shutdown signal...");
    //                 tui_cmd_tx.send(TuiCommand::Shutdown).await.ok();
    //                 break;
    //             }
    //             read_res = reader.read_line(&mut line) => {
    //                 if let Err(e) = read_res {
    //                     eprintln!("Error reading console input: {:?}", e);
    //                     break;
    //                 }

    //                 let trimmed = line.trim();
    //                 if trimmed.is_empty() {
    //                     continue;
    //                 }

    //                 if trimmed == "exit" || trimmed == "quit" {
    //                     tui_cmd_tx.send(TuiCommand::Shutdown).await.ok();
    //                     break;
    //                 }

    //                 match PathBuf::from_str(trimmed) {
    //                     Ok(path) => {
    //                         println!("[UI Simulator]: Dispatching sync command for {:?}", path);
    //                         // Drop the command straight down the pipe!
    //                         if tui_cmd_tx.send(TuiCommand::SyncPath(path)).await.is_err() {
    //                             eprintln!("[UI Simulator Error]: Backend command loop has shut down.");
    //                             break;
    //                         }
    //                     }
    //                     Err(e) => eprintln!("Invalid Path Syntax: {:?}", e),
    //                 }
    //             }
    //         }
    //     }
    // } else {
    //     // If it's a receiver, the main thread has no terminal inputs to capture.
    //     // We simply keep the process alive while the background loops handle everything.
    //     println!(
    //         "\n[UI Simulator]: --- Running in listen-only receiver mode. Press Ctrl+C to quit ---"
    //     );
    //     tokio::signal::ctrl_c().await?;
    //     println!("[UI Simulator]: Shutting down receiver.");
    // }

    // println!("[UI Simulator]: Waiting backend cleanup...");
    // // This ensures main doesn't terminate until the background thread completely resolves its loops
    // let _ = backend_handle.await;
    // println!("[UI Simulator]: Goodbye!");

    Ok(())
}
