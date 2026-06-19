use anyhow::Result;
use clap::{Parser, Subcommand};
use iroh::{Endpoint, SecretKey, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore, ticket::BlobTicket};
use std::path::PathBuf;

/// A simple file transfer utility
#[derive(Parser)]
#[command(name = "d2j", about = "Send and receive files peer-to-peer", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Send a file and generate a ticket
    Send {
        /// The path to the file you want to send
        filename: PathBuf,
    },
    /// Receive a file using a ticket
    Receive {
        /// The connection ticket provided by the sender
        ticket: String,
        /// The path where the received file should be saved
        filename: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let secret_key = find_or_create_secret_key("endpoint.key")?;

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .bind()
        .await?;

    let endpoint_id = endpoint.id();
    println!("My endpoint ID = {}", endpoint_id);

    let store = MemStore::new();
    let blobs = BlobsProtocol::new(&store, None);

    match cli.command {
        Commands::Send { filename } => {
            println!("Preparing to send {}", filename.display());
            let abs_path = std::path::absolute(&filename)?;

            println!("Hashing file...");
            let tag = store.blobs().add_path(abs_path).await?;

            let ticket = BlobTicket::new(endpoint_id.into(), tag.hash, tag.format);
            println!("File hashed. Fetch the file by running:");
            println!("cargo run -- receive {ticket} {}", filename.display());
        }
        Commands::Receive { ticket, filename } => {}
    }

    Ok(())
}

fn find_or_create_secret_key(filename: &str) -> Result<SecretKey> {
    match std::fs::read(filename) {
        Ok(bytes) => Ok(SecretKey::from_bytes(&bytes.as_slice().try_into()?)),
        Err(_) => {
            let key = SecretKey::generate();
            std::fs::write(filename, key.to_bytes())?;
            Ok(key)
        }
    }
}
