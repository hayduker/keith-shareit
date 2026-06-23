use clap::{Parser, Subcommand};
use iroh_blobs::ticket::BlobTicket;
use std::path::PathBuf;

/// Send a file or directory between two machines, using blake3 verified streaming.
#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Send a file or directory.
    Send(SendArgs),

    /// Receive a file or directory.
    #[clap(visible_alias = "recv")]
    Receive(ReceiveArgs),
}

#[derive(Parser, Debug)]
pub struct SendArgs {
    // Path to the file or directory to send.
    //
    // The last component of the path will be used as the name of the data
    // being shared.
    // pub path: PathBuf,
}

#[derive(Parser, Debug)]
pub struct ReceiveArgs {
    // The ticket to use to connect to the sender.
    // pub ticket: BlobTicket,
}
