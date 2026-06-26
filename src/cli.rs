use clap::{Parser, Subcommand};
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
    /// Send files or directories.
    Send(SendArgs),

    /// Receive files or directories.
    #[clap(visible_alias = "recv")]
    Receive(ReceiveArgs),
}

#[derive(Parser, Debug)]
pub struct SendArgs {
    /// Path of directory to browse and send files from.
    pub src_dir: PathBuf,
}

#[derive(Parser, Debug)]
pub struct ReceiveArgs {
    /// The directory in which to copy downloads.
    pub dst_dir: PathBuf,
}
