use clap::{Parser, Subcommand};

/// Send files or directories from one machine to another upon selecting them from
/// a directory tree rendered in a TUI.
#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Pick files to send interactively.
    Send,

    /// Receive files passively.
    #[clap(visible_alias = "recv")]
    Receive,
}
