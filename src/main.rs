use clap::Parser;

use crate::{
    cli::{Args, Commands},
    provider::send,
    requester::receive,
};

mod cli;
mod provider;
mod requester;
mod secret;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let result = match args.command {
        Commands::Send(args) => send(args.path).await,
        Commands::Receive(args) => receive(args.ticket).await,
    };

    if let Err(e) = &result {
        eprintln!("{e}");
    }

    match result {
        Ok(()) => std::process::exit(0),
        Err(_) => std::process::exit(1),
    }
}
