use anyhow::Result;
use iroh::EndpointId;
use iroh_blobs::HashAndFormat;
use std::path::PathBuf;

#[derive(Debug)]
pub enum TuiCommand {
    SyncPath(PathBuf),
    Shutdown,
}

#[derive(Debug, Clone)]
pub enum BackendEvent {
    PeerDiscovered(EndpointId),
    ConnectionSecured,
    DownloadStarted,
    DownloadComplete,
    StatusUpdate(String),
}

pub mod endpoint;
pub mod receiver;
pub mod sender;
