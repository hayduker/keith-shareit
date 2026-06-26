use std::path::PathBuf;

#[derive(Debug)]
pub enum TuiCommand {
    SyncPath(PathBuf, PathBuf),
    Shutdown,
}

#[derive(Debug, Clone)]
pub enum BackendEvent {
    _PeerDiscovered,
    ConnectionSecured,
    _DownloadStarted,
    _DownloadComplete,
    StatusUpdate(String),
}
