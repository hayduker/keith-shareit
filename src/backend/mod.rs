#[derive(Debug, Clone)]
pub enum BackendEvent {
    _PeerDiscovered,
    ConnectionSecured,
    _DownloadStarted,
    _DownloadComplete,
    StatusUpdate(String),
}

pub mod endpoint;
pub mod receiver;
pub mod sender;
pub mod store;
