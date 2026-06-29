#[derive(Debug, Clone)]
pub enum BackendEvent {
    _PeerDiscovered,
    ConnectionSecured,
    _DownloadStarted,
    _DownloadComplete,
    StatusUpdate(String),
    TicketRequest,
}

pub mod direct;
pub mod endpoint;
pub mod receiver;
pub mod secret;
pub mod sender;
pub mod store;
pub mod ticket;
