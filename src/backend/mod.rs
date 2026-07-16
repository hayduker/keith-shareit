//! The backend module handles the core logic for the media sharing library.
//!
//! This includes peer-to-peer connections, file transfer, and state management
//! for both the desktop (TUI) and phone (logging) applications.

/// Represents events that can occur in the backend, signaling various states
/// or actions during the media sharing process.
#[derive(Debug, Clone)]
pub enum BackendEvent {
    /// A peer has been discovered on the network.
    _PeerDiscovered,
    /// A secure connection has been established with a peer.
    ConnectionSecured,
    /// A file download has started.
    _DownloadStarted,
    /// A file download has completed.
    _DownloadComplete,
    /// An update regarding the current status of an operation.
    StatusUpdate(String),
    /// A request for a ticket to initiate a file transfer.
    TicketRequest,
}

pub mod direct;
pub mod endpoint;
pub mod receiver;
pub mod secret;
pub mod sender;
pub mod store;
pub mod ticket;
