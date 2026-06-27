use std::path::PathBuf;

#[derive(Debug)]
pub enum TuiCommand {
    SyncPath(PathBuf, PathBuf),
    Shutdown,
}

pub mod app;
pub mod cli;
pub mod log;
pub mod tree;
pub mod ui;
