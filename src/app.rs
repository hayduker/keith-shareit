use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use ratatui::{
    DefaultTerminal,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
};
use tokio::sync::mpsc;
use tui_tree_widget::TreeState;

use crate::backend::{BackendEvent, TuiCommand};

#[derive(Debug)]
pub struct App {
    pub library_tree_state: TreeState<(String, bool)>,
    should_exit: bool,
    tui_cmd_tx: mpsc::Sender<TuiCommand>,
    backend_event_rx: mpsc::Receiver<BackendEvent>,
    pub logs: Vec<String>,
    pub src_path: PathBuf,
}

impl App {
    pub fn new(
        src_path: PathBuf,
        tui_cmd_tx: mpsc::Sender<TuiCommand>,
        backend_event_rx: mpsc::Receiver<BackendEvent>,
    ) -> Self {
        Self {
            library_tree_state: TreeState::default(),
            should_exit: false,
            tui_cmd_tx,
            backend_event_rx,
            logs: vec!["Initializing system...".into()],
            src_path,
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        while !self.should_exit {
            terminal.draw(|frame| self.render(frame))?;

            while let Ok(event) = self.backend_event_rx.try_recv() {
                match event {
                    BackendEvent::StatusUpdate(msg) => self.logs.push(msg),
                    BackendEvent::ConnectionSecured => self.logs.push("Connected to Peer!".into()),
                    BackendEvent::DownloadStarted => self.logs.push("Downloading data...".into()),
                    BackendEvent::DownloadComplete => self.logs.push("Download Complete!".into()),
                    _ => {}
                }
            }

            self.handle_events()?;
        }
        Ok(())
    }

    fn handle_events(&mut self) -> Result<()> {
        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                    self.handle_key_event(key_event)
                }
                _ => {}
            };
        }
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Enter => {
                if let Some((selected, _)) = self.library_tree_state.selected().iter().last() {
                    let full_path = self.src_path.clone().join(selected);
                    self.logs.push(format!("Path selected: {:?}", full_path));

                    if let Err(e) = self
                        .tui_cmd_tx
                        .try_send(TuiCommand::SyncPath(full_path, self.src_path.clone()))
                    {
                        self.logs.push(format!("Failed to notify backend: {}", e));
                    }
                }
            }
            KeyCode::Char(' ') => {
                self.library_tree_state.toggle_selected();
            }
            KeyCode::Down => {
                self.library_tree_state.key_down();
            }
            KeyCode::Up => {
                self.library_tree_state.key_up();
            }
            KeyCode::Left => {
                self.library_tree_state.key_left();
            }
            KeyCode::Right => {
                self.library_tree_state.key_right();
            }
            _ => {}
        }
    }

    fn exit(&mut self) {
        let _ = self.tui_cmd_tx.try_send(TuiCommand::Shutdown);
        self.should_exit = true;
    }
}
