use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use copypasta::{ClipboardContext, ClipboardProvider};
use ratatui::{
    DefaultTerminal,
    crossterm::{
        event::{
            self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent,
            KeyEventKind,
        },
        execute,
    },
    layout::{Direction, Layout},
};
use tokio::sync::mpsc;
use tui_tree_widget::TreeState;

use crate::{
    backend::BackendEvent,
    frontend::{TuiCommand, log::LogState},
};

#[derive(Debug, PartialEq)]
pub enum ActivePane {
    Tree,
    Logs,
}

#[derive(Debug)]
pub struct App {
    pub library_tree_state: TreeState<(String, bool)>,
    should_exit: bool,
    tui_cmd_tx: mpsc::Sender<TuiCommand>,
    backend_event_rx: mpsc::Receiver<BackendEvent>,
    pub logs: Vec<String>,
    pub log_state: LogState,
    pub src_path: PathBuf,
    pub active_pane: ActivePane,
    pub show_popup: bool,
    pub input_value: String,
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
            logs: vec![],
            log_state: LogState::default(),
            src_path,
            active_pane: ActivePane::Tree,
            show_popup: false,
            input_value: String::new(),
        }
    }

    #[allow(clippy::single_match)]
    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        while !self.should_exit {
            terminal.draw(|frame| self.render(frame))?;

            while let Ok(event) = self.backend_event_rx.try_recv() {
                match event {
                    BackendEvent::StatusUpdate(msg) => self.logs.push(msg),
                    BackendEvent::TicketRequest => {
                        execute!(std::io::stdout(), EnableBracketedPaste);
                        self.show_popup = true
                    }
                    // BackendEvent::ConnectionSecured => self.logs.push("Connected to Peer!".into()),
                    // BackendEvent::DownloadStarted => self.logs.push("Downloading data...".into()),
                    // BackendEvent::DownloadComplete => self.logs.push("Download Complete!".into()),
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
                Event::Paste(pasted_text) => {
                    if self.show_popup {
                        self.input_value.push_str(&pasted_text);
                    }
                }
                _ => {}
            };
        }
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        if self.show_popup {
            match key_event.code {
                KeyCode::Char(c) => {
                    self.input_value.push(c);
                }
                KeyCode::Backspace => {
                    self.input_value.pop();
                }
                KeyCode::Enter => {
                    let ticket_str = self.input_value.drain(..).collect();

                    if let Err(e) = self
                        .tui_cmd_tx
                        .try_send(TuiCommand::TicketInput(ticket_str))
                    {
                        self.logs.push(format!("Failed to notify backend: {}", e));
                    }

                    self.input_value.clear();
                    execute!(std::io::stdout(), DisableBracketedPaste);
                    self.show_popup = false;
                }
                KeyCode::Esc => {
                    self.input_value.clear();
                    execute!(std::io::stdout(), DisableBracketedPaste);
                    self.show_popup = false;
                }
                _ => {}
            }
        } else {
            match key_event.code {
                KeyCode::Char('q') => self.exit(),
                KeyCode::Enter => {
                    if let Some((selected, _)) = self.library_tree_state.selected().iter().last() {
                        let full_path = self.src_path.clone().join(selected);

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
                KeyCode::Down => match self.active_pane {
                    ActivePane::Tree => {
                        self.library_tree_state.key_down();
                    }
                    ActivePane::Logs => self.log_state.scroll_down(),
                },
                KeyCode::Up => match self.active_pane {
                    ActivePane::Tree => {
                        self.library_tree_state.key_up();
                    }
                    ActivePane::Logs => self.log_state.scroll_up(),
                },
                KeyCode::Left => {
                    self.library_tree_state.key_left();
                }
                KeyCode::Right => {
                    self.library_tree_state.key_right();
                }
                KeyCode::Tab | KeyCode::BackTab => {
                    self.active_pane = match self.active_pane {
                        ActivePane::Tree => ActivePane::Logs,
                        ActivePane::Logs => ActivePane::Tree,
                    };
                }
                KeyCode::Char('c') => {
                    if let Ok(mut ctx) = ClipboardContext::new() {
                        if ctx
                            .set_contents("sneaky clipboard stuff".to_string())
                            .is_ok()
                        {
                            self.logs.push("Copied stuff to clipboard".into());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn exit(&mut self) {
        self.should_exit = true;
    }
}
