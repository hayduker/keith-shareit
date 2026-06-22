use anyhow::Result;
use ratatui::{
    DefaultTerminal,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
};
use tui_tree_widget::TreeState;

#[derive(Debug)]
pub struct App {
    pub library_tree_state: TreeState<(String, bool)>,
    exit: bool,
}

impl App {
    pub fn new() -> Self {
        Self {
            library_tree_state: TreeState::default(),
            exit: false,
        }
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        while !self.exit {
            terminal.draw(|frame| self.render(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn handle_events(&mut self) -> Result<()> {
        match event::read()? {
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_event(key_event)
            }
            _ => {}
        };
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Enter | KeyCode::Char(' ') => {
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
        self.exit = true;
    }
}
