use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    symbols::border,
    text::Line,
    widgets::{Block, Paragraph, Scrollbar, ScrollbarOrientation},
};
use tui_tree_widget::{Tree, TreeItem};
use walkdir::WalkDir;

use crate::ui::app::{ActivePane, App};

impl App {
    pub fn render(&mut self, frame: &mut Frame) {
        let main_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(0), Constraint::Length(1)])
            .split(frame.area());

        let body_area = main_layout[0];
        let footer_area = main_layout[1];

        let panes = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(body_area);

        self.render_tree(frame, panes[0]);
        self.render_log_screen(frame, panes[1]);
        self.render_help_text(frame, footer_area);
    }

    fn render_help_text(&mut self, frame: &mut Frame, area: Rect) {
        let help_text = Line::from(vec![
            " Navigate ".dark_gray(),
            "<↑/↓/←/→> ".yellow(),
            "| Transfer ".dark_gray(),
            "<enter> ".yellow(),
            "| Switch Pane ".dark_gray(),
            "<tab> ".yellow(),
            "| Quit ".dark_gray(),
            "<q> ".yellow(),
        ]);

        let footer = Paragraph::new(help_text);
        frame.render_widget(footer, area);
    }

    fn render_tree(&mut self, frame: &mut Frame, area: Rect) {
        let title = Line::from(" Library ".bold());
        let top_instructions = Line::from(vec![" Quit ".into(), "<q> ".blue().bold()]);

        let block = match self.active_pane {
            ActivePane::Tree => Block::bordered()
                .title(title.centered())
                .title_top(top_instructions.right_aligned())
                .border_set(border::THICK)
                .border_style(Style::default().fg(Color::Blue)),
            ActivePane::Logs => Block::bordered()
                .title(title.centered())
                .title_top(top_instructions.right_aligned())
                .border_set(border::THICK)
                .dark_gray(),
        };

        let mut root_item =
            TreeItem::new_leaf((String::new(), false), self.src_path.display().to_string());

        for entry in WalkDir::new(self.src_path.clone()).sort_by_file_name() {
            if entry.is_err() {
                eprintln!("error with entry: {:?}", entry);
                continue;
            }
            let entry = entry.unwrap();

            let short_path = entry.path().strip_prefix(self.src_path.clone());
            if short_path.is_err() {
                eprintln!("error removing prefix: {:?}", entry);
                continue;
            }

            let file = match short_path.unwrap().to_str() {
                Some(s) => s.to_string(),
                None => continue,
            };

            let mut parts = file
                .split('/')
                .filter(|p| !p.is_empty())
                .map(|p| p.to_string())
                .peekable();

            let mut current_item = &mut root_item;
            let mut current_path = String::new();

            while let Some(part) = parts.next() {
                // separate parts with /
                if !current_path.is_empty() {
                    current_path.push('/');
                }
                // accumulate full path
                current_path.push_str(&part);

                // find existing child with matching path
                let next_idx = current_item
                    .children()
                    .iter()
                    .position(|child| child.identifier().0 == current_path);

                if let Some(index) = next_idx {
                    // item exists, recurse
                    current_item = current_item.child_mut(index).unwrap();
                } else {
                    // item does not exist, create a new item
                    let is_leaf = parts.peek().is_none();
                    let new_item = TreeItem::new_leaf((current_path.clone(), is_leaf), part);

                    // recurse
                    current_item.add_child(new_item).unwrap();
                    current_item = current_item
                        .child_mut(current_item.children().len() - 1)
                        .unwrap();
                }
            }
        }

        let items = [root_item];
        let tree = Tree::new(&items)
            .unwrap()
            .experimental_scrollbar(Some(
                Scrollbar::new(ScrollbarOrientation::VerticalRight)
                    .begin_symbol(None)
                    .end_symbol(None)
                    .track_symbol(None)
                    .style(Color::Blue),
            ))
            .highlight_style(Style::new().fg(Color::Black).bg(Color::LightBlue))
            .node_closed_symbol("⏵ ")
            .node_open_symbol("⏷ ")
            .block(block);

        frame.render_stateful_widget(tree, area, &mut self.library_tree_state);
    }
}
