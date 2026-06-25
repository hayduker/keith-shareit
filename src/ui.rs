use ratatui::{
    Frame,
    style::{Color, Style, Stylize},
    symbols::border,
    text::Line,
    widgets::{Block, Scrollbar, ScrollbarOrientation},
};
use tui_tree_widget::{Tree, TreeItem};
use walkdir::WalkDir;

use crate::app::App;

impl App {
    pub fn render(&mut self, frame: &mut Frame) {
        let title = Line::from(" Library ".bold());
        let top_instructions = Line::from(vec![" Quit ".into(), "<q> ".blue().bold()]);
        let block = Block::bordered()
            .title(title.centered())
            .title_top(top_instructions.right_aligned())
            .border_set(border::THICK);

        let root_path = String::from("/home/derek/newmusic");
        let mut root_item = TreeItem::new_leaf((String::new(), false), root_path.clone());

        for entry in WalkDir::new(root_path.clone()).sort_by_file_name() {
            if entry.is_err() {
                eprintln!("error with entry: {:?}", entry);
                continue;
            }
            let entry = entry.unwrap();

            let short_path = entry.path().strip_prefix(root_path.clone());
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

        frame.render_stateful_widget(tree, frame.area(), &mut self.library_tree_state);
    }
}
