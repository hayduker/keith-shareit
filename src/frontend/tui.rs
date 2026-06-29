use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::Stylize,
    text::Line,
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

use crate::frontend::app::App;

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

        if self.show_popup {
            self.render_popup(frame);
        }
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

    fn render_popup(&mut self, frame: &mut Frame) {
        let max_width_percentage = 60;
        let total_screen_width = frame.area().width;
        let popup_width = (total_screen_width * max_width_percentage) / 100;

        let inner_width = popup_width.saturating_sub(2) as usize;

        let total_chars = self.input_value.len();
        let required_text_lines = if inner_width > 0 {
            ((total_chars + inner_width - 1) / inner_width).max(1)
        } else {
            1
        };

        let popup_height = (required_text_lines + 2) as u16;

        let area = get_centered_rect(max_width_percentage, popup_height, frame.area());

        frame.render_widget(Clear, area);

        let popup_block = Block::default()
            .title(" Enter peer's ticket ")
            .borders(Borders::ALL);

        let popup_text = Paragraph::new(self.input_value.as_str())
            .block(popup_block)
            .wrap(Wrap { trim: false });

        frame.render_widget(popup_text, area);

        if inner_width > 0 {
            let total_chars = self.input_value.len();

            let cursor_line = total_chars / inner_width;
            let cursor_col = total_chars % inner_width;

            frame.set_cursor_position((
                area.x + 1 + cursor_col as u16,
                area.y + 1 + cursor_line as u16,
            ));
        }
    }
}

fn get_centered_rect(percent_x: u16, height_y: u16, base_rect: Rect) -> Rect {
    let vertical_padding = base_rect.height.saturating_sub(height_y) / 2;

    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(vertical_padding),
            Constraint::Length(height_y),
            Constraint::Length(vertical_padding),
        ])
        .split(base_rect);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
