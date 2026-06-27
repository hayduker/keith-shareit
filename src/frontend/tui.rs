use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::Stylize,
    text::Line,
    widgets::Paragraph,
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
}
