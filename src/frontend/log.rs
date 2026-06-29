use crate::frontend::app::{ActivePane, App};
use ratatui::{
    Frame,
    layout::{Margin, Rect},
    style::{Color, Style, Stylize},
    symbols::border,
    text::{Line, Span},
    widgets::{Block, Scrollbar, ScrollbarOrientation, ScrollbarState, Widget},
};

#[derive(Debug)]
pub struct LogState {
    vertical_scroll: usize,
    tail: bool,

    scrollbar_state: ScrollbarState,
    content_height: usize,
    viewport_height: usize,
}

impl Default for LogState {
    fn default() -> Self {
        LogState {
            vertical_scroll: 0,
            tail: true,

            scrollbar_state: ScrollbarState::default(),
            content_height: 0,
            viewport_height: 0,
        }
    }
}

impl LogState {
    pub fn scroll_up(&mut self) {
        if self.tail {
            // stop tailing but don't scroll
            self.tail = false;
        } else {
            self.vertical_scroll = self.vertical_scroll.saturating_sub(1);
        }
    }

    pub fn scroll_down(&mut self) {
        if self.is_at_bottom() {
            // start tailing but don't scroll
            self.tail = true;
        } else {
            self.vertical_scroll = self.vertical_scroll.saturating_add(1);
        }
    }

    pub fn is_at_bottom(&self) -> bool {
        self.vertical_scroll >= self.content_height.saturating_sub(self.viewport_height)
    }
}

impl App {
    pub(super) fn render_log_screen(&mut self, frame: &mut Frame, area: Rect) {
        let title = Line::from(" Log ".bold());

        let block = {
            let block = match self.active_pane {
                ActivePane::Logs => Block::bordered()
                    .title(title.centered())
                    .border_set(border::THICK)
                    .border_style(Style::default().fg(Color::Blue)),
                ActivePane::Tree => Block::bordered()
                    .title(title.centered())
                    .border_set(border::THICK)
                    .dark_gray(),
            };

            let bottom_indicator = if self.log_state.is_at_bottom() {
                if self.log_state.tail {
                    Line::from(" … following ".bold())
                } else {
                    Line::from(" … at end ".bold())
                }
            } else {
                Line::from(" ↓ more below ".bold())
            };

            block.title_bottom(match self.active_pane {
                ActivePane::Logs => bottom_indicator.left_aligned(),
                ActivePane::Tree => bottom_indicator.dark_gray().left_aligned(),
            })
        };

        let inner_area = block.inner(area);

        // update viewport height
        self.log_state.viewport_height = inner_area.height as usize;

        let first_width = (inner_area.width as usize).saturating_sub(0);
        let cont_width = (inner_area.width as usize).saturating_sub(2);

        // build entries from messages and wrap long lines
        let entries = self
            .logs
            .iter()
            .map(|msg| {
                let wrapped = word_wrap(&msg, first_width, cont_width);
                let first = wrapped.first().cloned().unwrap_or_default();

                let mut lines = vec![Line::from(vec![Span::raw(first)])];

                for chunk in wrapped.into_iter().skip(1) {
                    lines.push(Line::from(vec![Span::raw(chunk)]));
                }

                lines
            })
            .collect::<Vec<_>>();

        // count total height of all entries
        let content_height = {
            let mut content_height = 0;
            for text in &entries {
                content_height += text.len();
            }
            content_height
        };
        self.log_state.content_height = content_height;

        // if tail is enabled, scroll to the bottom
        if self.log_state.tail {
            self.log_state.vertical_scroll =
                content_height.saturating_sub(inner_area.height as usize);
        }

        // calculate number of lines to skip
        let mut scroll_skip = self.log_state.vertical_scroll;

        // draw lines to buffer
        let mut screen_y = 0;
        'outer: for entry in entries {
            for line in &entry {
                if scroll_skip > 0 {
                    scroll_skip -= 1;
                    continue;
                }

                frame.buffer_mut().set_line(
                    inner_area.left(),
                    inner_area.top() + screen_y,
                    line,
                    u16::MAX,
                );

                screen_y += 1;

                if screen_y >= inner_area.height {
                    break 'outer;
                }
            }
        }

        // draw borders
        block.render(area, frame.buffer_mut());

        // update scrollbar state
        self.log_state.scrollbar_state = self
            .log_state
            .scrollbar_state
            .content_length(content_height.saturating_sub(inner_area.height as usize))
            .viewport_content_length(content_height.saturating_sub(inner_area.height as usize))
            .position(self.log_state.vertical_scroll);

        // draw scrollbar
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .symbols(ratatui::symbols::scrollbar::VERTICAL)
            .begin_symbol(None)
            .track_symbol(None)
            .end_symbol(None);

        frame.render_stateful_widget(
            scrollbar,
            area.inner(Margin {
                vertical: 1,
                horizontal: 0,
            }),
            &mut self.log_state.scrollbar_state,
        );
    }
}

fn word_wrap(text: &str, first_width: usize, cont_width: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }

    let mut lines: Vec<String> = Vec::new();
    let mut current = String::new();

    for word in text.split_whitespace() {
        let width = if lines.is_empty() {
            first_width
        } else {
            cont_width
        };
        let sep = if current.is_empty() { 0 } else { 1 };
        if current.chars().count() + sep + word.chars().count() <= width {
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(word);
        } else {
            if !current.is_empty() {
                lines.push(std::mem::take(&mut current));
            }

            let mut remaining = word;
            loop {
                let width = if lines.is_empty() {
                    first_width
                } else {
                    cont_width
                };
                if remaining.chars().count() <= width {
                    break;
                }
                let split = remaining
                    .char_indices()
                    .nth(width)
                    .map(|(i, _)| i)
                    .unwrap_or(remaining.len());
                lines.push(remaining[..split].to_string());
                remaining = &remaining[split..];
            }
            current.push_str(remaining);
        }
    }

    if !current.is_empty() {
        lines.push(current);
    }

    lines
}
