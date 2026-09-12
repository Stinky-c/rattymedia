use crate::components::Component;
use ratatui::prelude::*;
use ratatui::widgets::Paragraph;
use std::convert::Infallible;
use std::time::Instant;

pub struct FpsCounter;

#[derive(Debug, Clone, PartialEq)]
pub struct FpsCounterState {
    last_tick_update: Instant,
    tick_count: u32,
    ticks_per_second: f64,

    last_frame_update: Instant,
    frame_count: u32,
    frames_per_second: f64,
}

impl FpsCounterState {
    pub fn new() -> Self {
        Self {
            last_tick_update: Instant::now(),
            tick_count: 0,
            ticks_per_second: 0.0,
            last_frame_update: Instant::now(),
            frame_count: 0,
            frames_per_second: 0.0,
        }
    }

    fn tick(&mut self) {
        self.tick_count += 1;
        let now = Instant::now();
        let elapsed = (now - self.last_tick_update).as_secs_f64();
        if elapsed >= 1.0 {
            self.ticks_per_second = self.tick_count as f64 / elapsed;
            self.last_tick_update = now;
            self.tick_count = 0;
        }

        self.frame_count += 1;
        let now = Instant::now();
        let elapsed = (now - self.last_frame_update).as_secs_f64();
        if elapsed >= 1.0 {
            self.frames_per_second = self.frame_count as f64 / elapsed;
            self.last_frame_update = now;
            self.frame_count = 0;
        }
    }
}

impl Component for FpsCounter {}
impl StatefulWidget for FpsCounter {
    type State = FpsCounterState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        state.tick();
        let [top, _] = Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);
        let message = format!(
            "{:.2} ticks/sec, {:.2} FPS",
            state.ticks_per_second, state.frames_per_second
        );
        let span = Span::styled(message, Style::new().dim());
        let paragraph = Paragraph::new(span).right_aligned();
        paragraph.render(area, buf);
    }
}
