use crate::InputEvent;
use crate::components::fps::FpsCounterState;
use ratatui::backend::Backend;
use ratatui::layout::{Rect, Size};
use ratatui::style::Style;
use ratatui::widgets::{Block, RatatuiLogo, RatatuiMascot, Tabs};
use ratatui::{Frame, Terminal};
use std::time::{Duration, Instant};

pub struct App<TERM: Backend> {
    terminal: Terminal<TERM>,
    should_quit: bool,
    last_draw: Instant,
    state: AppState,
}
pub struct AppState {
    fps: FpsCounterState,
}

impl<TERM: Backend> App<TERM> {
    pub fn new(terminal: Terminal<TERM>) -> App<TERM> {
        Self {
            terminal,
            should_quit: false,
            last_draw: Instant::now(),
            state: AppState {
                fps: FpsCounterState::new(),
            },
        }
    }

    /// Returns true if loop should quit
    pub fn try_run(&mut self) -> Result<bool, <TERM as Backend>::Error> {
        let this_draw = Instant::now();
        let diff = this_draw.duration_since(self.last_draw);

        let frame = self
            .terminal
            .try_draw(|frame| Self::try_draw(&mut self.state, frame))?;
        Ok(self.should_quit)
    }

    pub fn send_input_event(&mut self, event: InputEvent) -> Result<(), <TERM as Backend>::Error> {
        match event {
            InputEvent::Noop => Ok(()),
            InputEvent::Click(_, _) => todo!(),
            InputEvent::Resize(x, y) => self.terminal.autoresize(),
            InputEvent::Quit => {
                self.should_quit = true;
                Ok(())
            }
        }
    }

    fn try_draw(state: &mut AppState, frame: &mut Frame) -> Result<(), <TERM as Backend>::Error> {
        let tabs = Tabs::new(vec!["Hello", "World"])
            .block(Block::bordered().title("rattymedia"))
            .style(Style::default().white());

        frame.render_widget(tabs, frame.area());
        let logo = RatatuiMascot::new();
        frame.render_widget(logo, frame.area());
        frame.render_stateful_widget(
            crate::components::fps::FpsCounter,
            frame.area(),
            &mut state.fps,
        );
        Ok(())
    }
}
