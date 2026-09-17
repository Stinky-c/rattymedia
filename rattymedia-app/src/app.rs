use crate::InputEvent;
use alloc::vec;
use log::info;
use ratatui::backend::Backend;
use ratatui::prelude::{Constraint, Layout};
use ratatui::style::Style;
use ratatui::widgets::{Block, RatatuiMascot, Tabs};
use ratatui::{Frame, Terminal};

pub struct App<TERM: Backend> {
    terminal: Terminal<TERM>,
    should_quit: bool,
    state: AppState,
}
pub struct AppState {
    // fps: FpsCounterState,
}

impl<TERM: Backend> App<TERM> {
    pub fn new(terminal: Terminal<TERM>) -> App<TERM> {
        Self {
            terminal,
            should_quit: false,
            state: AppState {
                // fps: FpsCounterState::new(),
            },
        }
    }

    /// Returns true if loop should quit
    pub fn try_run(&mut self) -> Result<bool, <TERM as Backend>::Error> {
        let _frame = self
            .terminal
            .try_draw(|frame| Self::try_draw(frame, &mut self.state))?;
        Ok(self.should_quit)
    }

    pub fn send_input_event(&mut self, event: InputEvent) -> Result<(), <TERM as Backend>::Error> {
        match event {
            InputEvent::Noop => Ok(()),
            InputEvent::Click(_, _) => todo!(),
            InputEvent::Resize(_, _) => {
                info!("Got resize event");
                self.terminal.autoresize()
            }
            InputEvent::Quit => {
                self.should_quit = true;
                Ok(())
            }
        }
    }

    fn try_draw(frame: &mut Frame, _state: &mut AppState) -> Result<(), <TERM as Backend>::Error> {
        let _horizontal = Layout::horizontal([Constraint::Percentage(100)]).spacing(1);
        let _vertical = Layout::vertical([Constraint::Length(2), Constraint::Fill(1)]).spacing(1);

        // let area

        let tabs = Tabs::new(vec!["Hello", "World"])
            .block(Block::bordered().title("rattymedia"))
            .style(Style::default().white());

        frame.render_widget(tabs, frame.area());

        let logo = RatatuiMascot::new();
        frame.render_widget(logo, frame.area());
        // frame.render_stateful_widget(
        //     crate::components::fps::FpsCounter,
        //     frame.area(),
        //     &mut state.fps,
        // );
        Ok(())
    }
}
