use ratatui::{
    crossterm::event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    crossterm::execute,
};
use rattymedia_app::{App, InputEvent};
use std::io;
use std::io::stdout;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let terminal = ratatui::init();
    execute!(stdout(), EnableMouseCapture)?;
    let mut app = App::new(terminal);

    let res = loop {
        if app.try_run()? {
            break Ok(());
        }
        let event = input_loop()?;
        app.send_input_event(event)?;
    };

    ratatui::restore();
    execute!(stdout(), DisableMouseCapture)?;
    res
}

fn input_loop() -> io::Result<InputEvent> {
    if event::poll(Duration::from_millis(250)).expect("event polling failed") {
        let event = event::read()?;

        match event {
            Event::Mouse(mouse_event) if mouse_event.kind.is_up() => {
                Ok(InputEvent::Click(mouse_event.column, mouse_event.row))
            }
            Event::Resize(x, y) => Ok(InputEvent::Resize(x, y)),
            Event::Key(key) if key.code.is_char('q') => Ok(InputEvent::Quit),
            _ => Ok(InputEvent::Noop),
        }
    } else {
        Ok(InputEvent::Noop)
    }
}
