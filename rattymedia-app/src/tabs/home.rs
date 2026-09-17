use crate::app::AppState;
use alloc::boxed::Box;
use core::error::Error;
use ratatui::Frame;

pub fn home(state: &mut AppState, frame: &mut Frame) -> Result<(), Box<dyn Error>> {
    Ok(())
}
