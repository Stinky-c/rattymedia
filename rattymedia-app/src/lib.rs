#![no_std]
extern crate alloc;

mod app;
mod components;
pub mod error;
mod input;
pub mod tabs;

pub use app::App;
pub use input::InputEvent;

/*
TODO:
Remote impl Terminal error into
 */
