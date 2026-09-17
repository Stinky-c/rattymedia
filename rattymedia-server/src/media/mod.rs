pub(crate) mod abc;
#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

#[cfg(windows)]
pub use windows::WindowsMedia;

pub use abc::{MediaSession, SessionControl};
