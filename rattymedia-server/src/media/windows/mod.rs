//! Code copied from and modified as needed
//! https://github.com/Nerixyz/current-song2/blob/7b447b1a3930aadb1a2686e955c733a6e5bf05f1/lib/win-gsmtc/Cargo.toml
//! source licensed under MIT or Apache 2.0
mod conversion;
mod manager;
mod model;
mod session;
mod util;

pub use manager::{ManagerEvent, SessionManager};
pub use model::*;
pub use session::SessionUpdateEvent;

pub(crate) type EventRegistrationToken = i64;
