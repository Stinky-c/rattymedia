#![cfg_attr(not(feature = "std"), no_std)]
#![doc = "Transport-agnostic full-duplex RPC protocol primitives and codec."]

#[cfg(feature = "std")]
extern crate std;

mod codec;
mod frame;
mod io;

#[cfg(feature = "std")]
pub mod endpoint;

pub use codec::{decode_packet, encode_packet, FrameDecoder};
pub use frame::{Frame, FrameHeader, MessageKind, ProtocolError, PROTOCOL_VERSION};
pub use io::{read_frame, write_frame};

#[cfg(test)]
mod tests;
