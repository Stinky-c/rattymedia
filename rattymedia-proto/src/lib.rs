#![cfg_attr(not(feature = "std"), no_std)]
#![doc = "Transport-agnostic full-duplex RPC protocol primitives, codec, and optional std endpoint runtime."]

#[cfg(feature = "std")]
extern crate std;

mod codec;
mod frame;
mod io;

#[cfg(feature = "std")]
pub mod endpoint;

/// Default maximum payload bytes for typed request/event bodies.
pub const DEFAULT_MAX_PAYLOAD: usize = 256;
/// Default scratch bytes used for postcard serialization buffers.
pub const DEFAULT_SERIALIZED_CAP: usize = 384;
/// Default frame decode/encode buffer capacity.
pub const DEFAULT_FRAME_CAP: usize = 512;
/// Default outbound queue capacity for endpoint startup helpers.
pub const DEFAULT_OUTBOUND_QUEUE_CAPACITY: usize = 16;

pub use codec::{decode_packet, encode_packet, FrameDecoder};
#[cfg(feature = "std")]
pub use endpoint::RpcEndpoint;
pub use frame::{Frame, FrameHeader, MessageKind, ProtocolError, PROTOCOL_VERSION};
pub use io::{read_frame, write_frame};

#[cfg(feature = "std")]
pub type DefaultRpcEndpoint<T> =
    endpoint::RpcEndpoint<T, DEFAULT_MAX_PAYLOAD, DEFAULT_SERIALIZED_CAP, DEFAULT_FRAME_CAP>;

#[cfg(test)]
mod tests;
