//! Core frame types and protocol invariants.

use heapless::Vec;
use serde::{Deserialize, Serialize};

/// Current protocol version encoded in every frame.
pub const PROTOCOL_VERSION: u8 = 1;

/// Logical type of a protocol message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageKind {
    Request = 1,
    Reply = 2,
    Event = 3,
    Error = 4,
}

/// Fixed header carried by each protocol frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameHeader {
    pub version: u8,
    pub kind: MessageKind,
    pub flags: u8,
    pub opcode: u16,
    pub request_id: u32,
    pub payload_len: u16,
}

/// Protocol frame containing header and payload bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame<const MAX_PAYLOAD: usize> {
    pub header: FrameHeader,
    pub payload: Vec<u8, MAX_PAYLOAD>,
}

/// Errors produced by protocol encoding, decoding, validation, and I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidVersion(u8),
    InvalidPayloadLength,
    InvalidEventRequestId,
    BufferTooSmall,
    EncodeError,
    DecodeError,
    UnsupportedOpcode(u16),
    MissingOutboundQueue,
    Closed,
    Timeout,
    Io,
}

impl<const MAX_PAYLOAD: usize> Frame<MAX_PAYLOAD> {
    pub fn new(
        kind: MessageKind,
        flags: u8,
        opcode: u16,
        request_id: u32,
        payload: &[u8],
    ) -> Result<Self, ProtocolError> {
        let mut payload_vec = Vec::<u8, MAX_PAYLOAD>::new();
        payload_vec
            .extend_from_slice(payload)
            .map_err(|_| ProtocolError::BufferTooSmall)?;

        let payload_len =
            u16::try_from(payload_vec.len()).map_err(|_| ProtocolError::InvalidPayloadLength)?;
        let header = FrameHeader {
            version: PROTOCOL_VERSION,
            kind,
            flags,
            opcode,
            request_id,
            payload_len,
        };
        let frame = Self {
            header,
            payload: payload_vec,
        };
        frame.validate()?;
        Ok(frame)
    }

    /// Enforces frame invariants shared by both endpoints.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.header.version != PROTOCOL_VERSION {
            return Err(ProtocolError::InvalidVersion(self.header.version));
        }

        if self.header.payload_len as usize != self.payload.len() {
            return Err(ProtocolError::InvalidPayloadLength);
        }

        if self.header.kind == MessageKind::Event && self.header.request_id != 0 {
            return Err(ProtocolError::InvalidEventRequestId);
        }

        if self.header.kind != MessageKind::Event && self.header.request_id == 0 {
            return Err(ProtocolError::InvalidEventRequestId);
        }

        Ok(())
    }
}
