//! Postcard + COBS packet codec and stream decoder.

use heapless::Vec;

use crate::{Frame, ProtocolError};

/// Encode a frame into `COBS(postcard(frame)) + 0x00`.
pub fn encode_packet<const MAX_PAYLOAD: usize, const SERIALIZED_CAP: usize, const TX_CAP: usize>(
    frame: &Frame<MAX_PAYLOAD>,
) -> Result<Vec<u8, TX_CAP>, ProtocolError> {
    frame.validate()?;

    let mut serialized_buf = [0u8; SERIALIZED_CAP];
    let serialized = postcard::to_slice(frame, &mut serialized_buf).map_err(|_| ProtocolError::EncodeError)?;

    let mut cobs_buf = [0u8; TX_CAP];
    let encoded_len = cobs::encode(serialized, &mut cobs_buf);
    if encoded_len + 1 > TX_CAP {
        return Err(ProtocolError::BufferTooSmall);
    }
    cobs_buf[encoded_len] = 0;

    let mut out = Vec::<u8, TX_CAP>::new();
    out.extend_from_slice(&cobs_buf[..encoded_len + 1])
        .map_err(|_| ProtocolError::BufferTooSmall)?;

    Ok(out)
}

/// Decode a single COBS packet without trailing delimiter.
pub fn decode_packet<const MAX_PAYLOAD: usize, const RX_CAP: usize>(
    packet_without_delimiter: &[u8],
) -> Result<Frame<MAX_PAYLOAD>, ProtocolError> {
    let mut decoded = [0u8; RX_CAP];
    let decoded_len = cobs::decode(packet_without_delimiter, &mut decoded).map_err(|_| ProtocolError::DecodeError)?;
    let frame: Frame<MAX_PAYLOAD> = postcard::from_bytes(&decoded[..decoded_len]).map_err(|_| ProtocolError::DecodeError)?;
    frame.validate()?;
    Ok(frame)
}

/// Incremental stream decoder that emits frames when `0x00` delimiter is observed.
pub struct FrameDecoder<const MAX_PAYLOAD: usize, const RX_CAP: usize> {
    buf: Vec<u8, RX_CAP>,
    _marker: core::marker::PhantomData<[u8; MAX_PAYLOAD]>,
}

impl<const MAX_PAYLOAD: usize, const RX_CAP: usize> FrameDecoder<MAX_PAYLOAD, RX_CAP> {
    pub const fn new() -> Self {
        Self {
            buf: Vec::new(),
            _marker: core::marker::PhantomData,
        }
    }

    pub fn push_byte(&mut self, byte: u8) -> Result<Option<Frame<MAX_PAYLOAD>>, ProtocolError> {
        if byte == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            }

            let frame = decode_packet::<MAX_PAYLOAD, RX_CAP>(self.buf.as_slice())?;
            self.buf.clear();
            return Ok(Some(frame));
        }

        self.buf.push(byte).map_err(|_| ProtocolError::BufferTooSmall)?;
        Ok(None)
    }
}
