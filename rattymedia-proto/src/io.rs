//! Async transport helpers built on `embedded-io-async` traits.

use crate::{encode_packet, Frame, FrameDecoder, ProtocolError};

/// Encode and write one protocol frame to the transport.
pub async fn write_frame<T, const MAX_PAYLOAD: usize, const SERIALIZED_CAP: usize, const TX_CAP: usize>(
    transport: &mut T,
    frame: &Frame<MAX_PAYLOAD>,
) -> Result<(), ProtocolError>
where
    T: embedded_io_async::Write,
{
    let encoded = encode_packet::<MAX_PAYLOAD, SERIALIZED_CAP, TX_CAP>(frame)?;
    write_all(transport, encoded.as_slice()).await?;
    transport.flush().await.map_err(|_| ProtocolError::Io)?;
    Ok(())
}

/// Read from transport until a full framed packet is decoded.
pub async fn read_frame<T, const MAX_PAYLOAD: usize, const RX_CAP: usize>(
    transport: &mut T,
    decoder: &mut FrameDecoder<MAX_PAYLOAD, RX_CAP>,
) -> Result<Frame<MAX_PAYLOAD>, ProtocolError>
where
    T: embedded_io_async::Read,
{
    loop {
        let mut byte = [0u8; 1];
        let read = transport.read(&mut byte).await.map_err(|_| ProtocolError::Io)?;
        if read == 0 {
            return Err(ProtocolError::Closed);
        }

        if let Some(frame) = decoder.push_byte(byte[0])? {
            return Ok(frame);
        }
    }
}

async fn write_all<T>(transport: &mut T, mut data: &[u8]) -> Result<(), ProtocolError>
where
    T: embedded_io_async::Write,
{
    while !data.is_empty() {
        let wrote = transport.write(data).await.map_err(|_| ProtocolError::Io)?;
        if wrote == 0 {
            return Err(ProtocolError::Closed);
        }
        data = &data[wrote..];
    }
    Ok(())
}
