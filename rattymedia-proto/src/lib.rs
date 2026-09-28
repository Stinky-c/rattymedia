#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
extern crate std;

use heapless::Vec;
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageKind {
    Request = 1,
    Reply = 2,
    Event = 3,
    Error = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameHeader {
    pub version: u8,
    pub kind: MessageKind,
    pub flags: u8,
    pub opcode: u16,
    pub request_id: u32,
    pub payload_len: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame<const MAX_PAYLOAD: usize> {
    pub header: FrameHeader,
    pub payload: Vec<u8, MAX_PAYLOAD>,
}

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

        let payload_len = u16::try_from(payload_vec.len()).map_err(|_| ProtocolError::InvalidPayloadLength)?;
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

pub fn decode_packet<const MAX_PAYLOAD: usize, const RX_CAP: usize>(
    packet_without_delimiter: &[u8],
) -> Result<Frame<MAX_PAYLOAD>, ProtocolError> {
    let mut decoded = [0u8; RX_CAP];
    let decoded_len = cobs::decode(packet_without_delimiter, &mut decoded).map_err(|_| ProtocolError::DecodeError)?;
    let frame: Frame<MAX_PAYLOAD> = postcard::from_bytes(&decoded[..decoded_len]).map_err(|_| ProtocolError::DecodeError)?;
    frame.validate()?;
    Ok(frame)
}

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

#[cfg(feature = "std")]
pub mod endpoint {
    use super::*;
    use core::future::Future;
    use core::pin::Pin;
    use futures::channel::{mpsc, oneshot};
    use futures_util::StreamExt;
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    pub type RequestFuture<const MAX_PAYLOAD: usize> = Pin<
        Box<
            dyn Future<Output = Result<Vec<u8, MAX_PAYLOAD>, ProtocolError>>
                + Send
                + 'static,
        >,
    >;

    pub type EventFuture = Pin<Box<dyn Future<Output = Result<(), ProtocolError>> + Send + 'static>>;

    pub type RequestHandler<const MAX_PAYLOAD: usize> =
        Box<dyn FnMut(Frame<MAX_PAYLOAD>) -> RequestFuture<MAX_PAYLOAD> + Send>;

    pub type EventHandler<const MAX_PAYLOAD: usize> =
        Box<dyn FnMut(Frame<MAX_PAYLOAD>) -> EventFuture + Send>;

    pub struct HandlerRegistry<const MAX_PAYLOAD: usize> {
        request_handlers: BTreeMap<u16, RequestHandler<MAX_PAYLOAD>>,
        event_handlers: BTreeMap<u16, EventHandler<MAX_PAYLOAD>>,
    }

    impl<const MAX_PAYLOAD: usize> Default for HandlerRegistry<MAX_PAYLOAD> {
        fn default() -> Self {
            Self {
                request_handlers: BTreeMap::new(),
                event_handlers: BTreeMap::new(),
            }
        }
    }

    impl<const MAX_PAYLOAD: usize> HandlerRegistry<MAX_PAYLOAD> {
        pub fn register_request_handler<F>(&mut self, opcode: u16, handler: F)
        where
            F: FnMut(Frame<MAX_PAYLOAD>) -> RequestFuture<MAX_PAYLOAD> + Send + 'static,
        {
            self.request_handlers.insert(opcode, Box::new(handler));
        }

        pub fn register_event_handler<F>(&mut self, opcode: u16, handler: F)
        where
            F: FnMut(Frame<MAX_PAYLOAD>) -> EventFuture + Send + 'static,
        {
            self.event_handlers.insert(opcode, Box::new(handler));
        }

        async fn dispatch_request(&mut self, frame: Frame<MAX_PAYLOAD>) -> Result<Vec<u8, MAX_PAYLOAD>, ProtocolError> {
            match self.request_handlers.get_mut(&frame.header.opcode) {
                Some(handler) => handler(frame).await,
                None => Err(ProtocolError::UnsupportedOpcode(frame.header.opcode)),
            }
        }

        async fn dispatch_event(&mut self, frame: Frame<MAX_PAYLOAD>) -> Result<(), ProtocolError> {
            match self.event_handlers.get_mut(&frame.header.opcode) {
                Some(handler) => handler(frame).await,
                None => Err(ProtocolError::UnsupportedOpcode(frame.header.opcode)),
            }
        }
    }

    pub enum OutboundMessage<const MAX_PAYLOAD: usize> {
        Request {
            opcode: u16,
            flags: u8,
            payload: Vec<u8, MAX_PAYLOAD>,
            request_id: u32,
        },
        Reply {
            opcode: u16,
            flags: u8,
            payload: Vec<u8, MAX_PAYLOAD>,
            request_id: u32,
        },
        Event {
            opcode: u16,
            flags: u8,
            payload: Vec<u8, MAX_PAYLOAD>,
        },
        Error {
            opcode: u16,
            flags: u8,
            payload: Vec<u8, MAX_PAYLOAD>,
            request_id: u32,
        },
    }

    struct PendingRequest<const MAX_PAYLOAD: usize> {
        sent_at: Instant,
        responder: oneshot::Sender<Result<Vec<u8, MAX_PAYLOAD>, ProtocolError>>,
    }

    pub struct RpcEndpoint<
        T,
        const MAX_PAYLOAD: usize,
        const SERIALIZED_CAP: usize,
        const FRAME_CAP: usize,
    > {
        transport: T,
        decoder: FrameDecoder<MAX_PAYLOAD, FRAME_CAP>,
        registry: HandlerRegistry<MAX_PAYLOAD>,
        outbound_rx: Option<mpsc::Receiver<OutboundMessage<MAX_PAYLOAD>>>,
        pending: BTreeMap<u32, PendingRequest<MAX_PAYLOAD>>,
        next_request_id: u32,
        pending_timeout: Duration,
    }

    impl<T, const MAX_PAYLOAD: usize, const SERIALIZED_CAP: usize, const FRAME_CAP: usize>
        RpcEndpoint<T, MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>
    where
        T: embedded_io_async::Read + embedded_io_async::Write,
    {
        pub fn new(transport: T) -> Self {
            Self {
                transport,
                decoder: FrameDecoder::new(),
                registry: HandlerRegistry::default(),
                outbound_rx: None,
                pending: BTreeMap::new(),
                next_request_id: 1,
                pending_timeout: Duration::from_secs(5),
            }
        }

        pub fn set_pending_timeout(&mut self, timeout: Duration) {
            self.pending_timeout = timeout;
        }

        pub fn registry_mut(&mut self) -> &mut HandlerRegistry<MAX_PAYLOAD> {
            &mut self.registry
        }

        pub fn start_listening(&mut self, queue_capacity: usize) -> mpsc::Sender<OutboundMessage<MAX_PAYLOAD>> {
            let (tx, rx) = mpsc::channel(queue_capacity);
            self.outbound_rx = Some(rx);
            tx
        }

        pub fn queue_request(
            &mut self,
            sender: &mut mpsc::Sender<OutboundMessage<MAX_PAYLOAD>>,
            opcode: u16,
            flags: u8,
            payload: &[u8],
        ) -> Result<oneshot::Receiver<Result<Vec<u8, MAX_PAYLOAD>, ProtocolError>>, ProtocolError> {
            let request_id = self.allocate_request_id();

            let mut payload_vec = Vec::new();
            payload_vec
                .extend_from_slice(payload)
                .map_err(|_| ProtocolError::BufferTooSmall)?;

            let (tx, rx) = oneshot::channel();
            self.pending.insert(
                request_id,
                PendingRequest {
                    sent_at: Instant::now(),
                    responder: tx,
                },
            );

            sender
                .try_send(OutboundMessage::Request {
                    opcode,
                    flags,
                    payload: payload_vec,
                    request_id,
                })
                .map_err(|_| ProtocolError::Closed)?;

            Ok(rx)
        }

        pub async fn run_tx_loop(&mut self) -> Result<(), ProtocolError> {
            let rx = self
                .outbound_rx
                .as_mut()
                .ok_or(ProtocolError::MissingOutboundQueue)?;

            while let Some(msg) = rx.next().await {
                let frame = Self::outbound_to_frame(msg)?;
                write_frame::<T, MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(&mut self.transport, &frame).await?;
            }

            Ok(())
        }

        pub async fn run_rx_loop(&mut self) -> Result<(), ProtocolError> {
            loop {
                let frame = read_frame::<T, MAX_PAYLOAD, FRAME_CAP>(&mut self.transport, &mut self.decoder).await?;
                self.dispatch_incoming(frame).await?;
                self.prune_timed_out();
            }
        }

        pub fn prune_timed_out(&mut self) {
            let timeout = self.pending_timeout;
            let now = Instant::now();

            let expired: Vec<u32, 64> = self
                .pending
                .iter()
                .filter_map(|(request_id, pending)| {
                    if now.duration_since(pending.sent_at) >= timeout {
                        Some(*request_id)
                    } else {
                        None
                    }
                })
                .take(64)
                .collect();

            for request_id in expired {
                if let Some(pending) = self.pending.remove(&request_id) {
                    let _ = pending.responder.send(Err(ProtocolError::Timeout));
                }
            }
        }

        fn allocate_request_id(&mut self) -> u32 {
            let current = self.next_request_id;
            self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
            current.max(1)
        }

        fn outbound_to_frame(msg: OutboundMessage<MAX_PAYLOAD>) -> Result<Frame<MAX_PAYLOAD>, ProtocolError> {
            match msg {
                OutboundMessage::Request {
                    opcode,
                    flags,
                    payload,
                    request_id,
                } => Frame::new(MessageKind::Request, flags, opcode, request_id, payload.as_slice()),
                OutboundMessage::Reply {
                    opcode,
                    flags,
                    payload,
                    request_id,
                } => Frame::new(MessageKind::Reply, flags, opcode, request_id, payload.as_slice()),
                OutboundMessage::Event {
                    opcode,
                    flags,
                    payload,
                } => Frame::new(MessageKind::Event, flags, opcode, 0, payload.as_slice()),
                OutboundMessage::Error {
                    opcode,
                    flags,
                    payload,
                    request_id,
                } => Frame::new(MessageKind::Error, flags, opcode, request_id, payload.as_slice()),
            }
        }

        async fn dispatch_incoming(&mut self, frame: Frame<MAX_PAYLOAD>) -> Result<(), ProtocolError> {
            match frame.header.kind {
                MessageKind::Request => {
                    let request_id = frame.header.request_id;
                    let opcode = frame.header.opcode;
                    match self.registry.dispatch_request(frame).await {
                        Ok(payload) => {
                            let reply = Frame::new(MessageKind::Reply, 0, opcode, request_id, payload.as_slice())?;
                            write_frame::<T, MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(&mut self.transport, &reply).await
                        }
                        Err(err) => {
                            let payload = error_payload::<MAX_PAYLOAD>(&err)?;
                            let error = Frame::new(MessageKind::Error, 0, opcode, request_id, payload.as_slice())?;
                            write_frame::<T, MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(&mut self.transport, &error).await
                        }
                    }
                }
                MessageKind::Event => {
                    let _ = self.registry.dispatch_event(frame).await;
                    Ok(())
                }
                MessageKind::Reply => {
                    if let Some(pending) = self.pending.remove(&frame.header.request_id) {
                        let _ = pending.responder.send(Ok(frame.payload));
                    }
                    Ok(())
                }
                MessageKind::Error => {
                    if let Some(pending) = self.pending.remove(&frame.header.request_id) {
                        let _ = pending.responder.send(Err(ProtocolError::DecodeError));
                    }
                    Ok(())
                }
            }
        }
    }

    fn error_payload<const MAX_PAYLOAD: usize>(err: &ProtocolError) -> Result<Vec<u8, MAX_PAYLOAD>, ProtocolError> {
        let mut payload = Vec::new();
        let code = match err {
            ProtocolError::InvalidVersion(_) => 1,
            ProtocolError::InvalidPayloadLength => 2,
            ProtocolError::InvalidEventRequestId => 3,
            ProtocolError::BufferTooSmall => 4,
            ProtocolError::EncodeError => 5,
            ProtocolError::DecodeError => 6,
            ProtocolError::UnsupportedOpcode(_) => 7,
            ProtocolError::MissingOutboundQueue => 8,
            ProtocolError::Closed => 9,
            ProtocolError::Timeout => 10,
            ProtocolError::Io => 11,
        };

        payload.push(code).map_err(|_| ProtocolError::BufferTooSmall)?;
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_PAYLOAD: usize = 64;
    const SERIALIZED_CAP: usize = 128;
    const FRAME_CAP: usize = 128;

    #[test]
    fn frame_round_trip_request() {
        let frame = Frame::<MAX_PAYLOAD>::new(MessageKind::Request, 0, 0x10, 7, b"hello").unwrap();
        let encoded = encode_packet::<MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(&frame).unwrap();
        let decoded = decode_packet::<MAX_PAYLOAD, FRAME_CAP>(&encoded[..encoded.len() - 1]).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn frame_round_trip_reply() {
        let frame = Frame::<MAX_PAYLOAD>::new(MessageKind::Reply, 0, 0x20, 8, b"world").unwrap();
        let encoded = encode_packet::<MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(&frame).unwrap();
        let decoded = decode_packet::<MAX_PAYLOAD, FRAME_CAP>(&encoded[..encoded.len() - 1]).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn frame_round_trip_event() {
        let frame = Frame::<MAX_PAYLOAD>::new(MessageKind::Event, 0, 0x30, 0, b"event").unwrap();
        let encoded = encode_packet::<MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(&frame).unwrap();
        let decoded = decode_packet::<MAX_PAYLOAD, FRAME_CAP>(&encoded[..encoded.len() - 1]).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn frame_round_trip_error() {
        let frame = Frame::<MAX_PAYLOAD>::new(MessageKind::Error, 0, 0x40, 3, b"err").unwrap();
        let encoded = encode_packet::<MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(&frame).unwrap();
        let decoded = decode_packet::<MAX_PAYLOAD, FRAME_CAP>(&encoded[..encoded.len() - 1]).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn rejects_event_with_request_id() {
        let err = Frame::<MAX_PAYLOAD>::new(MessageKind::Event, 0, 0x30, 55, b"event").unwrap_err();
        assert_eq!(err, ProtocolError::InvalidEventRequestId);
    }

    #[test]
    fn rejects_non_event_without_request_id() {
        let err = Frame::<MAX_PAYLOAD>::new(MessageKind::Request, 0, 0x10, 0, b"req").unwrap_err();
        assert_eq!(err, ProtocolError::InvalidEventRequestId);
    }

    #[test]
    fn malformed_packet_fails_decode() {
        let malformed = [0x01, 0x02, 0x03, 0x04];
        let err = decode_packet::<MAX_PAYLOAD, FRAME_CAP>(&malformed).unwrap_err();
        assert_eq!(err, ProtocolError::DecodeError);
    }

    #[test]
    fn stream_decode_with_delimiter() {
        let frame = Frame::<MAX_PAYLOAD>::new(MessageKind::Event, 0, 0x77, 0, b"stream").unwrap();
        let encoded = encode_packet::<MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(&frame).unwrap();

        let mut decoder = FrameDecoder::<MAX_PAYLOAD, FRAME_CAP>::new();
        let mut out = None;
        for b in encoded {
            out = decoder.push_byte(b).unwrap().or(out);
        }

        assert_eq!(out.unwrap(), frame);
    }
}
