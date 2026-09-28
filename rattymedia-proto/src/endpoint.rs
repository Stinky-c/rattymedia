//! `std` runtime endpoint with callback registry and mpsc-based outbound queue.

use core::future::Future;
use core::pin::Pin;

use futures::channel::{mpsc, oneshot};
use futures_util::StreamExt;
use heapless::Vec;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::{read_frame, write_frame, Frame, FrameDecoder, MessageKind, ProtocolError};

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
