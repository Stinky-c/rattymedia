//! `std` runtime endpoint with typed callback registry and mpsc-based outbound queue.

use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;

use alloc::{boxed::Box, collections::BTreeMap};
use futures::channel::oneshot;
use heapless::Vec;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thingbuf::mpsc;

use crate::{
    DEFAULT_OUTBOUND_QUEUE_CAPACITY, Frame, FrameDecoder, MessageKind, ProtocolError, read_frame,
    write_frame,
};

pub type RequestFuture<const MAX_PAYLOAD: usize> =
    Pin<Box<dyn Future<Output = Result<Vec<u8, MAX_PAYLOAD>, ProtocolError>> + Send + 'static>>;

pub type EventFuture = Pin<Box<dyn Future<Output = Result<(), ProtocolError>> + Send + 'static>>;

pub type RequestHandler<const MAX_PAYLOAD: usize> =
    Box<dyn FnMut(Frame<MAX_PAYLOAD>) -> RequestFuture<MAX_PAYLOAD> + Send>;

pub type EventHandler<const MAX_PAYLOAD: usize> =
    Box<dyn FnMut(Frame<MAX_PAYLOAD>) -> EventFuture + Send>;

/// Tracks an outstanding typed request and allows decoding the response into a struct.
pub struct PendingTypedResponse<R, const MAX_PAYLOAD: usize> {
    rx: oneshot::Receiver<Result<Vec<u8, MAX_PAYLOAD>, ProtocolError>>,
    _marker: PhantomData<R>,
}

impl<R, const MAX_PAYLOAD: usize> PendingTypedResponse<R, MAX_PAYLOAD>
where
    R: DeserializeOwned,
{
    pub async fn recv(self) -> Result<R, ProtocolError> {
        let payload = self.rx.await.map_err(|_| ProtocolError::Closed)??;
        deserialize_payload(payload.as_slice())
    }
}

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
    /// Register a typed request callback. The incoming payload is deserialized into `Req`
    /// and the returned `Resp` is serialized automatically.
    pub fn register_request_handler<Req, Resp, F, Fut>(&mut self, opcode: u16, mut handler: F)
    where
        Req: DeserializeOwned + Send + 'static,
        Resp: Serialize + Send + 'static,
        F: FnMut(Req) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Resp, ProtocolError>> + Send + 'static,
    {
        self.request_handlers.insert(
            opcode,
            Box::new(move |frame| {
                let request = deserialize_payload::<Req>(frame.payload.as_slice());
                match request {
                    Ok(request) => {
                        let future = handler(request);
                        Box::pin(async move {
                            let response = future.await?;
                            serialize_payload::<MAX_PAYLOAD, _>(&response)
                        })
                    }
                    Err(err) => Box::pin(async move { Err(err) }),
                }
            }),
        );
    }

    /// Register a typed event callback. Event payload bytes are deserialized into `Event`.
    pub fn register_event_handler<Event, F, Fut>(&mut self, opcode: u16, mut handler: F)
    where
        Event: DeserializeOwned + Send + 'static,
        F: FnMut(Event) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ProtocolError>> + Send + 'static,
    {
        self.event_handlers.insert(
            opcode,
            Box::new(move |frame| {
                let event = deserialize_payload::<Event>(frame.payload.as_slice());
                match event {
                    Ok(event) => {
                        let future = handler(event);
                        Box::pin(async move { future.await })
                    }
                    Err(err) => Box::pin(async move { Err(err) }),
                }
            }),
        );
    }

    pub fn register_request_frame_handler<F>(&mut self, opcode: u16, handler: F)
    where
        F: FnMut(Frame<MAX_PAYLOAD>) -> RequestFuture<MAX_PAYLOAD> + Send + 'static,
    {
        self.request_handlers.insert(opcode, Box::new(handler));
    }

    pub fn register_event_frame_handler<F>(&mut self, opcode: u16, handler: F)
    where
        F: FnMut(Frame<MAX_PAYLOAD>) -> EventFuture + Send + 'static,
    {
        self.event_handlers.insert(opcode, Box::new(handler));
    }

    async fn dispatch_request(
        &mut self,
        frame: Frame<MAX_PAYLOAD>,
    ) -> Result<Vec<u8, MAX_PAYLOAD>, ProtocolError> {
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

#[derive(Debug, Clone, Default)]
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
    // Thingbuf demands a default value. It should never send or receive this value. Does not have a buffer because that is a waste of memory
    #[default]
    InternalError,
}

struct PendingRequest<const MAX_PAYLOAD: usize> {
    // sent_at: Instant,
    responder: oneshot::Sender<Result<Vec<u8, MAX_PAYLOAD>, ProtocolError>>,
}

pub struct RpcEndpoint<
    T: embedded_io_async::Read + embedded_io_async::Write,
    const MAX_PAYLOAD: usize,
    const SERIALIZED_CAP: usize,
    const FRAME_CAP: usize,
> {
    /// Channel that handles read/write bytes.  Uses [embedded_io_async::Read] + [embedded_io_async::Write]
    transport: T,
    decoder: FrameDecoder<MAX_PAYLOAD, FRAME_CAP>,
    registry: HandlerRegistry<MAX_PAYLOAD>,
    outbound_rx: Option<mpsc::Receiver<OutboundMessage<MAX_PAYLOAD>>>,
    pending: BTreeMap<u32, PendingRequest<MAX_PAYLOAD>>,
    next_request_id: u32,
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
        }
    }

    pub fn registry_mut(&mut self) -> &mut HandlerRegistry<MAX_PAYLOAD> {
        &mut self.registry
    }

    pub fn start_listening(
        &mut self,
        queue_capacity: Option<usize>,
    ) -> mpsc::Sender<OutboundMessage<MAX_PAYLOAD>> {
        let (tx, rx) = mpsc::channel(queue_capacity.unwrap_or(DEFAULT_OUTBOUND_QUEUE_CAPACITY));
        self.outbound_rx = Some(rx);
        tx
    }

    pub fn queue_event<Event>(
        &self,
        sender: &mut mpsc::Sender<OutboundMessage<MAX_PAYLOAD>>,
        opcode: u16,
        flags: u8,
        event: &Event,
    ) -> Result<(), ProtocolError>
    where
        Event: Serialize,
    {
        let payload = serialize_payload::<MAX_PAYLOAD, _>(event)?;
        sender
            .try_send(OutboundMessage::Event {
                opcode,
                flags,
                payload,
            })
            .map_err(|_| ProtocolError::Closed)
    }

    pub fn queue_typed_request<Req, Resp>(
        &mut self,
        sender: &mut mpsc::Sender<OutboundMessage<MAX_PAYLOAD>>,
        opcode: u16,
        flags: u8,
        request: &Req,
    ) -> Result<PendingTypedResponse<Resp, MAX_PAYLOAD>, ProtocolError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let payload = serialize_payload::<MAX_PAYLOAD, _>(request)?;
        let rx = self.queue_request_raw(sender, opcode, flags, payload.as_slice())?;
        Ok(PendingTypedResponse {
            rx,
            _marker: PhantomData,
        })
    }

    pub async fn request<Req, Resp>(
        &mut self,
        sender: &mut mpsc::Sender<OutboundMessage<MAX_PAYLOAD>>,
        opcode: u16,
        flags: u8,
        request: &Req,
    ) -> Result<Resp, ProtocolError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        self.queue_typed_request::<Req, Resp>(sender, opcode, flags, request)?
            .recv()
            .await
    }

    pub fn queue_request_raw(
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
        self.pending
            .insert(request_id, PendingRequest { responder: tx });

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
        loop {
            if !self.process_next_outbound().await? {
                return Ok(());
            }
        }
    }

    pub async fn process_next_outbound(&mut self) -> Result<bool, ProtocolError> {
        let next = {
            let rx = self
                .outbound_rx
                .as_mut()
                .ok_or(ProtocolError::MissingOutboundQueue)?;
            rx.recv().await
        };

        let Some(msg) = next else {
            return Ok(false);
        };

        let frame = Self::outbound_to_frame(msg)?;
        write_frame::<T, MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(&mut self.transport, &frame)
            .await?;
        Ok(true)
    }

    pub async fn run_rx_loop(&mut self) -> Result<(), ProtocolError> {
        loop {
            self.process_next_incoming().await?;
        }
    }

    pub async fn process_next_incoming(&mut self) -> Result<(), ProtocolError> {
        let frame =
            read_frame::<T, MAX_PAYLOAD, FRAME_CAP>(&mut self.transport, &mut self.decoder).await?;
        self.dispatch_incoming(frame).await?;
        Ok(())
    }

    fn allocate_request_id(&mut self) -> u32 {
        let current = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        current.max(1)
    }

    fn outbound_to_frame(
        msg: OutboundMessage<MAX_PAYLOAD>,
    ) -> Result<Frame<MAX_PAYLOAD>, ProtocolError> {
        match msg {
            OutboundMessage::Request {
                opcode,
                flags,
                payload,
                request_id,
            } => Frame::new(
                MessageKind::Request,
                flags,
                opcode,
                request_id,
                payload.as_slice(),
            ),
            OutboundMessage::Reply {
                opcode,
                flags,
                payload,
                request_id,
            } => Frame::new(
                MessageKind::Reply,
                flags,
                opcode,
                request_id,
                payload.as_slice(),
            ),
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
            } => Frame::new(
                MessageKind::Error,
                flags,
                opcode,
                request_id,
                payload.as_slice(),
            ),
            OutboundMessage::InternalError => {
                // TODO: more sensible error handling
                panic!(
                    "Tried to frame an internal error. Likely caused by a channel producing an invalid state"
                );
                Err(ProtocolError::Io)
            }
        }
    }

    async fn dispatch_incoming(&mut self, frame: Frame<MAX_PAYLOAD>) -> Result<(), ProtocolError> {
        match frame.header.kind {
            MessageKind::Request => {
                let request_id = frame.header.request_id;
                let opcode = frame.header.opcode;
                match self.registry.dispatch_request(frame).await {
                    Ok(payload) => {
                        let reply = Frame::new(
                            MessageKind::Reply,
                            0,
                            opcode,
                            request_id,
                            payload.as_slice(),
                        )?;
                        write_frame::<T, MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(
                            &mut self.transport,
                            &reply,
                        )
                        .await
                    }
                    Err(err) => {
                        let payload = error_payload::<MAX_PAYLOAD>(&err)?;
                        let error = Frame::new(
                            MessageKind::Error,
                            0,
                            opcode,
                            request_id,
                            payload.as_slice(),
                        )?;
                        write_frame::<T, MAX_PAYLOAD, SERIALIZED_CAP, FRAME_CAP>(
                            &mut self.transport,
                            &error,
                        )
                        .await
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

fn serialize_payload<const MAX_PAYLOAD: usize, T: Serialize>(
    value: &T,
) -> Result<Vec<u8, MAX_PAYLOAD>, ProtocolError> {
    let mut serialized_buf = [0u8; MAX_PAYLOAD];
    let serialized =
        postcard::to_slice(value, &mut serialized_buf).map_err(|_| ProtocolError::EncodeError)?;

    let mut out = Vec::<u8, MAX_PAYLOAD>::new();
    out.extend_from_slice(serialized)
        .map_err(|_| ProtocolError::BufferTooSmall)?;
    Ok(out)
}

fn deserialize_payload<T: DeserializeOwned>(payload: &[u8]) -> Result<T, ProtocolError> {
    postcard::from_bytes(payload).map_err(|_| ProtocolError::DecodeError)
}

fn error_payload<const MAX_PAYLOAD: usize>(
    err: &ProtocolError,
) -> Result<Vec<u8, MAX_PAYLOAD>, ProtocolError> {
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

    payload
        .push(code)
        .map_err(|_| ProtocolError::BufferTooSmall)?;
    Ok(payload)
}
