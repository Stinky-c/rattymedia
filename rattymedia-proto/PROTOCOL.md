# rattymedia-proto Protocol

This crate defines a transport-agnostic full-duplex RPC protocol for `no_std` and `std` targets.

- **Transport agnostic:** the protocol works over any byte stream implementing `embedded-io-async` traits.
- **Full duplex:** both peers can send requests, replies, and events at any time.
- **Framed stream:** packets are serialized with `postcard`, COBS-encoded, and delimited with `0x00`.

## Message model

`MessageKind`:

- `Request`
- `Reply`
- `Event`
- `Error`

Semantics:

- `Request`, `Reply`, and `Error` participate in request correlation using `request_id`.
- `Event` is one-way and **must** use `request_id = 0`.
- Non-event messages must use non-zero `request_id`.

## Frame layout

Each packet carries a `Frame`:

- `header: FrameHeader`
  - `version: u8`
  - `kind: MessageKind`
  - `flags: u8`
  - `opcode: u16`
  - `request_id: u32`
  - `payload_len: u16`
- `payload: [u8]`

Validation invariants are enforced by `Frame::validate`.

## On-wire format

A single transmitted packet is:

1. Serialize `Frame` using `postcard`
2. COBS-encode serialized bytes
3. Append delimiter byte `0x00`

So the final stream representation is:

`COBS(postcard(Frame)) + 0x00`

`FrameDecoder` consumes stream bytes and emits one decoded frame per delimiter-terminated packet.

## Encoding/decoding APIs

Core codec functions:

- `encode_packet` — frame -> wire packet (`... + 0x00`)
- `decode_packet` — wire packet (without trailing delimiter) -> frame
- `FrameDecoder` — incremental stream decoding

Async transport helpers:

- `write_frame` — encode + write a frame to `embedded-io-async::Write`
- `read_frame` — read from `embedded-io-async::Read` until a full frame is decoded

## Default capacities

Global defaults exported by the crate:

- `DEFAULT_MAX_PAYLOAD = 256`
- `DEFAULT_SERIALIZED_CAP = 384`
- `DEFAULT_FRAME_CAP = 512`
- `DEFAULT_OUTBOUND_QUEUE_CAPACITY = 16`

`DefaultRpcEndpoint<T>` aliases `RpcEndpoint` with these defaults.

## std endpoint runtime

The `endpoint` module (behind `std` feature) provides:

- `RpcEndpoint` runtime with pending request tracking and timeout pruning
- `HandlerRegistry` keyed by `opcode`
- outbound queue setup via `start_listening` / `start_listening_default`

### Typed callback registration

Users can register callbacks on typed payload structs:

- `register_request_handler<Req, Resp>(opcode, handler)`
- `register_event_handler<Event>(opcode, handler)`

The endpoint auto-deserializes request/event payloads and auto-serializes typed request responses.

### Typed sending APIs

Users do not need to manually build frames or convert to bytes:

- `queue_event` sends typed event payloads
- `queue_typed_request` sends typed request payloads and returns a typed pending response
- `request` sends a typed request and awaits a typed reply
- `PendingTypedResponse::recv` decodes the reply into the requested type

Raw frame-based registration is still available via:

- `register_request_frame_handler`
- `register_event_frame_handler`

## Error handling

`ProtocolError` covers validation, buffer sizing, encode/decode, queue, timeout, and transport I/O errors.

Unknown opcodes from incoming request/event dispatch return `UnsupportedOpcode`.
