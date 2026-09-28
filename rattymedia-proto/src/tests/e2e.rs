use futures::channel::mpsc;
use futures::executor::block_on;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use crate::endpoint::RpcEndpoint;
use crate::{
    DEFAULT_FRAME_CAP, DEFAULT_MAX_PAYLOAD, DEFAULT_SERIALIZED_CAP, ProtocolError,
};

const OP_EVENT: u16 = 10;
const OP_ADD: u16 = 11;
const OP_INC: u16 = 12;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StatusEvent {
    code: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddRequest {
    a: u32,
    b: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddReply {
    sum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IncRequest {
    value: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IncReply {
    value: u32,
}

#[derive(Debug, Clone, Copy)]
struct MemIoError;

impl embedded_io_async::Error for MemIoError {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        embedded_io_async::ErrorKind::Other
    }
}

struct MemoryTransport {
    rx: mpsc::UnboundedReceiver<u8>,
    tx: mpsc::UnboundedSender<u8>,
}

impl embedded_io_async::ErrorType for MemoryTransport {
    type Error = MemIoError;
}

impl embedded_io_async::Read for MemoryTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }

        match self.rx.next().await {
            Some(byte) => {
                buf[0] = byte;
                Ok(1)
            }
            None => Ok(0),
        }
    }
}

impl embedded_io_async::Write for MemoryTransport {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        for &byte in buf {
            self.tx.unbounded_send(byte).map_err(|_| MemIoError)?;
        }
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn transport_pair() -> (MemoryTransport, MemoryTransport) {
    let (a_to_b_tx, a_to_b_rx) = mpsc::unbounded();
    let (b_to_a_tx, b_to_a_rx) = mpsc::unbounded();

    let a = MemoryTransport {
        rx: b_to_a_rx,
        tx: a_to_b_tx,
    };

    let b = MemoryTransport {
        rx: a_to_b_rx,
        tx: b_to_a_tx,
    };

    (a, b)
}

type TestEndpoint =
    RpcEndpoint<MemoryTransport, DEFAULT_MAX_PAYLOAD, DEFAULT_SERIALIZED_CAP, DEFAULT_FRAME_CAP>;

#[test]
fn typed_event_and_request_end_to_end() {
    block_on(async {
        let (client_transport, server_transport) = transport_pair();
        let mut client = TestEndpoint::new(client_transport);
        let mut server = TestEndpoint::new(server_transport);

        let seen_event = Arc::new(Mutex::new(None::<StatusEvent>));
        let seen_event_clone = seen_event.clone();

        server
            .registry_mut()
            .register_event_handler(OP_EVENT, move |event: StatusEvent| {
                let seen = seen_event_clone.clone();
                async move {
                    *seen.lock().expect("event lock poisoned") = Some(event);
                    Ok(())
                }
            });

        server
            .registry_mut()
            .register_request_handler(OP_ADD, |req: AddRequest| async move {
                Ok(AddReply { sum: req.a + req.b })
            });

        let mut client_tx = client.start_listening_default();

        client
            .queue_event(&mut client_tx, OP_EVENT, 0, &StatusEvent { code: 200 })
            .expect("queue event should succeed");

        let pending = client
            .queue_typed_request::<AddRequest, AddReply>(
                &mut client_tx,
                OP_ADD,
                0,
                &AddRequest { a: 2, b: 3 },
            )
            .expect("queue request should succeed");

        drop(client_tx);

        client.run_tx_loop().await.expect("client tx loop should run");

        server
            .process_next_incoming()
            .await
            .expect("server should process event");
        server
            .process_next_incoming()
            .await
            .expect("server should process request");

        client
            .process_next_incoming()
            .await
            .expect("client should process reply");

        let reply = pending.recv().await.expect("typed response should decode");
        assert_eq!(reply, AddReply { sum: 5 });
        assert_eq!(
            *seen_event.lock().expect("event lock poisoned"),
            Some(StatusEvent { code: 200 })
        );

        Ok::<(), ProtocolError>(())
    })
    .expect("end-to-end flow should succeed");
}

#[test]
fn full_duplex_typed_requests_both_directions() {
    block_on(async {
        let (client_transport, server_transport) = transport_pair();
        let mut client = TestEndpoint::new(client_transport);
        let mut server = TestEndpoint::new(server_transport);

        client
            .registry_mut()
            .register_request_handler(OP_INC, |req: IncRequest| async move {
                Ok(IncReply {
                    value: req.value + 1,
                })
            });

        server
            .registry_mut()
            .register_request_handler(OP_INC, |req: IncRequest| async move {
                Ok(IncReply {
                    value: req.value + 1,
                })
            });

        let mut client_tx = client.start_listening_default();
        let mut server_tx = server.start_listening_default();

        let client_pending = client
            .queue_typed_request::<IncRequest, IncReply>(
                &mut client_tx,
                OP_INC,
                0,
                &IncRequest { value: 10 },
            )
            .expect("queue client request");

        let server_pending = server
            .queue_typed_request::<IncRequest, IncReply>(
                &mut server_tx,
                OP_INC,
                0,
                &IncRequest { value: 20 },
            )
            .expect("queue server request");

        drop(client_tx);
        drop(server_tx);

        client.run_tx_loop().await.expect("client tx loop should run");
        server.run_tx_loop().await.expect("server tx loop should run");

        client
            .process_next_incoming()
            .await
            .expect("client should process incoming request");
        server
            .process_next_incoming()
            .await
            .expect("server should process incoming request");

        client
            .process_next_incoming()
            .await
            .expect("client should process incoming reply");
        server
            .process_next_incoming()
            .await
            .expect("server should process incoming reply");

        assert_eq!(
            client_pending.recv().await.expect("client reply decode"),
            IncReply { value: 11 }
        );
        assert_eq!(
            server_pending.recv().await.expect("server reply decode"),
            IncReply { value: 21 }
        );

        Ok::<(), ProtocolError>(())
    })
    .expect("full-duplex flow should succeed");
}
