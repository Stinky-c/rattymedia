use crate::{Frame, FrameDecoder, MessageKind, ProtocolError, decode_packet, encode_packet};

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
