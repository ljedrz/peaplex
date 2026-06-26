//! The peaplex wire format.
//!
//! A peaplex frame is a 9-byte header followed by an optional zero-copy
//! [`Bytes`] payload:
//!
//! ```text
//! +--------+-------------------------------------------------+
//! |  Flag  |                     Stream ID                   |  (1 + 4 bytes)
//! +--------+-------------------------------------------------+
//! |                       Length (bytes)                     |  (4 bytes)
//! +----------------------------------------------------------+
//! |                     Payload (Length bytes)               |  (0..16 MiB)
//! +----------------------------------------------------------+
//! ```
//!
//! Flag values:
//! - `0x01` Open   - announces a new substream
//! - `0x02` Data   - carries a chunk of substream payload
//! - `0x04` Close  - tears down a substream (full close, no half-close)
//! - `0x08` GoAway - tears down the entire connection; payload is a `u32` reason
//!
//! Stream IDs are partitioned by side to avoid collisions: the dialing side
//! (Initiator from the node's perspective) picks odd IDs starting at `1`, the
//! listening side (Responder) picks even IDs starting at `2`. `0` is reserved.

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// Size in bytes of the peaplex frame header.
pub const FRAME_HEADER_LEN: usize = 9;

/// Maximum size in bytes of a single frame's payload (16 MiB).
pub const MAX_FRAME_PAYLOAD: u32 = 16 * 1024 * 1024;

/// Flag byte for an [`Open`](Flag::Open) frame.
pub const FLAG_OPEN: u8 = 0x01;
/// Flag byte for a [`Data`](Flag::Data) frame.
pub const FLAG_DATA: u8 = 0x02;
/// Flag byte for a [`Close`](Flag::Close) frame.
pub const FLAG_CLOSE: u8 = 0x04;
/// Flag byte for a [`GoAway`](Flag::GoAway) frame.
pub const FLAG_GOAWAY: u8 = 0x08;

/// The kind of message carried by a [`Frame`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Flag {
    /// Announces a new substream.
    Open,
    /// Carries a chunk of substream payload.
    Data,
    /// Tears down a substream in both directions.
    Close,
    /// Tears down the entire connection; payload is a `u32` reason.
    GoAway,
}

impl Flag {
    /// Decodes a flag byte.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            FLAG_OPEN => Some(Self::Open),
            FLAG_DATA => Some(Self::Data),
            FLAG_CLOSE => Some(Self::Close),
            FLAG_GOAWAY => Some(Self::GoAway),
            _ => None,
        }
    }

    /// Returns the on-the-wire byte for this flag.
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Open => FLAG_OPEN,
            Self::Data => FLAG_DATA,
            Self::Close => FLAG_CLOSE,
            Self::GoAway => FLAG_GOAWAY,
        }
    }
}

/// Identifier of a single substream within a peaplex connection.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamId(pub u32);

impl StreamId {
    /// Constructs a `StreamId` from its raw value.
    #[inline]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Returns the raw numeric value.
    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// `true` if this ID was minted by the dialing side.
    #[inline]
    pub const fn is_initiator(self) -> bool {
        self.0 & 1 == 1
    }

    /// `true` if this ID was minted by the listening side.
    #[inline]
    pub const fn is_responder(self) -> bool {
        self.0 & 1 == 0
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A single decoded peaplex frame.
///
/// Payloads are wrapped in [`Bytes`], which is a refcounted, reference-counted
/// view onto the read buffer - decoding never copies the payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    /// The kind of message.
    pub flag: Flag,
    /// The substream this frame belongs to.
    pub stream_id: StreamId,
    /// The zero-copy payload; semantics depend on [`Self::flag`]:
    /// empty for Open/Close, substream bytes for Data, a `u32` reason for GoAway.
    pub payload: Bytes,
}

impl Frame {
    /// Constructs an [`Open`](Flag::Open) frame.
    #[inline]
    pub fn open(stream_id: StreamId) -> Self {
        Self {
            flag: Flag::Open,
            stream_id,
            payload: Bytes::new(),
        }
    }

    /// Constructs a [`Data`](Flag::Data) frame.
    #[inline]
    pub fn data(stream_id: StreamId, payload: Bytes) -> Self {
        Self {
            flag: Flag::Data,
            stream_id,
            payload,
        }
    }

    /// Constructs a [`Close`](Flag::Close) frame.
    #[inline]
    pub fn close(stream_id: StreamId) -> Self {
        Self {
            flag: Flag::Close,
            stream_id,
            payload: Bytes::new(),
        }
    }

    /// Constructs a [`GoAway`](Flag::GoAway) frame with the given reason
    /// code. The `stream_id` field is a placeholder; the dispatcher ignores
    /// it.
    #[inline]
    pub fn goaway(reason: u32) -> Self {
        Self {
            flag: Flag::GoAway,
            stream_id: StreamId(0),
            payload: Bytes::copy_from_slice(&reason.to_be_bytes()),
        }
    }
}

/// A stateless [`Decoder`]/[`Encoder`] for peaplex frames.
///
/// Used as the codec for both the `Reading` and `Writing` pea2pea protocols.
#[derive(Clone, Copy, Debug, Default)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Self::Error> {
        if src.len() < FRAME_HEADER_LEN {
            return Ok(None);
        }
        let flag = Flag::from_u8(src[0])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown peaplex flag"))?;
        let stream_id = StreamId(u32::from_be_bytes([src[1], src[2], src[3], src[4]]));
        let length = u32::from_be_bytes([src[5], src[6], src[7], src[8]]);
        if length > MAX_FRAME_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peaplex frame too large",
            ));
        }
        let total = FRAME_HEADER_LEN + length as usize;
        if src.len() < total {
            return Ok(None);
        }
        src.advance(FRAME_HEADER_LEN);
        // split_to().freeze() shares the underlying allocation with `src`,
        // so decoding is zero-copy.
        let payload = src.split_to(length as usize).freeze();
        Ok(Some(Frame {
            flag,
            stream_id,
            payload,
        }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let length = frame.payload.len() as u32;
        if length > MAX_FRAME_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peaplex frame too large",
            ));
        }
        dst.reserve(FRAME_HEADER_LEN + length as usize);
        dst.put_u8(frame.flag.to_u8());
        dst.put_u32(frame.stream_id.get());
        dst.put_u32(length);
        dst.extend_from_slice(&frame.payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(frame: Frame) -> Frame {
        let mut buf = BytesMut::new();
        FrameCodec.encode(frame.clone(), &mut buf).unwrap();
        let mut slice = buf.clone();
        let decoded = FrameCodec.decode(&mut slice).unwrap().unwrap();
        assert!(FrameCodec.decode(&mut slice).unwrap().is_none());
        assert_eq!(decoded, frame);
        // Encoding the same frame again must be byte-for-byte identical.
        let mut buf2 = BytesMut::new();
        FrameCodec.encode(frame, &mut buf2).unwrap();
        assert_eq!(buf, buf2);
        decoded
    }

    #[test]
    fn header_layout() {
        let frame = Frame::data(StreamId(0x01020304), Bytes::from_static(b"hi"));
        let mut buf = BytesMut::new();
        FrameCodec.encode(frame, &mut buf).unwrap();
        // 1 byte flag + 4 bytes id + 4 bytes length
        assert_eq!(buf.len(), FRAME_HEADER_LEN + 2);
        assert_eq!(buf[0], FLAG_DATA);
        assert_eq!(&buf[1..5], &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(&buf[5..9], &[0, 0, 0, 2]);
        assert_eq!(&buf[9..], b"hi");
    }

    #[test]
    fn empty_payloads() {
        round_trip(Frame::open(StreamId(1)));
        round_trip(Frame::close(StreamId(2)));
        round_trip(Frame::data(StreamId(3), Bytes::new()));
    }

    #[test]
    fn large_payload() {
        let payload = Bytes::copy_from_slice(&vec![0xABu8; 1024 * 1024]);
        round_trip(Frame::data(StreamId(42), payload));
    }

    #[test]
    fn partial_then_complete() {
        let mut buf = BytesMut::new();
        let frame = Frame::data(StreamId(7), Bytes::from_static(b"hello"));
        FrameCodec.encode(frame, &mut buf).unwrap();
        // Feed one byte at a time and ensure the decoder never panics and
        // never returns a frame before the header + payload are present.
        let mut accum = BytesMut::new();
        for b in buf.iter() {
            accum.extend_from_slice(&[*b]);
            let mut tmp = accum.clone();
            match FrameCodec.decode(&mut tmp) {
                Ok(None) => {}
                Ok(Some(f)) => {
                    assert_eq!(f.stream_id, StreamId(7));
                    assert_eq!(&f.payload[..], b"hello");
                    return;
                }
                Err(e) => panic!("decode error: {e}"),
            }
        }
        panic!("decoder never produced a frame");
    }

    #[test]
    fn unknown_flag_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xFF); // unknown flag
        buf.put_u32(1);
        buf.put_u32(0);
        let mut tmp = buf;
        let err = FrameCodec.decode(&mut tmp).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn oversize_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(FLAG_DATA);
        buf.put_u32(1);
        buf.put_u32(MAX_FRAME_PAYLOAD + 1);
        let mut tmp = buf;
        let err = FrameCodec.decode(&mut tmp).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
