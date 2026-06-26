//! # peaplex
//!
//! A profoundly minimal, zero-copy stream multiplexer. Pluggable onto any
//! `AsyncRead + AsyncWrite` connection - a `tokio::net::TcpStream`, the two
//! halves of a `tokio::io::duplex`, a TLS-wrapped stream, a `pea2pea`
//! connection (see `examples/pea2pea.rs`), etc.
//!
//! ## What it is
//!
//! A single duplex connection between two peers carries a peaplex session.
//! That session exposes an arbitrary number of independent, ordered,
//! full-duplex **substreams** that look like `tokio::io` byte streams.
//!
//! ## Design
//!
//! Four frame types (`Open`, `Data`, `Close`, `GoAway`) and a 9-byte
//! header. No per-stream flow-control windows, no keepalive pings, no
//! half-close, no `SYN`/`ACK`/`FIN` round-trips. peaplex moves frames;
//! any policy on top (flow control, backpressure, quotas) belongs to the
//! layer above.
//!
//! There are two ways to use the crate, and the choice decides whether
//! *you* can enforce flow control and memory bounds:
//!
//! - [`Multiplexer`] - the convenience path. It owns the transport and
//!   the dispatch loop; you get `open_stream`/`accept`/[`Stream`] for
//!   free, but the per-substream receive buffers are unbounded and there
//!   is no admission hook, so you cannot push back on a flooding peer
//!   from outside the API. Use it when you trust the peer or an outer
//!   layer already bounds traffic.
//! - [`Frame`] + [`FrameCodec`] - the control path. Plug the codec into
//!   your own read/write loop and *you* own dispatch, so you can cap
//!   per-substream buffers, drop/[`Close`](Flag::Close) misbehaving
//!   substreams, or apply socket backpressure. `examples/pea2pea.rs` is
//!   a complete worked example.
//!
//! Note: peaplex has no protocol-level flow control (no window/credit
//! frame). Cooperative flow control is an application concern you can run
//! inside `Data` payloads on either path; defensively bounding a hostile
//! peer requires owning dispatch, i.e. the control path. peaplex also
//! speaks only to peaplex - the wire format is its own and is not
//! compatible with yamux, mplex, or any other multiplexer.
//!
//! ## Wire format
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
//! | Flag   | Value | Meaning                                            |
//! |--------|-------|----------------------------------------------------|
//! | Open   | `0x01`| announces a new substream                          |
//! | Data   | `0x02`| carries a chunk of substream payload               |
//! | Close  | `0x04`| tears down a substream in both directions          |
//! | GoAway | `0x08`| tears down the entire connection                   |
//!
//! Stream IDs are partitioned by side to avoid collisions: the dialing
//! side ([`Side::Initiator`]) mints odd IDs starting at `1`; the listening
//! side ([`Side::Responder`]) mints even IDs starting at `2`. `0` is
//! reserved.
//!
//! ## Zero copy
//!
//! Payloads are wrapped in [`bytes::Bytes`], a refcounted view onto the
//! underlying read buffer. Decoding a frame is `O(1)` and never copies
//! the payload; an inbound `Data` chunk is delivered to the user as a
//! slice of the same memory the kernel handed us.
//!
//! ## Usage
//!
//! ```no_run
//! use peaplex::{Multiplexer, Side};
//! use tokio::io::duplex;
//!
//! async fn run() {
//!     // Any AsyncRead + AsyncWrite will do. Here we use an in-memory
//!     // duplex pair just to make the example self-contained.
//!     let (a, b) = duplex(64 * 1024);
//!
//!     let a = Multiplexer::new(a, Side::Initiator);
//!     let b = Multiplexer::new(b, Side::Responder);
//!
//!     // Spawn an accept-loop on b.
//!     tokio::spawn(async move {
//!         let mut s = b.accept().await.unwrap();
//!         // ... use `s` as an AsyncRead + AsyncWrite
//!     });
//!
//!     // Open a stream from a and write something.
//!     let mut s = a.open_stream().unwrap();
//!     // ... use `s` as an AsyncRead + AsyncWrite
//! }
//! ```
//!
//! For the `pea2pea` integration, see `examples/pea2pea.rs`.

#![deny(missing_docs)]
#![deny(unsafe_code)]

mod frame;
mod mplex;
mod stream;

pub use frame::{
    Flag, Frame, FrameCodec, StreamId, FLAG_CLOSE, FLAG_DATA, FLAG_GOAWAY, FLAG_OPEN,
    FRAME_HEADER_LEN, MAX_FRAME_PAYLOAD,
};
pub use mplex::{Accept, Multiplexer, Side};
pub use stream::Stream;
