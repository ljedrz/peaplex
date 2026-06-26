# peaplex

[![Crates.io](https://img.shields.io/crates/v/peaplex.svg)](https://crates.io/crates/peaplex)
[![Documentation](https://docs.rs/peaplex/badge.svg)](https://docs.rs/peaplex)
[![dependency status](https://deps.rs/repo/github/ljedrz/peaplex/status.svg)](https://deps.rs/repo/github/ljedrz/peaplex)

A minimal, zero-copy stream multiplexer that runs on top of any
`AsyncRead + AsyncWrite` connection.

A single duplex connection between two peers carries a peaplex session.
That session exposes an arbitrary number of independent, ordered,
full-duplex **substreams** that look like `tokio::io` byte streams and
can be plugged into anything that takes one.

---

### 📖 Table of Contents

- [At a glance](#at-a-glance)
- [Wire format](#wire-format)
- [Usage](#usage)
- [How it fits together](#how-it-fits-together)
- [Two ways to use it](#two-ways-to-use-it)
- [Flow control and backpressure](#flow-control-and-backpressure)
- [Performance](#performance)
- [Limitations](#limitations)
- [Interoperability](#interoperability)
- [License](#license)
- [Peapod](#-peapod)

---

## At a glance

- **Four frame types** - `Open`, `Data`, `Close`, `GoAway`. 9-byte
  header, big-endian, no padding.
- **Zero-copy payloads** - inbound `Data` chunks are delivered to the
  user as a slice of the same memory the transport handed us.
- **Transport-agnostic** - `tokio::net::TcpStream`, `tokio::io::duplex`
  halves, TLS-wrapped streams, anything `AsyncRead + AsyncWrite`.
- **One background task** per session. No per-stream tasks, no
  per-connection locks held across `.await` points.
- **No frills, on purpose** - no per-stream flow-control windows, no
  keepalive pings, no half-close, no `SYN`/`ACK`/`FIN` round-trips.
  peaplex moves frames; any policy on top of that (flow control,
  backpressure, quotas) is the layer above's job. See
  [Flow control and backpressure](#flow-control-and-backpressure).

## Wire format

```
+--------+-------------------------------------------------+
|  Flag  |                     Stream ID                   |  (1 + 4 bytes)
+--------+-------------------------------------------------+
|                       Length (bytes)                     |  (4 bytes)
+----------------------------------------------------------+
|                     Payload (Length bytes)               |  (0..16 MiB)
+----------------------------------------------------------+
```

| Flag   | Value | Meaning                                            |
|--------|-------|----------------------------------------------------|
| Open   | `0x01`| announces a new substream                          |
| Data   | `0x02`| carries a chunk of substream payload               |
| Close  | `0x04`| tears down a substream in both directions          |
| GoAway | `0x08`| tears down the entire connection; 4-byte reason    |

Stream IDs are partitioned by side to avoid collisions: the dialing side
mints odd IDs starting at `1`; the listening side mints even IDs starting
at `2`.

## Usage

```rust
use peaplex::{Multiplexer, Side};
use tokio::io::duplex;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Any AsyncRead + AsyncWrite will do. Here we use an in-memory
    // duplex pair to keep the example self-contained.
    let (a_io, b_io) = duplex(64 * 1024);

    let a = Multiplexer::new(a_io, Side::Initiator);
    let b = Multiplexer::new(b_io, Side::Responder);

    // Accept-loop on b.
    tokio::spawn(async move {
        while let Ok(mut s) = b.accept().await {
            // `s` is AsyncRead + AsyncWrite; use it like any byte stream.
            tokio::io::copy(&mut s, &mut tokio::io::sink()).await.unwrap();
        }
    });

    // Open a stream from a and write to it.
    let mut s = a.open_stream()?;
    s.write_all(b"hello, peaplex!").await?;
    drop(s); // sends Close

    Ok(())
}
```

## How it fits together

- `Multiplexer<IO>` takes ownership of the transport and spawns one
  background task (`drive`) that:
  1. decodes inbound bytes into `Frame`s and dispatches them to the
     per-stream state,
  2. pulls fully-encoded outgoing `Bytes` from an `mpsc` channel and
     serializes them through a single `write_buf` (coalesced writes),
  3. exits on transport EOF, transport error, or
     `Multiplexer::shutdown`.
- `Stream` is the user-facing handle for one substream. It implements
  `AsyncRead + AsyncWrite` and holds an `Arc<MultiplexerInner>`, so
  multiple `Stream`s and `Multiplexer` clones all share the same
  state. Dropping a `Stream` sends a `Close` and reclaims the
  per-substream state immediately (it does not wait for the peer to
  reciprocate).
- `Multiplexer::goaway(reason)` queues a `GoAway` frame, flushes it
  to the transport, and then signals shutdown. The peer is notified
  before the transport is torn down.

## Two ways to use it

This is the most important design decision in the crate, so it gets its
own section. peaplex ships at two levels, and **which one you pick decides
whether *you* can enforce flow control and memory bounds.** The dividing
line is *who owns the dispatch loop* - the code that decides what happens
to each inbound frame.

### 1. `Multiplexer` - the convenience path

`Multiplexer::new(io, side)` takes ownership of an
`AsyncRead + AsyncWrite` transport, spawns the drive task, and hands you
`open_stream`/`accept`/`Stream`/`shutdown`/`goaway`. Batteries included.

The trade-off: **peaplex owns the dispatch loop**, and it is deliberately
simple - every inbound `Data` frame is appended to that substream's
receive buffer, every inbound `Open` is queued for `accept()`, and neither
buffer is bounded. There is no admission hook and no way to push back on
the socket from your code. A cooperative peer is fine; a peer that floods
a substream you read slowly (or `Open`s faster than you `accept`) grows
memory on your side, and you cannot stop it from outside the API. Reach
for this path when you control both ends and trust the peer, or when an
outer layer already bounds traffic.

### 2. `Frame` + `FrameCodec` - the control path

`FrameCodec` is a plain `tokio_util` `Encoder`/`Decoder` for `Frame`. Plug
it into your own (or your framework's) read/write loop and **you own
dispatch.** That is the whole point: because the per-substream buffering
happens in *your* code, you can cap each buffer, drop or `Close` a
misbehaving substream, `GoAway` a misbehaving peer, or - if your loop is
async and can pend - stop reading from the socket so TCP backpressure
throttles the sender. Everything `Multiplexer` can't let you do, this path
can, because the policy is yours.

`examples/pea2pea.rs` is a complete worked example of this path over a
`pea2pea` connection. It reimplements the same dispatch `Multiplexer` does
- which is exactly where you would add the bounds described above.

## Flow control and backpressure

peaplex has **no protocol-level flow control**: there is no window-update
or credit frame, so the wire format itself never tells a sender to slow
down. This is intentional - it keeps the core to four frame types - but it
means flow control, if you want it, lives *above* peaplex. Two distinct
concerns, with different answers:

- **Cooperative flow control** (both peers well-behaved, you just don't
  want to over-buffer): solvable on either path, no peaplex involvement
  needed. Run a credit/ack scheme in your `Data` payloads - the receiver
  acks after consuming *N* bytes, the sender stays within a window. The
  substream is your byte stream; peaplex never needs to know.

- **Defensive bounding** (a peer that ignores the window, or is hostile):
  solvable **only on the control path (2)**, because it requires rejecting
  inbound frames or applying socket backpressure - and both happen in the
  dispatch loop. On the control path you cap the per-substream buffer and
  drop/`Close`/`GoAway` on overflow, and (with an async dispatch loop) you
  can stop reading the socket to throttle the sender at the transport. On
  the convenience `Multiplexer` path you cannot: dispatch is internal and
  never pends, so your only lever is to drain streams promptly - which
  bounds *your* memory but does nothing to throttle the sender's bandwidth.

Outbound is simpler. `Multiplexer`'s outgoing queue is unbounded by design
(see below); a fast writer over a slow transport grows it. On the control
path the outbound queue is whatever your framework provides - e.g.
`pea2pea`'s is bounded and returns an error on overflow rather than growing.

## Performance

- **One** background task per session. Reads, writes, and shutdown
  are multiplexed with a single `select!` in the drive task. No
  per-stream tasks, no per-connection locks held across `.await`.
- The inbound path decodes frames in a tight loop until the codec
  reports it needs more bytes, so a large receive coalesces into a
  single dispatch batch.
- The outbound path coalesces every frame queued in the same drive
  iteration into one `io.write` call.
- `parking_lot::Mutex` for the small per-connection state map
  (allocations, stream IDs, accept waker).
- `Bytes` payloads are refcounted end-to-end; the only unavoidable
  copy is `Bytes::copy_from_slice` of the user's `&[u8]` in
  `Stream::poll_write` (one allocation per write call, regardless of
  size).
- `Multiplexer`'s outgoing `mpsc` is intentionally unbounded;
  `Stream::poll_write` queues into it and returns immediately, never
  pending. See [Flow control and backpressure](#flow-control-and-backpressure)
  for what to do about it.

## Limitations

All of these are properties of the convenience `Multiplexer` path. The
control path (`Frame` + `FrameCodec`, where you own dispatch) can address
the buffering ones directly - see [Two ways to use it](#two-ways-to-use-it).

- **Unbounded receive buffers.** A peer that sends `Data` on a substream
  you read slowly, or `Open`s faster than you `accept()`, grows the
  per-substream buffer / the `new_streams` queue without bound, and the
  `Multiplexer` API gives you no way to push back. This is the central
  reason to drop to the control path when you don't fully trust the peer.
- **No write backpressure.** `Stream::poll_write` queues into the
  unbounded outgoing `mpsc` and returns `Ready` immediately; a fast writer
  over a slow transport grows that queue. Write fewer bytes per call, run
  a cooperative window (see above), or use the control path over a
  transport whose send queue is bounded.
- **`poll_flush` is a no-op.** It returns `Ready(Ok(()))` without waiting
  for bytes to reach the transport - peaplex hands frames to the drive
  task and has no per-stream flush point. "Wrote and flushed" does not
  imply "delivered."
- **The drive task does not auto-exit** when all `Multiplexer` clones and
  `Stream`s are dropped; it exits when the transport becomes unreadable or
  `shutdown()` is called. To release the transport promptly, always call
  `Multiplexer::shutdown()` (or `goaway`).

## Interoperability

peaplex speaks **only to peaplex.** The wire format is its own - a 9-byte
big-endian header and four frame types - and is not compatible with
yamux, mplex, or any other multiplexer. Both ends of a session must run
peaplex (and pick opposite `Side` values).

## License

Dual-licensed under CC0-1.0 or MIT, at your option.

## 🫛 Peapod

This library is part of the Peapod: a collection of small, composable Rust libraries for building robust peer-to-peer systems.

| Library | Purpose |
| ------- | ------- |
| `pea2pea` | Lightweight P2P networking primitive |
| `peashape` | Traffic shaping |
| `peaveil` | Privacy-oriented peer discovery |
| `peasub` | Metadata-private dissemination |
| `peaplex` | Optional stream multiplexing |
| `peaboard` | Reference application |

Each library does one thing well and composes naturally with the others.
