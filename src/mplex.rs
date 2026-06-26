//! The transport-agnostic peaplex core.
//!
//! [`Multiplexer`] is a zero-copy stream multiplexer that runs on top of any
//! `AsyncRead + AsyncWrite` connection - a `tokio::net::TcpStream`, the two
//! halves of a `tokio::io::duplex`, a TLS-wrapped stream, etc. The
//! `pea2pea` integration is provided separately in `examples/`.

use std::{
    collections::{HashMap, VecDeque},
    io,
    marker::PhantomData,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    sync::{mpsc, oneshot},
};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    Flag, Frame, FrameCodec, Stream, StreamId,
    frame::FRAME_HEADER_LEN,
    stream::{IncomingState, IncomingStateHandle},
};

/// Which side of the underlying connection this `Multiplexer` is.
///
/// Stream IDs are partitioned by side to avoid collisions: the dialing side
/// ([`Side::Initiator`]) mints odd IDs starting at `1`; the listening side
/// ([`Side::Responder`]) mints even IDs starting at `2`. The two sides of
/// a peaplex session **must** pick opposite `Side` values or the
/// per-stream IDs will collide.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Side {
    /// The dialing side; mints odd stream IDs (`1`, `3`, `5`, ...).
    Initiator,
    /// The listening side; mints even stream IDs (`2`, `4`, `6`, ...).
    Responder,
}

/// A zero-copy peaplex session running on top of an `AsyncRead + AsyncWrite`
/// connection.
///
/// `Multiplexer<IO>` is generic over the transport. The transport is moved
/// into a background task that drives reads and writes; clones of the
/// `Multiplexer` (and the [`Stream`]s it hands out) share the same
/// per-connection state via an `Arc`.
///
/// The drive task terminates when:
/// - the underlying transport returns EOF or an I/O error,
/// - [`Multiplexer::shutdown`] (or [`Multiplexer::goaway`]) is called, or
/// - the `Multiplexer` and every outstanding `Stream` are dropped
///   **and** the transport is no longer readable.
///
/// To release the transport promptly, call [`Multiplexer::shutdown`]
/// (or drop the transport yourself if you keep an external handle).
pub struct Multiplexer<IO> {
    inner: Arc<MultiplexerInner>,
    _io: PhantomData<fn() -> IO>,
}

impl<IO> Clone for Multiplexer<IO> {
    fn clone(&self) -> Self {
        // The IO is only referenced through `PhantomData<fn() -> IO>`,
        // so cloning the `Multiplexer` never requires `IO: Clone`.
        Self {
            inner: self.inner.clone(),
            _io: PhantomData,
        }
    }
}

impl<IO> std::fmt::Debug for Multiplexer<IO> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Multiplexer").finish_non_exhaustive()
    }
}

pub(crate) struct MultiplexerInner {
    state: Mutex<MultiplexerState>,
    /// Unbounded channel feeding the drive task. Writes from a `Stream` are
    /// non-blocking; the drive task applies backpressure naturally because
    /// it serializes writes against the underlying transport.
    outgoing_tx: mpsc::UnboundedSender<Bytes>,
    /// Fires once when [`Multiplexer::shutdown`] is called. The drive task
    /// takes the sender out of the `Mutex` so that only the first shutdown
    /// call actually fires the signal.
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
}

struct MultiplexerState {
    /// Active substreams, keyed by `StreamId`.
    streams: HashMap<StreamId, IncomingStateHandle>,
    /// Next stream ID to mint for a locally-opened substream.
    next_id: u32,
    /// Streams opened by the remote peer, awaiting `accept()`.
    new_streams: VecDeque<Stream>,
    /// Waker of a pending `accept` future, if any.
    accept_waker: Option<Waker>,
    /// Set when the multiplexer is shutting down.
    closed: bool,
    /// Reason code from a `GoAway` frame received from the peer, if the
    /// peer tore the connection down. `None` for a local shutdown or a
    /// transport-level close.
    goaway_reason: Option<u32>,
}

impl<IO> Multiplexer<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    /// Wraps the given connection in a new peaplex session.
    ///
    /// The connection is taken over by a background task; the caller no
    /// longer needs to interact with it directly.
    pub fn new(io: IO, side: Side) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let next_id = match side {
            Side::Initiator => 1,
            Side::Responder => 2,
        };
        let inner = Arc::new(MultiplexerInner {
            state: Mutex::new(MultiplexerState {
                streams: HashMap::new(),
                next_id,
                new_streams: VecDeque::new(),
                accept_waker: None,
                closed: false,
                goaway_reason: None,
            }),
            outgoing_tx,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
        });
        let inner_for_task = inner.clone();
        tokio::spawn(drive(io, outgoing_rx, shutdown_rx, inner_for_task));
        Self {
            inner,
            _io: PhantomData,
        }
    }

    /// Opens a new substream.
    ///
    /// Returns an error if the multiplexer has been shut down, or if the
    /// underlying connection has already been torn down.
    pub fn open_stream(&self) -> io::Result<Stream> {
        let (id, incoming) = {
            let mut state = self.inner.state.lock();
            if state.closed {
                return Err(io::ErrorKind::Other.into());
            }
            let id = StreamId(state.next_id);
            state.next_id = state.next_id.wrapping_add(2);
            let incoming: IncomingStateHandle = Arc::new(Mutex::new(IncomingState::default()));
            state.streams.insert(id, incoming.clone());
            (id, incoming)
        };
        if let Err(e) = self.inner.send_frame(Frame::open(id)) {
            // Roll back the registration so we don't leak state for a
            // stream that never went out. We deliberately do *not* rewind
            // `next_id`: between dropping the lock above and re-taking it
            // here another `open_stream` may have minted the next ID, so
            // rewinding could hand a live ID to a future stream and cause
            // a collision. Burning one ID slot is harmless (the space is
            // a wrapping `u32`).
            self.inner.state.lock().streams.remove(&id);
            return Err(e);
        }
        Ok(Stream {
            inner: self.inner.clone(),
            id,
            incoming,
            outgoing_closed: false,
        })
    }

    /// Returns a future that resolves to the next stream opened by the peer.
    ///
    /// Only one `accept` may be pending at a time. The canonical usage is a
    /// single accept-loop task that spawns a worker per stream.
    pub fn accept(&self) -> Accept {
        Accept {
            inner: self.inner.clone(),
        }
    }

    /// Returns the reason code from a `GoAway` frame received from the
    /// peer, if the peer tore the connection down with one.
    ///
    /// Returns `None` if no `GoAway` has been received - including when the
    /// connection was closed locally (via [`Multiplexer::shutdown`]) or by a
    /// transport-level EOF/error. Because reads on a closed substream report
    /// EOF rather than an error, this is the way to distinguish a peer-
    /// initiated `GoAway` (and recover its reason) from an ordinary close.
    pub fn goaway_reason(&self) -> Option<u32> {
        self.inner.state.lock().goaway_reason
    }

    /// Sends a `GoAway` frame to the peer and tears the multiplexer down.
    ///
    /// The frame is queued on the drive task's outbound channel before the
    /// shutdown signal fires, so the peer is notified before the
    /// transport is closed. As with [`Multiplexer::shutdown`], the
    /// transport is not forcibly closed here - the drive task drops it
    /// when it next returns from I/O.
    pub fn goaway(&self, reason: u32) {
        // Best-effort: the send fails only if the drive task has already
        // dropped its receiver, in which case the transport is gone and
        // the peer will see the close anyway.
        let _ = self.inner.send_frame(Frame::goaway(reason));
        self.shutdown();
    }

    /// Closes the multiplexer: every open stream is marked closed (reads
    /// return EOF) and any pending `accept` resolves to an error. The
    /// background drive task is signalled to exit, which drops the
    /// underlying transport and releases its resources.
    pub fn shutdown(&self) {
        let (read_wakers, accept_waker) = self.inner.close_all();
        for w in read_wakers {
            w.wake();
        }
        if let Some(w) = accept_waker {
            w.wake();
        }
        if let Some(tx) = self.inner.shutdown_tx.lock().take() {
            let _ = tx.send(());
        }
    }
}

/// Future returned by [`Multiplexer::accept`].
pub struct Accept {
    inner: Arc<MultiplexerInner>,
}

impl Future for Accept {
    type Output = io::Result<Stream>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.inner.state.lock();
        if let Some(stream) = state.new_streams.pop_front() {
            return Poll::Ready(Ok(stream));
        }
        if state.closed {
            return Poll::Ready(Err(io::ErrorKind::Other.into()));
        }
        // Register the waker on every poll; it's a refcounted clone of an
        // `Arc`, so this is cheap.
        state.accept_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl MultiplexerInner {
    /// Synchronous dispatch for an inbound frame. Returns the read and
    /// accept wakers that should be woken after the state lock is
    /// released.
    fn dispatch(self: &Arc<Self>, frame: Frame) -> (Vec<Waker>, Option<Waker>) {
        let mut state = self.state.lock();
        match frame.flag {
            Flag::Open => {
                // A late `Open` that arrived after shutdown would
                // resurrect an entry that `close_all` already drained
                // and that `accept()` will never return. Drop it on
                // the floor so we don't leak.
                if state.closed {
                    return (Vec::new(), None);
                }
                let id = frame.stream_id;
                let incoming: IncomingStateHandle = Arc::new(Mutex::new(IncomingState::default()));
                state.streams.insert(id, incoming.clone());
                let stream = Stream {
                    inner: self.clone(),
                    id,
                    incoming: incoming.clone(),
                    outgoing_closed: false,
                };
                state.new_streams.push_back(stream);
                (Vec::new(), state.accept_waker.take())
            }
            Flag::Data => {
                let id = frame.stream_id;
                let mut wakers = Vec::new();
                if let Some(incoming) = state.streams.get(&id) {
                    let mut inc = incoming.lock();
                    inc.data.push_back(frame.payload);
                    if let Some(w) = inc.read_waker.take() {
                        wakers.push(w);
                    }
                }
                (wakers, None)
            }
            Flag::Close => {
                let id = frame.stream_id;
                let mut wakers = Vec::new();
                if let Some(incoming) = state.streams.remove(&id) {
                    let mut inc = incoming.lock();
                    inc.closed = true;
                    if let Some(w) = inc.read_waker.take() {
                        wakers.push(w);
                    }
                }
                (wakers, None)
            }
            Flag::GoAway => {
                // Record the peer's reason code so the user can read it
                // back via `Multiplexer::goaway_reason`. A short/garbled
                // payload leaves the reason as `None`.
                if frame.payload.len() >= 4 {
                    state.goaway_reason = Some(u32::from_be_bytes([
                        frame.payload[0],
                        frame.payload[1],
                        frame.payload[2],
                        frame.payload[3],
                    ]));
                }
                let mut wakers = Vec::new();
                for (_, incoming) in state.streams.drain() {
                    let mut inc = incoming.lock();
                    inc.closed = true;
                    if let Some(w) = inc.read_waker.take() {
                        wakers.push(w);
                    }
                }
                (wakers, None)
            }
        }
    }

    /// Encode a frame and hand it to the drive task.
    pub(crate) fn send_frame(&self, frame: Frame) -> io::Result<()> {
        if self.state.lock().closed {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let mut buf = BytesMut::with_capacity(FRAME_HEADER_LEN + frame.payload.len());
        FrameCodec
            .encode(frame, &mut buf)
            .map_err(|_| io::ErrorKind::InvalidData)?;
        self.outgoing_tx
            .send(buf.freeze())
            .map_err(|_| io::ErrorKind::BrokenPipe.into())
    }

    /// Remove a locally-closed substream from the state. Called from
    /// `Stream`'s `Drop` so the per-stream state is reclaimed even if
    /// the peer never reciprocates with its own `Close`.
    pub(crate) fn close_local_stream(&self, id: StreamId) -> Option<Waker> {
        let mut state = self.state.lock();
        if let Some(incoming) = state.streams.remove(&id) {
            let mut inc = incoming.lock();
            inc.closed = true;
            return inc.read_waker.take();
        }
        None
    }

    /// Mark every active stream as closed and return the wakers to wake.
    /// Idempotent.
    fn close_all(&self) -> (Vec<Waker>, Option<Waker>) {
        let mut state = self.state.lock();
        state.closed = true;
        let accept_waker = state.accept_waker.take();
        let mut read_wakers = Vec::new();
        for (_, incoming) in state.streams.drain() {
            let mut inc = incoming.lock();
            inc.closed = true;
            if let Some(w) = inc.read_waker.take() {
                read_wakers.push(w);
            }
        }
        (read_wakers, accept_waker)
    }
}

/// The single background task that drives the underlying transport.
async fn drive<IO>(
    io: IO,
    mut outgoing_rx: mpsc::UnboundedReceiver<Bytes>,
    shutdown_rx: oneshot::Receiver<()>,
    inner: Arc<MultiplexerInner>,
) where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut shutdown_rx = shutdown_rx;
    // Split the transport so reads and writes are independent: a write
    // that blocks (transport buffer full) must not stop us reading, or two
    // peers both mid-write would each park in `write` with nobody draining
    // the other's bytes - a deadlock. With split halves the `select!` below
    // can make progress on the read arm while the write arm is `Pending`.
    let (mut rd, mut wr) = tokio::io::split(io);
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut write_buf = BytesMut::new();

    'drive: loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                // Drain anything already queued (e.g. a `GoAway` queued
                // just before `shutdown()` fired) before tearing down,
                // so the peer actually receives it. Without this flush, the
                // `biased` ordering that gives shutdown priority would
                // race the queued GoAway frame out of the select! and
                // silently drop it.
                while let Ok(bytes) = outgoing_rx.try_recv() {
                    write_buf.extend_from_slice(&bytes);
                }
                while !write_buf.is_empty() {
                    match wr.write(&write_buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let _ = write_buf.split_to(n);
                        }
                    }
                }
                break;
            }
            // 1. Flush pending output. Enabled only when there is something
            //    to write, so an empty `write_buf` doesn't busy-loop. A
            //    `Pending` here yields to the read arm rather than blocking.
            write_result = wr.write(&write_buf), if !write_buf.is_empty() => {
                match write_result {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let _ = write_buf.split_to(n);
                    }
                }
            }
            // 2. Outgoing frames: a `Stream` (or a GoAway) handed us a
            //    fully-encoded `Bytes`; append and let the write arm flush.
            maybe_bytes = outgoing_rx.recv() => {
                match maybe_bytes {
                    Some(bytes) => write_buf.extend_from_slice(&bytes),
                    None => break,
                }
            }
            // 3. Inbound bytes: drain everything the codec can produce.
            read_result = rd.read_buf(&mut read_buf) => {
                match read_result {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        loop {
                            match FrameCodec.decode(&mut read_buf) {
                                Ok(Some(frame)) => {
                                    let (read_wakers, accept_waker) = inner.dispatch(frame);
                                    for w in read_wakers { w.wake(); }
                                    if let Some(w) = accept_waker { w.wake(); }
                                }
                                Ok(None) => break,
                                Err(_) => {
                                    // Malformed frame: tear the session
                                    // down. Break to the common teardown
                                    // below so parked readers/accepters are
                                    // actually woken (a bare `return` here
                                    // would drop their wakers and hang them).
                                    break 'drive;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Transport closed; mark all streams as closed.
    let (read_wakers, accept_waker) = inner.close_all();
    for w in read_wakers {
        w.wake();
    }
    if let Some(w) = accept_waker {
        w.wake();
    }
}
