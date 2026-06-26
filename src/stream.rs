//! A peaplex substream handle, exposed to the user as [`Stream`].

use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use bytes::{Buf, Bytes};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{frame::MAX_FRAME_PAYLOAD, mplex::MultiplexerInner, Frame, StreamId};

/// Per-substream incoming state shared between the drive task and the
/// user-facing [`Stream`] handle.
#[derive(Default)]
pub(crate) struct IncomingState {
    /// Buffered payload chunks awaiting consumption by the user.
    pub(crate) data: VecDeque<Bytes>,
    /// `true` once the peer has closed this substream (or the
    /// connection).
    pub(crate) closed: bool,
    /// Waker registered by a pending [`Stream::poll_read`].
    pub(crate) read_waker: Option<Waker>,
}

/// A refcounted handle to a peaplex substream's incoming state.
pub(crate) type IncomingStateHandle = Arc<Mutex<IncomingState>>;

/// A single peaplex substream.
///
/// Implements [`AsyncRead`] and [`AsyncWrite`], so it can be used anywhere
/// a full-duplex byte stream is expected.
///
/// Dropping the stream is equivalent to a graceful close: a `Close` frame
/// is queued for the peer, and the per-substream state on both sides is
/// released immediately (without waiting for the peer to reciprocate).
/// For graceful shutdown without dropping the handle, call
/// [`AsyncWrite::poll_shutdown`]; peaplex will then refuse further
/// writes but leave the substream state in place until the peer's
/// `Close` arrives.
pub struct Stream {
    pub(crate) inner: Arc<MultiplexerInner>,
    pub(crate) id: StreamId,
    pub(crate) incoming: IncomingStateHandle,
    pub(crate) outgoing_closed: bool,
}

impl Stream {
    /// Returns the substream's identifier.
    #[inline]
    pub fn id(&self) -> StreamId {
        self.id
    }
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id)
            .field("outgoing_closed", &self.outgoing_closed)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut incoming = self.incoming.lock();
        if let Some(mut data) = incoming.data.pop_front() {
            let n = std::cmp::min(buf.remaining(), data.len());
            buf.put_slice(&data[..n]);
            if n < data.len() {
                // put the unread tail back at the front; zero-copy.
                data.advance(n);
                incoming.data.push_front(data);
            }
            return Poll::Ready(Ok(()));
        }
        if incoming.closed {
            // EOF: the peer has closed (or the connection has been torn
            // down).
            return Poll::Ready(Ok(()));
        }
        incoming.read_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.outgoing_closed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if buf.len() > MAX_FRAME_PAYLOAD as usize {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write buffer exceeds peaplex max frame size",
            )));
        }
        // copy_from_slice is the only unavoidable copy; the rest of the
        // pipeline (encode -> write -> read -> decode -> dispatch) is
        // refcounted `Bytes` all the way.
        let frame = Frame::data(self.id, Bytes::copy_from_slice(buf));
        match self.inner.send_frame(frame) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // peaplex hands the frame to a background task that flushes the
        // transport; there is no per-stream flush point to surface here.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().send_close();
        Poll::Ready(Ok(()))
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        // Make sure the peer always sees a Close so it can release its
        // side of the substream state. `send_frame` only fails if the
        // multiplexer is already shut down, in which case there's no
        // peer to notify anyway.
        self.send_close();
        // Release the per-substream state on our side immediately, so we
        // don't leak it if the peer never sends its own `Close` (e.g.
        // because it dropped its matching stream at the same time).
        // The waker, if any, is dropped here; if the dispatcher observes
        // a peer `Close` later it will find no entry and no-op.
        let _ = self.inner.close_local_stream(self.id);
    }
}

impl Stream {
    fn send_close(&mut self) {
        if !self.outgoing_closed {
            let _ = self.inner.send_frame(Frame::close(self.id));
            self.outgoing_closed = true;
        }
    }
}
