//! peaplex over a pea2pea connection.
//!
//! This example shows the minimal glue needed to make `peaplex` work as a
//! peer's `Reading` and `Writing` protocol. It implements a small
//! `MplexedNode` that owns a `pea2pea::Node` and per-connection peaplex
//! state, then exchanges a handful of substreams between two such nodes.
//!
//! Run it with `cargo run --example pea2pea`.

use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;
use pea2pea::{
    Config, Node, Pea2Pea,
    connections::DisconnectOrigin,
    protocols::{OnDisconnect, Reading, Writing},
    ConnectionSide,
};
use peaplex::{Flag, Frame, FrameCodec, StreamId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
struct MplexedNode {
    node: Node,
    state: Arc<Mutex<MplexState>>,
}

struct MplexState {
    conn_state: HashMap<SocketAddr, ConnState>,
    new_streams: VecDeque<(SocketAddr, Stream)>,
    accept_waker: Option<Waker>,
    closed: bool,
}

struct ConnState {
    streams: HashMap<StreamId, Arc<Mutex<SubstreamState>>>,
    next_id: u32,
}

#[derive(Default)]
struct SubstreamState {
    data: VecDeque<bytes::Bytes>,
    closed: bool,
    read_waker: Option<Waker>,
}

impl MplexedNode {
    fn new(node: Node) -> Self {
        Self {
            node,
            state: Arc::new(Mutex::new(MplexState {
                conn_state: HashMap::new(),
                new_streams: VecDeque::new(),
                accept_waker: None,
                closed: false,
            })),
        }
    }

    fn open_stream(&self, peer: SocketAddr) -> io::Result<Stream> {
        let (id, state) = {
            let mut s = self.state.lock();
            let conn = s.conn_state.get_mut(&peer).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "no peaplex connection to peer")
            })?;
            let id = StreamId(conn.next_id);
            conn.next_id = conn.next_id.wrapping_add(2);
            let state: Arc<Mutex<SubstreamState>> = Default::default();
            conn.streams.insert(id, state.clone());
            (id, state)
        };
        // pea2pea's outbound queue is bounded; under burst we can race it
        // and get `QuotaExceeded`. Roll back the registration on any
        // failure so we never leak state.
        if let Err(e) = self.unicast_fast(peer, Frame::open(id)) {
            let mut s = self.state.lock();
            if let Some(conn) = s.conn_state.get_mut(&peer) {
                conn.streams.remove(&id);
            }
            return Err(e);
        }
        Ok(Stream::new(self.clone(), peer, id, state))
    }

    fn accept(&self) -> Accept<'_> {
        Accept { node: self }
    }
}

/// Future returned by [`MplexedNode::accept`].
struct Accept<'a> {
    node: &'a MplexedNode,
}

impl<'a> Future for Accept<'a> {
    type Output = io::Result<(SocketAddr, Stream)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut s = self.node.state.lock();
        if let Some(item) = s.new_streams.pop_front() {
            return Poll::Ready(Ok(item));
        }
        if s.closed {
            return Poll::Ready(Err(io::ErrorKind::Other.into()));
        }
        s.accept_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

// `Stream` is provided by the peaplex crate, but its constructor is private.
// We need a tiny shim that holds the per-connection dispatch state and
// implements `AsyncRead`/`AsyncWrite` on top of it. To keep the example
// self-contained we define our own `Stream` type here that shares the same
// wire format but is driven by the pea2pea reading/writing protocols.
struct Stream {
    node: MplexedNode,
    peer: SocketAddr,
    id: StreamId,
    state: Arc<Mutex<SubstreamState>>,
    outgoing_closed: bool,
}

impl Stream {
    fn new(node: MplexedNode, peer: SocketAddr, id: StreamId, state: Arc<Mutex<SubstreamState>>) -> Self {
        Self {
            node,
            peer,
            id,
            state,
            outgoing_closed: false,
        }
    }
}

impl tokio::io::AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut s = self.state.lock();
        if let Some(mut data) = s.data.pop_front() {
            use bytes::Buf;
            let n = std::cmp::min(buf.remaining(), data.len());
            buf.put_slice(&data[..n]);
            if n < data.len() {
                data.advance(n);
                s.data.push_front(data);
            }
            return Poll::Ready(Ok(()));
        }
        if s.closed {
            return Poll::Ready(Ok(()));
        }
        s.read_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl tokio::io::AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.outgoing_closed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        let frame = Frame::data(self.id, bytes::Bytes::copy_from_slice(buf));
        match self.node.unicast_fast(self.peer, frame) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.outgoing_closed {
            let _ = self.node.unicast_fast(self.peer, Frame::close(self.id));
            self.outgoing_closed = true;
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        if !self.outgoing_closed {
            let _ = self.node.unicast_fast(self.peer, Frame::close(self.id));
        }
    }
}

impl Pea2Pea for MplexedNode {
    fn node(&self) -> &Node {
        &self.node
    }
}

impl Reading for MplexedNode {
    type Message = Frame;
    type Codec = FrameCodec;

    fn codec(&self, addr: SocketAddr, side: ConnectionSide) -> Self::Codec {
        // Lazily seed per-connection state.
        let mut s = self.state.lock();
        s.conn_state
            .entry(addr)
            .or_insert_with(|| ConnState::new(side));
        FrameCodec
    }

    fn process_message(
        &self,
        source: SocketAddr,
        message: Self::Message,
    ) -> impl Future<Output = ()> + Send {
        // Do the dispatch synchronously, then fire wakers after the lock
        // is released.
        let (read_wakers, accept_waker) = self.dispatch(source, message);
        for w in read_wakers {
            w.wake();
        }
        if let Some(w) = accept_waker {
            w.wake();
        }
        async {}
    }
}

impl MplexedNode {
    /// Synchronous dispatch for [`Reading::process_message`]. Returns the
    /// read and accept wakers to fire after the state lock is released.
    fn dispatch(
        &self,
        source: SocketAddr,
        message: Frame,
    ) -> (Vec<Waker>, Option<Waker>) {
        let mut s = self.state.lock();
        match message.flag {
            Flag::Open => {
                let id = message.stream_id;
                let conn = match s.conn_state.get_mut(&source) {
                    Some(c) => c,
                    None => return (Vec::new(), None),
                };
                let state: Arc<Mutex<SubstreamState>> = Default::default();
                conn.streams.insert(id, state.clone());
                let stream = Stream::new(self.clone(), source, id, state);
                s.new_streams.push_back((source, stream));
                (Vec::new(), s.accept_waker.take())
            }
            Flag::Data => {
                let id = message.stream_id;
                let mut wakers = Vec::new();
                if let Some(conn) = s.conn_state.get(&source)
                    && let Some(state) = conn.streams.get(&id)
                {
                    let mut inc = state.lock();
                    inc.data.push_back(message.payload);
                    if let Some(w) = inc.read_waker.take() {
                        wakers.push(w);
                    }
                }
                (wakers, None)
            }
            Flag::Close => {
                let id = message.stream_id;
                let mut wakers = Vec::new();
                if let Some(conn) = s.conn_state.get_mut(&source)
                    && let Some(state) = conn.streams.remove(&id)
                {
                    let mut inc = state.lock();
                    inc.closed = true;
                    if let Some(w) = inc.read_waker.take() {
                        wakers.push(w);
                    }
                }
                (wakers, None)
            }
            Flag::GoAway => {
                let mut wakers = Vec::new();
                if let Some(mut conn) = s.conn_state.remove(&source) {
                    for (_, state) in conn.streams.drain() {
                        let mut inc = state.lock();
                        inc.closed = true;
                        if let Some(w) = inc.read_waker.take() {
                            wakers.push(w);
                        }
                    }
                }
                (wakers, None)
            }
        }
    }

    /// Synchronous cleanup for [`OnDisconnect::on_disconnect`].
    fn cleanup_disconnect(&self, addr: SocketAddr) -> Vec<Waker> {
        let mut s = self.state.lock();
        match s.conn_state.entry(addr) {
            Entry::Occupied(e) => {
                let mut conn = e.remove();
                let mut wakers = Vec::new();
                for (_, state) in conn.streams.drain() {
                    let mut inc = state.lock();
                    inc.closed = true;
                    if let Some(w) = inc.read_waker.take() {
                        wakers.push(w);
                    }
                }
                wakers
            }
            Entry::Vacant(_) => Vec::new(),
        }
    }
}

impl Writing for MplexedNode {
    type Message = Frame;
    type Codec = FrameCodec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        FrameCodec
    }
}

impl OnDisconnect for MplexedNode {
    fn on_disconnect(
        &self,
        addr: SocketAddr,
        _origin: DisconnectOrigin,
    ) -> impl Future<Output = ()> + Send {
        let wakers = self.cleanup_disconnect(addr);
        for w in wakers {
            w.wake();
        }
        async {}
    }
}

impl ConnState {
    fn new(side: ConnectionSide) -> Self {
        let next_id = match side {
            ConnectionSide::Initiator => 1,
            ConnectionSide::Responder => 2,
        };
        Self {
            streams: HashMap::new(),
            next_id,
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> io::Result<()> {
    // Two pea2pea nodes.
    let listener_cfg = Config {
        listener_addr: Some("127.0.0.1:0".parse().unwrap()),
        ..Config::default()
    };
    let listener_node = Node::new(listener_cfg);
    let dialer_node = Node::new(Config::default());

    let listener = MplexedNode::new(listener_node.clone());
    let dialer = MplexedNode::new(dialer_node.clone());

    listener.enable_reading().await;
    listener.enable_writing().await;
    listener.enable_on_disconnect().await;
    dialer.enable_reading().await;
    dialer.enable_writing().await;
    dialer.enable_on_disconnect().await;

    let listener_addr = listener_node.toggle_listener().await.unwrap().unwrap();
    println!("listener bound to {listener_addr}");
    dialer_node.connect(listener_addr).await.unwrap();

    // Wait for the connection to show up on both sides.
    for _ in 0..200 {
        if dialer_node.is_connected(listener_addr)
            && listener_node.connected_addrs().iter().any(|a| a.ip() == listener_addr.ip())
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Spawn the accept-loop on the listener.
    let listener_clone = listener.clone();
    let accept_task = tokio::spawn(async move {
        let mut streams = Vec::new();
        for _ in 0..4 {
            let (peer, s) = listener_clone.accept().await.unwrap();
            println!("listener: accepted stream {} from {peer}", s.id);
            streams.push(s);
        }
        for mut s in streams {
            let mut buf = Vec::new();
            s.read_to_end(&mut buf).await.unwrap();
            println!("listener: read {} bytes: {:?}", buf.len(), String::from_utf8_lossy(&buf));
        }
    });

    // Open 4 substreams from the dialer, write, and drop (which sends CLOSE).
    for i in 0..4 {
        let mut s = dialer.open_stream(listener_addr).unwrap();
        let msg = format!("hello #{i} from the pea2pea dialer");
        s.write_all(msg.as_bytes()).await.unwrap();
        drop(s);
    }

    accept_task.await.unwrap();
    Ok(())
}
