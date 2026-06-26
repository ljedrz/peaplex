//! End-to-end tests for the standalone peaplex over a `tokio::io::duplex`
//! pair - no real network, no extra dependencies.

use std::time::Duration;

use peaplex::{FLAG_CLOSE, FLAG_DATA, FrameCodec, Multiplexer, Side, StreamId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, duplex},
    time::timeout,
};

fn small_pair() -> (
    Multiplexer<tokio::io::DuplexStream>,
    Multiplexer<tokio::io::DuplexStream>,
) {
    let (a, b) = duplex(64 * 1024);
    (
        Multiplexer::new(a, Side::Initiator),
        Multiplexer::new(b, Side::Responder),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basic_roundtrip() {
    let (a, b) = small_pair();

    let reader = tokio::spawn(async move {
        let mut s = b.accept().await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        buf
    });

    let mut s = a.open_stream().unwrap();
    let payload = b"hello, peaplex!";
    s.write_all(payload).await.unwrap();
    drop(s);

    let got = timeout(Duration::from_secs(5), reader)
        .await
        .expect("reader timed out")
        .unwrap();
    assert_eq!(got, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bidirectional_streams() {
    let (a, b) = small_pair();

    // a -> b
    let b2 = b.clone();
    let b_reader = tokio::spawn(async move {
        let mut s = b2.accept().await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        buf
    });
    let mut s_ab = a.open_stream().unwrap();
    s_ab.write_all(b"from a to b").await.unwrap();
    drop(s_ab);
    assert_eq!(
        timeout(Duration::from_secs(5), b_reader)
            .await
            .unwrap()
            .unwrap(),
        b"from a to b"
    );

    // b -> a
    let a2 = a.clone();
    let a_reader = tokio::spawn(async move {
        let mut s = a2.accept().await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        buf
    });
    let mut s_ba = b.open_stream().unwrap();
    s_ba.write_all(b"from b to a").await.unwrap();
    drop(s_ba);
    assert_eq!(
        timeout(Duration::from_secs(5), a_reader)
            .await
            .unwrap()
            .unwrap(),
        b"from b to a"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_streams() {
    let (a, b) = small_pair();

    const N: usize = 32;
    let b2 = b.clone();
    let reader = tokio::spawn(async move {
        let mut received = Vec::with_capacity(N);
        for _ in 0..N {
            let mut s = b2.accept().await.unwrap();
            let mut buf = Vec::new();
            s.read_to_end(&mut buf).await.unwrap();
            received.push(buf);
        }
        received
    });

    let mut writers = Vec::new();
    for i in 0..N {
        let a = a.clone();
        writers.push(tokio::spawn(async move {
            let mut s = a.open_stream().unwrap();
            let payload = format!("stream #{i}");
            s.write_all(payload.as_bytes()).await.unwrap();
            s.shutdown().await.unwrap();
        }));
    }
    for w in writers {
        w.await.unwrap();
    }

    let received = timeout(Duration::from_secs(10), reader)
        .await
        .unwrap()
        .unwrap();
    let mut expected: Vec<Vec<u8>> = (0..N)
        .map(|i| format!("stream #{i}").into_bytes())
        .collect();
    let mut got = received;
    expected.sort();
    got.sort();
    assert_eq!(got, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_payload_roundtrip() {
    let (a, b) = small_pair();

    let payload: Vec<u8> = (0..(4 * 1024 * 1024)).map(|i| (i % 251) as u8).collect();

    let b2 = b.clone();
    let reader = tokio::spawn(async move {
        let mut s = b2.accept().await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        buf
    });

    let mut s = a.open_stream().unwrap();
    for chunk in payload.chunks(64 * 1024) {
        s.write_all(chunk).await.unwrap();
    }
    drop(s);

    let received = timeout(Duration::from_secs(10), reader)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.len(), payload.len());
    assert_eq!(received, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_closes_streams() {
    let (a, b) = small_pair();
    let a2 = a.clone();

    let b_reader = tokio::spawn(async move {
        let mut s = b.accept().await.unwrap();
        let mut buf = Vec::new();
        // Read until EOF.
        let _ = s.read_to_end(&mut buf).await.unwrap();
        buf
    });

    let mut s = a.open_stream().unwrap();
    s.write_all(b"before shutdown").await.unwrap();

    // Give b a moment to receive the data.
    tokio::time::sleep(Duration::from_millis(20)).await;
    a.shutdown();

    // Further writes must fail.
    let err = s.write_all(b"after shutdown").await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);

    let got = timeout(Duration::from_secs(5), b_reader)
        .await
        .unwrap()
        .unwrap();
    // b might see the full payload and then EOF, or just EOF; both are
    // valid.
    assert!(
        got == b"before shutdown" || got.is_empty(),
        "unexpected payload: {got:?}"
    );
    let _ = a2;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_id_layout() {
    // Initiator mints odd IDs, Responder mints even IDs.
    let (a, b) = small_pair();
    let _ = b.clone();

    let s1 = a.open_stream().unwrap();
    let s2 = a.open_stream().unwrap();
    let s3 = b.open_stream().unwrap();
    let s4 = b.open_stream().unwrap();
    assert_eq!(s1.id(), StreamId(1));
    assert_eq!(s2.id(), StreamId(3));
    assert_eq!(s3.id(), StreamId(2));
    assert_eq!(s4.id(), StreamId(4));
    assert!(s1.id().is_initiator());
    assert!(s3.id().is_responder());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_stream_sends_close() {
    // If a stream is dropped without ever being written to or read from,
    // the peer's read should see EOF (the Close frame is sent on Drop).
    let (a, b) = small_pair();

    let b2 = b.clone();
    let reader = tokio::spawn(async move {
        let mut s = b2.accept().await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        buf
    });

    // Open and immediately drop on A. The Drop sends Close.
    {
        let _s = a.open_stream().unwrap();
    }

    let got = timeout(Duration::from_secs(5), reader)
        .await
        .expect("reader timed out")
        .unwrap();
    assert!(got.is_empty(), "expected EOF, got {} bytes", got.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goaway_marks_all_streams_closed() {
    let (a, b) = small_pair();

    // Synchronize so A doesn't goaway before B has accepted the
    // streams. Without this, the duplex can be torn down before the
    // Open frames reach B, and B's accept loop sees `closed = true`
    // with an empty `new_streams` queue.
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go_rx) = tokio::sync::oneshot::channel();

    let b2 = b.clone();
    let reader = tokio::spawn(async move {
        let mut s1 = b2.accept().await.unwrap();
        let mut s2 = b2.accept().await.unwrap();
        accepted_tx.send(()).unwrap();
        // Wait for A to fire goaway before reading.
        go_rx.await.unwrap();
        let mut buf = Vec::new();
        s1.read_to_end(&mut buf).await.unwrap();
        let mut buf2 = Vec::new();
        let _ = s2.read_to_end(&mut buf2).await.unwrap();
        (buf, buf2)
    });

    let mut s1 = a.open_stream().unwrap();
    let mut s2 = a.open_stream().unwrap();
    s1.write_all(b"hi").await.unwrap();
    s2.write_all(b"hello").await.unwrap();
    accepted_rx.await.unwrap();
    go_tx.send(()).unwrap();
    a.goaway(0);

    let (got, got2) = timeout(Duration::from_secs(5), reader)
        .await
        .expect("reader timed out")
        .unwrap();
    assert_eq!(got, b"hi");
    assert!(got2.is_empty() || got2 == b"hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goaway_frame_is_actually_delivered() {
    use tokio_util::codec::Decoder;

    // A goes away; B should receive the GoAway frame on the wire, not
    // just observe the EOF from the duplex closing.
    let (a_io, b_io) = duplex(64 * 1024);
    let _a = Multiplexer::new(a_io, Side::Initiator);
    let mut b_reader = b_io;

    // Give the drive task a moment to start, then go away.
    tokio::time::sleep(Duration::from_millis(20)).await;
    _a.goaway(42);

    let mut buf = bytes::BytesMut::with_capacity(64);
    let n = timeout(Duration::from_secs(5), b_reader.read_buf(&mut buf))
        .await
        .expect("read timed out")
        .unwrap();
    assert!(n > 0, "expected at least one frame from a, got EOF");

    let frame = FrameCodec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame.flag, peaplex::Flag::GoAway);
    assert_eq!(frame.payload.len(), 4);
    let reason = u32::from_be_bytes(frame.payload[..].try_into().unwrap());
    assert_eq!(reason, 42);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goaway_reason_is_readable_by_peer() {
    // When A goes away with a reason, B can recover that reason via
    // `goaway_reason()` once the frame has been dispatched.
    let (a, b) = small_pair();

    a.goaway(7);

    // Poll until B's drive task has dispatched the GoAway frame.
    let reason = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(r) = b.goaway_reason() {
                break r;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("never observed goaway reason");
    assert_eq!(reason, 7);

    // A initiated the shutdown locally, so it has no inbound GoAway reason.
    assert_eq!(a.goaway_reason(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debug_impls() {
    let (a, b) = small_pair();
    let _ = format!("{:?}", a);
    let s = a.open_stream().unwrap();
    let _ = format!("{:?}", s);
    let _ = b;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frame_codec_smoke() {
    use bytes::BytesMut;
    use peaplex::Frame;
    use tokio_util::codec::{Decoder, Encoder};

    let mut codec = FrameCodec;
    let mut buf = BytesMut::new();
    let f = Frame::data(StreamId(0x42), bytes::Bytes::from_static(b"abc"));
    codec.encode(f, &mut buf).unwrap();
    assert_eq!(buf[0], FLAG_DATA);
    let got = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(got.stream_id, StreamId(0x42));
    assert_eq!(&got.payload[..], b"abc");
    assert!(codec.decode(&mut buf).unwrap().is_none());

    let f = Frame::close(StreamId(7));
    codec.encode(f, &mut buf).unwrap();
    assert_eq!(buf[buf.len() - 9], FLAG_CLOSE);
}
