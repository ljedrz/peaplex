//! Stress / chaos tests for peaplex.
//!
//! The goal is empirical soundness: throw maximal randomized concurrency
//! and adversarial input at the multiplexer and assert that
//!
//! - it never panics, never deadlocks (every test has a watchdog), and
//!   never leaks bytes across substreams;
//! - a substream that closes cleanly delivers *exactly* what was written,
//!   in order, byte-for-byte;
//! - a substream torn down mid-flight delivers a correct *prefix* of what
//!   was written (no corruption, no cross-talk, no reordering);
//! - garbage on the wire and garbage into the codec are rejected without
//!   panicking or hanging.
//!
//! Everything is driven by a seeded PRNG so a failure is reproducible: the
//! seed is printed at the start of each test and can be pinned with
//! `PEAPLEX_CHAOS_SEED=<n>`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, BytesMut};
use peaplex::{Frame, FrameCodec, Multiplexer, Side, StreamId};
use tokio::{
    io::{duplex, AsyncReadExt, AsyncWriteExt, DuplexStream},
    time::timeout,
};
use tokio_util::codec::{Decoder, Encoder};

// --------------------------------------------------------------------------
// Tiny seeded PRNG (splitmix64). Inline so the tests carry no extra deps and
// are fully reproducible from a single u64 seed.
// --------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (n > 0).
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// `true` with probability `num/den`.
    fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
}

fn root_seed() -> u64 {
    if let Some(v) = std::env::var("PEAPLEX_CHAOS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return v;
    }
    // Time-based by default so repeated runs explore new schedules; printed
    // by each test so any failure can be replayed via PEAPLEX_CHAOS_SEED.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        | 1
}

// --------------------------------------------------------------------------
// Self-describing payloads: each substream carries a header
// `[seed: u64 BE][total_len: u32 BE]` followed by a keystream derived from
// the seed. A reader can recover the seed from the first bytes and verify
// every later byte, which detects corruption, reordering, and any byte
// delivered to the wrong substream.
// --------------------------------------------------------------------------

const HDR: usize = 12;

fn body_byte(seed: u64, i: usize) -> u8 {
    let mut x = seed ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 29)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    (x >> 33) as u8
}

fn make_payload(seed: u64, total_len: usize) -> Vec<u8> {
    assert!(total_len >= HDR);
    let mut v = Vec::with_capacity(total_len);
    v.extend_from_slice(&seed.to_be_bytes());
    v.extend_from_slice(&(total_len as u32).to_be_bytes());
    for i in 0..(total_len - HDR) {
        v.push(body_byte(seed, i));
    }
    v
}

/// Verify `received` is a byte-correct prefix of the payload its header
/// describes. Returns the intended total length. Panics on any mismatch.
fn verify_prefix(received: &[u8]) -> usize {
    assert!(
        received.len() >= HDR || received.is_empty(),
        "stream delivered {} bytes - not enough for a header and not empty",
        received.len()
    );
    if received.is_empty() {
        return 0;
    }
    let seed = u64::from_be_bytes(received[0..8].try_into().unwrap());
    let total_len = u32::from_be_bytes(received[8..12].try_into().unwrap()) as usize;
    assert!(
        received.len() <= total_len,
        "received {} bytes > intended {} (over-delivery)",
        received.len(),
        total_len
    );
    for (i, &b) in received.iter().enumerate().skip(HDR) {
        assert_eq!(
            b,
            body_byte(seed, i - HDR),
            "byte {i} corrupted or mis-delivered (seed {seed:#x})"
        );
    }
    total_len
}

// --------------------------------------------------------------------------
// The main chaos run: many independent sessions in parallel, each opening
// substreams in *both* directions, with random sizes, chunking, and random
// teardown. Clean sessions must deliver everything intact; torn sessions
// must deliver correct prefixes.
// --------------------------------------------------------------------------

const MAX_BODY: u64 = 64 * 1024;
const MAX_STREAMS: u64 = 24;

fn spawn_acceptor(
    m: Multiplexer<DuplexStream>,
    expected: usize,
) -> tokio::task::JoinHandle<Vec<Vec<u8>>> {
    tokio::spawn(async move {
        let mut out = Vec::new();
        for _ in 0..expected {
            match m.accept().await {
                Ok(mut s) => {
                    let mut buf = Vec::new();
                    // On a clean session this reads the whole payload; on a
                    // torn one it returns whatever was buffered before EOF
                    // (or an error, which we treat as "no more bytes").
                    let _ = s.read_to_end(&mut buf).await;
                    out.push(buf);
                }
                Err(_) => break, // session closed; stop accepting
            }
        }
        out
    })
}

fn spawn_writers(
    m: Multiplexer<DuplexStream>,
    n: usize,
    seed: u64,
    torn: bool,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut rng = Rng::new(seed);
        let mut tasks = Vec::new();
        for k in 0..n {
            let m = m.clone();
            let sseed = seed.wrapping_mul(0x100_0193).wrapping_add(k as u64 + 1);
            let total = HDR + rng.below(MAX_BODY) as usize;
            let chunk = 1 + rng.below(4096) as usize;
            let yield_often = rng.chance(1, 2);
            let shutdown_not_drop = rng.chance(1, 4);
            tasks.push(tokio::spawn(async move {
                let payload = make_payload(sseed, total);
                let mut s = match m.open_stream() {
                    Ok(s) => s,
                    Err(_) => return, // session already torn down
                };
                if torn {
                    for c in payload.chunks(chunk) {
                        if s.write_all(c).await.is_err() {
                            return; // torn mid-write: expected, not a panic
                        }
                        if yield_often {
                            tokio::task::yield_now().await;
                        }
                    }
                } else {
                    // Clean session: the write must fully succeed.
                    s.write_all(&payload).await.unwrap();
                }
                if shutdown_not_drop {
                    let _ = s.shutdown().await;
                }
                drop(s);
            }));
        }
        for t in tasks {
            let _ = t.await;
        }
    })
}

async fn run_session(seed: u64, torn: bool) {
    let mut rng = Rng::new(seed);
    // A small transport buffer relative to the aggregate payload exercises
    // partial writes and the read/write interleaving in the drive task.
    let (a_io, b_io) = duplex(256 * 1024);
    let a = Multiplexer::new(a_io, Side::Initiator);
    let b = Multiplexer::new(b_io, Side::Responder);

    let n_a = 1 + rng.below(MAX_STREAMS) as usize; // streams A opens (B accepts)
    let n_b = 1 + rng.below(MAX_STREAMS) as usize; // streams B opens (A accepts)

    let acc_a = spawn_acceptor(a.clone(), n_b);
    let acc_b = spawn_acceptor(b.clone(), n_a);
    let wr_a = spawn_writers(a.clone(), n_a, seed ^ 0xA1, torn);
    let wr_b = spawn_writers(b.clone(), n_b, seed ^ 0xB2, torn);

    // For torn sessions, fire a teardown from a random side after a short,
    // random delay - racing it against in-flight opens/writes/reads.
    if torn {
        let from_a = rng.chance(1, 2);
        let use_goaway = rng.chance(1, 2);
        let delay = rng.below(8);
        let reason = rng.next_u64() as u32;
        let m = if from_a { a.clone() } else { b.clone() };
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            if use_goaway {
                m.goaway(reason);
            } else {
                m.shutdown();
            }
        });
    }

    let _ = wr_a.await;
    let _ = wr_b.await;
    let ra = acc_a.await.unwrap();
    let rb = acc_b.await.unwrap();

    // Integrity oracle: every delivered stream is a correct prefix.
    for buf in ra.iter().chain(rb.iter()) {
        let total = verify_prefix(buf);
        if !torn {
            assert_eq!(buf.len(), total, "clean session delivered a short stream");
        }
    }
    // Completeness oracle: a clean session must deliver every stream in full.
    if !torn {
        assert_eq!(ra.len(), n_b, "A missed some of B's streams");
        assert_eq!(rb.len(), n_a, "B missed some of A's streams");
    }

    a.shutdown();
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_many_sessions() {
    let root = root_seed();
    println!("chaos_many_sessions seed = {root:#x} (set PEAPLEX_CHAOS_SEED to replay)");
    let mut rng = Rng::new(root);

    const SESSIONS: usize = 48;
    let mut handles = Vec::new();
    for i in 0..SESSIONS {
        let seed = rng.next_u64();
        let torn = rng.chance(2, 5); // ~40% of sessions get torn down mid-flight
        handles.push(tokio::spawn(async move {
            // Per-session watchdog: a regression that reintroduces a hang
            // fails loudly here instead of stalling the whole test.
            timeout(Duration::from_secs(20), run_session(seed, torn))
                .await
                .unwrap_or_else(|_| panic!("session {i} (seed {seed:#x}, torn={torn}) hung"));
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// --------------------------------------------------------------------------
// Regression: two peers each pushing a large payload at once over a small
// transport. Before the read/write split in the drive task, both sides
// parked in `write` with nobody reading -> deadlock.
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_bidirectional_bulk() {
    let (a_io, b_io) = duplex(64 * 1024);
    let a = Multiplexer::new(a_io, Side::Initiator);
    let b = Multiplexer::new(b_io, Side::Responder);

    const N: usize = 8 * 1024 * 1024;

    async fn drive_side(m: Multiplexer<DuplexStream>) -> usize {
        let recv = tokio::spawn({
            let m = m.clone();
            async move {
                let mut s = m.accept().await.unwrap();
                let mut buf = Vec::new();
                s.read_to_end(&mut buf).await.unwrap();
                buf.len()
            }
        });
        let mut s = m.open_stream().unwrap();
        s.write_all(&vec![0xAB_u8; N]).await.unwrap();
        drop(s);
        recv.await.unwrap()
    }

    let a_task = tokio::spawn(drive_side(a));
    let b_task = tokio::spawn(drive_side(b));

    let res = timeout(Duration::from_secs(15), async {
        (a_task.await.unwrap(), b_task.await.unwrap())
    })
    .await;

    match res {
        Ok((ra, rb)) => assert!(ra == N && rb == N, "got {ra}/{rb}, expected {N}"),
        Err(_) => panic!("DEADLOCK: simultaneous bidirectional {N}-byte transfer hung"),
    }
}

// --------------------------------------------------------------------------
// Adversarial wire input: a peer that sends a malformed frame must make the
// multiplexer tear down cleanly and wake any parked reader (return EOF),
// not hang or panic.
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn garbage_on_the_wire_wakes_readers() {
    let (a_io, mut peer) = duplex(64 * 1024);
    let a = Multiplexer::new(a_io, Side::Initiator);

    // Park a reader on an open stream.
    let mut s = a.open_stream().unwrap();
    let reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        buf.len()
    });

    // Let the reader register, then send a frame with an unknown flag byte,
    // which the decoder rejects with an error.
    tokio::time::sleep(Duration::from_millis(20)).await;
    peer.write_all(&[0xFF, 0, 0, 0, 1, 0, 0, 0, 0]).await.unwrap();

    let n = timeout(Duration::from_secs(5), reader)
        .await
        .expect("reader hung after malformed frame")
        .unwrap();
    assert_eq!(n, 0, "expected EOF after the session was torn down");
}

// --------------------------------------------------------------------------
// Codec property test: any sequence of frames, fed to a fresh decoder in
// arbitrarily-sized chunks, decodes back to exactly the original sequence.
// --------------------------------------------------------------------------

fn random_frame(rng: &mut Rng) -> Frame {
    let id = StreamId(rng.next_u64() as u32);
    match rng.below(4) {
        0 => Frame::open(id),
        1 => Frame::close(id),
        2 => Frame::goaway(rng.next_u64() as u32),
        _ => {
            let len = rng.below(8 * 1024) as usize;
            let mut p = BytesMut::with_capacity(len);
            let seed = rng.next_u64();
            for i in 0..len {
                p.put_u8(body_byte(seed, i));
            }
            Frame::data(id, p.freeze())
        }
    }
}

#[test]
fn codec_chunked_roundtrip() {
    let root = root_seed();
    println!("codec_chunked_roundtrip seed = {root:#x}");
    for round in 0..300u64 {
        let mut rng = Rng::new(root ^ round.wrapping_mul(0x9E37_79B9));
        let n = 1 + rng.below(20);
        let mut frames = Vec::new();
        let mut encoded = BytesMut::new();
        for _ in 0..n {
            let f = random_frame(&mut rng);
            FrameCodec.encode(f.clone(), &mut encoded).unwrap();
            frames.push(f);
        }

        let bytes = encoded.to_vec();
        let mut dec = FrameCodec;
        let mut acc = BytesMut::new();
        let mut got = Vec::new();
        let mut pos = 0;
        while pos < bytes.len() {
            let take = 1 + rng.below((bytes.len() - pos) as u64) as usize;
            acc.extend_from_slice(&bytes[pos..pos + take]);
            pos += take;
            while let Some(f) = dec.decode(&mut acc).unwrap() {
                got.push(f);
            }
        }
        assert_eq!(got, frames, "round {round}: chunked decode != original");
    }
}

// --------------------------------------------------------------------------
// Codec fuzz: arbitrary bytes must never panic the decoder and must always
// terminate (return Ok(None) or Err), never loop forever.
// --------------------------------------------------------------------------

#[test]
fn codec_garbage_never_panics() {
    let root = root_seed();
    println!("codec_garbage_never_panics seed = {root:#x}");
    for round in 0..2000u64 {
        let mut rng = Rng::new(root ^ round.wrapping_mul(0xD1B5_4A32) ^ 0xF00D);
        let len = rng.below(64) as usize;
        let mut buf = BytesMut::with_capacity(len);
        for _ in 0..len {
            buf.put_u8(rng.next_u64() as u8);
        }
        let mut dec = FrameCodec;
        // Terminates on Ok(None) (needs more) or Err (rejected); the test
        // asserts only that it never panics and always terminates.
        while let Ok(Some(_)) = dec.decode(&mut buf) {}
    }
}
