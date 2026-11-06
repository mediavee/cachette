//! Black-box tests of what the server does with bodies it does not store, bodies that stall, and
//! requests in flight at shutdown (D5, D7, D17, D19).

mod common;

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use common::Daemon;
use common::wire::Wire;

const MIB: usize = 1024 * 1024;
const AUTH: (&str, &str) = ("Authorization", "Bearer secret");
const TOO_LARGE: &str = "cachette_puts_dropped_total{reason=\"too_large\"}";

fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from((i * 7 + i / 509) % 256).expect("below 256"))
        .collect()
}

/// The daemon of D5's proof: objects above 1 MiB are not stored, and a client may send 8 MiB.
fn small_object_daemon() -> Daemon {
    Daemon::start_with(&[], &[("CACHETTE_MAX_OBJECT_SIZE", "1MiB")])
}

#[test]
fn an_oversized_put_without_expect_is_read_to_its_end_and_answered_413() {
    let daemon = small_object_daemon();
    let mut wire = Wire::connect(daemon.addr);
    let body = payload(8 * MIB);
    let length = body.len().to_string();
    // The client writes everything before it reads anything, like Gradle without `Expect`. A
    // server that answers without reading would reset the connection under this write.
    wire.send_head(
        "PUT",
        "/big",
        &[
            AUTH,
            ("Content-Length", &length),
            ("Content-Type", "application/octet-stream"),
        ],
    );
    wire.send(&body)
        .expect("the server must keep reading the body");
    let reply = wire.read_reply().expect("response");
    assert_eq!(
        reply.status, 413,
        "a PUT that is not stored answers 413 (D5)"
    );
    assert!(!reply.closes(), "{:?}", reply.headers);

    // The same connection still works.
    let miss = wire.request("GET", "/big", &[AUTH], b"");
    assert_eq!(miss.status, 404, "an oversized object is not stored");
    let small = wire.request("PUT", "/small", &[AUTH], &body[..4096]);
    assert_eq!(small.status, 201);
    assert_eq!(
        wire.request("GET", "/small", &[AUTH], b"").body,
        body[..4096]
    );

    assert_eq!(daemon.metric(TOO_LARGE), 1.0);
    assert_eq!(daemon.metric("cachette_written_bytes_total"), 4096.0);
    assert!(
        daemon.logs().contains("too_large"),
        "a dropped PUT is logged:\n{}",
        daemon.logs()
    );
}

#[test]
fn an_oversized_put_with_expect_is_answered_413_without_a_100_or_a_body() {
    let daemon = small_object_daemon();
    let mut wire = Wire::connect(daemon.addr);
    let length = (8 * MIB).to_string();
    wire.send_head(
        "PUT",
        "/big",
        &[
            AUTH,
            ("Content-Length", &length),
            ("Expect", "100-continue"),
        ],
    );
    let started = Instant::now();
    let reply = wire.read_reply().expect("response");
    assert_eq!(
        reply.status, 413,
        "no 100 may come first: the body is not wanted"
    );
    assert!(started.elapsed() < Duration::from_secs(2));

    // hyper cannot parse the next request after a body it was told to expect and never got, so
    // the connection ends. The answer must say so, or Gradle reuses a dead connection.
    assert!(reply.closes(), "close not announced: {:?}", reply.headers);
    assert!(
        wire.is_closed(Duration::from_secs(2)),
        "announced a close and stayed open"
    );
    let fresh = Daemon::request(&daemon, "GET", "/big", &[AUTH]);
    assert_eq!(fresh.status, 404);
    assert_eq!(daemon.metric(TOO_LARGE), 1.0);
}

#[test]
fn a_put_beyond_the_ceiling_is_dropped_the_same_way() {
    let daemon = Daemon::start_with(
        &[],
        &[
            ("CACHETTE_MAX_SIZE", "1MiB"),
            ("CACHETTE_MAX_OBJECT_SIZE", "1MiB"),
        ],
    );
    let mut wire = Wire::connect(daemon.addr);
    let first = wire.request("PUT", "/one", &[AUTH], &payload(800_000));
    assert_eq!(first.status, 201);
    let body = payload(8 * MIB);
    let length = body.len().to_string();
    wire.send_head("PUT", "/two", &[AUTH, ("Content-Length", &length)]);
    wire.send(&body).expect("the body is read and discarded");
    assert_eq!(wire.read_reply().expect("response").status, 413);
    let mut reader = Wire::connect(daemon.addr);
    assert_eq!(reader.request("GET", "/two", &[AUTH], b"").status, 404);
}

const GRADLE_ARTIFACT: &str = "application/vnd.gradle.build-cache-artifact.v1";
const NO_SPACE: &str = "cachette_puts_dropped_total{reason=\"no_space\"}";
const IO: &str = "cachette_puts_dropped_total{reason=\"io\"}";

/// How the client sends its body.
#[derive(Clone, Copy, Debug)]
enum Sending {
    /// The whole body goes out before any answer is read (Gradle's default).
    Eagerly,
    /// `Expect: 100-continue`, and the body is not sent.
    Expecting,
}

/// A PUT of `size` bytes, and its final answer. Eagerly, the connection must stay usable after it;
/// expecting, the answer must announce the close the unread body forces.
fn put_of(
    daemon: &Daemon,
    key: &str,
    size: usize,
    content_type: Option<&str>,
    sending: Sending,
) -> common::wire::Reply {
    let mut wire = Wire::connect(daemon.addr);
    let length = size.to_string();
    let mut headers = vec![AUTH, ("Content-Length", length.as_str())];
    if let Some(content_type) = content_type {
        headers.push(("Content-Type", content_type));
    }
    if matches!(sending, Sending::Expecting) {
        headers.push(("Expect", "100-continue"));
    }
    wire.send_head("PUT", key, &headers);
    if matches!(sending, Sending::Eagerly) {
        wire.send(&payload(size))
            .expect("the server must keep reading the body");
    }
    let reply = wire.read_reply().expect("response");
    match sending {
        Sending::Expecting => assert!(reply.closes(), "{:?}", reply.headers),
        Sending::Eagerly => {
            assert!(!reply.closes(), "{:?}", reply.headers);
            let miss = wire.request("GET", key, &[AUTH], b"");
            assert_eq!(miss.status, 404, "nothing was stored under {key}");
        }
    }
    reply
}

/// A daemon whose ceiling holds one 800 kB object, and the key that already filled it.
fn full_daemon() -> Daemon {
    let daemon = Daemon::start_with(
        &[],
        &[
            ("CACHETTE_MAX_SIZE", "1MiB"),
            ("CACHETTE_MAX_OBJECT_SIZE", "1MiB"),
        ],
    );
    let mut wire = Wire::connect(daemon.addr);
    let first = wire.request("PUT", "/one", &[AUTH], &payload(800_000));
    assert_eq!(first.status, 201, "the first object fits");
    daemon
}

#[test]
fn a_put_the_header_decision_drops_answers_413_and_gradle_gets_200() {
    for sending in [Sending::Eagerly, Sending::Expecting] {
        for (content_type, status) in [
            (None, 413),
            (Some("application/octet-stream"), 413),
            (Some(GRADLE_ARTIFACT), 200),
            (Some("APPLICATION/VND.GRADLE.BUILD-CACHE-ARTIFACT.V1"), 200),
            (Some("application/vnd.gradle.build-cache"), 413),
        ] {
            let context = format!("{content_type:?}, {sending:?}");
            let daemon = small_object_daemon();
            let reply = put_of(&daemon, "/big", 8 * MIB, content_type, sending);
            assert_eq!(reply.status, status, "too_large, {context}");
            assert_eq!(daemon.metric(TOO_LARGE), 1.0, "{context}");

            // Under max_object_size, over what the ceiling has left.
            let daemon = full_daemon();
            let reply = put_of(&daemon, "/two", 800_000, content_type, sending);
            assert_eq!(reply.status, status, "no_space, {context}");
            assert_eq!(daemon.metric(NO_SPACE), 1.0, "{context}");
            assert_eq!(daemon.metric(TOO_LARGE), 0.0, "{context}");
        }
    }
}

#[test]
fn a_put_that_fails_after_admission_answers_413_and_gradle_gets_200() {
    // The body is read before the failure is known, so `Expect` changes nothing here.
    for (content_type, status) in [(None, 413), (Some(GRADLE_ARTIFACT), 200)] {
        let daemon = Daemon::start(&[]);
        // The store creates shard directories on demand, so only a file in the way fails.
        let objects = daemon.data_dir.join("objects");
        fs::remove_dir_all(&objects).expect("remove the objects directory");
        fs::write(&objects, b"").expect("put a file where the directory was");
        let mut wire = Wire::connect(daemon.addr);
        let mut headers = vec![AUTH];
        if let Some(content_type) = content_type {
            headers.push(("Content-Type", content_type));
        }
        let reply = wire.request("PUT", "/lost", &headers, &payload(4096));
        assert_eq!(reply.status, status, "{content_type:?}");
        assert_eq!(daemon.metric(IO), 1.0, "{content_type:?}");
        assert_eq!(wire.request("GET", "/lost", &[AUTH], b"").status, 404);
    }
}

#[test]
fn a_stalled_body_is_cut_at_the_idle_timeout_and_stores_nothing() {
    let daemon = Daemon::start_with(&[], &[("CACHETTE_BODY_IDLE_TIMEOUT", "1s")]);
    let mut wire = Wire::connect(daemon.addr);
    let announced = (MIB).to_string();
    wire.send_head("PUT", "/stalled", &[AUTH, ("Content-Length", &announced)]);
    wire.send(&payload(100_000))
        .expect("first part of the body");
    let started = Instant::now();
    // Nothing more is sent. The server must give up on its own, with an answer or a close.
    let outcome = wire.read_reply();
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(800),
        "gave up before the idle timeout: {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(6),
        "the idle timeout did not cut the upload: {waited:?}"
    );
    if let Ok(reply) = outcome {
        assert_ne!(reply.status, 201, "a torn body must never be acknowledged");
    }

    let mut reader = Wire::connect(daemon.addr);
    assert_eq!(reader.request("GET", "/stalled", &[AUTH], b"").status, 404);
    assert_eq!(daemon.metric("cachette_written_bytes_total"), 0.0);
    assert_eq!(daemon.metric("cachette_entries"), 0.0);
    wait_for_no_temporaries(&daemon.data_dir.join("objects"));
}

#[test]
fn a_client_that_hangs_up_mid_body_stores_nothing() {
    let daemon = Daemon::start(&[]);
    {
        let mut wire = Wire::connect(daemon.addr);
        let announced = (MIB).to_string();
        wire.send_head("PUT", "/hangup", &[AUTH, ("Content-Length", &announced)]);
        wire.send(&payload(200_000))
            .expect("first part of the body");
    }
    let mut reader = Wire::connect(daemon.addr);
    assert_eq!(reader.request("GET", "/hangup", &[AUTH], b"").status, 404);
    wait_for_no_temporaries(&daemon.data_dir.join("objects"));
    assert_eq!(daemon.metric("cachette_entries"), 0.0);
}

fn wait_for_no_temporaries(objects: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let left: Vec<_> = fs::read_dir(objects)
            .expect("objects directory")
            .flatten()
            .filter_map(|shard| fs::read_dir(shard.path()).ok())
            .flatten()
            .flatten()
            .map(|entry| entry.file_name())
            .filter(|name| name.to_string_lossy().starts_with(".tmp-"))
            .collect();
        if left.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "temporary files left under {}: {left:?}",
            objects.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn sigterm_lets_a_put_in_flight_finish() {
    let mut daemon = Daemon::start_with(&[], &[("CACHETTE_SHUTDOWN_TIMEOUT", "20s")]);
    let body = payload(4 * MIB);
    let length = body.len().to_string();
    let mut wire = Wire::connect(daemon.addr);
    wire.send_head("PUT", "/inflight", &[AUTH, ("Content-Length", &length)]);
    wire.send(&body[..MIB]).expect("first part of the body");
    // Let the server take the first part, so the request is in flight when the signal comes.
    std::thread::sleep(Duration::from_millis(300));
    daemon.sigterm();
    std::thread::sleep(Duration::from_millis(300));
    wire.send(&body[MIB..]).expect("the rest of the body");
    let reply = wire.read_reply().expect("the PUT must be answered");
    assert_eq!(reply.status, 201, "the request in flight finishes");
    let status = daemon.wait(Duration::from_secs(10));
    assert!(status.success(), "{status:?}\n{}", daemon.logs());
    assert_eq!(
        stored_files(&daemon.data_dir.join("objects"), body.len()),
        1,
        "the object must be on disk after the clean exit"
    );
}

#[test]
fn sigterm_lets_a_get_in_flight_finish() {
    let mut daemon = Daemon::start_with(&[], &[("CACHETTE_SHUTDOWN_TIMEOUT", "20s")]);
    let body = payload(32 * MIB);
    let mut writer = Wire::connect(daemon.addr);
    assert_eq!(
        writer.request("PUT", "/download", &[AUTH], &body).status,
        201
    );

    let mut wire = Wire::connect(daemon.addr);
    wire.send_head("GET", "/download", &[AUTH]);
    // Take the head and a little of the body, then stop reading: the server is mid-stream.
    let head = wire.read_head_reply().expect("response head");
    assert_eq!(head.status, 200);
    assert_eq!(
        head.header("content-length"),
        Some(body.len().to_string().as_str())
    );
    let mut received = wire.read_some(64 * 1024).expect("start of the body");
    daemon.sigterm();
    std::thread::sleep(Duration::from_millis(500));
    while received.len() < body.len() {
        let chunk = wire
            .read_some(1 << 20)
            .expect("the download must continue after SIGTERM");
        received.extend_from_slice(&chunk);
    }
    assert_eq!(received.len(), body.len());
    assert!(received == body, "the download is not the stored object");
    let status = daemon.wait(Duration::from_secs(10));
    assert!(status.success(), "{status:?}\n{}", daemon.logs());
}

fn stored_files(objects: &Path, size: usize) -> usize {
    let Ok(buckets) = fs::read_dir(objects) else {
        return 0;
    };
    buckets
        .flatten()
        .filter_map(|bucket| fs::read_dir(bucket.path()).ok())
        .flat_map(Iterator::flatten)
        .filter(|file| {
            file.metadata()
                .is_ok_and(|meta| meta.len() == u64::try_from(size).unwrap_or(0))
        })
        .count()
}
