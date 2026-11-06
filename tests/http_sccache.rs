//! Black-box tests of the `WebDAV` subset sccache needs (D8) and of key handling (D9, D10). The
//! sequence is the one opendal 0.58.1 sends for sccache 0.19 (`sccache-webdav.md` of the chantier).

mod common;

use std::time::{Duration, SystemTime};

use common::Daemon;
use common::wire::Reply;
use common::wire::Wire;

const BEARER: (&str, &str) = ("Authorization", "Bearer secret");
/// The body opendal sends with every `PROPFIND` (91 bytes).
const PROPFIND_BODY: &[u8] =
    br#"<?xml version="1.0" encoding="utf-8" ?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>"#;

/// A `PROPFIND` as opendal sends it. The body follows the head in its own segment after a pause,
/// as it does when a client writes them separately: the server must still read it, or it closes
/// the connection the next request needs.
fn propfind(wire: &mut Wire, path: &str) -> Reply {
    let length = PROPFIND_BODY.len().to_string();
    wire.send_head(
        "PROPFIND",
        path,
        &[
            BEARER,
            ("Depth", "0"),
            ("Content-Type", "application/xml"),
            ("Content-Length", &length),
        ],
    );
    std::thread::sleep(Duration::from_millis(30));
    wire.send(PROPFIND_BODY).expect("send PROPFIND body");
    wire.read_reply().expect("PROPFIND response")
}

/// Asserts the reply is the minimal collection answer opendal accepts.
fn assert_collection(reply: &Reply, href: &str) {
    assert_eq!(reply.status, 207, "{}", reply.text());
    let xml = reply.text();
    assert!(xml.contains(&format!("<D:href>{href}</D:href>")), "{xml}");
    assert!(xml.contains("<D:collection/>"), "{xml}");
    assert!(xml.contains("HTTP/1.1 200 OK"), "{xml}");
    let modified = between(&xml, "<D:getlastmodified>", "</D:getlastmodified>");
    let parsed = httpdate::parse_http_date(modified).expect("getlastmodified is an HTTP date");
    let age = SystemTime::now().duration_since(parsed).unwrap_or_default();
    assert!(age < Duration::from_secs(3600), "{modified}");
    assert!(modified.ends_with(" GMT"), "{modified}");
    assert!(!xml.contains("getcontentlength"), "{xml}");
}

fn between<'a>(text: &'a str, start: &str, end: &str) -> &'a str {
    let from = text
        .find(start)
        .unwrap_or_else(|| panic!("no {start} in {text}"))
        + start.len();
    let to = text[from..]
        .find(end)
        .unwrap_or_else(|| panic!("no {end} in {text}"))
        + from;
    &text[from..to]
}

#[test]
fn replays_the_opendal_sequence_on_one_connection() {
    let daemon = Daemon::start(&[]);
    let mut wire = Wire::connect(daemon.addr);

    // Start-up check: read, then write.
    let check = wire.request("GET", "/dav/pre/.sccache_check", &[BEARER], b"");
    assert_eq!(check.status, 404, "the check file is a miss");
    assert_collection(&propfind(&mut wire, "/dav/pre/"), "/dav/pre/");
    assert_eq!(
        wire.request("MKCOL", "/dav/pre/", &[BEARER], b"").status,
        201
    );
    let hello = wire.request(
        "PUT",
        "/dav/pre/.sccache_check",
        &[BEARER],
        b"Hello, World!",
    );
    assert_eq!(hello.status, 201);

    // A put: PROPFIND on the directory of the key, then the PUT.
    assert_collection(&propfind(&mut wire, "/dav/pre/a/b/c/"), "/dav/pre/a/b/c/");
    let object = vec![7_u8; 70_000];
    let put = wire.request("PUT", "/dav/pre/a/b/c/abcdef", &[BEARER], &object);
    assert_eq!(put.status, 201);

    // A get: one GET, no Range, exact Content-Length.
    let get = wire.request("GET", "/dav/pre/a/b/c/abcdef", &[BEARER], b"");
    assert_eq!(get.status, 200);
    assert_eq!(get.body, object);
    assert_eq!(get.header("content-length"), Some("70000"));
    assert!(!get.closes());

    // PROPFIND on the stored key: a file with its length and no collection.
    let stat = propfind(&mut wire, "/dav/pre/a/b/c/abcdef");
    assert_eq!(stat.status, 207);
    let xml = stat.text();
    assert!(
        xml.contains("<D:getcontentlength>70000</D:getcontentlength>"),
        "{xml}"
    );
    assert!(!xml.contains("<D:collection/>"), "{xml}");
    assert!(xml.contains("<D:resourcetype/>"), "{xml}");
    httpdate::parse_http_date(between(&xml, "<D:getlastmodified>", "</D:getlastmodified>"))
        .expect("getlastmodified is an HTTP date");

    // A trailing `/` on the endpoint makes opendal send `//`: same keys.
    let slashed = wire.request("PUT", "/dav//pre//a/b/c/other", &[BEARER], b"xyz");
    assert_eq!(slashed.status, 201);
    let read = wire.request("GET", "/dav/pre/a/b/c/other", &[BEARER], b"");
    assert_eq!(
        (read.status, read.body.as_slice()),
        (200, b"xyz".as_slice())
    );
    assert_collection(&propfind(&mut wire, "/dav//pre//"), "/dav//pre//");
}

#[test]
fn the_root_and_missing_paths_are_collections() {
    let daemon = Daemon::start(&[]);
    let mut wire = Wire::connect(daemon.addr);
    assert_collection(&propfind(&mut wire, "/"), "/");
    assert_collection(&propfind(&mut wire, "/never/written"), "/never/written/");
    assert_eq!(wire.request("MKCOL", "/", &[BEARER], b"").status, 201);
    assert_eq!(wire.request("MKCOL", "/x/y/", &[BEARER], b"").status, 201);
    // MKCOL stored nothing.
    assert_eq!(wire.request("GET", "/x/y", &[BEARER], b"").status, 404);
}

#[test]
fn depth_is_zero_or_one_and_infinity_is_refused() {
    let daemon = Daemon::start(&[]);
    for (depth, status) in [
        ("0", 207),
        ("1", 207),
        ("infinity", 403),
        ("Infinity", 403),
        ("2", 400),
    ] {
        let response = daemon.request("PROPFIND", "/a/", &[BEARER, ("Depth", depth)]);
        assert_eq!(response.status, status, "Depth: {depth}");
    }
    let absent = daemon.request("PROPFIND", "/a/", &[BEARER]);
    assert_eq!(absent.status, 207, "no Depth header is served as depth 0");
    assert_eq!(
        daemon.request("PROPFIND", "/a/", &[("Depth", "0")]).status,
        401,
        "PROPFIND needs credentials"
    );
}

#[test]
fn options_announces_webdav() {
    let daemon = Daemon::start(&[]);
    let response = daemon.request("OPTIONS", "/anything", &[BEARER]);
    assert_eq!(response.status, 200);
    assert!(response.headers.contains("dav: 1"), "{}", response.headers);
    for method in ["get", "head", "put", "propfind", "mkcol", "options"] {
        assert!(
            response.headers.contains(method),
            "{method} missing from Allow: {}",
            response.headers
        );
    }
    for method in ["DELETE", "MOVE", "COPY", "LOCK", "PROPPATCH"] {
        let refused = daemon.request(method, "/a/b", &[BEARER, ("Content-Length", "0")]);
        assert_eq!(refused.status, 405, "{method}");
    }
}

#[test]
fn refused_keys_are_400_for_every_method() {
    let daemon = Daemon::start(&[]);
    let too_long = format!("/{}", "a".repeat(1025));
    let too_deep = format!("/{}", "a/".repeat(33));
    let refused = [
        "/a/../b",
        "/a/./b",
        "/a/%2e%2e/b",
        "/%2E/b",
        "/a%00b",
        "/a%1fb",
        "/a%7fb",
        "/%ff",
        "/",
        "//",
        too_long.as_str(),
        too_deep.as_str(),
    ];
    for path in refused {
        let shown: String = path.chars().take(40).collect();
        for method in ["GET", "HEAD"] {
            let response = daemon.request(method, path, &[BEARER]);
            assert_eq!(response.status, 400, "{method} {shown}");
        }
        let mut wire = Wire::connect(daemon.addr);
        let put = wire.request("PUT", path, &[BEARER], b"abc");
        assert_eq!(put.status, 400, "PUT {shown}");
    }
    // The limits are inclusive.
    let longest = format!("/{}", "a".repeat(1024));
    let deepest = format!("/{}", "a/".repeat(32));
    for path in [longest, deepest] {
        let mut wire = Wire::connect(daemon.addr);
        assert_eq!(wire.request("PUT", &path, &[BEARER], b"ok").status, 201);
    }
    // `%252e` is the literal text `%2e`, decoded once: a normal key.
    let mut wire = Wire::connect(daemon.addr);
    assert_eq!(
        wire.request("PUT", "/%252e%252e/k", &[BEARER], b"ok")
            .status,
        201
    );
}

#[test]
fn a_refused_key_does_not_wait_for_the_body() {
    let daemon = Daemon::start(&[]);
    let mut wire = Wire::connect(daemon.addr);
    wire.send_head(
        "PUT",
        "/a/../b",
        &[
            BEARER,
            ("Content-Length", "50000000"),
            ("Expect", "100-continue"),
        ],
    );
    assert_eq!(wire.read_reply().expect("response").status, 400);
}

#[test]
fn reserved_paths_cannot_be_written() {
    let daemon = Daemon::start(&[]);
    for path in ["/healthz", "/metrics", "//healthz", "/metrics/"] {
        let mut wire = Wire::connect(daemon.addr);
        let response = wire.request("PUT", path, &[BEARER], b"overwrite");
        assert_eq!(response.status, 405, "PUT {path}");
        let mkcol = daemon.request("MKCOL", path, &[BEARER]);
        assert_ne!(mkcol.status, 201, "MKCOL {path}");
    }
    assert_eq!(daemon.get("/healthz").status, 200, "healthz still answers");
    assert!(
        daemon.get("/metrics").body.contains("cachette_"),
        "the scrape still answers"
    );
}
