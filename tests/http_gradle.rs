//! Black-box tests of the Gradle protocol, authentication and `Expect: 100-continue` (D4 to D7,
//! D11), against the real binary.

mod common;

use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use common::Daemon;
use common::wire::Wire;

const ARTIFACT: &str = "application/vnd.gradle.build-cache-artifact.v2";

/// The secret of `common::TOKEN`.
const SECRET: &str = "secret";

fn basic(user: &str, password: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("{user}:{password}")))
}

/// A body that is not a repeated byte, so a truncated or shifted copy cannot compare equal.
fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from((i * 31 + i / 251) % 256).expect("below 256"))
        .collect()
}

fn key_for(name: &str) -> String {
    format!(
        "/{}",
        blake3::hash(name.as_bytes()).to_hex().as_str()[..32].to_owned()
    )
}

#[test]
fn replays_the_gradle_sequence() {
    let daemon = Daemon::start(&[]);
    let mut wire = Wire::connect(daemon.addr);
    let key = key_for("task");
    let body = payload(300_000);
    let auth = basic("gradle", SECRET);

    let miss = wire.request("GET", &key, &[("Authorization", &auth)], b"");
    assert_eq!(miss.status, 404, "an absent key is a miss");

    let stored = wire.request(
        "PUT",
        &key,
        &[("Authorization", &auth), ("Content-Type", ARTIFACT)],
        &body,
    );
    assert_eq!(stored.status, 201);

    let hit = wire.request(
        "GET",
        &key,
        &[
            ("Authorization", &auth),
            (
                "Accept",
                "application/vnd.gradle.build-cache-artifact.v2, */*",
            ),
        ],
        b"",
    );
    assert_eq!(hit.status, 200);
    assert_eq!(hit.body, body, "bytes differ from what was stored");
    assert_eq!(
        hit.header("content-length"),
        Some(body.len().to_string().as_str()),
        "opendal and Gradle need the exact length"
    );
    assert!(!hit.closes(), "the whole sequence reuses one connection");

    wire.send_head("HEAD", &key, &[("Authorization", &auth)]);
    let head = wire.read_head_reply().expect("HEAD response");
    assert_eq!(head.status, 200);
    assert_eq!(
        head.header("content-length"),
        Some(body.len().to_string().as_str())
    );
    let after = wire.request("GET", &key_for("other"), &[("Authorization", &auth)], b"");
    assert_eq!(after.status, 404, "a HEAD left the connection in sync");

    assert_eq!(daemon.metric("cachette_hits_total"), 1.0);
    assert_eq!(daemon.metric("cachette_misses_total"), 2.0);
    assert_eq!(daemon.metric("cachette_written_bytes_total"), 300_000.0);
    assert_eq!(
        daemon.metric("cachette_requests_total{method=\"PUT\",status=\"201\"}"),
        1.0
    );
}

#[test]
fn an_empty_object_is_stored() {
    let daemon = Daemon::start(&[]);
    let mut wire = Wire::connect(daemon.addr);
    let auth = basic("u", SECRET);
    let key = key_for("empty");
    assert_eq!(
        wire.request("PUT", &key, &[("Authorization", &auth)], b"")
            .status,
        201
    );
    let hit = wire.request("GET", &key, &[("Authorization", &auth)], b"");
    assert_eq!((hit.status, hit.body.len()), (200, 0));
    assert_eq!(hit.header("content-length"), Some("0"));
}

#[test]
fn expect_continue_with_a_token_gets_100_then_201() {
    let daemon = Daemon::start(&[]);
    let mut wire = Wire::connect(daemon.addr);
    let body = payload(200_000);
    let length = body.len().to_string();
    wire.send_head(
        "PUT",
        &key_for("expect"),
        &[
            ("Authorization", &basic("u", SECRET)),
            ("Content-Length", &length),
            ("Expect", "100-continue"),
        ],
    );
    let interim = wire.read_reply().expect("interim response");
    assert_eq!(interim.status, 100, "the server must invite the body");
    wire.send(&body).expect("send body");
    let done = wire.read_reply().expect("final response");
    assert_eq!(done.status, 201);
}

#[test]
fn expect_continue_without_a_token_gets_401_and_never_invites_the_body() {
    let daemon = Daemon::start(&[]);
    let mut wire = Wire::connect(daemon.addr);
    wire.send_head(
        "PUT",
        &key_for("expect"),
        &[("Content-Length", "50000000"), ("Expect", "100-continue")],
    );
    // The client sends no body and waits: the first thing it can read is the final answer.
    let started = Instant::now();
    let first = wire.read_reply().expect("response");
    assert_eq!(
        first.status, 401,
        "a 100 before the 401 means the body is read"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the refusal waited for the body: {:?}",
        started.elapsed()
    );
    assert!(
        first.closes(),
        "the unread body ends the connection: {:?}",
        first.headers
    );
    assert_eq!(
        first.headers_named("www-authenticate"),
        ["Basic realm=\"cachette\""]
    );
    assert_eq!(daemon.metric("cachette_auth_failures_total"), 1.0);
}

#[test]
fn expect_continue_with_a_read_token_gets_403_without_a_100() {
    let daemon = Daemon::start_with(&[], &[("CACHETTE_TOKENS", "ro:read:readsecret")]);
    let mut wire = Wire::connect(daemon.addr);
    wire.send_head(
        "PUT",
        &key_for("expect"),
        &[
            ("Authorization", &basic("u", "readsecret")),
            ("Content-Length", "50000000"),
            ("Expect", "100-continue"),
        ],
    );
    assert_eq!(wire.read_reply().expect("response").status, 403);
}

#[test]
fn every_form_of_the_secret_is_accepted() {
    // `?>?>?>` encodes with `+` and `/` in the standard alphabet and `-` and `_` in the URL one.
    let secret = "ab?>?>?>z";
    let daemon = Daemon::start_with(&[], &[("CACHETTE_TOKENS", &format!("ci:write:{secret}"))]);
    let credentials = format!("name:{secret}");
    let standard = STANDARD.encode(&credentials);
    let url_padded = URL_SAFE.encode(&credentials);
    let url_bare = URL_SAFE_NO_PAD.encode(&credentials);
    assert!(
        url_bare.contains(['-', '_']) && url_padded != url_bare,
        "the fixture must differ between alphabets and paddings: {url_padded}"
    );
    let forms = [
        format!("Basic {standard}"),
        format!("Basic {url_padded}"),
        format!("Basic {url_bare}"),
        format!("Basic {}", URL_SAFE_NO_PAD.encode(format!(":{secret}"))),
        format!("Bearer {secret}"),
        format!("bearer {secret}"),
    ];
    for (index, authorization) in forms.iter().enumerate() {
        let key = key_for(&format!("form-{index}"));
        let put = daemon.request(
            "PUT",
            &key,
            &[("Authorization", authorization), ("Content-Length", "0")],
        );
        assert_eq!(put.status, 201, "{authorization}");
        let get = daemon.request("GET", &key, &[("Authorization", authorization)]);
        assert_eq!(get.status, 200, "{authorization}");
    }
}

#[test]
fn unknown_credentials_are_401_with_the_right_challenge() {
    let daemon = Daemon::start(&[]);
    let key = key_for("auth");
    let none = daemon.request("GET", &key, &[]);
    assert_eq!(none.status, 401);
    assert_eq!(
        none.headers.matches("www-authenticate: basic").count(),
        1,
        "{}",
        none.headers
    );
    assert!(!none.headers.contains("bearer"), "{}", none.headers);

    let wrong_basic = daemon.request("GET", &key, &[("Authorization", &basic("u", "wrong"))]);
    assert_eq!(wrong_basic.status, 401);
    assert!(!wrong_basic.headers.contains("bearer"));

    let wrong_bearer = daemon.request("GET", &key, &[("Authorization", "Bearer wrong")]);
    assert_eq!(wrong_bearer.status, 401);
    assert!(
        wrong_bearer.headers.contains("www-authenticate: basic"),
        "{}",
        wrong_bearer.headers
    );
    assert!(
        wrong_bearer
            .headers
            .contains("www-authenticate: bearer error=\"invalid_token\""),
        "{}",
        wrong_bearer.headers
    );

    for garbage in ["Basic", "Basic !!!!", "Digest abc", "Basic bm9jb2xvbg=="] {
        let response = daemon.request("GET", &key, &[("Authorization", garbage)]);
        assert_eq!(response.status, 401, "{garbage}");
    }
    assert_eq!(daemon.metric("cachette_auth_failures_total"), 7.0);
}

#[test]
fn a_read_token_reads_but_cannot_write() {
    let daemon = Daemon::start_with(
        &[],
        &[(
            "CACHETTE_TOKENS",
            "ci:write:writesecret,dev:read:readsecret",
        )],
    );
    let key = key_for("rw");
    let writer = format!("Bearer {}", "writesecret");
    let reader = "Bearer readsecret";
    assert_eq!(
        daemon
            .request(
                "PUT",
                &key,
                &[("Authorization", &writer), ("Content-Length", "0")]
            )
            .status,
        201
    );
    assert_eq!(
        daemon
            .request("GET", &key, &[("Authorization", reader)])
            .status,
        200
    );
    let refused = daemon.request(
        "PUT",
        &key,
        &[("Authorization", reader), ("Content-Length", "0")],
    );
    assert_eq!(refused.status, 403);
    assert!(
        !refused.headers.contains("www-authenticate"),
        "a valid token that may not write is not asked to log in again"
    );
    // MKCOL writes too.
    assert_eq!(
        daemon
            .request("MKCOL", "/dir/", &[("Authorization", reader)])
            .status,
        403
    );
}

#[test]
fn anonymous_read_opens_reads_only() {
    let daemon = Daemon::start_with(&["--anonymous", "read"], &[]);
    let key = key_for("anon");
    let auth = basic("u", SECRET);
    let put = |authorization: Option<&str>| {
        let mut headers = vec![("Content-Length", "3")];
        if let Some(value) = authorization {
            headers.push(("Authorization", value));
        }
        let mut wire = Wire::connect(daemon.addr);
        wire.request("PUT", &key, &headers, b"abc").status
    };
    assert_eq!(put(None), 401, "no anonymous write");
    assert_eq!(put(Some(&basic("u", "wrong"))), 401);
    assert_eq!(put(Some(&auth)), 201);
    assert_eq!(
        daemon.request("GET", &key, &[]).status,
        200,
        "anonymous read"
    );
    assert_eq!(daemon.request("HEAD", &key, &[]).status, 200);
    assert_eq!(
        daemon
            .request("GET", &key, &[("Authorization", &basic("u", "wrong"))])
            .status,
        401,
        "wrong credentials are refused even where reads are open"
    );
}

#[test]
fn a_chunked_put_is_411() {
    let daemon = Daemon::start(&[]);
    let mut wire = Wire::connect(daemon.addr);
    wire.send_head(
        "PUT",
        &key_for("chunked"),
        &[
            ("Authorization", &basic("u", SECRET)),
            ("Transfer-Encoding", "chunked"),
        ],
    );
    wire.send(b"5\r\nhello\r\n0\r\n\r\n").ok();
    let reply = wire.read_reply().expect("response");
    assert_eq!(reply.status, 411);
    assert_eq!(daemon.get(&key_for("chunked")).status, 401);
}

#[test]
fn a_chunked_put_that_also_has_a_length_is_not_stored() {
    let daemon = Daemon::start(&[]);
    let mut wire = Wire::connect(daemon.addr);
    wire.send_head(
        "PUT",
        &key_for("both"),
        &[
            ("Authorization", &basic("u", SECRET)),
            ("Transfer-Encoding", "chunked"),
            ("Content-Length", "5"),
        ],
    );
    wire.send(b"5\r\nhello\r\n0\r\n\r\n").ok();
    let status = wire.read_reply().map(|reply| reply.status);
    assert!(
        matches!(status, Ok(400 | 411)) || status.is_err(),
        "a body of unknown length was accepted: {status:?}"
    );
    let mut reader = Wire::connect(daemon.addr);
    let auth = basic("u", SECRET);
    assert_eq!(
        reader
            .request("GET", &key_for("both"), &[("Authorization", &auth)], b"")
            .status,
        404
    );
}

#[test]
fn a_put_without_any_framing_is_411() {
    let daemon = Daemon::start(&[]);
    let response = daemon.request(
        "PUT",
        &key_for("unframed"),
        &[("Authorization", &basic("u", SECRET))],
    );
    assert_eq!(response.status, 411);
}

#[test]
fn unsupported_methods_are_405() {
    let daemon = Daemon::start(&[]);
    for method in [
        "DELETE",
        "MOVE",
        "COPY",
        "LOCK",
        "UNLOCK",
        "PROPPATCH",
        "POST",
        "PATCH",
    ] {
        let response = daemon.request(method, &key_for("x"), &[("Content-Length", "0")]);
        assert_eq!(response.status, 405, "{method}");
        assert!(
            response.headers.contains("allow:"),
            "{method}: {}",
            response.headers
        );
    }
}

#[test]
fn secrets_never_reach_the_logs() {
    let secret = "tok-9f3a7c1e5b42d";
    let wrong = "wrong-0a8b6c4d2e1f9";
    let daemon = Daemon::start_with(
        &["--access-log", "all"],
        &[
            ("CACHETTE_TOKENS", &format!("ci:write:{secret}")),
            ("CACHETTE_LOG_FORMAT", "json"),
        ],
    );
    let key = key_for("logs");
    let encoded = [
        STANDARD.encode(format!("user:{secret}")),
        URL_SAFE_NO_PAD.encode(format!("user:{secret}")),
        STANDARD.encode(format!("user:{wrong}")),
        STANDARD.encode(secret),
    ];
    let mut forms: Vec<String> = encoded
        .iter()
        .map(|value| format!("Basic {value}"))
        .collect();
    forms.push(format!("Bearer {secret}"));
    forms.push(format!("Bearer {wrong}"));
    for authorization in &forms {
        daemon.request("GET", &key, &[("Authorization", authorization)]);
    }
    let stored = Wire::connect(daemon.addr).request(
        "PUT",
        &key,
        &[("Authorization", &format!("Bearer {secret}"))],
        b"x",
    );
    assert_eq!(stored.status, 201);
    // The token name must be there (the capture works), and no secret in any form.
    let deadline = Instant::now() + Duration::from_secs(5);
    let logs = loop {
        let logs = daemon.logs();
        if logs.matches("\"token\":\"ci\"").count() >= 4 {
            break logs;
        }
        assert!(
            Instant::now() < deadline,
            "access log never showed the token name:\n{logs}"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    for forbidden in [secret, wrong]
        .into_iter()
        .map(str::to_owned)
        .chain(encoded.iter().cloned())
    {
        assert!(
            !logs.contains(&forbidden),
            "{forbidden} leaked into:\n{logs}"
        );
    }
    assert!(
        logs.contains("\"status\":401"),
        "refusals are logged:\n{logs}"
    );
}

/// The access lines of `logs`, one per request.
fn access_lines(logs: &str) -> Vec<&str> {
    logs.lines()
        .filter(|line| line.contains("\"message\":\"request\""))
        .collect()
}

#[test]
fn the_default_access_log_keeps_errors_and_drops_but_not_hits_or_misses() {
    let daemon = Daemon::start_with(
        &[],
        &[
            ("CACHETTE_LOG_FORMAT", "json"),
            ("CACHETTE_MAX_OBJECT_SIZE", "1KiB"),
        ],
    );
    let auth = basic("u", SECRET);
    let mut wire = Wire::connect(daemon.addr);
    assert_eq!(
        wire.request("PUT", "/k", &[("Authorization", &auth)], b"abc")
            .status,
        201
    );
    assert_eq!(
        wire.request("GET", "/k", &[("Authorization", &auth)], b"")
            .status,
        200
    );
    assert_eq!(
        wire.request("GET", "/absent", &[("Authorization", &auth)], b"")
            .status,
        404
    );
    assert_eq!(wire.request("GET", "/k", &[], b"").status, 401);
    let big = vec![0_u8; 4096];
    let dropped = wire.request("PUT", "/big", &[("Authorization", &auth)], &big);
    assert_eq!(dropped.status, 200);
    // The line for the last request is written after its response.
    let deadline = Instant::now() + Duration::from_secs(5);
    let lines = loop {
        let logs = daemon.logs();
        let lines: Vec<String> = access_lines(&logs).into_iter().map(str::to_owned).collect();
        if lines.len() >= 2 || Instant::now() > deadline {
            break lines;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(
        lines.len(),
        2,
        "only the refusal and the dropped PUT: {lines:#?}"
    );
    assert!(lines[0].contains("\"status\":401"), "{}", lines[0]);
    assert!(
        lines[1].contains("\"status\":200") && lines[1].contains("\"note\":\"too_large\""),
        "{}",
        lines[1]
    );
    assert!(lines[1].contains("\"level\":\"WARN\""), "{}", lines[1]);
}
