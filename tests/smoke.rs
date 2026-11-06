//! Black-box smoke tests: the real binary, on port 0, driven over HTTP and signals.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Daemon, parse_response, run_to_exit};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream as AsyncTcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

#[test]
fn serves_healthz() {
    let daemon = Daemon::start(&[]);
    let response = daemon.get("/healthz");
    assert_eq!(response.status, 200, "stderr:\n{}", daemon.logs());
    assert_eq!(daemon.scheme, "http");
}

#[test]
fn scrape_exposes_prometheus_text() {
    let daemon = Daemon::start(&["--max-size", "64MiB"]);
    let response = daemon.get("/metrics");
    assert_eq!(response.status, 200);
    assert!(
        response
            .headers
            .contains("content-type: text/plain; version=0.0.4"),
        "{}",
        response.headers
    );
    assert!(
        response.body.contains("cachette_size_limit_bytes 67108864"),
        "{}",
        response.body
    );
    for name in [
        "cachette_size_bytes 0",
        "cachette_entries 0",
        "cachette_oldest_entry_age_seconds",
        "cachette_hits_total 0",
        "cachette_crash_purges_total 0",
        "cachette_misses_total 0",
        "cachette_read_bytes_total 0",
        "cachette_written_bytes_total 0",
        "cachette_auth_failures_total 0",
        "cachette_evictions_total 0",
        "cachette_evicted_bytes_total 0",
        "cachette_puts_dropped_total{reason=\"too_large\"} 0",
        "cachette_puts_dropped_total{reason=\"no_space\"} 0",
        "cachette_puts_dropped_total{reason=\"not_ready\"} 0",
        "cachette_puts_dropped_total{reason=\"io\"} 0",
    ] {
        assert!(
            response.body.contains(name),
            "{name} missing from:\n{}",
            response.body
        );
    }
}

#[test]
fn metrics_token_guards_the_scrape() {
    let daemon = Daemon::start(&["--metrics-token", "scrape-me"]);
    let refused = daemon.get("/metrics");
    assert_eq!(refused.status, 401);
    assert!(
        refused.headers.contains("www-authenticate: bearer"),
        "{}",
        refused.headers
    );
    let wrong = daemon.request("GET", "/metrics", &[("Authorization", "Bearer nope")]);
    assert_eq!(wrong.status, 401);
    let allowed = daemon.request("GET", "/metrics", &[("Authorization", "Bearer scrape-me")]);
    assert_eq!(allowed.status, 200);
    assert!(allowed.body.contains("cachette_size_limit_bytes"));
    assert_eq!(daemon.get("/healthz").status, 200, "healthz stays open");
}

#[test]
fn reserved_paths_are_not_keys() {
    let daemon = Daemon::start(&[]);
    assert_eq!(daemon.request("PUT", "/healthz", &[]).status, 405);
    assert_eq!(daemon.request("PUT", "/metrics", &[]).status, 405);
}

#[test]
fn invalid_configuration_stops_startup_and_names_the_key() {
    let missing = run_to_exit(&[], &[("CACHETTE_MAX_SIZE", "")]);
    assert!(!missing.status.success());
    let stderr = String::from_utf8_lossy(&missing.stderr);
    assert!(stderr.contains("max_size"), "{stderr}");

    let oversized = run_to_exit(&["--max-object-size", "128MiB"], &[]);
    assert!(!oversized.status.success());
    assert!(String::from_utf8_lossy(&oversized.stderr).contains("max_object_size"));

    let nobody = run_to_exit(&[], &[("CACHETTE_TOKENS", "")]);
    assert!(!nobody.status.success());
    assert!(String::from_utf8_lossy(&nobody.stderr).contains("tokens"));

    let twins = run_to_exit(&[], &[("CACHETTE_TOKENS", "a:read:x,a:write:y")]);
    assert!(!twins.status.success());
    assert!(String::from_utf8_lossy(&twins.stderr).contains("duplicate name `a`"));

    // A malformed token must never be repeated in the error, from the flag or the environment.
    let flag = run_to_exit(&["--token", "ci:bogus:hunter2"], &[("CACHETTE_TOKENS", "")]);
    assert!(!flag.status.success());
    let stderr = String::from_utf8_lossy(&flag.stderr);
    assert!(
        stderr.contains("tokens") && !stderr.contains("hunter2"),
        "{stderr}"
    );
    let env = run_to_exit(&[], &[("CACHETTE_TOKENS", "ci:bogus:envsecret")]);
    assert!(!env.status.success());
    let stderr = String::from_utf8_lossy(&env.stderr);
    assert!(
        stderr.contains("tokens") && !stderr.contains("envsecret"),
        "{stderr}"
    );

    let garbled = run_to_exit(&["--max-size", "lots"], &[]);
    assert!(!garbled.status.success());
    assert!(String::from_utf8_lossy(&garbled.stderr).contains("max-size"));
}

#[test]
fn file_then_environment_then_flag() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("cachette.toml");
    // 192.0.2.1 (TEST-NET-1) is not a local address, so binding it always fails, as any user:
    // a layer that fails to override it makes the start fail.
    std::fs::write(&file, "listen = \"192.0.2.1:1\"\nmax_size = \"8MiB\"\n").unwrap();
    let file = file.to_str().unwrap();

    let from_file = run_to_exit(
        &["--config", file],
        &[("CACHETTE_LISTEN", ""), ("CACHETTE_MAX_SIZE", "")],
    );
    assert!(
        !from_file.status.success(),
        "the file's listen address was not used"
    );
    assert!(String::from_utf8_lossy(&from_file.stderr).contains("192.0.2.1:1"));

    let daemon = Daemon::start_with(&["--config", file], &[("CACHETTE_LISTEN", "127.0.0.1:0")]);
    assert_eq!(
        daemon.get("/healthz").status,
        200,
        "environment must beat file"
    );
    let size = daemon.get("/metrics").body;
    assert!(
        size.contains("cachette_size_limit_bytes 67108864"),
        "environment max_size must beat file:\n{size}"
    );

    let daemon = Daemon::start_with(
        &["--config", file, "--listen", "127.0.0.1:0"],
        &[("CACHETTE_LISTEN", "192.0.2.1:1")],
    );
    assert_eq!(
        daemon.get("/healthz").status,
        200,
        "flag must beat environment"
    );
}

#[test]
fn a_second_process_on_the_same_directory_refuses_to_start() {
    let first = Daemon::start(&[]);
    let refused = Daemon::start_refused_on(&first, &[]);
    assert!(refused.contains("in use by another process"), "{refused}");
    assert_eq!(first.get("/healthz").status, 200);
}

#[test]
fn sigterm_stops_cleanly_even_with_an_idle_keep_alive_connection() {
    let mut daemon = Daemon::start(&["--shutdown-timeout", "30s"]);
    let mut idle = TcpStream::connect(daemon.addr).unwrap();
    idle.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    idle.write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();
    let mut first = [0_u8; 12];
    idle.read_exact(&mut first).unwrap();
    assert_eq!(
        &first, b"HTTP/1.1 200",
        "keep-alive connection must be established"
    );

    let started = Instant::now();
    let status = daemon.terminate(Duration::from_secs(10));
    assert!(
        status.success(),
        "exit status {status:?}; stderr:\n{}",
        daemon.logs()
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "waited for the idle connection: {:?}",
        started.elapsed()
    );
    assert!(
        daemon.logs().contains("SIGTERM received"),
        "{}",
        daemon.logs()
    );
    assert!(
        TcpStream::connect(daemon.addr).is_err(),
        "still listening after exit"
    );
}

#[test]
fn oversized_headers_are_refused() {
    let daemon = Daemon::start(&[]);
    let small = "a".repeat(2 * 1024);
    assert_eq!(
        daemon
            .request("GET", "/healthz", &[("X-Pad", &small)])
            .status,
        200
    );
    let big = "a".repeat(64 * 1024);
    let mut stream = TcpStream::connect(daemon.addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    // The server may answer and close before the whole request is written: ignore that error.
    let _ = stream
        .write_all(format!("GET /healthz HTTP/1.1\r\nHost: x\r\nX-Pad: {big}\r\n\r\n").as_bytes());
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    if !raw.is_empty() {
        let status = parse_response(&raw).expect("an HTTP response").status;
        assert!(
            status == 431 || status == 400,
            "oversized headers got {status}"
        );
    }
}

#[test]
fn slow_headers_are_cut_off() {
    let daemon = Daemon::start(&["--header-read-timeout", "1s"]);
    let mut stream = TcpStream::connect(daemon.addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(8)))
        .unwrap();
    stream.write_all(b"GET /healthz HTTP/1.1\r\nHost:").unwrap();
    let started = Instant::now();
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "connection kept open for {:?}",
        started.elapsed()
    );
    assert!(
        !String::from_utf8_lossy(&raw).starts_with("HTTP/1.1 200"),
        "answered a request that never completed"
    );
}

#[tokio::test]
async fn serves_over_tls_when_configured() {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = (dir.path().join("cert.pem"), dir.path().join("key.pem"));
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();

    let missing = run_to_exit(
        &[
            "--tls-cert",
            cert.to_str().unwrap(),
            "--tls-key",
            "/nonexistent/key.pem",
        ],
        &[],
    );
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("/nonexistent/key.pem"));

    let daemon = Daemon::start(&[
        "--tls-cert",
        cert.to_str().unwrap(),
        "--tls-key",
        key.to_str().unwrap(),
    ]);
    assert_eq!(daemon.scheme, "https");

    let mut roots = RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let tcp = AsyncTcpStream::connect(daemon.addr).await.unwrap();
    let mut tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    tls.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut raw = Vec::new();
    // The server closes without a TLS close_notify; the response is complete by then.
    let _ = tls.read_to_end(&mut raw).await;
    assert_eq!(parse_response(&raw).expect("an HTTP response").status, 200);

    assert!(
        common::try_request(daemon.addr, "GET", "/healthz", &[]).map_or(true, |r| r.status != 200),
        "plain HTTP must not be answered on a TLS port"
    );
}

#[test]
fn runtime_and_log_settings_are_applied() {
    let daemon = Daemon::start(&["--worker-threads", "2", "--access-log", "off"]);
    assert_eq!(daemon.get("/healthz").status, 200);
    let logs = daemon.logs();
    assert!(logs.contains("worker_threads=2"), "{logs}");
    assert!(logs.contains("access_log=Off"), "{logs}");
    assert!(
        logs.contains(if cfg!(target_os = "linux") {
            "durability=PurgeOnCrash"
        } else {
            "durability=Fsync"
        }),
        "default durability is platform dependent: {logs}"
    );
}

#[test]
fn purge_on_crash_needs_syncfs() {
    if cfg!(target_os = "linux") {
        let daemon = Daemon::start(&["--durability", "purge-on-crash"]);
        assert_eq!(daemon.get("/healthz").status, 200);
    } else {
        let refused = run_to_exit(&["--durability", "purge-on-crash"], &[]);
        assert!(!refused.status.success());
        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert!(
            stderr.contains("durability") && stderr.contains("syncfs"),
            "{stderr}"
        );
    }
}
