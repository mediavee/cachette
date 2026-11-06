//! Prometheus metrics: every name in one place, and the functions the other modules call.
//!
//! `store` records the bytes it writes and the entries it evicts; `http` records requests, hits,
//! misses, bytes read, dropped PUTs and authentication failures; `server` publishes the gauges
//! just before each scrape ([`set_store_gauges`]).

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the store and http lots call the remaining recorders"
    )
)]

use std::time::Duration;

use axum::http::Method;
use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder, PrometheusHandle};

use crate::store::{DropReason, Stats};

const REQUESTS_TOTAL: &str = "cachette_requests_total";
const REQUEST_DURATION_SECONDS: &str = "cachette_request_duration_seconds";
const HITS_TOTAL: &str = "cachette_hits_total";
const MISSES_TOTAL: &str = "cachette_misses_total";
const READ_BYTES_TOTAL: &str = "cachette_read_bytes_total";
const WRITTEN_BYTES_TOTAL: &str = "cachette_written_bytes_total";
const PUTS_DROPPED_TOTAL: &str = "cachette_puts_dropped_total";
const AUTH_FAILURES_TOTAL: &str = "cachette_auth_failures_total";
const EVICTIONS_TOTAL: &str = "cachette_evictions_total";
const EVICTED_BYTES_TOTAL: &str = "cachette_evicted_bytes_total";
const SIZE_BYTES: &str = "cachette_size_bytes";
const SIZE_LIMIT_BYTES: &str = "cachette_size_limit_bytes";
const ENTRIES: &str = "cachette_entries";
const OLDEST_ENTRY_AGE_SECONDS: &str = "cachette_oldest_entry_age_seconds";

/// Request duration buckets in seconds: from a cached 4 KiB hit to a 32 MiB upload.
const DURATION_BUCKETS: [f64; 12] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
];

/// Installs the process-wide recorder and returns the handle that renders the scrape output.
///
/// Every unlabelled counter, and every `puts_dropped_total` reason, is published at zero from the
/// start so dashboards and alerts see the series before the first event.
///
/// # Errors
///
/// Fails when the histogram buckets are rejected or a recorder is already installed.
pub(crate) fn install() -> Result<PrometheusHandle, BuildError> {
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(REQUEST_DURATION_SECONDS.to_owned()),
            &DURATION_BUCKETS,
        )?
        .install_recorder()?;
    describe();
    for name in [
        HITS_TOTAL,
        MISSES_TOTAL,
        READ_BYTES_TOTAL,
        WRITTEN_BYTES_TOTAL,
        AUTH_FAILURES_TOTAL,
        EVICTIONS_TOTAL,
        EVICTED_BYTES_TOTAL,
    ] {
        counter!(name).absolute(0);
    }
    for reason in DropReason::ALL {
        counter!(PUTS_DROPPED_TOTAL, "reason" => reason.as_str()).absolute(0);
    }
    Ok(handle)
}

fn describe() {
    describe_counter!(REQUESTS_TOTAL, "Requests served, by method and status code");
    describe_histogram!(
        REQUEST_DURATION_SECONDS,
        Unit::Seconds,
        "Request duration, by method"
    );
    describe_counter!(HITS_TOTAL, "Reads that found the object");
    describe_counter!(MISSES_TOTAL, "Reads that did not find the object");
    describe_counter!(
        READ_BYTES_TOTAL,
        Unit::Bytes,
        "Object bytes sent to clients"
    );
    describe_counter!(WRITTEN_BYTES_TOTAL, Unit::Bytes, "Object bytes stored");
    describe_counter!(
        PUTS_DROPPED_TOTAL,
        "PUTs answered 200 but not stored, by reason"
    );
    describe_counter!(
        AUTH_FAILURES_TOTAL,
        "Requests refused for missing or invalid credentials"
    );
    describe_counter!(EVICTIONS_TOTAL, "Entries evicted to make room");
    describe_counter!(EVICTED_BYTES_TOTAL, Unit::Bytes, "Bytes freed by evictions");
    describe_gauge!(SIZE_BYTES, Unit::Bytes, "Bytes held by stored objects");
    describe_gauge!(SIZE_LIMIT_BYTES, Unit::Bytes, "Configured max_size");
    describe_gauge!(ENTRIES, "Stored objects");
    describe_gauge!(
        OLDEST_ENTRY_AGE_SECONDS,
        Unit::Seconds,
        "Age of the least recently used entry; size max_size against it"
    );
}

/// The `method` label value: the standard methods by name, anything else as `OTHER`, so a client
/// cannot create series by inventing methods.
fn method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::HEAD => "HEAD",
        Method::PUT => "PUT",
        Method::POST => "POST",
        Method::DELETE => "DELETE",
        Method::OPTIONS => "OPTIONS",
        Method::PATCH => "PATCH",
        _ => match method.as_str() {
            "PROPFIND" => "PROPFIND",
            "MKCOL" => "MKCOL",
            "MOVE" => "MOVE",
            "COPY" => "COPY",
            "LOCK" => "LOCK",
            "UNLOCK" => "UNLOCK",
            "PROPPATCH" => "PROPPATCH",
            _ => "OTHER",
        },
    }
}

/// Records one finished request.
pub(crate) fn request(method: &Method, status: u16, elapsed: Duration) {
    let method = method_label(method);
    counter!(REQUESTS_TOTAL, "method" => method, "status" => status.to_string()).increment(1);
    histogram!(REQUEST_DURATION_SECONDS, "method" => method).record(elapsed.as_secs_f64());
}

/// Records a read that found its object.
pub(crate) fn hit() {
    counter!(HITS_TOTAL).increment(1);
}

/// Records a read that did not find its object.
pub(crate) fn miss() {
    counter!(MISSES_TOTAL).increment(1);
}

/// Records object bytes sent to a client.
pub(crate) fn read_bytes(bytes: u64) {
    counter!(READ_BYTES_TOTAL).increment(bytes);
}

/// Records object bytes that became visible in the store.
pub(crate) fn written_bytes(bytes: u64) {
    counter!(WRITTEN_BYTES_TOTAL).increment(bytes);
}

/// Records a PUT that was answered 200 without being stored.
pub(crate) fn put_dropped(reason: DropReason) {
    counter!(PUTS_DROPPED_TOTAL, "reason" => reason.as_str()).increment(1);
}

/// Records a request refused for missing or invalid credentials.
pub(crate) fn auth_failure() {
    counter!(AUTH_FAILURES_TOTAL).increment(1);
}

/// Records one eviction and the bytes it freed.
pub(crate) fn eviction(bytes: u64) {
    counter!(EVICTIONS_TOTAL).increment(1);
    counter!(EVICTED_BYTES_TOTAL).increment(bytes);
}

/// Publishes the store gauges.
#[expect(
    clippy::cast_precision_loss,
    clippy::as_conversions,
    reason = "gauges are f64; sizes below 2^53 bytes are exact"
)]
pub(crate) fn set_store_gauges(stats: &Stats) {
    gauge!(SIZE_BYTES).set(stats.size_bytes as f64);
    gauge!(SIZE_LIMIT_BYTES).set(stats.size_limit_bytes as f64);
    gauge!(ENTRIES).set(stats.entries as f64);
    gauge!(OLDEST_ENTRY_AGE_SECONDS)
        .set(stats.oldest_entry_age.map_or(0.0, |age| age.as_secs_f64()));
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    #[test]
    fn unknown_methods_share_one_label() {
        assert_eq!(method_label(&Method::GET), "GET");
        assert_eq!(
            method_label(&Method::from_bytes(b"PROPFIND").unwrap()),
            "PROPFIND"
        );
        assert_eq!(method_label(&Method::from_bytes(b"BREW").unwrap()), "OTHER");
    }

    #[test]
    fn every_recorder_emits_its_d18_name() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            request(&Method::GET, 200, Duration::from_millis(3));
            hit();
            miss();
            read_bytes(1);
            written_bytes(1);
            put_dropped(DropReason::NoSpace);
            auth_failure();
            eviction(1);
            set_store_gauges(&Stats {
                size_bytes: 1,
                size_limit_bytes: 2,
                entries: 1,
                oldest_entry_age: None,
            });
        });
        let text = handle.render();
        for name in [
            "cachette_requests_total{method=\"GET\",status=\"200\"}",
            "cachette_request_duration_seconds",
            "cachette_hits_total",
            "cachette_misses_total",
            "cachette_read_bytes_total",
            "cachette_written_bytes_total",
            "cachette_puts_dropped_total{reason=\"no_space\"}",
            "cachette_auth_failures_total",
            "cachette_evictions_total",
            "cachette_evicted_bytes_total",
            "cachette_size_bytes",
            "cachette_size_limit_bytes 2",
            "cachette_entries",
            "cachette_oldest_entry_age_seconds",
        ] {
            assert!(text.contains(name), "{name} missing from:\n{text}");
        }
    }
}
