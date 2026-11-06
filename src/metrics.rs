//! Prometheus metrics: every name in one place, and the recorders the other modules call.
//!
//! [`Metrics`] resolves its handles once, so recording on a request costs an atomic add instead
//! of a registry lookup. `store` records the bytes it writes, the entries it evicts and the
//! crash purges; `http` records requests, hits, misses, bytes read, dropped PUTs and
//! authentication failures; `server` renders the scrape, which computes the gauges then.

use std::fmt;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::http::Method;
use metrics::{
    Counter, Gauge, Histogram, Unit, counter, describe_counter, describe_gauge, describe_histogram,
    gauge, histogram,
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
const CRASH_PURGES_TOTAL: &str = "cachette_crash_purges_total";
const SIZE_BYTES: &str = "cachette_size_bytes";
const SIZE_LIMIT_BYTES: &str = "cachette_size_limit_bytes";
const ENTRIES: &str = "cachette_entries";
const OLDEST_ENTRY_AGE_SECONDS: &str = "cachette_oldest_entry_age_seconds";

/// Request duration buckets in seconds: from a cached 4 KiB hit to a 32 MiB upload.
const DURATION_BUCKETS: [f64; 12] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
];

/// The values of the `method` label: the standard and `WebDAV` methods by name, and `OTHER`, so a
/// client cannot create series by inventing methods.
const METHODS: [&str; 15] = [
    "GET",
    "HEAD",
    "PUT",
    "POST",
    "DELETE",
    "OPTIONS",
    "PATCH",
    "PROPFIND",
    "MKCOL",
    "MOVE",
    "COPY",
    "LOCK",
    "UNLOCK",
    "PROPPATCH",
    "OTHER",
];
const OTHER_METHOD: usize = METHODS.len() - 1;
/// Status codes 100 to 599 each get a slot; anything else takes the slow path.
const FIRST_STATUS: u16 = 100;
const STATUSES: u16 = 500;

/// Handles to every metric. Cloning is cheap and shares the handles.
#[derive(Clone)]
pub(crate) struct Metrics {
    inner: Arc<Inner>,
}

struct Inner {
    render: PrometheusHandle,
    hits: Counter,
    misses: Counter,
    read_bytes: Counter,
    written_bytes: Counter,
    auth_failures: Counter,
    evictions: Counter,
    evicted_bytes: Counter,
    crash_purges: Counter,
    puts_dropped: [Counter; DropReason::ALL.len()],
    size: Gauge,
    size_limit: Gauge,
    entries: Gauge,
    oldest_entry_age: Gauge,
    /// One slot per method and status, resolved on first use: the series appear when they happen.
    requests: Box<[OnceLock<Counter>]>,
    durations: [OnceLock<Histogram>; METHODS.len()],
}

impl fmt::Debug for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Metrics")
    }
}

impl Metrics {
    /// Installs the process-wide recorder and resolves every handle.
    ///
    /// # Errors
    ///
    /// Fails when the histogram buckets are rejected or a recorder is already installed.
    pub(crate) fn install() -> Result<Self, BuildError> {
        let render = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full(REQUEST_DURATION_SECONDS.to_owned()),
                &DURATION_BUCKETS,
            )?
            .install_recorder()?;
        describe();
        Ok(Self::resolve(render))
    }

    /// Handles that record nowhere, for tests that only need something to call.
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self::resolve(PrometheusBuilder::new().build_recorder().handle())
    }

    /// Resolves the handles against the current recorder. Unlabelled counters and every
    /// `puts_dropped_total` reason exist at zero from the start, so dashboards and alerts see the
    /// series before the first event.
    fn resolve(render: PrometheusHandle) -> Self {
        let slots = METHODS.len() * usize::from(STATUSES);
        Self {
            inner: Arc::new(Inner {
                render,
                hits: counter!(HITS_TOTAL),
                misses: counter!(MISSES_TOTAL),
                read_bytes: counter!(READ_BYTES_TOTAL),
                written_bytes: counter!(WRITTEN_BYTES_TOTAL),
                auth_failures: counter!(AUTH_FAILURES_TOTAL),
                evictions: counter!(EVICTIONS_TOTAL),
                evicted_bytes: counter!(EVICTED_BYTES_TOTAL),
                crash_purges: counter!(CRASH_PURGES_TOTAL),
                puts_dropped: DropReason::ALL
                    .map(|reason| counter!(PUTS_DROPPED_TOTAL, "reason" => reason.as_str())),
                size: gauge!(SIZE_BYTES),
                size_limit: gauge!(SIZE_LIMIT_BYTES),
                entries: gauge!(ENTRIES),
                oldest_entry_age: gauge!(OLDEST_ENTRY_AGE_SECONDS),
                requests: (0..slots).map(|_| OnceLock::new()).collect(),
                durations: std::array::from_fn(|_| OnceLock::new()),
            }),
        }
    }

    /// Records one finished request.
    pub(crate) fn request(&self, method: &Method, status: u16, elapsed: Duration) {
        let method = method_index(method);
        let label = METHODS[method];
        let counter =
            || counter!(REQUESTS_TOTAL, "method" => label, "status" => status.to_string());
        let slot = status
            .checked_sub(FIRST_STATUS)
            .filter(|offset| *offset < STATUSES)
            .and_then(|offset| {
                self.inner
                    .requests
                    .get(method * usize::from(STATUSES) + usize::from(offset))
            });
        match slot {
            Some(cell) => cell.get_or_init(counter).increment(1),
            None => counter().increment(1),
        }
        self.inner.durations[method]
            .get_or_init(|| histogram!(REQUEST_DURATION_SECONDS, "method" => label))
            .record(elapsed.as_secs_f64());
    }

    /// Records a read that found its object.
    pub(crate) fn hit(&self) {
        self.inner.hits.increment(1);
    }

    /// Records a read that did not find its object.
    pub(crate) fn miss(&self) {
        self.inner.misses.increment(1);
    }

    /// Records object bytes sent to a client.
    pub(crate) fn read_bytes(&self, bytes: u64) {
        self.inner.read_bytes.increment(bytes);
    }

    /// Records object bytes that became visible in the store.
    pub(crate) fn written_bytes(&self, bytes: u64) {
        self.inner.written_bytes.increment(bytes);
    }

    /// Records a PUT that was answered 200 without being stored.
    pub(crate) fn put_dropped(&self, reason: DropReason) {
        let slot = DropReason::ALL.iter().position(|known| *known == reason);
        if let Some(counter) = slot.and_then(|slot| self.inner.puts_dropped.get(slot)) {
            counter.increment(1);
        }
    }

    /// Records a request refused for missing or invalid credentials.
    pub(crate) fn auth_failure(&self) {
        self.inner.auth_failures.increment(1);
    }

    /// Records one eviction and the bytes it freed.
    pub(crate) fn eviction(&self, bytes: u64) {
        self.inner.evictions.increment(1);
        self.inner.evicted_bytes.increment(bytes);
    }

    /// Records a start-up purge after an unclean shutdown that the kernel did not survive.
    pub(crate) fn crash_purge(&self) {
        self.inner.crash_purges.increment(1);
    }

    /// The scrape output. The gauges are computed from `stats` now, not kept up to date.
    #[expect(
        clippy::cast_precision_loss,
        clippy::as_conversions,
        reason = "gauges are f64; sizes below 2^53 bytes are exact"
    )]
    pub(crate) fn render(&self, stats: &Stats) -> String {
        let inner = &self.inner;
        inner.size.set(stats.size_bytes as f64);
        inner.size_limit.set(stats.size_limit_bytes as f64);
        inner.entries.set(stats.entries as f64);
        inner
            .oldest_entry_age
            .set(stats.oldest_entry_age.map_or(0.0, |age| age.as_secs_f64()));
        inner.render.render()
    }

    /// Drains the exporter's histogram buffers every `period`, for ever. The exporter requires it
    /// whether or not anyone scrapes. Spawn it once.
    pub(crate) async fn upkeep(self, period: Duration) {
        let mut ticker = tokio::time::interval(period);
        loop {
            ticker.tick().await;
            self.inner.render.run_upkeep();
        }
    }
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
    describe_counter!(
        CRASH_PURGES_TOTAL,
        "Start-ups that purged the store after an unclean shutdown"
    );
    describe_gauge!(SIZE_BYTES, Unit::Bytes, "Bytes held by stored objects");
    describe_gauge!(SIZE_LIMIT_BYTES, Unit::Bytes, "Configured max_size");
    describe_gauge!(ENTRIES, "Stored objects");
    describe_gauge!(
        OLDEST_ENTRY_AGE_SECONDS,
        Unit::Seconds,
        "Age of the least recently used entry; size max_size against it"
    );
}

fn method_index(method: &Method) -> usize {
    match method.as_str() {
        "GET" => 0,
        "HEAD" => 1,
        "PUT" => 2,
        "POST" => 3,
        "DELETE" => 4,
        "OPTIONS" => 5,
        "PATCH" => 6,
        "PROPFIND" => 7,
        "MKCOL" => 8,
        "MOVE" => 9,
        "COPY" => 10,
        "LOCK" => 11,
        "UNLOCK" => 12,
        "PROPPATCH" => 13,
        _ => OTHER_METHOD,
    }
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    const STATS: Stats = Stats {
        size_bytes: 1,
        size_limit_bytes: 2,
        entries: 1,
        oldest_entry_age: None,
    };

    /// Runs `record` against a fresh recorder and returns what a scrape would show.
    fn scrape(record: impl FnOnce(&Metrics)) -> String {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let metrics = Metrics::resolve(handle);
            record(&metrics);
            metrics.render(&STATS)
        })
    }

    #[test]
    fn every_method_has_its_own_label() {
        for (index, label) in METHODS.iter().enumerate() {
            let method = Method::from_bytes(label.as_bytes()).unwrap();
            let expected = if *label == "OTHER" {
                OTHER_METHOD
            } else {
                index
            };
            assert_eq!(method_index(&method), expected, "{label}");
        }
        let unknown = Method::from_bytes(b"BREW").unwrap();
        assert_eq!(METHODS[method_index(&unknown)], "OTHER");
    }

    #[test]
    fn every_recorder_emits_its_d18_name() {
        let text = scrape(|metrics| {
            metrics.request(&Method::GET, 200, Duration::from_millis(3));
            metrics.hit();
            metrics.miss();
            metrics.read_bytes(1);
            metrics.written_bytes(1);
            metrics.put_dropped(DropReason::NoSpace);
            metrics.auth_failure();
            metrics.eviction(1);
            metrics.crash_purge();
        });
        for name in [
            "cachette_requests_total{method=\"GET\",status=\"200\"} 1",
            "cachette_request_duration_seconds_count{method=\"GET\"} 1",
            "cachette_hits_total 1",
            "cachette_misses_total 1",
            "cachette_read_bytes_total 1",
            "cachette_written_bytes_total 1",
            "cachette_puts_dropped_total{reason=\"no_space\"} 1",
            "cachette_auth_failures_total 1",
            "cachette_evictions_total 1",
            "cachette_evicted_bytes_total 1",
            "cachette_crash_purges_total 1",
            "cachette_size_bytes 1",
            "cachette_size_limit_bytes 2",
            "cachette_entries 1",
            "cachette_oldest_entry_age_seconds 0",
        ] {
            assert!(text.contains(name), "{name} missing from:\n{text}");
        }
    }

    #[test]
    fn series_exist_at_zero_before_any_event() {
        let text = scrape(|_| {});
        for name in [
            "cachette_hits_total 0",
            "cachette_crash_purges_total 0",
            "cachette_puts_dropped_total{reason=\"too_large\"} 0",
            "cachette_puts_dropped_total{reason=\"io\"} 0",
        ] {
            assert!(text.contains(name), "{name} missing from:\n{text}");
        }
        assert!(
            !text.contains("cachette_requests_total{"),
            "request series must appear only when requests happen:\n{text}"
        );
    }

    #[test]
    fn requests_count_per_method_and_status_including_odd_ones() {
        let text = scrape(|metrics| {
            for _ in 0..3 {
                metrics.request(&Method::PUT, 201, Duration::ZERO);
            }
            metrics.request(&Method::from_bytes(b"BREW").unwrap(), 418, Duration::ZERO);
            // Outside the slot table: still counted, by the slow path.
            metrics.request(&Method::GET, 999, Duration::ZERO);
        });
        for line in [
            "cachette_requests_total{method=\"PUT\",status=\"201\"} 3",
            "cachette_requests_total{method=\"OTHER\",status=\"418\"} 1",
            "cachette_requests_total{method=\"GET\",status=\"999\"} 1",
        ] {
            assert!(text.contains(line), "{line} missing from:\n{text}");
        }
    }
}
