//! Micro-benchmark of the write path, run by hand:
//! `CACHETTE_PUT_BENCH_DIR=/path cargo test --release put_bench -- --ignored --nocapture`.
//! It prints one line per size × concurrency cell.

use std::convert::Infallible;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::stream;
use tempfile::TempDir;

use super::{Admission, Durability, Key, Metrics, Options, Outcome, Store};

const KEYS_PER_TASK: u64 = 256;
const FRAME: usize = 16 * 1024;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURE: Duration = Duration::from_secs(5);

fn mode() -> Durability {
    match env::var("CACHETTE_PUT_BENCH_MODE").as_deref() {
        Ok("fsync") => Durability::Fsync,
        _ => Durability::PurgeOnCrash,
    }
}

async fn cell(size: u64, tasks: u64) -> (u64, Duration, Duration) {
    let dir = match env::var_os("CACHETTE_PUT_BENCH_DIR") {
        Some(base) => TempDir::new_in(base).expect("bench dir is writable"),
        None => TempDir::new().expect("temporary directory"),
    };
    let store = Store::open(
        dir.path(),
        Options {
            max_size: 4 << 30,
            max_object_size: 4 << 30,
            min_free_disk: 0,
            durability: mode(),
            touch_interval: Duration::from_secs(300),
            metrics: Metrics::detached(),
        },
    )
    .await
    .expect("store opens");
    while !store.is_ready() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let payload = Bytes::from(vec![
        0xAB_u8;
        usize::try_from(size).expect("bench size fits")
    ]);
    let stop = Arc::new(AtomicBool::new(false));
    let counting = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for task in 0..tasks {
        let (store, payload, stop, counting, done) = (
            store.clone(),
            payload.clone(),
            Arc::clone(&stop),
            Arc::clone(&counting),
            Arc::clone(&done),
        );
        workers.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            let mut n = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                let key = Key::new(&format!("bench/{task}/{}", n % KEYS_PER_TASK)).expect("key");
                n += 1;
                let started = Instant::now();
                let Admission::Reserved(reservation) = store.admit(&key, size) else {
                    panic!("bench PUT refused");
                };
                let frames: Vec<Result<Bytes, Infallible>> = payload
                    .chunks(FRAME)
                    .map(|frame| Ok(payload.slice_ref(frame)))
                    .collect();
                let outcome = reservation.write(stream::iter(frames)).await;
                assert_eq!(outcome, Outcome::Stored);
                if counting.load(Ordering::Relaxed) {
                    latencies.push(started.elapsed());
                    done.fetch_add(1, Ordering::Relaxed);
                }
            }
            latencies
        }));
    }
    tokio::time::sleep(WARMUP).await;
    counting.store(true, Ordering::Relaxed);
    tokio::time::sleep(MEASURE).await;
    let total = done.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    let mut all = Vec::new();
    for worker in workers {
        all.extend(worker.await.expect("worker"));
    }
    all.sort_unstable();
    let pick = |percent: usize| all[(all.len() * percent / 100).min(all.len() - 1)];
    (total / MEASURE.as_secs(), pick(50), pick(99))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "micro-benchmark, run by hand"]
async fn put_bench() {
    for size in [4 * 1024, 64 * 1024] {
        for tasks in [16, 64] {
            let (rate, p50, p99) = cell(size, tasks).await;
            println!(
                "put_bench mode={:?} size={}KiB tasks={tasks} rate={rate}/s p50={p50:?} p99={p99:?}",
                mode(),
                size / 1024
            );
        }
    }
}
