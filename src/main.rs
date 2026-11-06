//! cachette: a remote build cache server for Gradle and sccache.

mod config;
mod http;
mod key;
mod metrics;
mod server;
mod store;

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use tracing::{info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

use config::{Cli, Config, LogFormat};
use metrics::Metrics;
use server::Server;
use store::{Options, Store};

/// musl's allocator is 29 to 200 times slower under contention (D39).
#[cfg(target_env = "musl")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// How often the metrics exporter drains its histogram buffers.
const METRICS_UPKEEP_PERIOD: Duration = Duration::from_secs(5);

fn main() -> anyhow::Result<()> {
    let config = Config::load(Cli::parse()).context("invalid configuration")?;
    // Held until the end of `main`: dropping it flushes the log lines still queued.
    let _logs = init_tracing(config.log_format)?;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.worker_threads)
        .enable_all()
        .build()
        .context("cannot start the async runtime")?
        .block_on(run(config))
}

async fn run(config: Config) -> anyhow::Result<()> {
    let metrics = Metrics::install().context("cannot install the metrics recorder")?;
    tokio::spawn(metrics.clone().upkeep(METRICS_UPKEEP_PERIOD));

    let store = Store::open(
        &config.data_dir,
        Options {
            max_size: config.max_size,
            max_object_size: config.max_object_size,
            min_free_disk: config.min_free_disk,
            durability: config.durability,
            touch_interval: config.touch_interval,
            metrics: metrics.clone(),
        },
    )
    .await
    .with_context(|| {
        format!(
            "cannot open the data directory {}",
            config.data_dir.display()
        )
    })?;

    let server = Server::bind(&config).await?;
    let shutdown = server::shutdown_signal()?;
    let addr = server
        .local_addr()
        .context("cannot read the bound address")?;
    // The one line tests and wrappers parse to learn the port when listening on port 0.
    writeln!(
        io::stdout().lock(),
        "listening {}://{addr}",
        server.scheme()
    )
    .context("cannot announce the address")?;
    info!(
        %addr,
        tls = server.scheme() == "https",
        data_dir = %config.data_dir.display(),
        worker_threads = tokio::runtime::Handle::current().metrics().num_workers(),
        durability = ?config.durability,
        access_log = ?config.access_log,
        "listening"
    );

    // Stamp when the signal lands: after `serve` returns, an elapsed time at or past
    // the shutdown timeout means the deadline, not a drain, ended it.
    let shutdown_at = Arc::new(Mutex::new(None));
    let stamped = {
        let shutdown_at = Arc::clone(&shutdown_at);
        async move {
            shutdown.await;
            *shutdown_at.lock().expect("shutdown stamp poisoned") = Some(Instant::now());
        }
    };
    server
        .serve(server::app(&config, store.clone(), metrics), stamped)
        .await;
    if deadline_missed(
        *shutdown_at.lock().expect("shutdown stamp poisoned"),
        config.shutdown_timeout,
    ) {
        // D32: a missed shutdown deadline forbids `clean = true`. The state written at
        // open already says not clean; skipping `close` is what keeps it that way.
        warn!(
            timeout = ?config.shutdown_timeout,
            "shutdown deadline missed; leaving the store not clean"
        );
    } else {
        store.close().await.context("cannot flush the store")?;
    }
    info!("stopped");
    Ok(())
}

/// Whether the shutdown signal fired at least `timeout` ago, i.e. `serve` returned
/// because its deadline elapsed rather than because the connections drained.
fn deadline_missed(signal_at: Option<Instant>, timeout: Duration) -> bool {
    signal_at.is_some_and(|at| at.elapsed() >= timeout)
}

/// Sends every log line through a queue and a writer thread, so logging never blocks a request.
/// When the queue is full the new line is dropped, never the request.
fn init_tracing(format: LogFormat) -> anyhow::Result<WorkerGuard> {
    let (writer, guard) = tracing_appender::non_blocking(io::stderr());
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(writer);
    match format {
        LogFormat::Text => builder.try_init(),
        LogFormat::Json => builder.json().try_init(),
    }
    .map_err(|e| anyhow::anyhow!("cannot initialise logging: {e}"))?;
    Ok(guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_rule() {
        assert!(!deadline_missed(None, Duration::from_secs(1)));
        assert!(!deadline_missed(
            Some(Instant::now()),
            Duration::from_secs(60)
        ));
        assert!(deadline_missed(Some(Instant::now()), Duration::ZERO));
    }
}
