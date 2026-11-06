//! cachette: a remote build cache server for Gradle and sccache.

mod config;
mod http;
mod key;
mod metrics;
mod server;
mod store;

use std::io::{self, Write};
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tracing::info;
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

    server
        .serve(server::app(&config, store.clone(), metrics), shutdown)
        .await;
    store.close().await.context("cannot flush the store")?;
    info!("stopped");
    Ok(())
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
