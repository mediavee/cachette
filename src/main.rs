//! cachette: a remote build cache server for Gradle and sccache.

mod config;
mod http;
mod key;
mod metrics;
mod server;
mod store;

use std::io::{self, Write};

use anyhow::Context;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use config::{Cli, Config, LogFormat};
use server::Server;
use store::{Options, Store};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load(Cli::parse()).context("invalid configuration")?;
    init_tracing(config.log_format)?;
    let recorder = metrics::install().context("cannot install the metrics recorder")?;

    let store = Store::open(
        &config.data_dir,
        Options {
            max_size: config.max_size,
            max_object_size: config.max_object_size,
            min_free_disk: config.min_free_disk,
            fsync: config.fsync,
            touch_interval: config.touch_interval,
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
    info!(%addr, tls = server.scheme() == "https", data_dir = %config.data_dir.display(), "listening");

    server
        .serve(server::app(&config, store.clone(), recorder), shutdown)
        .await;
    store.close().await.context("cannot flush the store")?;
    info!("stopped");
    Ok(())
}

fn init_tracing(format: LogFormat) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr);
    match format {
        LogFormat::Text => builder.try_init(),
        LogFormat::Json => builder.json().try_init(),
    }
    .map_err(|e| anyhow::anyhow!("cannot initialise logging: {e}"))
}
