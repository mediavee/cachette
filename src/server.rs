//! The listener: TCP or TLS, connection limits, `/healthz`, `/metrics` and graceful shutdown.
//!
//! The key routes come from [`crate::http`]. This module owns what concerns the connection, not
//! the protocol: how long a client may take to send headers, how big they may be, how long a body
//! may stay silent, and how the process stops.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use metrics_exporter_prometheus::PrometheusHandle;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tower::ServiceExt;
use tower_http::timeout::RequestBodyTimeoutLayer;
use tracing::{debug, info, warn};

use crate::config::{Config, Secret, Tls};
use crate::http;
use crate::metrics;
use crate::store::Store;

/// Pause after a failed `accept`, so a descriptor shortage does not spin the CPU.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Why the server could not start.
#[derive(Debug, Error)]
pub(crate) enum Error {
    /// The listen address could not be bound.
    #[error("cannot listen on {addr}: {source}")]
    Bind {
        /// The configured address.
        addr: SocketAddr,
        /// The underlying error.
        source: io::Error,
    },
    /// The signal handlers could not be installed.
    #[error("cannot install signal handlers: {0}")]
    Signals(io::Error),
    /// A TLS file cannot be read or parsed.
    #[error("cannot load TLS file {}: {source}", path.display())]
    TlsFile {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: rustls_pki_types::pem::Error,
    },
    /// The certificate and key were rejected.
    #[error("invalid TLS certificate or key: {0}")]
    TlsConfig(tokio_rustls::rustls::Error),
}

/// A bound listener, ready to serve.
pub(crate) struct Server {
    tcp: TcpListener,
    tls: Option<TlsAcceptor>,
    header_read_timeout: Duration,
    max_header_size: usize,
    shutdown_timeout: Duration,
}

impl Server {
    /// Binds the configured address and loads the TLS material, if any.
    ///
    /// # Errors
    ///
    /// [`Error`] when the address is taken or the TLS files are unusable.
    pub(crate) async fn bind(config: &Config) -> Result<Self, Error> {
        let tls = config.tls.as_ref().map(tls_acceptor).transpose()?;
        let tcp = TcpListener::bind(config.listen)
            .await
            .map_err(|source| Error::Bind {
                addr: config.listen,
                source,
            })?;
        Ok(Self {
            tcp,
            tls,
            header_read_timeout: config.header_read_timeout,
            max_header_size: config.max_header_size,
            shutdown_timeout: config.shutdown_timeout,
        })
    }

    /// The address actually bound, which differs from the configured one for port 0.
    ///
    /// # Errors
    ///
    /// Fails when the socket no longer has an address.
    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp.local_addr()
    }

    /// `https` when TLS is on, `http` otherwise.
    pub(crate) fn scheme(&self) -> &'static str {
        if self.tls.is_some() { "https" } else { "http" }
    }

    /// Serves `app` until `shutdown` completes, then stops accepting and lets requests in flight
    /// finish for at most `shutdown_timeout`.
    pub(crate) async fn serve(self, app: Router, shutdown: impl Future<Output = ()>) {
        let graceful = GracefulShutdown::new();
        let service = app.map_request(|request: Request<Incoming>| request.map(Body::new));
        let limits = Limits {
            header_read_timeout: self.header_read_timeout,
            max_header_size: self.max_header_size,
        };
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            let (stream, peer) = tokio::select! {
                () = &mut shutdown => break,
                accepted = self.tcp.accept() => match accepted {
                    Ok(accepted) => accepted,
                    Err(e) => {
                        warn!(error = %e, "accept failed");
                        tokio::time::sleep(ACCEPT_BACKOFF).await;
                        continue;
                    }
                },
            };
            if let Err(e) = stream.set_nodelay(true) {
                debug!(%peer, error = %e, "cannot set TCP_NODELAY");
            }
            let service = TowerToHyperService::new(service.clone());
            let watcher = graceful.watcher();
            let tls = self.tls.clone();
            tokio::spawn(async move {
                let result = match tls {
                    Some(acceptor) => {
                        match tokio::time::timeout(
                            limits.header_read_timeout,
                            acceptor.accept(stream),
                        )
                        .await
                        {
                            Ok(Ok(stream)) => serve_connection(stream, service, limits, watcher)
                                .await
                                .map_err(BoxError::from),
                            Ok(Err(e)) => Err(e.into()),
                            Err(_elapsed) => Err("TLS handshake timed out".into()),
                        }
                    }
                    None => serve_connection(stream, service, limits, watcher)
                        .await
                        .map_err(BoxError::from),
                };
                if let Err(e) = result {
                    debug!(%peer, error = %e, "connection ended with an error");
                }
            });
        }
        info!("shutting down: no longer accepting connections");
        tokio::select! {
            () = graceful.shutdown() => info!("all connections drained"),
            () = tokio::time::sleep(self.shutdown_timeout) => {
                warn!(timeout = ?self.shutdown_timeout, "requests still in flight at the shutdown deadline");
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Limits {
    header_read_timeout: Duration,
    max_header_size: usize,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

async fn serve_connection<I>(
    io: I,
    service: TowerToHyperService<
        impl tower::Service<
            Request<Incoming>,
            Response = Response,
            Error = std::convert::Infallible,
            Future: Send,
        > + Clone
        + Send
        + 'static,
    >,
    limits: Limits,
    watcher: hyper_util::server::graceful::Watcher,
) -> Result<(), hyper::Error>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let connection = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_read_timeout)
        .max_buf_size(limits.max_header_size)
        .serve_connection(TokioIo::new(io), service);
    watcher.watch(connection).await
}

fn tls_acceptor(tls: &Tls) -> Result<TlsAcceptor, Error> {
    let file_error = |path: &PathBuf| {
        let path = path.clone();
        move |source| Error::TlsFile { path, source }
    };
    let certs = CertificateDer::pem_file_iter(&tls.cert)
        .map_err(file_error(&tls.cert))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(file_error(&tls.cert))?;
    let key = PrivateKeyDer::from_pem_file(&tls.key).map_err(file_error(&tls.key))?;
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(Error::TlsConfig)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Registers the SIGTERM and SIGINT handlers now and returns a future that completes on the first
/// of them. Register before announcing readiness, so a signal sent right after cannot kill the
/// process with the default action.
///
/// # Errors
///
/// [`Error::Signals`] when the handlers cannot be installed.
pub(crate) fn shutdown_signal() -> Result<impl Future<Output = ()>, Error> {
    let mut terminate = signal(SignalKind::terminate()).map_err(Error::Signals)?;
    let mut interrupt = signal(SignalKind::interrupt()).map_err(Error::Signals)?;
    Ok(async move {
        tokio::select! {
            _ = terminate.recv() => info!("SIGTERM received"),
            _ = interrupt.recv() => info!("SIGINT received"),
        }
    })
}

#[derive(Debug, Clone)]
struct Operations {
    store: Store,
    recorder: PrometheusHandle,
    metrics_token: Option<Secret>,
}

/// The complete application: `/healthz`, `/metrics`, and every other path to [`http::router`].
pub(crate) fn app(config: &Config, store: Store, recorder: PrometheusHandle) -> Router {
    let operations = Operations {
        store: store.clone(),
        recorder,
        metrics_token: config.metrics_token,
    };
    let reserved = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(scrape))
        .with_state(operations);
    let keys = http::router(http::State {
        store,
        tokens: config.tokens.clone().into(),
        anonymous: config.anonymous,
    });
    reserved
        .merge(keys)
        .layer(RequestBodyTimeoutLayer::new(config.body_idle_timeout))
}

async fn healthz(State(operations): State<Operations>) -> StatusCode {
    if operations.store.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn scrape(State(operations): State<Operations>, headers: HeaderMap) -> Response {
    if let Some(expected) = operations.metrics_token {
        let presented = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if !presented.is_some_and(|secret| expected.matches(secret)) {
            metrics::auth_failure();
            return (
                StatusCode::UNAUTHORIZED,
                [(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))],
            )
                .into_response();
        }
    }
    metrics::set_store_gauges(&operations.store.stats());
    (
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )],
        operations.recorder.render(),
    )
        .into_response()
}
