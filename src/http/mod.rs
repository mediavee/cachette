//! The key router: `GET`, `HEAD`, `PUT` and `WebDAV` on `/<key>`. A stub until the `http` lot.
//!
//! `server` mounts [`router`] under `/healthz` and `/metrics`, which are never keys (D10).

#![expect(
    dead_code,
    reason = "the state is read by the real router of the http lot"
)]

use std::sync::Arc;

use axum::Router;
use axum::http::StatusCode;

use crate::config::{AccessLog, Anonymous, Token};
use crate::metrics::Metrics;
use crate::store::Store;

/// What the key handlers need.
#[derive(Debug, Clone)]
pub(crate) struct State {
    /// Where objects are read and written.
    pub(crate) store: Store,
    /// Credentials; a request matching none is refused unless `anonymous` allows it.
    pub(crate) tokens: Arc<[Token]>,
    /// Whether requests without credentials may read.
    pub(crate) anonymous: Anonymous,
    /// Which requests the access log records. Errors, refusals and dropped PUTs are always logged.
    pub(crate) access_log: AccessLog,
    /// Recorders for requests, hits, misses, bytes read, dropped PUTs and refusals.
    pub(crate) metrics: Metrics,
}

/// The router for every path that is not reserved.
pub(crate) fn router(state: State) -> Router {
    Router::new().fallback(not_implemented).with_state(state)
}

async fn not_implemented() -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}
