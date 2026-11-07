//! `GET` and `HEAD` on a key.

use axum::body::Body;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use tracing::error;

use super::{Handled, State};
use crate::key::Key;

/// `200` with the exact `Content-Length` (opendal 0.59 fails without it), or `404`.
///
/// A store failure is answered as a miss and logged: a 5xx would make Gradle drop the remote
/// cache for the rest of the build, where a miss only costs one rebuilt task.
pub(super) async fn get(state: &State, key: &Key) -> Handled {
    match state.store.read(key).await {
        Ok(Some(object)) => {
            state.metrics.hit();
            state.metrics.read_bytes(object.size);
            let size = object.size;
            let mut response = Response::new(Body::from_stream(object.into_stream()));
            describe(&mut response, size);
            Handled {
                bytes: size,
                ..Handled::new(response)
            }
        }
        Ok(None) => miss(state),
        Err(e) => {
            error!(%key, error = %e, "cannot read an object; answering as a miss");
            miss(state)
        }
    }
}

/// The headers of a `GET` without its body. Not counted as a hit or a miss: Gradle and sccache
/// never send `HEAD`, and a client that does (ccache before each store) would skew the ratio.
pub(super) async fn head(state: &State, key: &Key) -> Handled {
    match state.store.stat(key).await {
        Ok(Some(meta)) => {
            let mut response = Response::new(Body::empty());
            describe(&mut response, meta.size);
            Handled::new(response)
        }
        Ok(None) => Handled::status(StatusCode::NOT_FOUND),
        Err(e) => {
            error!(%key, error = %e, "cannot stat an object; answering as a miss");
            Handled::status(StatusCode::NOT_FOUND)
        }
    }
}

fn miss(state: &State) -> Handled {
    state.metrics.miss();
    Handled::status(StatusCode::NOT_FOUND)
}

fn describe(response: &mut Response, size: u64) {
    let headers = response.headers_mut();
    headers.insert(CONTENT_LENGTH, HeaderValue::from(size));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
}
