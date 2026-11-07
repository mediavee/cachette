//! `PUT` on a key.

use axum::body::Body;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE, EXPECT};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};

use super::{Handled, Note, State, discard};
use crate::key::Key;
use crate::store::{Admission, DropReason, Outcome};

/// Decides on the headers, then streams the body into the store.
///
/// Nothing between the headers and the first read of the body waits: the store admits
/// synchronously, and `write` starts consuming the stream at once.
pub(super) async fn put(
    state: &State,
    parts: &Parts,
    body: &mut Option<Body>,
    key: &Key,
) -> Handled {
    let size = match announced_size(&parts.headers) {
        Ok(size) => size,
        Err(refusal) => return refusal.into_response(),
    };
    match state.store.admit(key, size) {
        Admission::Dropped(reason) => abandon(state, parts, body, reason).await,
        Admission::Reserved(reservation) => {
            // The frames go to the store as they come: no extractor buffers or decodes them.
            match reservation
                .write(body.take().unwrap_or_default().into_data_stream())
                .await
            {
                Outcome::Stored => Handled {
                    bytes: size,
                    body_read: true,
                    ..Handled::status(StatusCode::CREATED)
                },
                Outcome::Dropped(reason) => {
                    state.metrics.put_dropped(reason);
                    Handled {
                        body_read: true,
                        ..not_stored(parts, reason)
                    }
                }
                Outcome::Aborted => Handled {
                    note: Some(Note::Aborted),
                    body_read: true,
                    ..Handled::text(
                        StatusCode::BAD_REQUEST,
                        "the body does not match the Content-Length\n",
                    )
                },
            }
        }
    }
}

/// The PUT is not stored; the answer must still leave the connection usable.
///
/// With `Expect: 100-continue` the answer goes out unread: hyper sends no `100`, and the client
/// keeps its body. Without it the client has already written the body, and answering before it
/// is read would make hyper close the connection under the client's feet, so it is read and
/// discarded to its end first.
async fn abandon(
    state: &State,
    parts: &Parts,
    body: &mut Option<Body>,
    reason: DropReason,
) -> Handled {
    state.metrics.put_dropped(reason);
    let mut handled = not_stored(parts, reason);
    if !expects_continue(&parts.headers) {
        discard(body.take().unwrap_or_default()).await;
        handled.body_read = true;
    }
    handled
}

/// The answer to a PUT that is not stored, whatever the reason: the one place that picks
/// its status.
fn not_stored(parts: &Parts, reason: DropReason) -> Handled {
    // Despite its docs, Gradle's `HttpBuildCache` turns the remote cache off for the rest of the
    // build on any non-2xx status of a PUT, so it gets 200; the `refused` scenario of
    // `tests/clients/gradle/run.sh` fails when Gradle gets 413.
    let status = if is_gradle_artifact(&parts.headers) {
        StatusCode::OK
    } else {
        StatusCode::PAYLOAD_TOO_LARGE
    };
    Handled::dropped(reason, status)
}

fn is_gradle_artifact(headers: &HeaderMap) -> bool {
    const PREFIX: &[u8] = b"application/vnd.gradle.build-cache-artifact";
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.as_bytes().get(..PREFIX.len()))
        .is_some_and(|start| start.eq_ignore_ascii_case(PREFIX))
}

/// A refused PUT whose body was not read. A client that sends first, as Gradle does without
/// `Expect`, loses the answer to a reset if the server stops reading, so the body is read and
/// discarded unless it is announced larger than any object the store takes (or not announced:
/// then, like under `Expect`, the answer goes out at once and the connection closes).
pub(super) async fn drain_refused(state: &State, parts: &Parts, body: Body, handled: &mut Handled) {
    let worth_reading = !expects_continue(&parts.headers)
        && announced_size(&parts.headers).is_ok_and(|size| size <= state.max_object_size);
    if worth_reading {
        discard(body).await;
        handled.body_read = true;
    }
}

fn expects_continue(headers: &HeaderMap) -> bool {
    headers
        .get(EXPECT)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"100-continue"))
}

/// Why a PUT has no usable `Content-Length`.
#[derive(Debug, Clone, Copy)]
enum Unframed {
    /// Chunked or unframed: `411`.
    Missing,
    /// Present but not a number.
    Invalid,
}

impl Unframed {
    fn into_response(self) -> Handled {
        match self {
            Self::Missing => Handled::text(
                StatusCode::LENGTH_REQUIRED,
                "PUT needs a Content-Length; chunked uploads are not accepted\n",
            ),
            Self::Invalid => Handled::text(StatusCode::BAD_REQUEST, "invalid Content-Length\n"),
        }
    }
}

/// The body length from `Content-Length`, which a PUT must carry. hyper drops the header of a
/// chunked request, so a chunked PUT has none.
fn announced_size(headers: &HeaderMap) -> Result<u64, Unframed> {
    let value = headers.get(CONTENT_LENGTH).ok_or(Unframed::Missing)?;
    value
        .to_str()
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(Unframed::Invalid)
}

#[cfg(test)]
mod tests {
    use axum::http::Request;

    use super::*;

    fn status_for(content_type: Option<&str>, reason: DropReason) -> StatusCode {
        let mut request = Request::builder().method("PUT").uri("/k");
        if let Some(content_type) = content_type {
            request = request.header(CONTENT_TYPE, content_type);
        }
        let (parts, ()) = request.body(()).expect("request").into_parts();
        not_stored(&parts, reason).response.status()
    }

    #[test]
    fn every_drop_reason_answers_413_except_to_gradle() {
        for reason in DropReason::ALL {
            assert_eq!(status_for(None, reason), StatusCode::PAYLOAD_TOO_LARGE);
            assert_eq!(
                status_for(Some("application/octet-stream"), reason),
                StatusCode::PAYLOAD_TOO_LARGE
            );
            assert_eq!(
                status_for(
                    Some("application/vnd.gradle.build-cache-artifact.v1"),
                    reason
                ),
                StatusCode::OK,
                "{reason:?}"
            );
        }
    }
}
