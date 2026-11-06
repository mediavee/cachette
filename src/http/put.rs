//! `PUT` on a key (D4 to D7).

use axum::body::Body;
use axum::http::header::{CONTENT_LENGTH, EXPECT};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use futures_util::StreamExt;
use tracing::debug;

use super::{Handled, Note, State};
use crate::key::Key;
use crate::store::{Admission, DropReason, Outcome};

/// Decides on the headers, then streams the body into the store.
///
/// Nothing between the headers and the first read of the body waits (D7): the store admits
/// synchronously, and `write` starts consuming the stream at once.
pub(super) async fn put(state: &State, parts: &Parts, body: Body, key: &Key) -> Handled {
    let size = match announced_size(&parts.headers) {
        Ok(size) => size,
        Err(refusal) => return refusal.into_response(),
    };
    match state.store.admit(key, size) {
        Admission::Dropped(reason) => abandon(state, parts, body, reason).await,
        Admission::Reserved(reservation) => {
            // The frames go to the store as they come: no extractor buffers or decodes them.
            match reservation.write(body.into_data_stream()).await {
                Outcome::Stored => Handled {
                    bytes: size,
                    body_read: true,
                    ..Handled::status(StatusCode::CREATED)
                },
                Outcome::Dropped(reason) => {
                    state.metrics.put_dropped(reason);
                    Handled {
                        body_read: true,
                        ..Handled::dropped(reason)
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

/// The PUT is not stored but must not hurt the client (D5, D7).
///
/// With `Expect: 100-continue` the answer goes out unread: hyper sends no `100`, and the client
/// keeps its body. Without it the client has already written the body, and answering before it
/// is read would make hyper close the connection under the client's feet, so it is read and
/// discarded to its end first.
async fn abandon(state: &State, parts: &Parts, body: Body, reason: DropReason) -> Handled {
    state.metrics.put_dropped(reason);
    let mut handled = Handled::dropped(reason);
    if !expects_continue(&parts.headers) {
        let mut frames = body.into_data_stream();
        while let Some(frame) = frames.next().await {
            if let Err(e) = frame {
                debug!(error = %e, "client stopped sending a body that was being discarded");
                break;
            }
        }
        handled.body_read = true;
    }
    handled
}

fn expects_continue(headers: &HeaderMap) -> bool {
    headers
        .get(EXPECT)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"100-continue"))
}

/// Why a PUT has no usable `Content-Length`.
#[derive(Debug, Clone, Copy)]
enum Unframed {
    /// Chunked or unframed: `411` (D6).
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

/// The body length from `Content-Length`, which D6 makes mandatory. hyper drops the header of a
/// chunked request, so a chunked PUT has none.
fn announced_size(headers: &HeaderMap) -> Result<u64, Unframed> {
    let value = headers.get(CONTENT_LENGTH).ok_or(Unframed::Missing)?;
    value
        .to_str()
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(Unframed::Invalid)
}
