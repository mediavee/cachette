//! The `WebDAV` subset sccache needs (D8): `PROPFIND`, `MKCOL` and `OPTIONS`.
//!
//! Directories are virtual and always exist, so sccache's existence check succeeds and it never
//! sends a `MKCOL`; the one it would send is a no-op.

use std::fmt::Write as _;
use std::time::SystemTime;

use axum::body::Body;
use axum::http::header::{ALLOW, CONTENT_TYPE};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode};
use httpdate::fmt_http_date;
use tracing::error;

use super::{ALLOWED_METHODS, Handled, State, Target, discard};

const DEPTH: &str = "depth";
const MULTISTATUS: StatusCode = StatusCode::MULTI_STATUS;

/// `Allow` and `DAV: 1`.
pub(super) fn options() -> Handled {
    let mut handled = Handled::status(StatusCode::OK);
    let headers = handled.response.headers_mut();
    headers.insert(ALLOW, HeaderValue::from_static(ALLOWED_METHODS));
    headers.insert("dav", HeaderValue::from_static("1"));
    handled
}

/// `201` and nothing else: opendal accepts only `201` and `405`, a `200` fails the write.
pub(super) fn mkcol() -> Handled {
    Handled::status(StatusCode::CREATED)
}

/// A `207` about the resource itself, whatever the depth: there is no listing in v1.
pub(super) async fn propfind(state: &State, parts: &Parts, body: Body, target: &Target) -> Handled {
    // The body names nothing we need, but hyper closes the connection after the answer if it is
    // left unread, and opendal sends one with every PROPFIND.
    discard(body).await;
    match parts.headers.get(DEPTH).map(HeaderValue::as_bytes) {
        None | Some(b"0" | b"1") => {}
        Some(depth) if depth.eq_ignore_ascii_case(b"infinity") => {
            return Handled::text(StatusCode::FORBIDDEN, "Depth: infinity is not supported\n");
        }
        Some(_) => return Handled::text(StatusCode::BAD_REQUEST, "invalid Depth\n"),
    }
    let path = parts.uri.path();
    let stored = match target {
        Target::Key(key) => match state.store.stat(key).await {
            Ok(meta) => meta,
            Err(e) => {
                error!(%key, error = %e, "cannot stat an object for PROPFIND");
                return Handled::status(StatusCode::INTERNAL_SERVER_ERROR);
            }
        },
        Target::Root => None,
    };
    let mut xml = String::with_capacity(512);
    xml.push_str(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
         <D:multistatus xmlns:D=\"DAV:\"><D:response><D:href>",
    );
    escape_into(&mut xml, path);
    if let Some(meta) = stored {
        {
            let _ = write!(
                xml,
                "</D:href><D:propstat><D:prop>\
                 <D:getcontentlength>{}</D:getcontentlength>\
                 <D:getlastmodified>{}</D:getlastmodified>\
                 <D:resourcetype/>",
                meta.size,
                fmt_http_date(meta.modified)
            );
        }
    } else {
        {
            if !path.ends_with('/') {
                xml.push('/');
            }
            let _ = write!(
                xml,
                "</D:href><D:propstat><D:prop>\
                 <D:getlastmodified>{}</D:getlastmodified>\
                 <D:resourcetype><D:collection/></D:resourcetype>",
                fmt_http_date(SystemTime::now())
            );
        }
    }
    xml.push_str(
        "</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>\
         </D:response></D:multistatus>",
    );
    let mut handled = Handled::text(MULTISTATUS, xml);
    handled.response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/xml; charset=utf-8"),
    );
    handled
}

/// Escapes the characters that end an XML text node.
fn escape_into(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            other => out.push(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_markup() {
        let mut out = String::new();
        escape_into(&mut out, "/a&b<c>d");
        assert_eq!(out, "/a&amp;b&lt;c&gt;d");
    }
}
