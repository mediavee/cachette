//! The key router: `GET`, `HEAD`, `PUT` and the `WebDAV` subset sccache needs, on `/<key>`.
//!
//! `server` mounts [`router`] under `/healthz` and `/metrics`, which are never keys (D10).
//!
//! A PUT is decided on its headers, in this order (D7): authentication, key, `Content-Length`,
//! admission by the store. Only then is the body touched, so a refusal sent under
//! `Expect: 100-continue` makes hyper skip the `100` and the client skip the body.

mod auth;
mod dav;
mod put;
mod read;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::State as Shared;
use axum::http::header::{ALLOW, CONNECTION, WWW_AUTHENTICATE};
use axum::http::request::Parts;
use axum::http::{HeaderValue, Method, Request, StatusCode};
use axum::response::Response;
use tracing::{Level, event};

use crate::config::{Access, AccessLog, Anonymous, Token};
use crate::key::{Key, KeyError};
use crate::metrics::Metrics;
use crate::store::{DropReason, Store};

use auth::{Presented, Refusal};

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

/// Methods this server answers, as sent in `Allow`.
const ALLOWED_METHODS: &str = "OPTIONS, GET, HEAD, PUT, PROPFIND, MKCOL";

/// The router for every path that is not reserved.
pub(crate) fn router(state: State) -> Router {
    Router::new().fallback(handle).with_state(Arc::new(state))
}

/// What the server does for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Get,
    Head,
    Put,
    Propfind,
    Mkcol,
    Options,
}

impl Op {
    fn parse(method: &Method) -> Option<Self> {
        match method.as_str() {
            "GET" => Some(Self::Get),
            "HEAD" => Some(Self::Head),
            "PUT" => Some(Self::Put),
            "PROPFIND" => Some(Self::Propfind),
            "MKCOL" => Some(Self::Mkcol),
            "OPTIONS" => Some(Self::Options),
            _ => None,
        }
    }

    const fn needs(self) -> Access {
        match self {
            Self::Put | Self::Mkcol => Access::Write,
            Self::Get | Self::Head | Self::Propfind | Self::Options => Access::Read,
        }
    }
}

/// The resource a request names.
#[derive(Debug)]
enum Target {
    /// The root collection, which `PROPFIND` and `MKCOL` accept (sccache without a key prefix).
    Root,
    Key(Key),
}

/// Why a request was not served as asked, beyond its status. Shown in the access log.
#[derive(Debug, Clone, Copy)]
enum Note {
    /// The PUT was answered 200 without being stored (D5).
    Dropped(DropReason),
    /// The PUT body ended early or failed; nothing was stored.
    Aborted,
}

impl Note {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Dropped(reason) => reason.as_str(),
            Self::Aborted => "aborted",
        }
    }
}

/// A response with what the dispatcher records about it.
#[derive(Debug)]
struct Handled {
    response: Response,
    /// Object bytes read or stored.
    bytes: u64,
    note: Option<Note>,
    /// Whether the request body was consumed to its end (or failed).
    body_read: bool,
}

impl Handled {
    fn new(response: Response) -> Self {
        Self {
            response,
            bytes: 0,
            note: None,
            body_read: false,
        }
    }

    fn status(status: StatusCode) -> Self {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = status;
        Self::new(response)
    }

    fn text(status: StatusCode, message: impl Into<Body>) -> Self {
        let mut response = Response::new(message.into());
        *response.status_mut() = status;
        Self::new(response)
    }

    /// A PUT that is not stored and still answered 200 (D5).
    fn dropped(reason: DropReason) -> Self {
        Self {
            note: Some(Note::Dropped(reason)),
            ..Self::status(StatusCode::OK)
        }
    }

    fn method_not_allowed() -> Self {
        let mut handled = Self::status(StatusCode::METHOD_NOT_ALLOWED);
        handled
            .response
            .headers_mut()
            .insert(ALLOW, HeaderValue::from_static(ALLOWED_METHODS));
        handled
    }
}

/// A handled request with the facts the access log needs.
struct Served<'a> {
    handled: Handled,
    /// The token name, or `-` when the request never got as far as authentication.
    token: &'a str,
    key: Option<Key>,
}

async fn handle(Shared(state): Shared<Arc<State>>, request: Request<Body>) -> Response {
    let started = Instant::now();
    let (parts, body) = request.into_parts();
    let mut served = serve(&state, &parts, body).await;
    if parts.method == Method::PUT && !served.handled.body_read {
        // hyper cannot parse another request after a body it announced and never read, so it
        // closes the connection once the answer is out, without saying so. Saying so lets the
        // client open a new connection instead of reusing a dead one.
        served
            .handled
            .response
            .headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("close"));
    }
    let status = served.handled.response.status();
    let elapsed = started.elapsed();
    state
        .metrics
        .request(&parts.method, status.as_u16(), elapsed);
    log_access(&state, &parts, &served, status, elapsed);
    served.handled.response
}

async fn serve<'a>(state: &'a State, parts: &Parts, body: Body) -> Served<'a> {
    let anonymous = |handled| Served {
        handled,
        token: "-",
        key: None,
    };
    let Some(op) = Op::parse(&parts.method) else {
        return anonymous(Handled::method_not_allowed());
    };
    let identity =
        match auth::authenticate(&parts.headers, &state.tokens, state.anonymous, op.needs()) {
            Ok(identity) => identity,
            Err(Refusal::Unauthorized(presented)) => {
                state.metrics.auth_failure();
                return anonymous(unauthorized(presented));
            }
            Err(Refusal::Forbidden(identity)) => {
                return Served {
                    handled: Handled::text(StatusCode::FORBIDDEN, "this token cannot write\n"),
                    token: identity.name,
                    key: None,
                };
            }
        };
    let served = |handled, key| Served {
        handled,
        token: identity.name,
        key,
    };
    let target = match target(op, parts.uri.path()) {
        Ok(target) => target,
        Err(error) => {
            return served(
                Handled::text(StatusCode::BAD_REQUEST, format!("{error}\n")),
                None,
            );
        }
    };
    if let Target::Key(key) = &target
        && op.needs() == Access::Write
        && is_reserved(key)
    {
        return served(Handled::method_not_allowed(), None);
    }
    let handled = match (op, &target) {
        (Op::Get, Target::Key(key)) => read::get(state, key).await,
        (Op::Head, Target::Key(key)) => read::head(state, key).await,
        (Op::Put, Target::Key(key)) => put::put(state, parts, body, key).await,
        (Op::Propfind, target) => dav::propfind(state, parts, target).await,
        (Op::Mkcol, _) => dav::mkcol(),
        (Op::Options, _) => dav::options(),
        (Op::Get | Op::Head | Op::Put, Target::Root) => {
            Handled::text(StatusCode::BAD_REQUEST, format!("{}\n", KeyError::Empty))
        }
    };
    served(
        handled,
        match target {
            Target::Key(key) => Some(key),
            Target::Root => None,
        },
    )
}

/// Reads the key from the raw path, borrowed: no extractor decodes it into a new allocation.
fn target(op: Op, raw_path: &str) -> Result<Target, KeyError> {
    if op == Op::Options {
        return Ok(Target::Root);
    }
    match Key::from_uri_path(raw_path) {
        Ok(key) => Ok(Target::Key(key)),
        Err(KeyError::Empty) if matches!(op, Op::Propfind | Op::Mkcol) => Ok(Target::Root),
        Err(error) => Err(error),
    }
}

/// `/healthz` and `/metrics` are routed before this module; a request that normalises to the same
/// words (`//healthz`) must not be able to write what `GET /healthz` can never read back.
fn is_reserved(key: &Key) -> bool {
    matches!(key.as_str(), "healthz" | "metrics")
}

fn unauthorized(presented: Presented) -> Handled {
    let mut handled = Handled::text(StatusCode::UNAUTHORIZED, "credentials required\n");
    let headers = handled.response.headers_mut();
    headers.append(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"cachette\""),
    );
    if presented == Presented::Bearer {
        headers.append(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer error=\"invalid_token\""),
        );
    }
    handled
}

/// How loudly a request is logged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Severity {
    Info,
    Warn,
    Error,
}

/// Logs the request when `access_log` asks for it (D38). A dropped PUT or a server error is
/// logged whatever the setting; refusals and client errors need `errors` or `all`. A miss is a
/// normal answer, not an error.
fn log_access(
    state: &State,
    parts: &Parts,
    served: &Served<'_>,
    status: StatusCode,
    elapsed: Duration,
) {
    let note = served.handled.note;
    let severity =
        if status.is_server_error() || matches!(note, Some(Note::Dropped(DropReason::Io))) {
            Severity::Error
        } else if note.is_some() || (status.is_client_error() && status != StatusCode::NOT_FOUND) {
            Severity::Warn
        } else {
            Severity::Info
        };
    let wanted = match state.access_log {
        AccessLog::All => true,
        AccessLog::Errors => severity != Severity::Info,
        AccessLog::Off => severity == Severity::Error || matches!(note, Some(Note::Dropped(_))),
    };
    if !wanted {
        return;
    }
    // A valid key has no control character, and hyper refuses ASCII control characters in a
    // request target, so neither can forge a log line.
    let key = served
        .key
        .as_ref()
        .map_or_else(|| parts.uri.path(), Key::as_str);
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    macro_rules! access {
        ($level:expr) => {
            event!(
                $level,
                method = parts.method.as_str(),
                key = %key,
                status = status.as_u16(),
                bytes = served.handled.bytes,
                micros,
                token = served.token,
                note = note.map(Note::as_str),
                "request"
            )
        };
    }
    match severity {
        Severity::Info => access!(Level::INFO),
        Severity::Warn => access!(Level::WARN),
        Severity::Error => access!(Level::ERROR),
    }
}
