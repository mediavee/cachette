//! Authentication: who is asking, and whether their token allows the request.
//!
//! The secret comes as the password of a Basic header (the user name is ignored, since Gradle can
//! only do Basic), as Basic encoded in base64url with or without padding (Bazel), or as a Bearer
//! token (sccache). Secrets are never copied out of the header and never logged.

use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use base64::alphabet::{STANDARD, URL_SAFE};
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::{DecodeSliceError, Engine};

use crate::config::{Access, Anonymous, Token};

/// Name shown in the access log for a request without credentials.
const ANONYMOUS: &str = "anonymous";

/// Decoded Basic credentials up to this size are decoded on the stack.
const STACK_CREDENTIALS: usize = 256;

/// Accepts the alphabet's characters with or without `=` padding.
const LENIENT: GeneralPurposeConfig =
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent);
const STANDARD_ENGINE: GeneralPurpose = GeneralPurpose::new(&STANDARD, LENIENT);
const URL_SAFE_ENGINE: GeneralPurpose = GeneralPurpose::new(&URL_SAFE, LENIENT);

/// Who a request is, once it passed authentication.
#[derive(Debug, Clone, Copy)]
pub(super) struct Identity<'a> {
    /// The token name, or `anonymous`.
    pub(super) name: &'a str,
    access: Access,
}

impl Identity<'_> {
    fn allows(self, needed: Access) -> bool {
        match needed {
            Access::Read => true,
            Access::Write => self.access == Access::Write,
        }
    }
}

/// What the client sent, which decides the challenge in the `401`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Presented {
    /// Nothing usable, or Basic: the answer asks for Basic.
    Basic,
    /// A Bearer header: the answer also says `invalid_token`.
    Bearer,
}

/// Why a request is refused.
#[derive(Debug, Clone, Copy)]
pub(super) enum Refusal<'a> {
    /// No credentials, or credentials that match no token: `401`.
    Unauthorized(Presented),
    /// A valid token without the right to do this: `403`.
    Forbidden(Identity<'a>),
}

/// Checks the credentials of a request that needs `needed`.
///
/// Without an `Authorization` header only a read can pass, and only when `anonymous` allows it.
/// Credentials that are present but wrong are refused even where anonymous reads are open, so a
/// misconfigured client sees the error instead of silently reading.
pub(super) fn authenticate<'a>(
    headers: &HeaderMap,
    tokens: &'a [Token],
    anonymous: Anonymous,
    needed: Access,
) -> Result<Identity<'a>, Refusal<'a>> {
    let Some(header) = headers.get(AUTHORIZATION) else {
        return match (anonymous, needed) {
            (Anonymous::Read, Access::Read) => Ok(Identity {
                name: ANONYMOUS,
                access: Access::Read,
            }),
            _ => Err(Refusal::Unauthorized(Presented::Basic)),
        };
    };
    let value = header.to_str().unwrap_or_default();
    let (scheme, credentials) = value.split_once(' ').unwrap_or((value, ""));
    let credentials = credentials.trim();
    let bearer = scheme.eq_ignore_ascii_case("Bearer");
    let presented = if bearer {
        Presented::Bearer
    } else {
        Presented::Basic
    };
    let token = if bearer {
        find(tokens, credentials)
    } else if scheme.eq_ignore_ascii_case("Basic") {
        with_basic_password(credentials, |password| find(tokens, password)).flatten()
    } else {
        None
    };
    let Some(token) = token else {
        return Err(Refusal::Unauthorized(presented));
    };
    let identity = Identity {
        name: &token.name,
        access: token.access,
    };
    if identity.allows(needed) {
        Ok(identity)
    } else {
        Err(Refusal::Forbidden(identity))
    }
}

fn find<'a>(tokens: &'a [Token], candidate: &str) -> Option<&'a Token> {
    tokens.iter().find(|token| token.secret.matches(candidate))
}

/// Runs `use_password` on the password of a Basic header: the text after the first `:` of the
/// decoded credentials. `None` when the header is not valid base64 in either alphabet, not UTF-8,
/// or has no `:`.
fn with_basic_password<T>(encoded: &str, use_password: impl FnOnce(&str) -> T) -> Option<T> {
    let mut stack = [0_u8; STACK_CREDENTIALS];
    let mut heap = Vec::new();
    let decoded = decode(encoded, &mut stack, &mut heap)?;
    let (_user, password) = std::str::from_utf8(decoded).ok()?.split_once(':')?;
    Some(use_password(password))
}

/// Decodes in the standard alphabet, then the URL-safe one, on the stack unless it does not fit.
fn decode<'b>(
    encoded: &str,
    stack: &'b mut [u8; STACK_CREDENTIALS],
    heap: &'b mut Vec<u8>,
) -> Option<&'b [u8]> {
    for engine in [&STANDARD_ENGINE, &URL_SAFE_ENGINE] {
        match engine.decode_slice(encoded, stack) {
            Ok(len) => return Some(&stack[..len]),
            Err(DecodeSliceError::OutputSliceTooSmall) => {
                if let Ok(decoded) = engine.decode(encoded) {
                    *heap = decoded;
                    return Some(heap);
                }
            }
            Err(DecodeSliceError::DecodeError(_)) => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn tokens() -> Vec<Token> {
        ["ci:write:s3cr3t:with:colons", "dev:read:readonly"]
            .map(|text| text.parse().expect("valid token"))
            .to_vec()
    }

    fn headers(authorization: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(authorization).expect("visible ASCII"),
        );
        headers
    }

    fn name(headers: &HeaderMap, needed: Access) -> Result<String, String> {
        match authenticate(headers, &tokens(), Anonymous::None, needed) {
            Ok(identity) => Ok(identity.name.to_owned()),
            Err(Refusal::Unauthorized(presented)) => Err(format!("401 {presented:?}")),
            Err(Refusal::Forbidden(identity)) => Err(format!("403 {}", identity.name)),
        }
    }

    #[test]
    fn every_form_of_the_secret_is_accepted() {
        let engine = base64::engine::general_purpose::STANDARD;
        let url_safe = base64::engine::general_purpose::URL_SAFE;
        let unpadded = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let raw = ["any:s3cr3t:with:colons", "x:s3cr3t:with:colons"];
        for header in [
            format!("Basic {}", engine.encode(raw[0])),
            format!("basic {}", url_safe.encode(raw[1])),
            format!("Basic {}", unpadded.encode(raw[0])),
            format!("Basic {}", unpadded.encode(":s3cr3t:with:colons")),
            "Bearer s3cr3t:with:colons".to_owned(),
        ] {
            assert_eq!(
                name(&headers(&header), Access::Write),
                Ok("ci".to_owned()),
                "{header}"
            );
        }
    }

    #[test]
    fn url_safe_characters_decode() {
        let secret = "ab?>?>?>";
        let token: Token = format!("odd:write:{secret}").parse().expect("valid token");
        let encoded =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("u:{secret}"));
        assert!(
            encoded.contains(['-', '_']),
            "the fixture must use the URL alphabet: {encoded}"
        );
        let binding = [token];
        let result = authenticate(
            &headers(&format!("Basic {encoded}")),
            &binding,
            Anonymous::None,
            Access::Write,
        );
        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn unknown_secrets_and_malformed_headers_are_401() {
        for header in [
            "Bearer nope",
            "Basic !!!!",
            "Basic",
            "Basic bm9jb2xvbg==",
            "Negotiate abc",
            "",
        ] {
            assert!(
                matches!(
                    name(&headers(header), Access::Read),
                    Err(ref refusal) if refusal.starts_with("401")
                ),
                "{header:?}"
            );
        }
        assert_eq!(
            name(&headers("Bearer nope"), Access::Read),
            Err("401 Bearer".to_owned())
        );
        assert_eq!(
            name(&headers("Basic !!!!"), Access::Read),
            Err("401 Basic".to_owned())
        );
    }

    #[test]
    fn a_read_token_cannot_write() {
        assert_eq!(
            name(&headers("Bearer readonly"), Access::Write),
            Err("403 dev".to_owned())
        );
        assert_eq!(
            name(&headers("Bearer readonly"), Access::Read),
            Ok("dev".to_owned())
        );
    }

    #[test]
    fn anonymous_reads_only_where_allowed() {
        let none = HeaderMap::new();
        let tokens = tokens();
        assert!(authenticate(&none, &tokens, Anonymous::None, Access::Read).is_err());
        assert!(authenticate(&none, &tokens, Anonymous::Read, Access::Read).is_ok());
        assert!(authenticate(&none, &tokens, Anonymous::Read, Access::Write).is_err());
        // Wrong credentials are not anonymous.
        assert!(
            authenticate(
                &headers("Bearer nope"),
                &tokens,
                Anonymous::Read,
                Access::Read
            )
            .is_err()
        );
    }

    #[test]
    fn long_credentials_still_decode() {
        let secret = "k".repeat(600);
        let token: Token = format!("long:write:{secret}").parse().expect("valid token");
        let encoded = base64::engine::general_purpose::STANDARD.encode(format!("u:{secret}"));
        let binding = [token];
        let result = authenticate(
            &headers(&format!("Basic {encoded}")),
            &binding,
            Anonymous::None,
            Access::Write,
        );
        assert!(result.is_ok(), "{result:?}");
    }
}
