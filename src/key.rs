//! Cache keys: the normalised path a client addresses an object by.
//!
//! A key is never used as a file name. Objects live under `objects/<2 hex>/<64 hex>` where the hex
//! is the blake3 digest of the normalised key, so no key can address anything outside that tree.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the http and store lots are the callers; removed once both use it"
    )
)]

use std::fmt;
use std::path::PathBuf;

use percent_encoding::percent_decode_str;
use thiserror::Error;

/// Longest accepted key, in bytes, after normalisation.
const MAX_KEY_BYTES: usize = 1024;
/// Most segments an accepted key may have.
const MAX_SEGMENTS: usize = 32;

/// A validated, normalised cache key such as `a/b/c`.
///
/// Normalisation merges repeated `/` and strips leading and trailing ones, so `//a///b/` and `a/b`
/// are the same key. It is idempotent: [`Key::new`] on [`Key::as_str`] returns the same key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key(String);

/// Why a path was refused as a key. Every variant maps to a `400` response.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum KeyError {
    /// The path has no segment.
    #[error("empty key")]
    Empty,
    /// The normalised key exceeds [`MAX_KEY_BYTES`].
    #[error("key is {len} bytes, the limit is {MAX_KEY_BYTES}")]
    TooLong {
        /// Length of the normalised key in bytes.
        len: usize,
    },
    /// The key has more than [`MAX_SEGMENTS`] segments.
    #[error("key has more than {MAX_SEGMENTS} segments")]
    TooManySegments,
    /// A segment is `.` or `..`.
    #[error("key contains a `.` or `..` segment")]
    DotSegment,
    /// The key contains a control character.
    #[error("key contains a control character")]
    ControlCharacter,
    /// The percent-decoded path is not valid UTF-8.
    #[error("key is not valid UTF-8 after percent-decoding")]
    NotUtf8,
}

impl Key {
    /// Builds a key from an already decoded path, normalising it.
    ///
    /// # Errors
    ///
    /// Returns a [`KeyError`] when the path is empty, too long, has too many segments, contains a
    /// `.` or `..` segment, or contains a control character.
    pub(crate) fn new(decoded: &str) -> Result<Self, KeyError> {
        let mut normalised = String::with_capacity(decoded.len().min(MAX_KEY_BYTES));
        let mut segments = 0_usize;
        for segment in decoded.split('/').filter(|segment| !segment.is_empty()) {
            if segment == "." || segment == ".." {
                return Err(KeyError::DotSegment);
            }
            if segment.chars().any(char::is_control) {
                return Err(KeyError::ControlCharacter);
            }
            segments += 1;
            if segments > MAX_SEGMENTS {
                return Err(KeyError::TooManySegments);
            }
            if segments > 1 {
                normalised.push('/');
            }
            normalised.push_str(segment);
        }
        if normalised.is_empty() {
            return Err(KeyError::Empty);
        }
        if normalised.len() > MAX_KEY_BYTES {
            return Err(KeyError::TooLong {
                len: normalised.len(),
            });
        }
        Ok(Self(normalised))
    }

    /// Builds a key from the raw, still percent-encoded path of a request URI.
    ///
    /// The path is decoded exactly once, strictly as UTF-8, so `%252e` is the literal segment
    /// `%2e`, not `.`. An encoded slash (`%2F`) separates segments like a plain one.
    ///
    /// # Errors
    ///
    /// Returns [`KeyError::NotUtf8`] when the decoded bytes are not UTF-8, and any error of
    /// [`Key::new`] otherwise.
    pub(crate) fn from_uri_path(raw: &str) -> Result<Self, KeyError> {
        let decoded = percent_decode_str(raw)
            .decode_utf8()
            .map_err(|_| KeyError::NotUtf8)?;
        Self::new(&decoded)
    }

    /// The normalised key, without leading or trailing `/`.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// Where the object of this key lives, relative to the data directory:
    /// `objects/<first 2 hex of the digest>/<64 hex of the digest>`.
    ///
    /// Every component is a plain name, so joining it onto the data directory stays below
    /// `<data dir>/objects` whatever the key was.
    pub(crate) fn relative_path(&self) -> PathBuf {
        let digest = blake3::hash(self.0.as_bytes()).to_hex();
        let digest = digest.as_str();
        ["objects", &digest[..2], digest].iter().collect()
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use proptest::prelude::*;

    use super::*;

    /// Strings biased towards what the normaliser must handle: slashes, dots, `%`, controls.
    fn awkward() -> impl Strategy<Value = String> {
        prop_oneof![
            "[a-z./%0-9eEfF\\x00-\\x1f\\x7f]{0,80}",
            "(/|\\.|\\.\\.|[a-c]){0,40}",
            any::<String>(),
        ]
    }

    fn assert_invariants(key: &Key) {
        let text = key.as_str();
        assert!(!text.is_empty(), "empty key accepted");
        assert!(
            text.len() <= MAX_KEY_BYTES,
            "oversized key accepted: {text:?}"
        );
        assert!(
            !text.starts_with('/') && !text.ends_with('/'),
            "edge slash in {text:?}"
        );
        assert!(!text.contains("//"), "repeated slash in {text:?}");
        assert!(
            text.split('/').count() <= MAX_SEGMENTS,
            "too many segments in {text:?}"
        );
        for segment in text.split('/') {
            assert!(segment != "." && segment != "..", "dot segment in {text:?}");
        }
        assert!(
            !text.chars().any(char::is_control),
            "control character in {text:?}"
        );
    }

    proptest! {
        #[test]
        fn normalisation_is_idempotent(input in awkward()) {
            if let Ok(key) = Key::new(&input) {
                prop_assert_eq!(Key::new(key.as_str()), Ok(key), "input {:?}", input);
            }
        }

        #[test]
        fn accepted_keys_are_clean(input in awkward()) {
            if let Ok(key) = Key::new(&input) {
                assert_invariants(&key);
            }
        }

        #[test]
        fn accepted_uri_paths_are_clean(input in awkward()) {
            if let Ok(key) = Key::from_uri_path(&input) {
                assert_invariants(&key);
            }
        }

        #[test]
        fn disk_path_stays_under_objects(input in awkward()) {
            let root = Path::new("/data");
            for key in [Key::new(&input), Key::from_uri_path(&input)].into_iter().flatten() {
                let relative = key.relative_path();
                let parts: Vec<_> = relative.components().collect();
                prop_assert_eq!(parts.len(), 3, "path {:?}", relative);
                prop_assert!(parts.iter().all(|c| matches!(c, std::path::Component::Normal(_))));
                let full = root.join(&relative);
                prop_assert!(full.starts_with("/data/objects"), "escaped: {:?}", full);
                let name = full.file_name().and_then(|n| n.to_str()).unwrap_or_default();
                prop_assert_eq!(name.len(), 64);
                prop_assert!(name.bytes().all(|b| b.is_ascii_hexdigit()));
                prop_assert_eq!(&name[..2], parts[1].as_os_str().to_str().unwrap_or_default());
            }
        }
    }

    #[test]
    fn merges_and_strips_slashes() {
        assert_eq!(
            Key::new("//a///b/").map(|k| k.to_string()),
            Ok("a/b".to_owned())
        );
        assert_eq!(
            Key::from_uri_path("/a%2Fb//c/").map(|k| k.to_string()),
            Ok("a/b/c".to_owned())
        );
    }

    #[test]
    fn decodes_once() {
        assert_eq!(
            Key::from_uri_path("/%252e%252e").map(|k| k.to_string()),
            Ok("%2e%2e".to_owned())
        );
        assert_eq!(Key::from_uri_path("/%2e%2e/x"), Err(KeyError::DotSegment));
    }

    #[test]
    fn refuses_what_d9_lists() {
        assert_eq!(Key::new(""), Err(KeyError::Empty));
        assert_eq!(Key::new("///"), Err(KeyError::Empty));
        assert_eq!(Key::new("a/./b"), Err(KeyError::DotSegment));
        assert_eq!(Key::new("a/../b"), Err(KeyError::DotSegment));
        assert_eq!(Key::new("a\u{0}b"), Err(KeyError::ControlCharacter));
        assert_eq!(Key::new("a\u{7f}b"), Err(KeyError::ControlCharacter));
        assert_eq!(
            Key::from_uri_path("/a%00b"),
            Err(KeyError::ControlCharacter)
        );
        assert_eq!(Key::from_uri_path("/%ff"), Err(KeyError::NotUtf8));
        assert_eq!(Key::new(&"a/".repeat(33)), Err(KeyError::TooManySegments));
        assert!(Key::new(&"a/".repeat(32)).is_ok());
        assert_eq!(
            Key::new(&"a".repeat(1025)),
            Err(KeyError::TooLong { len: 1025 })
        );
        assert!(Key::new(&"a".repeat(1024)).is_ok());
    }
}
