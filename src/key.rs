//! Cache keys: the normalised path a client addresses an object by.
//!
//! A key is never used as a file name. Objects live under `objects/<2 hex>/<64 hex>` where the hex
//! is the blake3 digest of the normalised key, so no key can address anything outside that tree.

use std::borrow::Cow;
use std::fmt;
use std::path::{Path, PathBuf};

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
        Ok(Self(normalise(decoded)?.into_owned()))
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
        // Most paths carry no escape; skip the byte-wise decoder for them.
        let decoded = if raw.contains('%') {
            percent_decode_str(raw)
                .decode_utf8()
                .map_err(|_| KeyError::NotUtf8)?
        } else {
            Cow::Borrowed(raw)
        };
        Self::new(&decoded)
    }

    /// The normalised key, without leading or trailing `/`.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// The blake3 digest of the normalised key: the store's index key, and the name of the
    /// object's file through [`object_path`] (D9).
    pub(crate) fn digest(&self) -> [u8; 32] {
        *blake3::hash(self.0.as_bytes()).as_bytes()
    }
}

/// Where the object of `digest` lives under the data directory `root`:
/// `<root>/objects/<first 2 hex of the digest>/<64 hex of the digest>`.
///
/// Every component added to `root` is a plain hex name, so the path stays below
/// `<root>/objects` whatever the key was. The store builds every object path here.
pub(crate) fn object_path(root: &Path, digest: &[u8; 32]) -> PathBuf {
    let hex = blake3::Hash::from_bytes(*digest).to_hex();
    let hex = hex.as_str();
    let mut path = PathBuf::with_capacity(root.as_os_str().len() + "/objects/xx/".len() + 64);
    path.push(root);
    path.push("objects");
    path.push(&hex[..2]);
    path.push(hex);
    path
}

/// Validates `decoded` as a key and returns it normalised, borrowed from `decoded` whenever
/// possible.
///
/// A `/` at either end is only trimmed, so the usual key (`/<hex>`, `/<prefix>/a/b/<hex>`) is a
/// sub-slice and allocates nothing. Only a repeated `/` inside the path forces a new string.
fn normalise(decoded: &str) -> Result<Cow<'_, str>, KeyError> {
    let trimmed = decoded.trim_matches('/');
    let mut segments = 0_usize;
    let mut bytes = 0_usize;
    let mut repeated = false;
    for segment in trimmed.split('/') {
        if segment.is_empty() {
            repeated = true;
            continue;
        }
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
        bytes += segment.len();
    }
    if segments == 0 {
        return Err(KeyError::Empty);
    }
    // One `/` between segments.
    let len = bytes + segments - 1;
    if len > MAX_KEY_BYTES {
        return Err(KeyError::TooLong { len });
    }
    if !repeated {
        return Ok(Cow::Borrowed(trimmed));
    }
    let mut normalised = String::with_capacity(len);
    for (index, segment) in trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .enumerate()
    {
        if index > 0 {
            normalised.push('/');
        }
        normalised.push_str(segment);
    }
    Ok(Cow::Owned(normalised))
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {

    use proptest::prelude::*;

    use super::*;

    /// Normalisation as it was before D39: always builds a `String`. Kept as the specification
    /// the borrowing implementation must agree with, byte for byte.
    fn reference_new(decoded: &str) -> Result<String, KeyError> {
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
        Ok(normalised)
    }

    fn reference_from_uri_path(raw: &str) -> Result<String, KeyError> {
        let decoded = percent_decode_str(raw)
            .decode_utf8()
            .map_err(|_| KeyError::NotUtf8)?;
        reference_new(&decoded)
    }

    /// The on-disk path of a key, from its text alone: the digest D9 names the file by.
    fn reference_path(key: &str) -> PathBuf {
        let digest = blake3::hash(key.as_bytes()).to_hex();
        let digest = digest.as_str();
        PathBuf::from(format!("objects/{}/{digest}", &digest[..2]))
    }

    /// What a key and its path are, or why the input is refused, as the reference computes it.
    fn reference_outcome(key: Result<String, KeyError>) -> Result<(String, PathBuf), KeyError> {
        key.map(|text| {
            let path = reference_path(&text);
            (text, path)
        })
    }

    fn outcome(key: Result<Key, KeyError>) -> Result<(String, PathBuf), KeyError> {
        key.map(|key| {
            let path = object_path(Path::new(""), &key.digest());
            (key.as_str().to_owned(), path)
        })
    }

    /// Inputs that reach every branch of normalisation: escapes, edge and repeated slashes, dot
    /// segments, controls, and sizes around both limits.
    fn near_the_rules() -> impl Strategy<Value = String> {
        prop_oneof![
            "(/|%2[fFeE]|%25|%00|%7[fF]|%ff|%[0-9a-fA-F]{2}|[a-c.]|\\u{e9}){0,40}",
            "(/{0,3}[a-b]{1,60}){0,40}",
            "/{0,2}[a-b]{1015,1030}/{0,2}",
            "(/{0,2}[a-b]{0,3}){28,36}",
            "/?([0-9a-f]{32}|[a-c]/){0,3}[0-9a-f]{0,64}/?",
        ]
    }

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
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[test]
        fn new_agrees_with_the_reference(
            input in prop_oneof![awkward(), near_the_rules()]
        ) {
            prop_assert_eq!(
                outcome(Key::new(&input)),
                reference_outcome(reference_new(&input)),
                "input {:?}",
                input
            );
        }

        #[test]
        fn from_uri_path_agrees_with_the_reference(
            input in prop_oneof![awkward(), near_the_rules()]
        ) {
            prop_assert_eq!(
                outcome(Key::from_uri_path(&input)),
                reference_outcome(reference_from_uri_path(&input)),
                "input {:?}",
                input
            );
        }
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
                let full = object_path(root, &key.digest());
                let relative = full.strip_prefix(root).unwrap_or(&full).to_path_buf();
                let parts: Vec<_> = relative.components().collect();
                prop_assert_eq!(parts.len(), 3, "path {:?}", relative);
                prop_assert!(parts.iter().all(|c| matches!(c, std::path::Component::Normal(_))));
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

    #[test]
    fn borrows_unless_a_slash_repeats_inside() {
        for borrowed in ["a/b", "a", "/a/b", "a/b/", "//a/b///"] {
            assert!(
                matches!(normalise(borrowed), Ok(Cow::Borrowed("a/b" | "a"))),
                "{borrowed:?} was copied"
            );
        }
        for rewritten in ["a//b", "/a///b/"] {
            assert_eq!(normalise(rewritten), Ok(Cow::Owned("a/b".to_owned())));
        }
    }

    /// Allocation counts. Only on targets where the test binary may own its global allocator: the
    /// musl build already ships mimalloc as the global one (`main.rs`), and there can be one.
    #[cfg(not(target_env = "musl"))]
    mod allocations {
        use std::hint::black_box;

        use super::*;

        mod counting {
            use std::alloc::{GlobalAlloc, Layout, System};
            use std::cell::Cell;

            thread_local! {
                // Per thread, so tests running in parallel do not count each other's allocations.
                static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
            }

            struct Counting;

            fn record() {
                // Fails only while the thread is being torn down; nothing is left to count then.
                let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
            }

            // SAFETY: every method forwards its arguments unchanged to `System`, which upholds the
            // `GlobalAlloc` contract; counting only touches a const-initialised thread-local `Cell`,
            // which never allocates.
            unsafe impl GlobalAlloc for Counting {
                unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                    record();
                    // SAFETY: the caller upholds the contract of `GlobalAlloc::alloc`.
                    unsafe { System.alloc(layout) }
                }

                unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
                    record();
                    // SAFETY: the caller upholds the contract of `GlobalAlloc::alloc_zeroed`.
                    unsafe { System.alloc_zeroed(layout) }
                }

                unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
                    record();
                    // SAFETY: the caller upholds the contract of `GlobalAlloc::realloc`.
                    unsafe { System.realloc(ptr, layout, new_size) }
                }

                unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                    // SAFETY: the caller upholds the contract of `GlobalAlloc::dealloc`.
                    unsafe { System.dealloc(ptr, layout) }
                }
            }

            #[global_allocator]
            static COUNTING: Counting = Counting;

            /// Runs `work` and returns its result with the allocations this thread made meanwhile.
            pub(super) fn allocations_in<T>(work: impl FnOnce() -> T) -> (T, usize) {
                let before = ALLOCATIONS.with(Cell::get);
                let result = work();
                (result, ALLOCATIONS.with(Cell::get) - before)
            }
        }

        /// A Gradle key and an sccache key, as they reach `from_uri_path`.
        const NORMAL_PATHS: [&str; 2] = [
            "/0123456789abcdef0123456789abcdef",
            "/sccache/a/b/c/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ];

        #[test]
        fn normalising_a_gradle_or_sccache_key_allocates_nothing() {
            for path in NORMAL_PATHS {
                let path = black_box(path);
                let (normalised, allocations) = counting::allocations_in(|| normalise(path));
                assert_eq!(normalised.as_deref(), Ok(&path[1..]), "path {path:?}");
                assert_eq!(allocations, 0, "path {path:?}");
            }
        }

        #[test]
        fn a_key_costs_the_one_allocation_that_owns_it() {
            for path in NORMAL_PATHS {
                let path = black_box(path);
                let (built, allocations) = counting::allocations_in(|| Key::from_uri_path(path));
                assert_eq!(built.map(|key| key.to_string()), Ok(path[1..].to_owned()));
                assert_eq!(allocations, 1, "Key::from_uri_path({path:?})");
            }
        }

        /// Guards the counter itself: a rewrite must allocate, or a count of zero proves nothing.
        #[test]
        fn the_counter_sees_a_rewrite() {
            let (rewritten, allocations) =
                counting::allocations_in(|| normalise(black_box("/a//b")));
            assert_eq!(rewritten, Ok(Cow::Owned("a/b".to_owned())));
            assert!(
                allocations >= 1,
                "the counter saw {allocations} allocations"
            );
        }
    }
}
