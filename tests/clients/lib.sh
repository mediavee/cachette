#!/usr/bin/env bash
# Shared helpers for the real-client scripts: a throwaway cachette, token generation, assertions.
# Sourced, never run. Works on bash 3.2 (macOS) and on the Linux runners.

: "${CLIENTS_ROOT:?set by the calling script}"
REPO_ROOT="$(cd "$CLIENTS_ROOT/../.." && pwd)"
CACHETTE_BIN="${CACHETTE_BIN:-$REPO_ROOT/target/release/cachette}"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/cachette-clients.XXXXXX")"
SERVER_PID=""

log() { printf '== %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# Stops only the server this run started, by pid, and removes the work directory. On failure the
# server log is printed first: it holds the refusals and dropped PUTs, never a secret.
cleanup() {
    status=$?
    stop_cachette
    if [ "$status" -ne 0 ] && [ -f "$WORK/cachette.log" ]; then
        printf -- '--- cachette log (tail) ---\n' >&2
        tail -n 40 "$WORK/cachette.log" >&2
    fi
    if [ -z "${CLIENTS_KEEP_WORK:-}" ]; then rm -rf "$WORK"; fi
    exit "$status"
}
trap cleanup EXIT

random_token() { openssl rand -hex 16; }

# start_cachette PORT MAX_OBJECT_SIZE [MAX_SIZE]
# Exports WRITE_TOKEN and READ_TOKEN (fresh per start) and BASE_URL.
start_cachette() {
    port="$1"
    max_object="$2"
    max_size="${3:-256MiB}"
    [ -x "$CACHETTE_BIN" ] || fail "no cachette binary at $CACHETTE_BIN (cargo build --release, or set CACHETTE_BIN)"
    stop_cachette
    WRITE_TOKEN="$(random_token)"
    READ_TOKEN="$(random_token)"
    BASE_URL="http://127.0.0.1:$port"
    rm -rf "$WORK/data" && mkdir -p "$WORK/data"
    # An empty HOME-less environment: only what the server needs, tokens through the variable
    # (flags show in the process list).
    env -i PATH="$PATH" HOME="$WORK/home" \
        CACHETTE_LISTEN="127.0.0.1:$port" \
        CACHETTE_DATA_DIR="$WORK/data" \
        CACHETTE_MAX_SIZE="$max_size" \
        CACHETTE_MAX_OBJECT_SIZE="$max_object" \
        CACHETTE_ACCESS_LOG="all" \
        CACHETTE_TOKENS="ci-main:write:$WRITE_TOKEN,dev:read:$READ_TOKEN" \
        "$CACHETTE_BIN" >"$WORK/cachette.log" 2>&1 &
    SERVER_PID=$!
    i=0
    while [ "$i" -lt 100 ]; do
        kill -0 "$SERVER_PID" 2>/dev/null || fail "cachette exited at start (port $port busy?)"
        code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/healthz" || true)"
        [ "$code" = "200" ] && return 0
        sleep 0.1
        i=$((i + 1))
    done
    fail "cachette did not become healthy on port $port"
}

stop_cachette() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

# metric NAME [LABEL_FRAGMENT]: sum of the matching series on /metrics, 0 if absent.
metric() {
    curl -s "$BASE_URL/metrics" | awk -v name="$1" -v frag="${2:-}" '
        $0 !~ /^#/ && index($0, name) == 1 {
            split($0, parts, " "); series = parts[1]
            base = series; sub(/\{.*/, "", base)
            if (base == name && index(series, frag) > 0) sum += parts[2]
        }
        END { print sum + 0 }'
}

assert_ge() { # DESCRIPTION ACTUAL MINIMUM
    [ "$2" -ge "$3" ] || fail "$1: got $2, expected at least $3"
    printf 'ok   %s: %s (>= %s)\n' "$1" "$2" "$3"
}

# Resolves GRADLE_VERSION ("9.8.1", or "8" for the latest 8.x release) and puts that distribution
# first on PATH, downloaded once into $CLIENTS_CACHE (default under TMPDIR) and checked against the
# checksum Gradle publishes. Without GRADLE_VERSION the gradle on PATH is used as it is.
use_gradle() {
    [ -n "${GRADLE_VERSION:-}" ] || { GRADLE_BIN="$(command -v gradle)"; return 0; }
    version="$GRADLE_VERSION"
    case "$version" in
        *.*) ;;
        *) version="$(curl -fsS https://services.gradle.org/versions/all | jq -r --arg major "$version" '
              [ .[] | select(.snapshot == false and .nightly == false and .releaseNightly == false
                             and .rcFor == "" and .milestoneFor == "" and .broken == false)
                    | .version | select(test("^" + $major + "\\.[0-9]+(\\.[0-9]+)?$")) ][0]')" ;;
    esac
    [ -n "$version" ] && [ "$version" != "null" ] || fail "cannot resolve Gradle $GRADLE_VERSION"
    cache="${CLIENTS_CACHE:-${TMPDIR:-/tmp}/cachette-clients-cache}"
    mkdir -p "$cache"
    dir="$cache/gradle-$version"
    if [ ! -x "$dir/bin/gradle" ]; then
        url="https://services.gradle.org/distributions/gradle-$version-bin.zip"
        part="$cache/gradle-$version-bin.zip"
        curl -fsSL "$url" -o "$part"
        expected="$(curl -fsSL "$url.sha256")"
        actual="$(shasum -a 256 "$part" | awk '{print $1}')"
        [ "$expected" = "$actual" ] || fail "Gradle $version checksum mismatch"
        unpack="$(mktemp -d "$cache/unpack.XXXXXX")"
        unzip -q "$part" -d "$unpack"
        mv "$unpack/gradle-$version" "$dir"
        rmdir "$unpack"
        rm -f "$part"
    fi
    GRADLE_BIN="$dir/bin/gradle"
}

# fetch_sccache: the pinned sccache release binary into $CLIENTS_CACHE, never installed anywhere
# else. SCCACHE_BIN set by the caller wins. The sha256 values are the ones sccache publishes next
# to each asset (checked when they were pinned).
SCCACHE_VERSION="v0.19.0"
fetch_sccache() {
    if [ -n "${SCCACHE_BIN:-}" ]; then return 0; fi
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64)
            target="x86_64-unknown-linux-musl"
            sha="aa73c7270c997894038c0d8f0d297865f57569e1687883322bc431713d4bb9dd" ;;
        Darwin-arm64)
            target="aarch64-apple-darwin"
            sha="a35a0b7d622618774bc1bb8102d72562dfb21e619f88b153b370d7f89bc286af" ;;
        *) fail "no pinned sccache for $(uname -s)-$(uname -m); set SCCACHE_BIN" ;;
    esac
    cache="${CLIENTS_CACHE:-${TMPDIR:-/tmp}/cachette-clients-cache}"
    dir="$cache/sccache-$SCCACHE_VERSION-$target"
    if [ ! -x "$dir/sccache" ]; then
        mkdir -p "$cache"
        archive="$cache/sccache-$SCCACHE_VERSION-$target.tar.gz"
        curl -fsSL "https://github.com/mozilla/sccache/releases/download/$SCCACHE_VERSION/sccache-$SCCACHE_VERSION-$target.tar.gz" -o "$archive"
        [ "$(shasum -a 256 "$archive" | awk '{print $1}')" = "$sha" ] || fail "sccache archive checksum mismatch"
        unpack="$(mktemp -d "$cache/unpack.XXXXXX")"
        tar -xzf "$archive" -C "$unpack"
        mkdir -p "$dir"
        mv "$unpack/sccache-$SCCACHE_VERSION-$target/sccache" "$dir/sccache"
        rm -rf "$unpack" "$archive"
    fi
    SCCACHE_BIN="$dir/sccache"
}
