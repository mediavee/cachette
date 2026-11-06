#!/usr/bin/env bash
# Real sccache (pinned v0.19.0 release binary) against a real cachette, over WebDAV (D8, D26).
#
#   tests/clients/sccache/run.sh [default|no-create-dir ...]    (default: both)
#
# First build writes with a write token; after `cargo clean` the second build reads with a read
# token in READ_ONLY mode and must hit remotely.
#
# Environment:
#   CACHETTE_BIN       the server binary (default target/release/cachette)
#   SCCACHE_BIN        use this sccache instead of the pinned download
#   CLIENTS_SKIP_WARMUP=1   skips the first build: the hit checks must then fail
# Ports 18092 (cachette) and 18094 (the sccache server); 18093 stays free for the second variant.
set -euo pipefail

CLIENTS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib.sh
. "$CLIENTS_ROOT/lib.sh"

CRATE="$CLIENTS_ROOT/sccache/crate"
# Cargo and rustup keep their state under HOME unless told otherwise: pin them before HOME moves.
export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
if [ -d "$HOME/.rustup" ]; then export RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"; fi
export HOME="$WORK/home"
mkdir -p "$HOME"
fetch_sccache
log "sccache: $("$SCCACHE_BIN" --version) ($SCCACHE_BIN)"

SCCACHE_PORT=18094
export SCCACHE_SERVER_PORT="$SCCACHE_PORT"
export SCCACHE_CONF="$WORK/no-sccache-config.toml"
export SCCACHE_DIR="$WORK/sccache-local"
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$WORK/target"
export RUSTC_WRAPPER="$SCCACHE_BIN"

# sccache_server: restarts the sccache server with the given WebDAV settings (it reads its
# environment once, at start).
sccache_server() { # TOKEN [extra VAR=VALUE...]
    token="$1"; shift
    "$SCCACHE_BIN" --stop-server >/dev/null 2>&1 || true
    env SCCACHE_WEBDAV_ENDPOINT="$BASE_URL" SCCACHE_WEBDAV_TOKEN="$token" "$@" \
        "$SCCACHE_BIN" --start-server
}

stats() { "$SCCACHE_BIN" --show-stats --stats-format json; }

build() { (cd "$CRATE" && cargo build --locked --quiet); }

variant() { # NAME PORT DIRECTORY_TRAFFIC(yes|no) [VAR=VALUE...]
    name="$1"; port="$2"; directories="$3"; shift 3
    log "sccache variant: $name"
    start_cachette "$port" 64MiB
    rm -rf "$CARGO_TARGET_DIR"

    if [ -z "${CLIENTS_SKIP_WARMUP:-}" ]; then
        sccache_server "$WRITE_TOKEN" "$@"
        build
        s="$(stats)"
        printf 'first build: %s misses, %s writes\n' \
            "$(echo "$s" | jq '[.stats.cache_misses.counts[]] | add // 0')" "$(echo "$s" | jq '.stats.cache_writes')"
        assert_ge "cache writes" "$(echo "$s" | jq '.stats.cache_writes')" 4
        assert_ge "entries stored on the server" "$(metric cachette_entries)" 4
    fi

    (cd "$CRATE" && cargo clean --quiet)
    # The second build reads only: a read token, and sccache told not to write.
    sccache_server "$READ_TOKEN" SCCACHE_WEBDAV_RW_MODE=READ_ONLY "$@"
    "$SCCACHE_BIN" --zero-stats >/dev/null
    build
    s="$(stats)"
    echo "$s" | jq '.stats | {cache_hits, cache_misses, cache_errors, cache_read_errors, cache_write_errors}' -c
    "$SCCACHE_BIN" --show-stats | sed -n '1,/^Cache location/p' | sed 's/^/     /' | grep -E 'Compile requests|Cache hits|Cache misses|Cache location'
    hits="$(echo "$s" | jq '[.stats.cache_hits.counts[]] | add // 0')"
    [ "$hits" -gt 0 ] || fail "zero sccache hits"
    assert_ge "sccache remote hits" "$hits" 4
    [ "$(echo "$s" | jq '.stats.cache_read_errors // 0')" -eq 0 ] || fail "sccache read errors"
    assert_ge "server hits" "$(metric cachette_hits_total)" "$hits"
    echo "$s" | jq -e '.cache_location | test("webdav"; "i")' >/dev/null || fail "sccache is not using the WebDAV backend"

    propfinds="$(metric cachette_requests_total 'method="PROPFIND"')"
    mkcols="$(metric cachette_requests_total 'method="MKCOL"')"
    printf 'directory traffic on the server: PROPFIND=%s MKCOL=%s\n' "$propfinds" "$mkcols"
    if [ "$directories" = "yes" ]; then
        assert_ge "PROPFIND before PUT" "$propfinds" 1
    else
        [ "$propfinds" -eq 0 ] && [ "$mkcols" -eq 0 ] || fail "SCCACHE_WEBDAV_DISABLE_CREATE_DIR still sent directory requests"
    fi
    "$SCCACHE_BIN" --stop-server >/dev/null 2>&1 || true
    stop_cachette
}

scenarios="${*:-default no-create-dir}"
for scenario in $scenarios; do
    case "$scenario" in
        default) variant default 18092 yes ;;
        no-create-dir) variant no-create-dir 18093 no SCCACHE_WEBDAV_DISABLE_CREATE_DIR=true ;;
        *) fail "unknown scenario $scenario" ;;
    esac
done
log "all sccache scenarios passed"
