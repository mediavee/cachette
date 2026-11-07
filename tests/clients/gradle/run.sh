#!/usr/bin/env bash
# Real Gradle against a real cachette: relocation, a refused object (d5) and the read-token push.
#
#   tests/clients/gradle/run.sh [roundtrip|d5|read-push ...]    (default: all three)
#
# Environment:
#   CACHETTE_BIN       the server binary (default target/release/cachette)
#   GRADLE_VERSION     "9.8.1", or "8" for the latest 8.x; default: the gradle on PATH
#   CLIENTS_SKIP_WARMUP=1   skips the build that fills the cache: every check on hits must then
#                      fail, which proves the script goes red on an empty server
# Ports 18090 (roundtrip, read-push) and 18091 (d5).
set -euo pipefail

CLIENTS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib.sh
. "$CLIENTS_ROOT/lib.sh"

PROJECT="$CLIENTS_ROOT/gradle/project"
# The cacheable tasks of the demo project; each must be served from the cache at the second build.
CACHEABLE="compileJava compileTestJava test generateReport"
export GRADLE_USER_HOME="$WORK/gradle-home"
export HOME="$WORK/home"
mkdir -p "$HOME"
use_gradle
log "gradle: $("$GRADLE_BIN" --version | sed -n 's/^Gradle //p' | head -n1) ($GRADLE_BIN)"

# checkout NAME: a fresh copy of the project at its own path, as another CI workspace would be.
checkout() {
    dir="$WORK/$1/workspace"
    rm -rf "${WORK:?}/$1" && mkdir -p "$WORK/$1"
    cp -R "$PROJECT" "$dir"
    echo "$dir"
}

# build DIR TOKEN PUSH EXPECT LOG [GRADLE ARGS...]: the build must succeed.
build() {
    dir="$1"; token="$2"; push="$3"; expect="$4"; out="$5"; shift 5
    (cd "$dir" && CACHETTE_TOKEN="$token" "$GRADLE_BIN" --no-daemon --console=plain --build-cache \
        --no-configuration-cache "-PcachetteUrl=$BASE_URL" "-PcachettePush=$push" \
        "-PcachetteExpectContinue=$expect" "$@") >"$out" 2>&1 \
        || { cat "$out" >&2; fail "build failed in $dir"; }
}

task_state() { # LOG TASK -> FROM-CACHE | EXECUTED | absent
    line="$(grep -E "^> Task :$2( |$)" "$1" | head -n1 || true)"
    case "$line" in
        '') echo absent ;;
        *FROM-CACHE*) echo FROM-CACHE ;;
        *UP-TO-DATE* | *NO-SOURCE* | *SKIPPED*) echo skipped ;;
        *) echo EXECUTED ;;
    esac
}

assert_all_from_cache() { # LOG
    hits=0
    for task in $CACHEABLE; do
        state="$(task_state "$1" "$task")"
        printf '     :%s %s\n' "$task" "$state"
        [ "$state" = "FROM-CACHE" ] && hits=$((hits + 1))
    done
    printf 'FROM-CACHE: %s of %s cacheable tasks\n' "$hits" "$(echo $CACHEABLE | wc -w | tr -d ' ')"
    [ "$hits" -gt 0 ] || fail "zero FROM-CACHE tasks"
    [ "$hits" -eq "$(echo $CACHEABLE | wc -w | tr -d ' ')" ] || fail "some cacheable tasks were not served from the cache"
}

assert_cache_stayed_on() { # LOG
    if grep -Eiq 'remote build cache was disabled|Could not (load|store) entry' "$1"; then
        grep -Ei 'remote build cache was disabled|Could not (load|store) entry' "$1" >&2
        fail "Gradle reported a remote cache problem"
    fi
    echo "ok   no remote cache warning"
}

# Writes with a write token from one checkout, then rebuilds from another with a read token.
roundtrip() { # EXPECT_CONTINUE
    expect="$1"
    log "roundtrip, useExpectContinue=$expect"
    start_cachette 18090 64MiB
    a="$(checkout "rt-$expect-a")"
    b="$(checkout "rt-$expect-b-elsewhere")"
    if [ -z "${CLIENTS_SKIP_WARMUP:-}" ]; then
        build "$a" "$WRITE_TOKEN" true "$expect" "$WORK/rt-$expect-1.log" build generateReport
        assert_cache_stayed_on "$WORK/rt-$expect-1.log"
        assert_ge "entries stored by the first build" "$(metric cachette_entries)" 4
    fi
    build "$b" "$READ_TOKEN" false "$expect" "$WORK/rt-$expect-2.log" build generateReport
    assert_cache_stayed_on "$WORK/rt-$expect-2.log"
    assert_all_from_cache "$WORK/rt-$expect-2.log"
    assert_ge "server hits" "$(metric cachette_hits_total)" 4
}

# d5: an output larger than max_object_size and than any socket buffer is dropped, and Gradle gets
# a 200 (any other status turns its remote cache off); the build passes, the cache stays on, the
# other entries are stored and served.
d5() { # EXPECT_CONTINUE
    expect="$1"
    blob_mib=10
    log "d5, ${blob_mib} MiB output over max_object_size=1MiB, useExpectContinue=$expect"
    start_cachette 18091 1MiB
    a="$(checkout "d5-$expect-a")"
    b="$(checkout "d5-$expect-b-elsewhere")"
    tasks="build generateReport generateBlob -PblobMiB=$blob_mib"
    # shellcheck disable=SC2086
    build "$a" "$WRITE_TOKEN" true "$expect" "$WORK/d5-$expect-1.log" $tasks
    assert_cache_stayed_on "$WORK/d5-$expect-1.log"
    [ "$(task_state "$WORK/d5-$expect-1.log" generateBlob)" = "EXECUTED" ] || fail "generateBlob did not run"
    [ -s "$a/build/blob/random.bin" ] || fail "no blob output"
    size="$(wc -c <"$a/build/blob/random.bin" | tr -d ' ')"
    [ "$size" -ge $((8 * 1024 * 1024)) ] || fail "blob is only $size bytes"
    assert_ge "dropped PUTs (too_large)" "$(metric cachette_puts_dropped_total 'reason="too_large"')" 1
    assert_ge "entries stored besides the blob" "$(metric cachette_entries)" 4

    # The next build: small entries come from the cache, the blob was never stored so it runs
    # again and its PUT is dropped again, still without a warning.
    # shellcheck disable=SC2086
    build "$b" "$WRITE_TOKEN" true "$expect" "$WORK/d5-$expect-2.log" $tasks
    assert_cache_stayed_on "$WORK/d5-$expect-2.log"
    assert_all_from_cache "$WORK/d5-$expect-2.log"
    [ "$(task_state "$WORK/d5-$expect-2.log" generateBlob)" = "EXECUTED" ] || fail "generateBlob should run again"
    assert_ge "dropped PUTs after two builds" "$(metric cachette_puts_dropped_total 'reason="too_large"')" 2
}

# A read token that pushes: Gradle warns, turns the remote cache off for the rest of the build and
# the build still succeeds. The reads that happened before keep their FROM-CACHE.
read_push() {
    log "read token with push = true"
    start_cachette 18090 64MiB
    a="$(checkout "rp-a")"
    b="$(checkout "rp-b-elsewhere")"
    build "$a" "$WRITE_TOKEN" true false "$WORK/rp-1.log" build generateReport
    # generateBlob has never been cached, so it executes and tries to store with the read token.
    build "$b" "$READ_TOKEN" true false "$WORK/rp-2.log" build generateReport generateBlob
    assert_all_from_cache "$WORK/rp-2.log"
    grep -Eq 'Could not store entry .*403' "$WORK/rp-2.log" || { cat "$WORK/rp-2.log" >&2; fail "no store warning for the read token"; }
    grep -q 'remote build cache was disabled' "$WORK/rp-2.log" || fail "no 'disabled' warning"
    grep -E 'Could not store entry|remote build cache was disabled' "$WORK/rp-2.log" | sed 's/^/     /'
    assert_ge "refused writes (403) on the server" "$(metric cachette_requests_total 'status="403"')" 1
}

scenarios="${*:-roundtrip d5 read-push}"
for scenario in $scenarios; do
    case "$scenario" in
        roundtrip) roundtrip false; roundtrip true ;;
        d5) d5 false; d5 true ;;
        read-push) read_push ;;
        *) fail "unknown scenario $scenario" ;;
    esac
done
log "all gradle scenarios passed"
