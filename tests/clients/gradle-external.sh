#!/usr/bin/env bash
# Runs an existing Gradle project against a throwaway cachette through an init script, without
# modifying the project: it is built once with a write token from one clone, then again from
# another clone with a read token, and the FROM-CACHE tasks of the second build are counted.
#
#   tests/clients/gradle-external.sh <checkout> [gradle args...]    (default args: build)
#
# The clones are made from the committed HEAD of <checkout>. The project's own ./gradlew is used
# when it has one (GRADLE_VERSION, then the gradle on PATH, otherwise). GRADLE_USER_HOME stays the
# caller's, because a private project may need the credentials and caches kept there; the init
# script turns the local build cache off. Port 18090 (CACHETTE_PORT overrides).
# Exits non-zero when the second build has no FROM-CACHE task.
set -euo pipefail

CLIENTS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "$CLIENTS_ROOT/lib.sh"

[ $# -ge 1 ] || { echo "usage: $0 <checkout> [gradle args...]" >&2; exit 2; }
checkout="$(cd "$1" && pwd)"
shift
[ $# -gt 0 ] || set -- build

start_cachette "${CACHETTE_PORT:-18090}" 512MiB 4GiB
export CACHETTE_URL="$BASE_URL"
init="$CLIENTS_ROOT/gradle-external.init.gradle"

# Both clones use the same gradle command; ./gradlew is resolved inside each clone.
if [ -x "$checkout/gradlew" ]; then
    gradle_cmd="./gradlew"
else
    use_gradle
    gradle_cmd="$GRADLE_BIN"
fi

# gradle_build CLONE TOKEN PUSH LOG ARGS...
gradle_build() {
    clone="$1"; token="$2"; push="$3"; out="$4"; shift 4
    (cd "$clone" && CACHETTE_TOKEN="$token" CACHETTE_PUSH="$push" "$gradle_cmd" --no-daemon \
        --console=plain --build-cache --no-configuration-cache -I "$init" "$@") >"$out" 2>&1 \
        || { tail -n 40 "$out" >&2; fail "build failed in $clone (log kept only until exit)"; }
}

git clone -q --no-hardlinks "$checkout" "$WORK/a"
git clone -q --no-hardlinks "$checkout" "$WORK/b-elsewhere"

log "first build, write token"
gradle_build "$WORK/a" "$WRITE_TOKEN" true "$WORK/first.log" "$@"
log "second build, other checkout, read token"
gradle_build "$WORK/b-elsewhere" "$READ_TOKEN" false "$WORK/second.log" "$@"

# Task lines look like `> Task :app:compileJava FROM-CACHE`; a plain `> Task :x` ran.
count() { grep -cE "$1" "$2" || true; }
first_run="$(count '^> Task [^ ]+$' "$WORK/first.log")"
from_cache="$(count '^> Task .* FROM-CACHE$' "$WORK/second.log")"
second_run="$(count '^> Task [^ ]+$' "$WORK/second.log")"
echo "first build:  $first_run tasks executed"
echo "second build: $from_cache tasks FROM-CACHE, $second_run executed"
grep -Ei 'remote build cache was disabled|Could not (load|store) entry' "$WORK/first.log" "$WORK/second.log" || true
assert_ge "server hits" "$(metric cachette_hits_total)" 1
assert_ge "FROM-CACHE tasks in the second build" "$from_cache" 1
