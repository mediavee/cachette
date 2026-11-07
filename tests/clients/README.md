# Real clients against the real binary

Each script starts its own throwaway `cachette` (random tokens, temporary data directory, fixed
local ports) and drives the genuine client. Nothing is installed globally.

| Script | What it proves |
|---|---|
| `gradle/run.sh roundtrip` | A build with a `write` token fills the cache; after the project is copied elsewhere, a build with a `read` token serves every cacheable task `FROM-CACHE`. With and without `useExpectContinue`. |
| `gradle/run.sh d5` | An output of 10 MiB (more than any socket buffer) over `max_object_size = 1MiB` is dropped with a 200, the status Gradle needs (a 413 disables its remote cache): the build passes, no remote-cache warning, the other entries are stored and served at the next build. With and without `useExpectContinue`. |
| `gradle/run.sh read-push` | A `read` token with `push = true`: reads work, the first store gets a 403, Gradle warns and turns the remote cache off for the rest of that build, and the build succeeds. |
| `sccache/run.sh` | sccache 0.19.0 (pinned release binary, checksum checked) over WebDAV with `SCCACHE_WEBDAV_TOKEN`: build, `cargo clean`, second build reads with a `read` token in `READ_ONLY` mode and reports remote hits. Second variant with `SCCACHE_WEBDAV_DISABLE_CREATE_DIR=true`, which must send no `PROPFIND` and no `MKCOL`. `sccache/run.sh drop`: `max_object_size` splits the demo crate's objects, the oversized ones get a 413 (counted by sccache as failed writes), the build succeeds, the others are stored and the second build hits them. |
| `gradle-external.sh <checkout>` | Applies the cache to any Gradle project through an init script, without modifying it. Prints the `FROM-CACHE` count of the second build. |

```sh
cargo build --release
tests/clients/gradle/run.sh                      # the gradle on PATH
GRADLE_VERSION=9.8.1 tests/clients/gradle/run.sh # downloaded and checksum-verified
GRADLE_VERSION=8 tests/clients/gradle/run.sh     # latest 8.x; needs a JDK 21 (JAVA_HOME)
tests/clients/sccache/run.sh
tests/clients/gradle-external.sh ~/path/to/checkout build
```

`CACHETTE_BIN` points at another binary. `CLIENTS_CACHE` is where Gradle distributions and the
sccache binary are kept between runs. `CLIENTS_KEEP_WORK=1` keeps the temporary directory.

## Every check fails on a cache that holds nothing

`CLIENTS_SKIP_WARMUP=1` skips the build that fills the cache. The hit checks then fail
(`zero FROM-CACHE tasks`, `zero sccache hits`), which the `clients` workflow asserts in a control
step.

## What Gradle prints for a `read` token that pushes

Gradle 8.14 and 9.8, the second build of `read-push`. The cacheable tasks were served from the cache
and `generateBlob`, never cached, ran and tried to store:

```
> Task :compileJava FROM-CACHE
> Task :test FROM-CACHE
> Task :generateBlob
Could not store entry <key> in remote build cache: Storing entry at 'http://127.0.0.1:18090/<key>' response status 403: Forbidden
The remote build cache was disabled during the build due to errors.

BUILD SUCCESSFUL
```

## The Gradle project

`gradle/project` builds Java 17 sources with JUnit 5 from Maven Central. Its cacheable tasks are
`compileJava`, `compileTestJava`, `test` and `generateReport`, a custom `@CacheableTask` writing 64
KiB of seeded pseudo-random bytes. `generateBlob` is the same task class sized by `-PblobMiB`; the
bytes are random so the packed entry is as large as the output. `processResources` and `jar` are
not cacheable in Gradle and are not asserted.
