# cachette

A remote build cache server written in Rust. One static binary. It speaks the protocols of
Gradle's `HttpBuildCache` and of sccache's WebDAV backend, stores objects on disk under a hard size
ceiling, and exposes Prometheus metrics.

## Why

Develocity's free Build Cache Node is going away. Its documentation says support ends on
**31 December 2026** ([user manual](https://docs.develocity.ai/bcn/21.2/)), and that Develocity
2027.1 and later will not support the component
([Develocity Build Cache](https://docs.develocity.ai/2026.3/using-develocity/build-cache/)). The vendor points to
Develocity Edge as the replacement.

Gradle's remote cache is a plain HTTP protocol, so any server that speaks it can take the node's
place. cachette is one, built to be small to run and honest about its limits.

## What it does

- **Gradle** `HttpBuildCache`, and **sccache** over WebDAV. Both are proven with the real client in
  CI ([`clients` workflow](.github/workflows/clients.yml)).
- A disk store bounded by `max_size`. The bound covers objects, uploads in flight and evicted objects
  not yet deleted. Eviction is least recently used, and the order survives a restart.
- Named tokens, `read` or `write`. The secret is accepted as the Basic password (Gradle), as a
  Bearer token (sccache), or as base64url Basic.
- A PUT the server will not store (too large, no room) still answers `200`, because Gradle turns
  its remote cache off for the rest of the build on any other status. See
  [protocol](docs/protocol.md#a-put-the-server-does-not-store).
- Prometheus metrics, text or JSON logs, optional TLS, graceful shutdown.
- Static musl binaries for Linux, a macOS binary, a `FROM scratch` container image, a hardened
  systemd unit.

[Maven](#maven-and-bazel) and Bazel should work. They are not tested.

## Quick start

### Binary

Each `v*` tag publishes archives for Linux x86_64 and aarch64 (static musl) and macOS aarch64, with
a `SHA256SUMS` file, on the [releases page](https://github.com/mediavee/cachette/releases)
([`release.yml`](.github/workflows/release.yml)).

Or build it. It needs Rust 1.89 or later:

```sh
cargo build --release        # target/release/cachette
```

Run it. `max_size` and a token are the only things it needs:

```sh
CI_TOKEN=$(openssl rand -hex 32)
DEV_TOKEN=$(openssl rand -hex 32)
echo "write: $CI_TOKEN"; echo "read:  $DEV_TOKEN"

CACHETTE_MAX_SIZE=20GiB \
CACHETTE_DATA_DIR=/var/lib/cachette \
CACHETTE_TOKENS="ci-main:write:$CI_TOKEN,developers:read:$DEV_TOKEN" \
./cachette
```

It listens on `127.0.0.1:5071`. Set `CACHETTE_LISTEN` to serve other machines. Check it:

```sh
curl http://127.0.0.1:5071/healthz                                   # 200 once ready
curl -u any:$CI_TOKEN -X PUT --data-binary hello http://127.0.0.1:5071/some/key   # 201
curl -u any:$DEV_TOKEN http://127.0.0.1:5071/some/key                # hello
```

All settings: [docs/configuration.md](docs/configuration.md).

### Container

```sh
docker run -d --name cachette \
  -v cachette-data:/data -p 5071:5071 \
  -e CACHETTE_MAX_SIZE=26GiB \
  -e CACHETTE_TOKENS="ci-main:write:<secret>,developers:read:<secret>" \
  --stop-timeout 120 \
  ghcr.io/mediavee/cachette
```

Two things matter:

- **`--stop-timeout 120`.** On SIGTERM the server drains requests (up to 30 s) and syncs the disk.
  Docker's default is 10 s, then `SIGKILL`. Under the default `purge-on-crash` durability, a kill
  empties the cache at the next start.
- **A bind mount must be owned by uid 65532**, the user the image runs as. A named volume already
  is. Without it the server stops at start with `cannot open the data directory`.

Details: [docs/operations.md](docs/operations.md#container). The image is published on each tag.

### systemd

```sh
install -m 0755 cachette /usr/local/bin/cachette
install -m 0644 contrib/systemd/cachette.service /etc/systemd/system/
install -d /etc/cachette
install -m 0600 contrib/cachette.example.toml /etc/cachette/cachette.toml   # then edit it
systemctl enable --now cachette
```

The [unit](contrib/systemd/cachette.service) runs as a dynamic user, writes only
`/var/lib/cachette`, and gives the service two minutes to stop. The
[example configuration](contrib/cachette.example.toml) explains each value.

## Configure the clients

Give `write` to the CI of your main branch only. Give every other build and every developer a
`read` token. A `read` token that tries to push gets `403`: Gradle warns and turns its remote cache
off for that build, which makes the mistake visible.

### Gradle

The secret goes in as the Basic **password**. The user name is ignored.

`settings.gradle`:

```groovy
buildCache {
    remote(HttpBuildCache) {
        url = 'http://cachette.internal:5071/'
        allowInsecureProtocol = true
        push = Boolean.parseBoolean(System.getenv('CACHETTE_PUSH') ?: 'false')
        credentials {
            username = 'gradle'
            password = System.getenv('CACHETTE_TOKEN')
        }
    }
}
```

Set `CACHETTE_TOKEN` to the token, and `CACHETTE_PUSH=true` on the CI of `main` only. These are
variables of your build, not settings of the server. Gradle's
local cache can stay on. Run builds with `--build-cache`, or set `org.gradle.caching=true`.

Or an init script, which leaves the project untouched. This is
[`tests/clients/gradle-external.init.gradle`](tests/clients/gradle-external.init.gradle) as CI runs it:

```groovy
gradle.settingsEvaluated { settings ->
    settings.buildCache {
        local {
            enabled = false
        }
        remote(HttpBuildCache) {
            url = System.getenv('CACHETTE_URL') + '/'
            allowInsecureProtocol = true
            push = Boolean.parseBoolean(System.getenv('CACHETTE_PUSH') ?: 'true')
            credentials {
                username = 'gradle'
                password = System.getenv('CACHETTE_TOKEN')
            }
        }
    }
}
```

Use it with `gradle -I cachette.init.gradle --build-cache build`. The tested script turns the local
cache off so that a `FROM-CACHE` line can only come from cachette. You may keep yours.

The tested equivalent is
[`tests/clients/gradle/project/settings.gradle`](tests/clients/gradle/project/settings.gradle). It has
the same credentials and `allowInsecureProtocol` lines, and reads its URL and `push` from Gradle
properties so that one project serves every scenario.

**`allowInsecureProtocol = true`** is required by Gradle for a plain `http://` URL. Gradle sends the
credentials in clear text then. Use it only behind a private network or a VPN. With TLS
(`tls_cert` and `tls_key`), use an `https://` URL and drop that line.

### sccache

```sh
export SCCACHE_WEBDAV_ENDPOINT=http://cachette.internal:5071
export SCCACHE_WEBDAV_TOKEN=<token>
export SCCACHE_WEBDAV_DISABLE_CREATE_DIR=true
export RUSTC_WRAPPER=sccache
# developers and branch CI, which hold a read token:
export SCCACHE_WEBDAV_RW_MODE=READ_ONLY
```

These are the variables [`tests/clients/sccache/run.sh`](tests/clients/sccache/run.sh) sets for the
real sccache 0.19.0. The token is sent as a Bearer token. sccache reads its environment when its
server starts: run `sccache --stop-server` after a change.

**Why `SCCACHE_WEBDAV_DISABLE_CREATE_DIR=true`.** By default sccache's WebDAV backend sends
`PROPFIND`, and `MKCOL` when needed, before each write, to make sure the parent directory exists
([sccache docs](https://github.com/mozilla/sccache/blob/v0.19.0/docs/Webdav.md#disable-create-dir)).
cachette has no directories: it answers that every one exists, so sccache works without the
variable. Setting it removes that extra request per write. We have not measured what that saves. CI
runs both variants: with the variable the server sees no `PROPFIND` and no `MKCOL`, without it at
least one `PROPFIND`.

### Maven and Bazel

They should work, and are untested. Keys are paths, so the Maven build cache extension
(`v1.2/<groupId>/<artifactId>/<checksum>/<file>`) and Bazel (`/ac/<hex>`, `/cas/<hex>`) fit. No test
runs a real Maven or Bazel. They become "supported" the day one does. What each client sends, from
its code: [docs/protocol.md](docs/protocol.md#maven-build-cache-extension-should-work-untested).

## Compared with nginx WebDAV, fosterstack/cache and bazel-remote

Every line below points to a benchmark file, a test or a CI run. Losses are listed with the gains.

### How the numbers were made

[`bench/run.py`](bench/run.py) ran on a 16-thread Linux x86_64 host, from the source of commit
`deef48e`. Results:
[`bench/results/2026-11-06T204503Z-dev2.md`](bench/results/2026-11-06T204503Z-dev2.md). Method:
[`bench/README.md`](bench/README.md).

- Each server ran alone in a container on `--network host`, on loopback. Load came from
  [oha](https://github.com/hatoo/oha) 1.14.0, HTTP/1.1 keep-alive, 10 s per point.
- Sizes 4 KiB, 64 KiB, 1 MiB and 32 MiB, at 1, 16 and 64 clients. Each point ran twice in a row. The
  numbers below are the **mean of the two runs**.
- The cap was 4 GiB for every server that has one.
- Versions: nginx 1.26.3 with `libnginx-mod-http-dav-ext`, bazel-remote v2.6.4 (uncompressed storage,
  endpoint metrics off), fosterstack/cache at `31e69559` with its defaults.
- Durability differs, and it decides PUT speed. cachette ran in both of its modes. nginx does not
  fsync. bazel-remote and fosterstack do.
- nginx and bazel-remote ran without authentication. cachette and fosterstack checked a Basic
  credential. No TLS anywhere.
- bazel-remote stores under `/cas/<sha256>` and its PUT test re-uploads one object, so its PUT does
  not necessarily do the same work as the others.

Read the numbers with these limits:

- The page cache was hot, so disk reads are mostly absent. Loopback has no network cost.
- The host was shared. Its one-minute load average was 1.03 at the start and 28.23 at the end. Load
  is recorded for every point in the raw data; it is not a guarantee of isolation.
- Repeatability: 59 of the 60 GET pairs agree within ±10 % (the exception is fosterstack at 32 MiB
  with 16 clients). **PUT pairs do not**: 19 of the 60 differ by more than 10 %, 15 of them at 1 MiB
  and above ([Repeatability, lines 324 to 450](bench/results/2026-11-06T204503Z-dev2.md#repeatability)).
  Treat PUT rankings at 1 MiB and above as rough.

### Clients at 16 connections

Requests per second, mean of two runs. Rows are the sizes. Per-run rows, all concurrencies and p50
and p99: [Throughput and latency, lines 75 to 322](bench/results/2026-11-06T204503Z-dev2.md#throughput-and-latency).

| | cachette `purge-on-crash` | cachette `fsync` | nginx | bazel-remote | fosterstack |
|---|---:|---:|---:|---:|---:|
| **GET** 4 KiB | 283 206 | 280 560 | 587 353 | 271 656 | 186 |
| **GET** 64 KiB | 198 235 | 191 169 | 425 549 | 241 857 | 188 |
| **GET** 1 MiB | 23 944 | 25 328 | 77 164 | 69 092 | 189 |
| **GET** 32 MiB | 781 | 831 | 2 156 | 2 492 | 295 |
| **PUT** 4 KiB | 53 827 | 1 159 | 91 528 | 1 597 | 91 |
| **PUT** 64 KiB | 30 981 | 1 493 | 24 175 | 1 442 | 89 |
| **PUT** 1 MiB | 4 284 | 1 072 | 1 827 | 1 052 | 87 |
| **PUT** 32 MiB | 75 | 31 | 52 | 39 | 18 |

### Line by line

| Claim | Result | Evidence |
|---|---|---|
| **GET hits against nginx**, all sizes and concurrencies | **Behind.** nginx is 1.4 times faster (4 KiB, 1 client) to 3.6 times (32 MiB, 1 client) in `purge-on-crash`. Its p99 is lower too at 16 and 64 clients, at every size. | [bench, Throughput and latency](bench/results/2026-11-06T204503Z-dev2.md#throughput-and-latency) |
| **GET hits against bazel-remote** | **Level at 4 KiB** (0.96 to 1.17 times its rate), **behind from 64 KiB up**: 1.2 to 1.5 times at 64 KiB, 2.2 to 3.1 times at 1 MiB, 3.2 to 3.7 times at 32 MiB. | [bench, Throughput and latency](bench/results/2026-11-06T204503Z-dev2.md#throughput-and-latency) |
| **GET p99** against bazel-remote | **Ahead at 4 KiB and 64 KiB** with 16 and 64 clients (0.14 against 0.24 ms, 0.47 against 1.21 ms at 4 KiB). **Behind at 1 MiB and 32 MiB**: 8.6 against 3.8 ms at 1 MiB and 64 clients. | same section |
| **Peak memory** during 32 MiB GETs at 64 clients | **Behind.** cachette 196 to 202 MiB (`purge-on-crash`) and 179 to 185 MiB (`fsync`); nginx 97, fosterstack 95, bazel-remote 46 to 47 (two runs each). Sampled every 100 ms, summed over processes; shared pages can count twice. Measured in this one cell only. | [bench, lines 123, 124, 171, 172, 219, 220, 267, 268, 315 and 316](bench/results/2026-11-06T204503Z-dev2.md#throughput-and-latency) (column Peak RSS) |
| **PUT against bazel-remote** | **Far ahead in `purge-on-crash`** for small objects: 9 to 132 times at 4 KiB, 3 to 77 times at 64 KiB, 2 to 15 times at 1 MiB. At 32 MiB it ranges from 3 times ahead to 0.5 times (bazel-remote ahead at 64 clients). **This compares no fsync with fsync.** In `fsync` mode the two are level, within about 1.5 times either way: bazel-remote ahead for 4 KiB with 1 and 16 clients (225 against 169, 1 597 against 1 159 per second). | [bench, Throughput and latency](bench/results/2026-11-06T204503Z-dev2.md#throughput-and-latency) |
| **PUT against nginx**, both without per-object fsync | **Mixed.** nginx is 1.4 to 1.7 times faster at 4 KiB. At 64 KiB nginx is ahead with 1 and 64 clients, cachette with 16. cachette is ahead at 1 MiB with 16 and 64 clients (2.3 and 1.8 times). The larger sizes are the unstable ones. | [bench, Throughput and latency](bench/results/2026-11-06T204503Z-dev2.md#throughput-and-latency) |
| **Disk ceiling** under pressure (3 times the cap in distinct 32 MiB objects, 16 clients) | **Ahead.** Allocated bytes peaked at 0.96 of the cap (`purge-on-crash`) and 0.98 (`fsync`). bazel-remote reached 1.001, fosterstack 1.13. nginx has no cap and reached 3.0. The measure includes directories, metadata and temporary files. | [bench, lines 451 to 688](bench/results/2026-11-06T204503Z-dev2.md#eviction-pressure); the store test `ceiling_holds_with_pending_deletions_held` in [`src/store/mod.rs`](src/store/mod.rs) |
| **What the ceiling costs** | **Dropped writes.** In that test cachette answered `200` without storing 82 of 384 uploads (`purge-on-crash`) and 154 of 384 (`fsync`). nginx, bazel-remote and fosterstack stored all 384. | [bench, HTTP probes, lines 50 and 55](bench/results/2026-11-06T204503Z-dev2.md#http-probes) |
| **fosterstack GET** | **Slow: 184 to 191 requests per second** at 4 KiB, 64 KiB and 1 MiB, at 1, 16 and 64 clients, with p99 up to 406 ms. Its code does a database write transaction for each GET ([`cache.go`](https://github.com/fosterstack/cache/blob/31e69559cde78277192281173176a9e25d13a3bb/internal/cache/cache.go#L337-L346)); we did not isolate that as the cause. | [bench, Throughput and latency](bench/results/2026-11-06T204503Z-dev2.md#throughput-and-latency) |
| **fosterstack PUT at 64 clients** | **99.94 to 99.99 % of requests failed**, whatever the size. They are `429` answers: its default limit is 32 concurrent uploads (`FSCACHE_MAX_CONCURRENT_UPLOADS`, [`main.go`](https://github.com/fosterstack/cache/blob/31e69559cde78277192281173176a9e25d13a3bb/cmd/fscache/main.go#L59)). At 4 KiB, 916 of 4 338 580 requests were stored (201), the rest were `429`. We ran its defaults; raising the limit would change this. | [bench, Throughput and latency](bench/results/2026-11-06T204503Z-dev2.md#throughput-and-latency) (Errors %), and the raw `statusCodeDistribution` ([line 17825](bench/results/2026-11-06T204503Z-dev2.md)) |
| **Gradle never loses its remote cache to a refused object** | **Proven with the real Gradle**, with and without `useExpectContinue`: a 10 MiB output over a 1 MiB limit is dropped with a `200`, the build passes, no warning, the other entries are served next build. Not tested against the other servers. | [`gradle/run.sh d5`](tests/clients/gradle/run.sh), [`clients` run on `main`](https://github.com/mediavee/cachette/actions/runs/42291630374), [`http_limits.rs`](tests/http_limits.rs) |
| **sccache** | **Proven with the real sccache 0.19.0**: second build reads remote hits with a `read` token. The bench probed nginx with `dav-ext` for `PROPFIND` (answer `207`) but did not run sccache against nginx, bazel-remote or fosterstack. | [`clients` run on `main`](https://github.com/mediavee/cachette/actions/runs/42291630374), [`tests/clients/sccache/run.sh`](tests/clients/sccache/run.sh), [bench, HTTP probes](bench/results/2026-11-06T204503Z-dev2.md#http-probes) |
| **Gradle round trip** | **Proven**: a build with a `write` token fills the cache, a build from another directory with a `read` token serves every cacheable task `FROM-CACHE`. Gradle 9.8.1 and the latest 8.x, Linux and macOS. | [`tests/clients/gradle/run.sh`](tests/clients/gradle/run.sh), [`clients` run on `main`](https://github.com/mediavee/cachette/actions/runs/42291630374) |
| **Gate** | `cargo fmt`, `clippy -D warnings`, `cargo test` and `cargo deny` pass on Linux and macOS, plus a build on the declared Rust version and the tests on musl. | [`CI` run on `main`](https://github.com/mediavee/cachette/actions/runs/42291630227), [`ci.yml`](.github/workflows/ci.yml) |

### What this does not show

- **The real network.** Everything above is loopback. A run from another machine was not made.
  Over a real network with a round trip of milliseconds, per-request costs of microseconds matter less.
- **Real builds at scale.** The Gradle project in CI is small (Java 17, JUnit 5, four cacheable
  tasks). No result on a large project is published.
- **PUT rankings at 1 MiB and above**, as said above.
- **Startup time with many entries**, memory with many entries, TLS throughput, macOS throughput.
- **Maven and Bazel**, which are untested.
- Other servers' behaviours that this table does not list. Absence from the table is not a claim.

To replay the benchmark: [`bench/README.md`](bench/README.md).

## Documentation

- [docs/configuration.md](docs/configuration.md): every key, default, environment variable and limit.
- [docs/operations.md](docs/operations.md): metrics and how to read them, sizing `max_size`, health,
  backup, updates, TLS, `purge-on-crash` against `fsync`, the container.
- [docs/protocol.md](docs/protocol.md): what the server answers to each client, with the sources in
  the clients' code.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo deny check
```

The real-client tests are described in [`tests/clients/README.md`](tests/clients/README.md).
Integration tests start the real binary on a free port, with a temporary data directory.

## License

[MIT](LICENSE)
