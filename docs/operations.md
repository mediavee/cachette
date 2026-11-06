# Operations

What to watch, how to size the cache, how to stop it, and what a crash costs.
Settings are in [configuration](configuration.md).

## Health

`GET /healthz` answers `200` once the index is built, `503` before. It needs no credentials.

At start the server scans `objects/` to rebuild its index. The time grows with the number of
objects. While it runs:

- reads are served straight from disk,
- PUTs are not stored. They answer `413` (`200` to Gradle) and count under `reason="not_ready"` (see
  [protocol](protocol.md#a-put-the-server-does-not-store)).

Wait for `200` before sending a build to a freshly started server. The image has no shell and no
`curl`, so a Docker `HEALTHCHECK` is not possible inside it: probe `/healthz` from outside.

## Metrics

`GET /metrics` serves Prometheus text. It is open unless `metrics_token` is set, then it needs
`Authorization: Bearer <metrics_token>` and a wrong token answers `401`.

Every name below is defined in [`src/metrics.rs`](../src/metrics.rs).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `cachette_requests_total` | counter | `method`, `status` | Requests served. `method` is one of 14 known methods or `OTHER`. A series appears on its first request. |
| `cachette_request_duration_seconds` | histogram | `method` | Request duration. Buckets run from 0.5 ms to 5 s. |
| `cachette_hits_total` | counter | | `GET`s that found their object. `HEAD` is not counted. |
| `cachette_misses_total` | counter | | `GET`s that did not. |
| `cachette_read_bytes_total` | counter | | Object bytes sent to clients. |
| `cachette_written_bytes_total` | counter | | Object bytes that became visible in the store. |
| `cachette_puts_dropped_total` | counter | `reason` | PUTs not stored, answered `413` (`200` to Gradle). `reason` is `too_large`, `no_space`, `not_ready` or `io`. All four exist at zero from the start. |
| `cachette_auth_failures_total` | counter | | Requests refused with `401`, and `/metrics` scrapes with a wrong token. A `403` (valid token, no right to write) is not counted. |
| `cachette_evictions_total` | counter | | Entries evicted to make room. |
| `cachette_evicted_bytes_total` | counter | | Bytes those evictions freed. |
| `cachette_crash_purges_total` | counter | | Starts that deleted every object after an unclean stop. |
| `cachette_size_bytes` | gauge | | Bytes held by stored objects, each rounded up to 4 KiB. |
| `cachette_size_limit_bytes` | gauge | | The configured `max_size`. |
| `cachette_entries` | gauge | | Stored objects. |
| `cachette_oldest_entry_age_seconds` | gauge | | Age of the least recently used entry. `0` when the store is empty. |

The gauges are computed at each scrape.

### Reading them

- **Hit ratio**: `rate(cachette_hits_total[1h]) / (rate(cachette_hits_total[1h]) + rate(cachette_misses_total[1h]))`.
  A miss is normal: it is the first build of a change.
- **Dropped PUTs**: any `no_space` means the ceiling is the limit. `too_large` means an object
  above `max_object_size`: Gradle builds still pass. `io` is a disk error and is logged at `error`.
- **Refusals**: a steady `status="403"` on `PUT` means a `read` token is configured to push.
  Gradle warns and turns its remote cache off for the rest of that build.
- **Crash purges**: any increase means the previous run did not stop cleanly.
  See [Durability](#durability-purge-on-crash-or-fsync).

### Sizing `max_size`

`cachette_oldest_entry_age_seconds` tells how long the cache remembers: the time since the least
recently used entry was last read or written.

1. Decide how far back your builds look. A branch that went quiet for a week, a nightly job: that
   is the age you want the cache to keep.
2. Let the cache fill. A background sweep evicts ahead of the writes, to keep live bytes at or
   below 95 % of `max_size`. A burst of PUTs can pass that mark for a moment.
3. Read the oldest age once it is full. Younger than your target: `max_size` is too small, raise it.
   Far older than your target: the cache is larger than it needs to be.

An oldest entry only hours old on a full cache means most of what is stored gets evicted before
the next build asks for it.

```promql
# the cache is full and forgets within a day
cachette_oldest_entry_age_seconds < 86400
  and cachette_size_bytes > 0.9 * cachette_size_limit_bytes

# writes are being dropped for lack of room
rate(cachette_puts_dropped_total{reason="no_space"}[15m]) > 0
```

The age is as precise as `touch_interval` (5 minutes by default): an entry's recency is written to
its file's modification time at most that often. After a restart the order is rebuilt from those
times.

Leave room on the disk beyond `max_size`: the ceiling counts objects, not directory blocks or other
files. In the [final benchmark](../bench/results/2026-11-06T204503Z-linux-x86_64-16t.md) (lines 451 to 688), a
4 GiB ceiling peaked at 3.84 GiB (`purge-on-crash`) and 3.93 GiB (`fsync`) of allocated blocks,
directories and temporary files included.
`min_free_disk` is the second guard.

## The ceiling and its one exception

`max_size` is a hard ceiling: stored objects, uploads in flight and evicted objects whose files are
not yet deleted never add up to more. A PUT that would break it is dropped, not admitted.

One exception comes from POSIX. A file that is evicted or replaced while a `GET` reads it keeps its
blocks on disk until that read ends. The excess is transient and bounded by the reads in flight
of evicted files. bazel-remote behaves the same way.

Evidence: the store test `ceiling_holds_with_pending_deletions_held` in
[`src/store/mod.rs`](../src/store/mod.rs), and the eviction-pressure traces of the
[final benchmark](../bench/results/2026-11-06T204503Z-linux-x86_64-16t.md).

## Backup

None. It is a cache: everything in it is rebuilt by the next build. Do not back up `data_dir`.

Do not restore a snapshot of it either. `purge-on-crash` assumes the volume is never restored
independently of its `state` file.

## Updating

1. Install the new binary or pull the new image.
2. Send `SIGTERM` (`systemctl restart cachette`, `docker stop`) and let it finish.
3. Start the new one on the same `data_dir`. The index is rebuilt from disk.

A clean stop keeps the cache. A stop that ends in `SIGKILL` empties it under `purge-on-crash`.
Give the stop enough time (see [Stopping](#stopping)).

Builds that overlap the restart may run without the cache: after a failed request, Gradle turns
its remote cache off for the rest of that build.

The on-disk layout has no version field. If a new version cannot read an old directory,
emptying `data_dir` is always safe.

## Stopping

`SIGTERM` or `SIGINT` stops cleanly:

1. no new connection is accepted,
2. requests in flight get `shutdown_timeout` (30 s) to finish,
3. writers and background tasks end, the disk is synced, and the run is marked clean. This shares
   the same `shutdown_timeout` budget.

If the budget runs out, the run is **not** marked clean and the next start under `purge-on-crash`
deletes every object.

So whatever stops the process must wait longer than `shutdown_timeout` plus the final sync:

| Runner | Setting |
|---|---|
| systemd | `TimeoutStopSec=2min`, already in [the unit](../contrib/systemd/cachette.service). Raise it with `shutdown_timeout`. |
| Docker | `--stop-timeout 120`. Docker's default is 10 s, below the 30 s drain, and the `SIGKILL` that follows empties the cache. |

## TLS

Set `tls_cert` and `tls_key` (PEM files) to serve HTTPS. Both or neither. Only HTTP/1.1 is
offered, and clients are not asked for certificates.

The files are read at start. After renewing a certificate, restart the server.

With HTTPS, Gradle needs no `allowInsecureProtocol`. Without TLS, keep the server on a private
network: Basic credentials and bearer tokens travel in clear text.

Under systemd, hand the files over with `LoadCredential` and point the settings at `%d/tls.pem` and
`%d/tls.key`. The unit shows how.

TLS is covered by the test `serves_over_tls_when_configured` in
[`tests/smoke.rs`](../tests/smoke.rs). No benchmark ran over TLS.

## Durability: `purge-on-crash` or `fsync`

Setting: `durability`. The default is `purge-on-crash` on Linux and `fsync` on macOS.

### `purge-on-crash`

A PUT writes its file and renames it. Nothing is synced per object. A `state` file in `data_dir`
records the mode and whether the run ended cleanly:

- At start, before serving anything, the server writes `clean = false` and syncs it.
- On a clean stop it waits for every writer, runs `syncfs` on the objects' filesystem, checks that
  it succeeded, then writes `clean = true`.
- If the previous run left `clean = false` (or the file is missing or unreadable), **every object
  is deleted** before the first read. A purge that is interrupted starts over at the next start.
  `cachette_crash_purges_total` counts it and a `warn` line says so.

A crash, a power cut, a `SIGKILL`, an out-of-memory kill, a stop that overran `shutdown_timeout`
and a failed `syncfs` all count as unclean. A first start on an empty directory is not a crash
purge.

The price is a cold cache after any of those. The gain is that a torn file is never served, and no
`fsync` is paid per object. In the [final benchmark](../bench/results/2026-11-06T204503Z-linux-x86_64-16t.md),
4 KiB PUTs at one client ran at 29 823 requests/s in this mode and 169 in `fsync` mode (mean of two
runs, lines 83 to 178).

Assumptions. Outside them, use `fsync`:

- a local filesystem (ext4, XFS),
- a disk that honours write barriers: a hypervisor with a writeback cache that lies breaks it,
- no restore or snapshot of the volume that is independent of the `state` file.

Linux only. macOS has no `syncfs`, and `purge-on-crash` is refused at start there.

### `fsync`

A PUT flushes its buffer and calls `sync_all` on the file before the rename. The directory is not
synced. After a crash, a name that exists always names a complete file, and the latest PUTs may be
missing. A run in this mode that stops badly does not purge, unless the run before it was
`purge-on-crash` and not clean.

Use it on a network or restored volume, and on macOS.

### What is and is not tested

Tests simulate a crash at each persistent step, in
[`src/store/mod.rs`](../src/store/mod.rs): `purge_decision_table`,
`unclean_purge_on_crash_state_purges`, `absent_or_malformed_state_purges`,
`interrupted_purge_repeats_even_switching_to_fsync`, `clean_purge_on_crash_state_keeps_objects`,
`syncfs_failure_forbids_clean_and_purges_next_start` and `clean_close_keeps_a_purge_on_crash_store`.

A real power cut on real hardware is not tested. The modes rely on what the filesystem and the disk
promise, which is why the assumptions above are listed.

## Container

```sh
docker run -d --name cachette \
  -v cachette-data:/data -p 5071:5071 \
  -e CACHETTE_MAX_SIZE=26GiB \
  -e CACHETTE_TOKENS="ci-main:write:<secret>,developers:read:<secret>" \
  --stop-timeout 120 \
  ghcr.io/mediavee/cachette
```

- **`--stop-timeout 120`**. Docker's default of 10 s is shorter than the server's 30 s drain, and
  a `SIGKILL` under `purge-on-crash` empties the cache. Keep it above `shutdown_timeout`.
- **Owner of the data directory**. The image runs as uid/gid `65532`, and `/data` belongs to it. A
  named volume copies that ownership and just works. A **bind mount** keeps the host's ownership,
  so make it writable by `65532` first:

  ```sh
  install -d -o 65532 -g 65532 -m 0700 /srv/cachette-data
  docker run ... -v /srv/cachette-data:/data ...
  ```

  Otherwise the server stops at start with `cannot open the data directory`.
- The image sets `CACHETTE_LISTEN=0.0.0.0:5071` and `CACHETTE_DATA_DIR=/data`. The binary alone
  listens on `127.0.0.1`, which a published port cannot reach.
- Every setting is a `CACHETTE_*` variable. To use a file, mount it and pass
  `--config /path` as the container arguments.

The image is built `FROM scratch` from the same static binaries as the release archives.
[`Dockerfile`](../Dockerfile) and [`release.yml`](../.github/workflows/release.yml) have the details.

## systemd

[`contrib/systemd/cachette.service`](../contrib/systemd/cachette.service) is a hardened unit for a
binary at `/usr/local/bin/cachette`. It needs systemd 247 or later.

```sh
install -m 0755 cachette /usr/local/bin/cachette
install -m 0644 contrib/systemd/cachette.service /etc/systemd/system/
install -d /etc/cachette
install -m 0600 contrib/cachette.example.toml /etc/cachette/cachette.toml   # then edit it
systemctl enable --now cachette
```

- The service runs as a dynamic user and may write only `/var/lib/cachette`. Set `data_dir` to it.
- The configuration file stays root-only (`0600`): `LoadCredential` hands the service a private
  copy.
- Settings and secrets that belong in the environment go in `/etc/cachette/cachette.env`
  (`EnvironmentFile`), such as `CACHETTE_TOKENS` or `CACHETTE_METRICS_TOKEN`.
- `TimeoutStopSec=2min` must stay above `shutdown_timeout`. See [Stopping](#stopping).

## Security notes

- Anything that can write can poison what every other build reads. Give `write` to the CI of your
  main branch only, and `read` to everything else.
- `anonymous = "read"` opens reads to anyone who can reach the port. `anonymous = "none"` is the
  default.
- There is no rate limit and no per-token quota.
- `/metrics` is open by default. It holds no secret, but it shows your cache's traffic: set
  `metrics_token` if Prometheus scrapes across a boundary.
