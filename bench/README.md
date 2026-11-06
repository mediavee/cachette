# Reproducible HTTP cache benchmark

`run.py` is the single entry point. It uses Python's standard library, Docker on
Linux x86_64 for servers and the local client, and native oha on the Mac mini
for the Tailscale client. No Python packages are needed. No result here establishes
that cachette meets D30: its real store/HTTP implementation is still unavailable.

## Loopback replay on dev2

From the repository root, with Docker and `timeout`, `df`, `findmnt`, `lsblk`,
`uptime` and `nproc` installed:

```sh
sudo timeout 3600 python3 -B bench/run.py --servers nginx foster bazel --seconds 5 --work-dir /home/debian/cachette-bench
```

After store/http merge, omit `--servers` to include **both** cachette variants:

```sh
sudo timeout 3600 python3 -B bench/run.py --seconds 10 --work-dir /home/debian/cachette-bench
```

All 24 points (GET/PUT × 4 KiB/64 KiB/1 MiB/32 MiB × c1/16/64) run twice
consecutively per server/variant. The 64 KiB point is the D33 inclusive small-read
boundary. The four-server run has five variants and at least 40 minutes of timed
load at the default 10 seconds, before builds, pressure and request drain.
For longer stability measurements use `timeout 10800` and `--seconds 30`.

`--seconds`, `--cap-mib` (default 4096), `--max-load` (default 2) and `--results`
are recorded overrides. The runner refuses any occupied port 18080–18083, a
1-minute load above the threshold (also checked after builds), and any existing
`cachette-bench-*` container. It records all three load averages; historical
5/15-minute load does not reject a currently quiet host. Let build load settle
before replaying. Load is not a disk-isolation guarantee.

Use only `~/cachette-bench/` and the `cachette-bench-*` container prefix on dev2.
Each server runs alone with `--network host`, on 18080 (cachette), 18081 (nginx),
18082 (fosterstack), or 18083 (bazel-remote). Loopback runs bind only 127.0.0.1.
Each variant starts from the same freshly emptied `<work-dir>/run-*/data` path
on the same disk, bind-mounted at `/data`; nginx temporary bodies share that disk.
Containers and data are removed in `finally`, including SIGTERM. Images/build
cache remain. SIGKILL, host failure or Docker failure can prevent cleanup:
inspect the run's named resources before removing them; never stop another service.

## Tailscale replay: mini client, dev2 servers

Build the same pinned oha source natively on the mini. External source goes in
`/tmp`; dependencies are locked. This is setup, outside all measurement intervals:

```sh
git clone https://github.com/hatoo/oha /tmp/cachette-bench-oha
git -C /tmp/cachette-bench-oha checkout 9766ca2b725b69b7606b99861d01c779b4aeb65d
timeout 900 cargo build --locked --release --manifest-path /tmp/cachette-bench-oha/Cargo.toml --target-dir "$HOME/cachette-bench/oha-target" -j 2
```

The two checkouts must have the same commit. Update the dedicated checkout on
dev2 to this branch, then run **on the mini**, without sudo:

```sh
timeout 3600 python3 -B bench/run.py --remote dev2 --remote-root /home/debian/cachette-bench/repo --oha "$HOME/cachette-bench/oha-target/release/oha" --servers nginx foster bazel --seconds 10 --work-dir /home/debian/cachette-bench
```

After store/http merge, omit `--servers` for both cachette durability modes.
The subset is GET hit and PUT × 4 KiB/1 MiB × c1/16, twice consecutively: 48
measurements for the three competitors, or 80 including both cachette modes.
RSS and eviction pressure belong to the full loopback matrix and are not repeated
over Tailscale. `--preflight-only` with the same remote/client flags checks the
mini's load and oha without starting remote servers.

SSH is the control plane only. Timed HTTP goes directly from native oha on the
mini to dev2's `tailscale ip -4`, without SSH forwarding. Servers bind **only that
Tailscale IPv4**, never 0.0.0.0 or the public address. No firewall or tailnet ACL is
changed. Existing tailnet policy must allow the mini to dev2 TCP 18080–18083;
otherwise the run fails and no successful result is published. Other tailnet
members allowed by that policy can reach the disposable listeners during the run.
nginx/bazel are unauthenticated; all listeners disappear at cleanup.

The mini refuses load >2 by default, initially and before every measured cell.
The remote runner applies the dev2 guard before and after builds. Do not raise
the threshold just to obtain a publishable result from a busy mini. A refusal
is a missing measurement, not evidence of network speed.

Reports retain `tailscale ping --c 3 --until-direct=false` before and after each
cell: RTT samples and the direct endpoint or DERP route, not an inferred path.
They record host versions, initial uptime, per-cell loads, useful MiB/s, p50/p99,
errors and whole-host busy CPU percentages. CPU comes from cumulative Linux
`/proc/stat` counters (idle+iowait counted idle), and macOS `host_statistics`
CPU ticks. The mini interval brackets oha; dev2's interval brackets the measure request
and its reply, adding about one control-plane round trip. Body transfer and
the pre/post ping probes are outside both intervals. These host-wide intervals
include unrelated workloads and Tailscale and do not attribute CPU to the server alone. A route can change during an HTTP interval.

## Versions, durability and semantics

| Variant | Durability used in tables | Configuration |
|---|---|---|
| cachette | `purge-on-crash` | D32, no per-object fsync; purge after unclean run |
| cachette | `fsync` | D32, file fsync before publication; no directory fsync |
| nginx | no fsync | Debian DAV and DAV-ext, sendfile, automatic workers |
| fosterstack | file fsync + bbolt | default admission limit of 32 uploads retained |
| bazel-remote | file fsync | uncompressed CAS; endpoint metrics disabled |

cachette builds from the current checkout as a static musl binary in
`cachette.Dockerfile`, using the repository's release profile and allocator
(D39), packaged in scratch. The runner does not override workers, allocator,
TCP settings or release flags to improve results. TOML uses `durability`, not
the removed `fsync` boolean. Each mode gets fresh storage and an explicitly
labelled table cell. This build/config/start path remains unverified until the
real runtime is merged; no cachette figures are fabricated.

- oha **1.14.0**, commit `9766ca2b725b69b7606b99861d01c779b4aeb65d`, locked build.
- fosterstack `31e69559cde78277192281173176a9e25d13a3bb`, vendored dependencies,
  Go 1.27.3. `FSCACHE_MAX_BYTES` equals the common cap; maximum body is 64 MiB.
- bazel-remote [v2.6.4](https://github.com/buchgr/bazel-remote/releases/tag/v2.6.4),
  latest published release verified 2026-11-06, official Linux amd64 digest
  `sha256:10285c1183e606679eb450b725c2495c055a45eb532a6c80f6b6f9771f6d2f67`.
  Its entrypoint is overridden to disable profiling and gRPC. Endpoint metrics
  are explicitly disabled (D30). CAS hash checking and default disk-I/O admission
  remain. UID 0 matches disposable bind-mount permissions on the other servers.
- nginx packages and image identities/layers for all builds are recorded.
  A live PROPFIND must return 207. Tags and Debian repositories can move;
  retain the images to replay the exact binaries.

bazel keys are `/cas/<SHA-256 of body>`. Timed PUT repeatedly uploads one object,
whereas the other servers replace one arbitrary key: these operations need not
do equivalent internal work. Uncompressed bazel storage avoids comparing zstd
with raw files. Its file fsync is not a general proof of equal crash durability.
[Source and CLI](https://github.com/buchgr/bazel-remote/tree/v2.6.4) document these
semantics. Authentication differs: cachette/foster authenticate the same Basic
credential sent to all servers; nginx/bazel are unauthenticated. There is no TLS.

The common cap is 4 GiB: bazel accepts only whole GiB, and c64 × 32 MiB can reserve
2 GiB plus retained objects. Fractional-GiB caps with bazel are refused. nginx
has no built-in cap or eviction policy; none is invented. Historical runs with
256 MiB or 1 GiB are retained separately, never mixed with the 4 GiB comparison.

## Measurement and interpretation

Seed PUT is checked by a full GET, exact length and SHA-256 before timing.
GET hits a warmed object. oha uses HTTP/1.1 keepalive, closed-loop concurrency,
fixed duration, `-w` to drain in-flight requests and a 120-second request timeout.
Bodies are generated outside timed load. p50/p99 include HTTP errors; error
rate includes non-2xx and transport failures. Useful MiB/s uses only 2xx count
and elapsed time; a fast 429 is not successful storage throughput.

During 32 MiB GET/c64, `docker top -eo pid,rss` samples **all container processes**
every 100 ms and sums master and every nginx worker. This is sampled peak RSS,
not a kernel high-water mark. Shared pages can be counted more than once;
page cache and client memory are excluded. The boundary test supplies one master
and two workers and fails if only the first process is counted.

Pressure resets storage, uploads 3× the cap in distinct 32 MiB bodies at c16,
and samples allocated bytes (`st_blocks × 512`) every 100 ms through one second
after completion. The first eight body bytes encode the object number for every
server, preventing CAS deduplication. Hash/body preparation is client work during
pressure; pressure elapsed time is not PUT throughput. All statuses are retained.
Directory blocks, metadata and temporary files count; configured caps cover blobs.
Walks are not atomic: only ENOENT is tolerated. Short overshoots may be missed;
this scenario does not prove LRU order, quota reservations or durable retention.

One **Markdown file per successful run** contains tables and embedded raw JSON
(metadata, exact versions, all oha outputs, RSS, disk trace, probes and loads).
Failed runs write `failed-<timestamp>.md` outside `results/`, in the working
directory for diagnosis. They are not committed. Instability, HTTP errors and
unfavorable successful runs remain; success means completion, not good performance.

Repeatability flags second req/s outside ±10% of the first. Such points cannot
establish a stable ranking. Shared CPU/disk, scheduler, filesystem writeback,
frequency changes, warmup and in-flight drain are confounders, not established
causes. Replay on a quiet host for 30 seconds before drawing a conclusion.
Loopback shares client/server CPU and memory. Page cache is hot; no cold-disk
or remote-network claim follows. A successful PUT is not proof of retention
(cachette can acknowledge dropped uploads with 200). True Gradle/sccache build
results belong to the clients lot; D30 remains unproven here.

## Recorded runs

The historical successful reports retain their original source/configuration
and measurements, with raw evidence consolidated into each Markdown file:

- [4 GiB, three servers, 5 s](results/2026-11-06T174832Z-dev2.md): 108 measurements,
  all GET repeat pairs within ±10%; 10/54 points unstable, all PUT. No timed HTTP
  errors for nginx/bazel; foster predominantly 429 at PUT/c64. All 1152 pressure
  PUTs are 2xx. This predates the 64 KiB point and Tailscale path.
- [1 GiB, three servers, 5 s](results/2026-11-06T173458Z-dev2.md): 108 measurements,
  7/54 points unstable. bazel returns predominantly 507 at PUT 32 MiB/c64;
  those uploads can exceed the cap. This is capacity-constrained evidence.
- [256 MiB, nginx/foster, 5 s](results/2026-11-06T171913Z-dev2.md): 72 measurements,
  9/36 points unstable. No cause established.

## Runner verification

```sh
HOME=$(mktemp -d) python3 -B -m unittest discover -s bench -v
python3 -B bench/run.py --preflight-only
```

Boundary tests cover load/ports (including 18083), both cachette modes and size
boundary, all-process RSS, CAS addresses and unique pressure bodies, goodput/error
accounting and disappearing files without hiding permission errors. Mutation
results and runtime commands are recorded in the PR body.
