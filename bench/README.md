# Reproducible HTTP cache benchmark

`run.py` is the single entry point. It uses Python's standard library, Docker on
the Linux x86_64 server host for servers and the loopback client, and a native
oha build on the macOS arm64 client host for the Tailscale client. No Python
packages are needed. No result here establishes that cachette meets D30: its real
store/HTTP implementation is still unavailable.

## Loopback replay on the server host

On the server host, from the repository root, with Docker and `timeout`, `df`,
`findmnt`, `lsblk`, `uptime` and `nproc` installed:

```sh
sudo timeout 3600 python3 -B bench/run.py --servers nginx foster bazel --seconds 5 --work-dir ~/cachette-bench
```

After store/http merge, omit `--servers` to include **both** cachette variants:

```sh
sudo timeout 3600 python3 -B bench/run.py --seconds 10 --work-dir ~/cachette-bench
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

Use only `~/cachette-bench/` and the `cachette-bench-*` container prefix on the
server host.
Each server runs alone with `--network host`, on 18080 (cachette), 18081 (nginx),
18082 (fosterstack), or 18083 (bazel-remote). Loopback runs bind only 127.0.0.1.
Each variant starts from the same freshly emptied `<work-dir>/run-*/data` path
on the same disk, bind-mounted at `/data`; nginx temporary bodies share that disk.
Containers and data are removed in `finally`, including SIGTERM. Images/build
cache remain. SIGKILL, host failure or Docker failure can prevent cleanup:
inspect the run's named resources before removing them; never stop another service.

## Tailscale replay: client host and server host

Build the same pinned oha source natively on the client host. External source goes
in `/tmp`; dependencies are locked. This is setup, outside all measurement
intervals:

```sh
git clone https://github.com/hatoo/oha /tmp/cachette-bench-oha
git -C /tmp/cachette-bench-oha checkout 9766ca2b725b69b7606b99861d01c779b4aeb65d
timeout 900 cargo build --locked --release --manifest-path /tmp/cachette-bench-oha/Cargo.toml --target-dir "$HOME/cachette-bench/oha-target" -j 2
```

The two checkouts must have the same commit. Update the dedicated checkout on
the server host to this branch, then run **on the client host**, without sudo:

```sh
timeout 3600 python3 -B bench/run.py --remote <server-host> --oha "$HOME/cachette-bench/oha-target/release/oha" --servers nginx foster bazel --seconds 10
```

`<server-host>` is an SSH alias for the server host. `--remote-root` defaults to
`~/cachette-bench/repo` there, and the worker's work directory to `~/cachette-bench`.
A path starting with `~/` is resolved against the home of the user SSH logs in as.

After store/http merge, omit `--servers` for both cachette durability modes.
The subset is GET hit and PUT × 4 KiB/1 MiB × c1/16, twice consecutively: 48
measurements for the three competitors, or 80 including both cachette modes.
RSS and eviction pressure belong to the full loopback matrix and are not repeated
over Tailscale. `--preflight-only` with the same remote/client flags checks the
client host's load and oha without starting remote servers.

SSH is the control plane only. Timed HTTP goes directly from native oha on the
client host to the server host's `tailscale ip -4`, without SSH forwarding.
Servers bind **only that Tailscale IPv4**, never 0.0.0.0 or the public address. No
firewall or tailnet ACL is changed. Existing tailnet policy must allow the client
host to the server host on TCP 18080–18083; otherwise the run fails and no
successful result is published. Other tailnet members allowed by that policy can
reach the disposable listeners during the run. nginx/bazel are unauthenticated;
all listeners disappear at cleanup.

The client host refuses load >2 by default, initially and before every measured
cell. The remote runner applies the server host's guard before and after builds.
Do not raise the threshold just to obtain a publishable result from a busy client
host. A refusal is a missing measurement, not evidence of network speed.

Reports retain `tailscale ping --c 3 --until-direct=false` before and after each
cell: RTT samples and the direct endpoint or DERP route, not an inferred path.
They record host versions, initial uptime, per-cell loads, useful MiB/s, p50/p99,
errors and whole-host busy CPU percentages. CPU comes from cumulative Linux
`/proc/stat` counters (idle+iowait counted idle), and macOS `host_statistics`
CPU ticks. The client host's interval brackets oha; the server host's interval
brackets the measure request and its reply, adding about one control-plane round
trip. Body transfer and the pre/post ping probes are outside both intervals. These
host-wide intervals include unrelated workloads and Tailscale and do not attribute
CPU to the server alone. A route can change during an HTTP interval.

## Versions, durability and semantics

| Variant | Durability used in tables | Configuration |
|---|---|---|
| cachette | `purge-on-crash` | D32, no per-object fsync; purge after unclean run |
| cachette | `fsync` | D32, file fsync before publication; no directory fsync |
| nginx | no fsync | `dav` and `dav-ext` modules, sendfile, automatic workers |
| fosterstack | file fsync + bbolt | default admission limit of 32 uploads retained |
| bazel-remote | file fsync | uncompressed CAS; endpoint metrics disabled |

cachette builds from the current checkout as a static musl binary in
`cachette.Dockerfile`, using the repository's release profile and allocator
(D39), packaged in scratch. The runner does not override workers, allocator,
TCP settings or release flags to improve results. The builder compiles with
two jobs and copies only Cargo manifests and `src/`, excluding host build outputs
and benchmark reports from its inputs. TOML uses `durability`, not
the removed `fsync` boolean. Each mode gets fresh storage and an explicitly
labelled table cell. The musl build and both configurations were smoke-tested against the foundation
at source `7ab6586`: the scratch image returns 200 on `/healthz` in each mode,
and the legacy `fsync` field is rejected with exit 1. This validates build,
configuration and startup only. The real blob store/HTTP path remains unverified;
no cachette performance figures are fabricated.

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
  A live PROPFIND must return 207. Tags and apt repositories can move;
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
The runner names it `<utc>-<os>-<arch>-<nproc>t.md` and puts the same label in its
`host` field, so a report says what kind of machine produced it and never its name.
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

- [Expanded 4 GiB matrix, 5 s](results/2026-11-06T181342Z-linux-x86_64-16t.md):
  exit 0 on pushed source `c557b0bb45230633bac544edfd73d2b1951eb00f`;
  144 measurements including 64 KiB, six positive RSS peaks and three pressure
  traces. Ten of 72 pairs exceed ±10% (four nginx, two foster, four bazel): nine
  PUT and one nginx 32 MiB GET/c16. All GET pairs through 1 MiB meet the tolerance
  in this run. No HTTP/transport errors for nginx/bazel; foster returns 429 in
  PUT/c64. All 1152 pressure PUTs are 2xx. Initial server host load was 1.03; load
  rose during the run and is retained per point (final 26.65). No cause is
  established for variation; these shared-host results do not justify a stable PUT
  ranking. The final renderer fixes table formatting without changing raw
  observations.

  The Tailscale timed run was refused: client host load 34.13 exceeded the default
  2 on 2026-11-06. No network throughput/CPU benchmark result is published. The
  remote replay command above remains required when both hosts are quiet.
  Functional startup probes did confirm that each competitor binds only the server
  host's Tailscale IPv4 and serves a 4 KiB GET to the client host (200); bazel's
  body matched its CAS key. They intentionally stop before timed load and publish
  no benchmark result. Their diagnostic failure reports remain outside the
  repository.
- [4 GiB, three servers, 5 s](results/2026-11-06T174832Z-linux-x86_64-16t.md):
  108 measurements, all GET repeat pairs within ±10%; 10/54 points unstable, all
  PUT. No timed HTTP errors for nginx/bazel; foster predominantly 429 at PUT/c64.
  All 1152 pressure PUTs are 2xx. This predates the 64 KiB point and Tailscale path.
- [1 GiB, three servers, 5 s](results/2026-11-06T173458Z-linux-x86_64-16t.md):
  108 measurements, 7/54 points unstable. bazel returns predominantly 507 at
  PUT 32 MiB/c64; those uploads can exceed the cap. This is capacity-constrained
  evidence.
- [256 MiB, nginx/foster, 5 s](results/2026-11-06T171913Z-linux-x86_64-16t.md):
  72 measurements, 9/36 points unstable. No cause established.

## Runner verification

```sh
HOME=$(mktemp -d) python3 -B -m unittest discover -s bench -v
python3 -B bench/run.py --preflight-only
```

Boundary tests cover load/ports (including 18083), both cachette modes and size
boundary, all-process RSS, CAS addresses and unique pressure bodies, goodput/error
accounting, disappearing files without hiding permission errors, the report name
and host field against the machine label, and remote `~/` paths. Mutation results
and runtime commands are recorded in the PR body.
