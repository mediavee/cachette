# Reproducible HTTP cache benchmark

`run.py` is the loopback benchmark; [`gradle.py`](#a-real-gradle-build-over-a-real-network) is the real-network
Gradle build. `run.py` uses Python's standard library and Docker on
a Linux x86_64 host, for the servers and the loopback client. No Python
packages are needed. The [final run](results/2026-11-06T204503Z-linux-x86_64-16t.md)
measures cachette's real store and HTTP path; the comparison in the top-level README
comes from it.

## Loopback replay

On the Linux host, from the repository root, with Docker and `timeout`, `df`,
`findmnt`, `lsblk`, `uptime` and `nproc` installed:

```sh
sudo timeout 3600 python3 -B bench/run.py --servers nginx foster bazel --seconds 5 --work-dir ~/cachette-bench
```

Omit `--servers` to include **both** cachette variants:

```sh
sudo timeout 3600 python3 -B bench/run.py --seconds 10 --work-dir ~/cachette-bench
```

All 24 points (GET/PUT × 4 KiB/64 KiB/1 MiB/32 MiB × c1/16/64) run twice
consecutively per server/variant. The 64 KiB point is the inclusive size up to which
cachette reads an object whole instead of streaming it. The four-server run has five
variants and at least 40 minutes of timed load at the default 10 seconds, before builds, pressure and request drain.
For longer stability measurements use `timeout 10800` and `--seconds 30`.

`--seconds`, `--cap-mib` (default 4096), `--max-load` (default 2) and `--results`
are recorded overrides. The runner refuses any occupied port 18080–18083, a
1-minute load above the threshold (also checked after builds), and any existing
`cachette-bench-*` container. It records all three load averages; historical
5/15-minute load does not reject a currently quiet host. Let build load settle
before replaying. Load is not a disk-isolation guarantee.

Use only `~/cachette-bench/` and the `cachette-bench-*` container prefix on the
host.
Each server runs alone with `--network host`, on 18080 (cachette), 18081 (nginx),
18082 (fosterstack), or 18083 (bazel-remote). Loopback runs bind only 127.0.0.1.
Each variant starts from the same freshly emptied `<work-dir>/run-*/data` path
on the same disk, bind-mounted at `/data`; nginx temporary bodies share that disk.
Containers and data are removed in `finally`, including SIGTERM. Images/build
cache remain. SIGKILL, host failure or Docker failure can prevent cleanup:
inspect the run's named resources before removing them; never stop another service.

## A real Gradle build over a real network

`gradle.py` measures what the loopback runs cannot: a clean Gradle build with and without the
remote cache, against a server on another machine. Python's standard library, Gradle and the
project are all it needs. It assumes no VPN product, host or path.

```sh
BENCH_WRITE_TOKEN=… BENCH_READ_TOKEN=… python3 -B bench/gradle.py \
  --project /path/to/project --url http://server:5071 --tasks "build -x test" --repetitions 5 \
  --server-load-command "ssh server cat /proc/loadavg"
```

The tokens come from the environment, never from the command line. The server must be freshly
started: the script reads `cachette_entries` on `/metrics` and refuses to run unless it is 0.
`BENCH_METRICS_TOKEN` is sent as a bearer token when `/metrics` is protected.

Every build is a clean build of a fresh copy of the project in its own directory
(`--exclude`, repeatable, names what the copy leaves out: `build`, `.gradle` and `.kotlin` by
default). Gradle's local cache is disabled by an init script, so the project is left untouched, and
the daemon is off for every build. An unrecorded warm-up build fills a private Gradle user home so
that no recorded build downloads dependencies. Then, with the same tasks each time:

1. **fill**, once: remote cache with push and the write token;
2. each repetition, **baseline** (no remote cache) and **hit** (remote cache, read token, another
   directory), in alternating order so that both see the same drift.

A repetition does not start while the client's one-minute load is above `--max-client-load` (16)
or the server's above `--max-server-load` (8), read from `--server-load-command`: any command that
prints the server's `/proc/loadavg`. `--max-wait` (two hours) is the total wait; the report says
what ran when it ran out. `--gradle-arg` adds an argument to every build.

The report in `results/<utc>-gradle-<machine label>.md` has the setup, the wall time of every build,
the outcome of every task (`FROM-CACHE`, executed, up-to-date, no-source), the hit rate, baseline
against hit with its spread, the round trip (`GET /healthz` on a kept-alive connection), the loads
and the server's `/metrics` before and after. Module names, the project name and the machines are
replaced by neutral labels before anything is written, and a last check refuses a report that
still holds one. The tests: `python3 -B -m unittest discover -s bench -p 'test_gradle.py' -v`.

## Versions, durability and semantics

| Variant | Durability used in tables | Configuration |
|---|---|---|
| cachette | `purge-on-crash` | no per-object fsync; the store is purged after an unclean run |
| cachette | `fsync` | file fsync before publication; no directory fsync |
| nginx | no fsync | `dav` and `dav-ext` modules, sendfile, automatic workers |
| fosterstack | file fsync + bbolt | default admission limit of 32 uploads retained |
| bazel-remote | file fsync | uncompressed CAS; endpoint metrics disabled |

cachette builds from the current checkout as a static musl binary in
`cachette.Dockerfile`, using the repository's release profile and allocator,
packaged in scratch. The runner does not override workers, allocator,
TCP settings or release flags to improve results. The builder compiles with
two jobs and copies only Cargo manifests and `src/`, excluding host build outputs
and benchmark reports from its inputs. TOML uses `durability`, not
the removed `fsync` boolean. Each mode gets fresh storage and an explicitly
labelled table cell. Runs before `2026-11-06T204503Z` predate cachette's store and
HTTP path: they contain no cachette figures.

- oha **1.14.0**, commit `9766ca2b725b69b7606b99861d01c779b4aeb65d`, locked build.
- fosterstack `31e69559cde78277192281173176a9e25d13a3bb`, vendored dependencies,
  Go 1.27.3. `FSCACHE_MAX_BYTES` equals the common cap; maximum body is 64 MiB.
- bazel-remote [v2.6.4](https://github.com/buchgr/bazel-remote/releases/tag/v2.6.4),
  latest published release verified 2026-11-06, official Linux amd64 digest
  `sha256:10285c1183e606679eb450b725c2495c055a45eb532a6c80f6b6f9771f6d2f67`.
  Its entrypoint is overridden to disable profiling and gRPC. Endpoint metrics
  are explicitly disabled. CAS hash checking and default disk-I/O admission
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
(a cachette under its ceiling drops uploads it cannot store; the reports record
the statuses it answered). Real Gradle builds are
`gradle.py`'s; the `clients` workflow checks the protocol with real Gradle and sccache.

## Recorded runs

- [A real Gradle build over a VPN, five repetitions](results/2026-11-07T021448Z-gradle-darwin-arm64-10t.md):
  `gradle.py` on a private 13-project build, with cachette 0.1.0 built from `main` at `a55ffd3` on a
  Linux host. The top-level README quotes it; `python3 -B bench/gradle.py --summary <report>` prints
  each figure it quotes from the report's raw data.

The loopback reports made by `run.py` retain their original source/configuration and
measurements, with raw evidence consolidated into each Markdown file:

- [Final run, 4 GiB, 10 s, two runs per point](results/2026-11-06T204503Z-linux-x86_64-16t.md):
  cachette in both durability modes against nginx, bazel-remote and fosterstack,
  from source `deef48e`. The top-level README summarises it with its limits.

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
- [4 GiB, three servers, 5 s](results/2026-11-06T174832Z-linux-x86_64-16t.md):
  108 measurements, all GET repeat pairs within ±10%; 10/54 points unstable, all
  PUT. No timed HTTP errors for nginx/bazel; foster predominantly 429 at PUT/c64.
  All 1152 pressure PUTs are 2xx. This predates the 64 KiB point.
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
accounting, disappearing files without hiding permission errors, and the report name
and host field against the machine label.
