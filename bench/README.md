# Reproducible local HTTP cache benchmark

Run from a checkout on Linux x86_64 with Python 3, Git, Docker, `timeout`,
`df`, `findmnt`, `lsblk`, `uptime` and `nproc`. No Python packages are needed.
Docker builds install the pinned clients and servers before measuring them.

```sh
sudo timeout 3600 python3 bench/run.py --work-dir /home/debian/cachette-bench
```

Before cachette's store and HTTP implementation are merged, use:

```sh
sudo timeout 3600 python3 bench/run.py --servers nginx foster bazel --work-dir /home/debian/cachette-bench
```

The default duration is 10 seconds **per point per repetition**. All 18 points
(GET/PUT × 4 KiB/1 MiB/32 MiB × concurrency 1/16/64) run twice consecutively
for each server. `--seconds`, `--cap-mib` (default 4096), `--max-load` (default 2)
and `--results` are explicit overrides, recorded in the generated report.
The default four-server matrix takes at least 24 minutes, excluding builds.
Run only when the shared host is quiet; the preflight rejects any of the four
occupied TCP ports and a 1-minute load average above the threshold,
including after building. The 5/15-minute historical averages are recorded,
not used to reject a currently quiet host. If compilation raised the load, let it settle and
replay the same command (Docker reuses the build cache). There is no automatic
load-threshold bypass. CPU load is not a disk-I/O isolation guarantee.

On dev2, clone under `~/cachette-bench/repo`; use only this directory and
`cachette-bench-*` container names. The runner refuses if a container with this
prefix already exists. It never stops another service. Data directories and
containers are removed in `finally`, including on SIGTERM; images/build cache
are retained for reproducibility. SIGKILL, host failure or a Docker failure can
prevent cleanup: inspect that run before removing its named resources.

## What is measured

Each server runs alone in `--network host`, listening on loopback ports 18080
(cachette), 18081 (nginx), 18082 (fosterstack), or 18083 (bazel-remote). The same freshly emptied
`<work-dir>/run-*/data` path on the same filesystem is bind-mounted at `/data`.
nginx's body temporary directory is also on that filesystem. nginx uses Debian
packages with both DAV and DAV-ext; a live PROPFIND must return 207. The runner
checks each seeded object by a full GET, exact length and SHA-256 before timing.
All clients send the same Basic credential; nginx is intentionally unauthenticated,
whereas cachette and fosterstack authenticate it. bazel-remote is also
unauthenticated, with the same credential header still sent. No TLS is used.

- oha **1.14.0**, source commit `9766ca2b725b69b7606b99861d01c779b4aeb65d`,
  builds with its locked dependencies. HTTP/1.1 keepalive, closed-loop concurrency,
  fixed duration and `-w` to finish in-flight requests. Each request has a 120 s
  timeout. p50/p99 are full-response latencies, including HTTP errors; error rate
  explicitly includes non-2xx and transport errors. Raw status/error distributions
  are retained. Successful MiB/s uses 2xx count / elapsed time × object size.
- GET reads one warmed object. PUT repeatedly uploads that same key: this
  measures contention and sustained replacement, **not** unique-key ingest or
  large-index metadata scanning. Random object bytes are generated once per size,
  not in the timed request loop. oha holds the PUT body in its own memory.
- During 32 MiB GET at concurrency 64, process RSS is sampled every 100 ms with
  `docker top`, summed across all server processes. nginx master/workers can share
  pages, so their sum can overcount. This is sampled peak RSS, not a kernel high
  water mark; it excludes page-cache memory and client memory.
- Pressure starts with fresh server data: 3× the cap in distinct 32 MiB objects,
  16 concurrent uploads, with allocated bytes (sum of `st_blocks * 512`) sampled every 100 ms
  through one second after uploads finish. The report includes all statuses and
  time samples. This includes directories, metadata and temporary files, whereas
  the configured caps account for blobs. Scans are not atomic: files unlinked by publication/eviction during a scan are
  ignored; other I/O errors abort the run. Sampling can miss short overshoots.
  This scenario does not prove LRU order or reservation correctness.

cachette is built from the **current checkout** using a static musl target in
`cachette.Dockerfile`, then packaged in scratch. Its TOML uses `data_dir`,
`max_size`, `max_object_size`, `min_free_disk`, `fsync` and `[[tokens]]`, and its
listen address is passed by CLI. It uses fsync=true, not a tuning exemption.
Until the foundation/store/http lots are available, this build/config/start
path is unverified: the competitors-only run cannot validate it. The final
four-server command above is mandatory before any claim about cachette.

fosterstack is built without source modifications at
`31e69559cde78277192281173176a9e25d13a3bb`, with vendored dependencies and
Go 1.27.3. `FSCACHE_MAX_BYTES` has the same value as cachette's cap and
`FSCACHE_MAX_BODY_BYTES` is 64 MiB. Its default 32-upload bound is retained.
nginx has no built-in storage cap or eviction policy: none is invented here.
The pressure workload itself bounds its data to 3× the cap. nginx uses sendfile,
automatic workers and no access log. Container log collection is disabled for
all four; application-side formatting costs can still differ.

bazel-remote uses the official image for [v2.6.4](https://github.com/buchgr/bazel-remote/releases/tag/v2.6.4),
the latest published release verified on 2026-11-06. Select it as `bazel` in
`--servers`. `BAZEL_IMAGE` in `run.py`
pins that release's Linux amd64 manifest by digest, also retained in the report.
Its HTTP keys are `/cas/<sha256 of body>`; both seed readback and the pressure
workload use valid content addresses. Repeated CAS uploads may have different
semantics from overwriting an arbitrary cache key; this benchmark does not
turn that difference into a claim of equivalent implementation work.

The [official CLI](https://github.com/buchgr/bazel-remote/blob/v2.6.4/README.md#command-line-flags)
accepts `--max_size` only in whole GiB. The common default cap is 4 GiB for every
bounded server: 64 concurrent 32 MiB uploads can reserve 2 GiB in addition to
retained objects. This leaves room for the throughput matrix; the separate
3×-cap scenario still drives eviction. Selecting bazel rejects non-whole-GiB
caps before Docker starts. Historical 256 MiB and 1 GiB reports must not be
merged into a same-cap comparison with a 4 GiB run.

bazel-remote runs with `--storage_mode uncompressed`, matching the uncompressed
storage in the other servers; its default zstd storage is not measured. Requests
are uncompressed HTTP. The server still verifies CAS hashes and syncs files
before completing PUTs ([implementation](https://github.com/buchgr/bazel-remote/blob/v2.6.4/cache/disk/disk.go)).
This does not establish durability equivalent to cachette's file and directory
syncs. No extra hard-limit or disk-I/O-concurrency tuning is added. The stock
image's entrypoint enables a profiling listener, so the runner overrides that
entrypoint and explicitly disables profiling and gRPC: only loopback HTTP
18083 is enabled. It uses uid 0 for the same disposable bind-mount permissions
as the other benchmark servers.

For pressure, every server now receives distinct 32 MiB bodies: the first eight
bytes encode the object number. This is required to prevent CAS deduplication
from turning a nominal 3×-cap workload into one retained blob. Creation of each
body and its CAS key happens on the client during the pressure scenario; pressure
wall time is not a timed PUT-throughput comparison. Hash generation for the
fixed timed object is outside the oha load interval.

## Reading the results

`results/<UTC timestamp>-<hostname>.md` and the adjacent raw JSON are generated
by the script, including exact package versions, image content IDs/layers,
source commits, kernel, disk/mount, CPU count and initial/final uptime. The raw
JSON also records load after each measured point. A partial run writes its
failure and completed points; it is not a successful matrix.

The repeatability table flags a second req/s outside ±10% of the first. Such
points cannot establish a stable performance ranking. This is a shared host,
without CPU pinning, frequency control or exclusive disk access; scheduler,
background activity, per-hit metadata I/O and filesystem writeback can affect
repeats. A short closed-loop saturation benchmark is also sensitive to warmup
and the drain of in-flight requests. A flag identifies instability, not its
cause: repeat on an idle host with longer `--seconds 30` before drawing a
conclusion. No causal attribution to a server's implementation is established
by this benchmark alone.

Client and server share CPU, RAM and disk, so loopback results do not represent
remote networking. The kernel page cache is hot; this is not a cold-disk test.
Durability differs (nginx does not fsync writes; cachette, fosterstack and
bazel-remote have different sync/persistence paths);
this is not an equal-durability comparison. Image tags and Debian package
repositories can move: exact built identities/versions are recorded, but a
future rebuild is not guaranteed byte-identical. Retain the Docker images for
an exact binary replay. A successful PUT is not proof of durable retention;
cachette may intentionally acknowledge a dropped upload with 200. Only seed
readback and the separate pressure evidence establish the behavior observed.

### Recorded dev2 runs

- [Complete three-server run, 4 GiB, 5 s per point](results/2026-11-06T174832Z-dev2.md):
  exit 0 on source head `1b02c456ca1be7b1411f292d30a1892a44327d04`;
  108 measurements, three pressure traces and six RSS sample sets. All bazel
  and nginx timed requests returned 2xx; fosterstack still returns predominantly
  429 for PUT at concurrency 64 with its default 32-upload bound. All 1152
  pressure PUTs returned 2xx. Ten of 54 repeated points exceed ±10% (two bazel,
  five nginx, three fosterstack), all in PUT. All GET pairs meet that throughput
  tolerance in this run. The overall stability target remains unmet and the
  cause of the PUT variation is not established; the setup's confounders above
  still apply. No stable ranking is claimed for those PUT points.

  Replay this workload with the current default cap:

  ```sh
  sudo timeout 3600 python3 -B bench/run.py --servers bazel nginx foster --seconds 5 --work-dir /home/debian/cachette-bench
  ```
- [Capacity-constrained three-server run, 1 GiB, 5 s per point](results/2026-11-06T173458Z-dev2.md):
  exit 0 on source head `667d95b02d061271964c8ba3671561ad5a1b2d3c`;
  108 measurements, three pressure traces and six RSS sample sets. Seven of
  54 repeated points exceed ±10%. bazel-remote returns predominantly 507 for
  32 MiB PUT at concurrency 64; those 64 uploads can total 2 GiB against a 1 GiB
  cap. This constrained point cannot establish its unsaturated write throughput.
  The default is now 4 GiB for all bounded servers to leave room for that point.
- [Complete competitors run, 5 s per point](results/2026-11-06T171913Z-dev2.md):
  exit 0 on source head `6cf1af4424601be9cce1eceb130f47e888470dd1`;
  72 measurements, both pressure traces and four RSS sample sets. Nine of the
  36 repeated points exceed ±10% (five nginx, four fosterstack). This fails the
  stability target. Its cause is **not established**: this setup does not isolate
  client CPU, server scheduling or disk writeback. Longer idle-host runs are
  needed before claiming a stable ranking; the available measurements do not
  justify selecting a favorable repetition. The same issues also occur in the
  longer partial run, so shortening the duration is not established as their cause.
- [Partial competitors run, 10 s per point](results/2026-11-06T170350Z-dev2.md):
  exit 1 on source head `463ef608e71dc288f219a4eac0fdcafd77ba9a69`.
  All 72 throughput rows were collected, but `du` failed on a temporary file
  removed during fosterstack's pressure scenario. That pressure trace is
  incomplete. The current block sampler handles this race; this report remains
  explicitly partial and is not a second successful full run.

In the complete runs, fosterstack PUT at concurrency 64 returns predominantly
429. Its high response rate and low all-response latencies must not be read as
successful storage performance; use the error and useful MiB/s columns together.
The default 32-upload admission bound is retained, not tuned for this benchmark.

To replay the historical two-server, 256 MiB workload with the current runner:

```sh
sudo timeout 3600 python3 -B bench/run.py --servers nginx foster --seconds 5 --cap-mib 256 --work-dir /home/debian/cachette-bench
```

No cachette comparison follows from these competitors-only results.
Implementation-level causes remain hypotheses unless tested separately.

## Runner checks

```sh
HOME=$(mktemp -d) python3 -B -m unittest discover -s bench -v
python3 -B bench/run.py --preflight-only
```

The CLI tests exercise both refusal paths before Docker and the free/quiet
path, correct error/goodput reporting, and disappearing temporary files during disk
scans without hiding permission failures. CAS tests send real HTTP PUTs to a
validating fixture and check content addresses and distinct pressure objects.
The port check includes 18083; fractional-GiB caps are rejected for bazel. Disabling the preflight call must make the two refusal tests fail.
