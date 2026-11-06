#!/usr/bin/env python3
"""Run on Linux x86_64; only Python's standard library and Docker are needed."""
import argparse
import base64
import concurrent.futures
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import sys
import subprocess
import tempfile
import threading
import time

HERE = Path(__file__).resolve().parent
PORTS = {"cachette": 18080, "nginx": 18081, "foster": 18082, "bazel": 18083}
BAZEL_IMAGE = "buchgr/bazel-remote-cache:v2.6.4@sha256:10285c1183e606679eb450b725c2495c055a45eb532a6c80f6b6f9771f6d2f67"
SIZES = [4096, 65536, 1048576, 33554432]
DURABILITY = {"nginx": "no fsync", "foster": "file fsync + bbolt", "bazel": "file fsync"}
LISTEN = "127.0.0.1"

TOKEN = "benchmark-disposable-write-token"
AUTH = "Basic " + base64.b64encode(("bench:" + TOKEN).encode()).decode()


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, timeout=1800, **kwargs).strip()


def preflight(max_load):
    load = os.getloadavg()
    if load[0] > max_load:
        raise RuntimeError(f"Refusing: 1-minute load {load[0]} exceeds {max_load} (1/5/15 min: {load})")
    for port in PORTS.values():
        with socket.socket() as sock:
            try:
                sock.bind(("0.0.0.0", port))
            except OSError as exc:
                raise RuntimeError(f"Refusing: port {port} is occupied") from exc


def request(server, method, key, payload=None):
    connection = http.client.HTTPConnection(LISTEN, PORTS[server], timeout=120)
    try:
        connection.request(method, "/" + key, body=payload, headers={"Authorization": AUTH})
        response = connection.getresponse()
        digest = hashlib.sha256()
        length = 0
        while chunk := response.read(1024 * 1024):
            digest.update(chunk)
            length += len(chunk)
        return response.status, length, digest.hexdigest()
    finally:
        connection.close()


def object_key(server, payload, label):
    return "cas/" + hashlib.sha256(payload).hexdigest() if server == "bazel" else label


def pressure_put(server, number, payload):
    body = number.to_bytes(8, "big") + payload[8:]
    return request(server, "PUT", object_key(server, body, f"pressure-{number}"), body)[0]


def prepare_data(server, path):
    path.mkdir()
    if server != "bazel":
        (path / "objects").mkdir()
        (path / "tmp").mkdir()


def statistics(data):
    statuses = data["statusCodeDistribution"]
    transport = sum(data["errorDistribution"].values())
    total = sum(statuses.values()) + transport
    bad = transport + sum(n for s, n in statuses.items() if not 200 <= int(s) < 300)
    return data["summary"]["requestsPerSec"], 100 * bad / total if total else 100


def allocated_bytes(root):
    total = 0
    def walk_error(error):
        if not isinstance(error, FileNotFoundError):
            raise error
    for directory, _, files in os.walk(root, onerror=walk_error):
        for path in [Path(directory)] + [Path(directory) / name for name in files]:
            try:
                total += path.stat().st_blocks * 512
            except FileNotFoundError:
                # Upload publication and eviction can unlink a file during the scan.
                continue
    return total


def variants(servers):
    return [(server, mode) for server in servers for mode in
            (["purge-on-crash", "fsync"] if server == "cachette" else [DURABILITY[server]])]


def process_rss(text):
    return sum(int(line.split()[1]) for line in text.splitlines()[1:])


def cachette_config(cap_mib, durability):
    return (f'data_dir = "/data"\nmax_size = "{cap_mib}MiB"\nmax_object_size = "64MiB"\n'
            f'min_free_disk = "1GiB"\ndurability = "{durability}"\n[[tokens]]\n'
            f'name = "bench"\nsecret = "{TOKEN}"\naccess = "write"\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--servers", nargs="+", choices=PORTS, default=list(PORTS))
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--max-load", type=float, default=2.0)
    parser.add_argument("--cap-mib", type=int, default=4096)
    parser.add_argument("--work-dir", type=Path, default=Path.home() / "cachette-bench")
    parser.add_argument("--results", type=Path, default=HERE / "results")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--remote", help="Run the client here and servers over SSH on this Tailscale host")
    parser.add_argument("--remote-root", default="/home/debian/cachette-bench/repo")
    parser.add_argument("--oha", type=Path, help="Native oha 1.14.0 binary for the Tailscale client")
    parser.add_argument("--tailscale-worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.seconds < 1 or args.cap_mib < 64 or args.max_load < 0:
        parser.error("seconds >= 1, cap-mib >= 64 and max-load >= 0 required")
    if "bazel" in args.servers and (args.cap_mib < 1024 or args.cap_mib % 1024):
        parser.error("bazel requires cap-mib to be a positive multiple of 1024 (whole GiB)")
    if args.remote:
        from network import coordinate
        return coordinate(args)
    global LISTEN
    if args.tailscale_worker:
        from network import rpc
        LISTEN = command("tailscale", "ip", "-4").splitlines()[0]
        sys.stdout = sys.stderr
    preflight(args.max_load)
    if args.preflight_only:
        print("Preflight passed: load low, all four ports free")
        return
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        parser.error("Linux x86_64 required")
    command("docker", "version")
    existing = command("docker", "ps", "-a", "--format", "{{.Names}}")
    if any(name.startswith("cachette-bench-") for name in existing.splitlines()):
        raise RuntimeError("Refusing: cachette-bench-* container already exists; inspect the previous run")
    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.results.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    report = args.results / f"{stamp}-{socket.gethostname()}.md"
    metadata = {
        "utc": stamp, "host": socket.gethostname(), "kernel": platform.platform(),
        "uptime": command("uptime"), "nproc": command("nproc"),
        "disk": command("df", "-T", str(args.work_dir)),
        "mount": command("findmnt", "-T", str(args.work_dir), "-o", "SOURCE,FSTYPE,OPTIONS"),
        "block_devices": command("lsblk", "-d", "-o", "NAME,MODEL,SIZE,ROTA"),
        "docker": command("docker", "version", "--format", "{{.Server.Version}}"),
        "git_head": command("git", "-C", str(HERE), "rev-parse", "HEAD"),
        "seconds": args.seconds, "cap_mib": args.cap_mib, "max_load": args.max_load,
        "oha": "1.14.0 / 9766ca2b725b69b7606b99861d01c779b4aeb65d",
        "foster": "31e69559cde78277192281173176a9e25d13a3bb",
        "bazel": BAZEL_IMAGE, "bazel_storage_mode": "uncompressed",
    }
    metadata["path"] = "Tailscale" if args.tailscale_worker else "loopback"
    metadata["listen"] = LISTEN
    if args.tailscale_worker:
        metadata["client"] = rpc(action="context")
    rows, pressure, versions, probes = [], [], {}, []
    containers = []
    pressure_monitor = None
    sample_errors = []
    pressure_stop = threading.Event()
    run_dir = Path(tempfile.mkdtemp(prefix="run-", dir=args.work_dir.resolve()))
    # An interrupted run still takes the same cleanup and writes its partial evidence.
    def interrupted(signum, frame):
        raise RuntimeError(f"Interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    failure = None
    try:
        for target in ([] if args.tailscale_worker else ["oha"]) + args.servers:
            if target == "bazel":
                command("docker", "pull", BAZEL_IMAGE)
                command("docker", "tag", BAZEL_IMAGE, "cachette-bench-bazel")
            else:
                dockerfile = HERE / ("cachette.Dockerfile" if target == "cachette" else "Dockerfile")
                build = ["docker", "build", "-f", str(dockerfile), "-t", f"cachette-bench-{target}"]
                if target != "cachette":
                    build += ["--target", target]
                command(*build, str(HERE.parent if target == "cachette" else HERE))
            versions[target] = command("docker", "image", "inspect", f"cachette-bench-{target}",
                                       "--format", "{{.Id}} {{json .RepoDigests}} {{json .RootFS.Layers}}")
        versions["nginx-packages"] = command("docker", "run", "--rm", "--name", "cachette-bench-version",
            "--entrypoint", "dpkg-query", "cachette-bench-nginx", "-W", "nginx", "libnginx-mod-http-dav-ext") if "nginx" in args.servers else "not selected"
        # Build activity must settle before any server/client measurement.
        preflight(args.max_load)
        for server, durability in variants(args.servers):
            print(f"Measuring {server} ({durability})", flush=True)
            data = run_dir / "data"
            prepare_data(server, data)
            config = run_dir / "cachette.toml"
            config.write_text(cachette_config(args.cap_mib, durability))
            name = f"cachette-bench-{server}"
            containers.append(name)
            start = ["docker", "run", "-d", "--name", name, "--network", "host",
                     "--log-driver", "none", "-v", f"{data}:/data", "-v", f"{HERE}:/bench:ro"]
            if server == "foster":
                env = {"FSCACHE_ADDR": f"{LISTEN}:18082", "FSCACHE_DATA_DIR": "/data",
                       "FSCACHE_MAX_BYTES": str(args.cap_mib * 1048576),
                       "FSCACHE_MAX_BODY_BYTES": str(64 * 1048576),
                       "FSCACHE_USERNAME": "bench", "FSCACHE_PASSWORD": TOKEN}
                for key, value in env.items():
                    start += ["-e", f"{key}={value}"]
            if server == "cachette":
                start += ["-v", f"{config}:/config.toml:ro"]
            if server == "bazel":
                start += ["--user", "0:0", "--entrypoint", "/bazel-remote-linux-amd64"]
            if server == "nginx":
                nginx_config = run_dir / "nginx.conf"
                nginx_config.write_text((HERE / "nginx.conf").read_text().replace("127.0.0.1:18081", f"{LISTEN}:18081"))
                start += ["-v", f"{nginx_config}:/bench/nginx.conf:ro"]
            start += [f"cachette-bench-{server}"]
            if server == "cachette":
                start += ["--config", "/config.toml", "--listen", f"{LISTEN}:18080"]
            if server == "bazel":
                start += ["--dir", "/data", "--max_size", str(args.cap_mib // 1024),
                          "--storage_mode", "uncompressed", "--http_address", f"{LISTEN}:18083",
                          "--grpc_address", "none", "--profile_address", "none", "--enable_endpoint_metrics=false"]
            command(*start)
            for attempt in range(100):
                try:
                    if request(server, "HEAD", object_key(server, b"bench-ready", "bench-ready"))[0] == 404:
                        break
                except OSError:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError(f"{server} did not become ready")
            if server == "nginx":
                status = request(server, "PROPFIND", "")[0]
                probes.append(f"nginx PROPFIND /: {status}")
                if status != 207:
                    raise RuntimeError("nginx dav-ext probe failed")
            for size in ([4096, 1048576] if args.tailscale_worker else SIZES):
                payload = os.urandom(size)
                body = run_dir / "body"
                body.write_bytes(payload)
                key = object_key(server, payload, f"bench-{size}")
                if request(server, "PUT", key, payload)[0] not in (200, 201, 204):
                    raise RuntimeError(f"{server}: seed PUT failed")
                status, length, digest = request(server, "GET", key)
                if (status, length, digest) != (200, size, hashlib.sha256(payload).hexdigest()):
                    raise RuntimeError(f"{server}: seed readback failed")
                probes.append(f"{server} {size} bytes: PUT and full GET SHA-256 match")
                for method in ["GET", "PUT"]:
                    for concurrency in ([1, 16] if args.tailscale_worker else [1, 16, 64]):
                        for repeat in [1, 2]:
                            peak = [0]
                            stop = threading.Event()
                            def sample():
                                while not stop.is_set():
                                    # docker top includes the master and all nginx workers, in KiB.
                                    try:
                                        text = command("docker", "top", name, "-eo", "pid,rss")
                                        peak[0] = max(peak[0], process_rss(text))
                                    except Exception as exc:
                                        sample_errors.append(exc)
                                        return
                                    stop.wait(0.1)
                            monitor = None
                            if method == "GET" and size == 33554432 and concurrency == 64:
                                monitor = threading.Thread(target=sample)
                                monitor.start()
                            oha = ["docker", "run", "--rm", "--name", "cachette-bench-client", "--network", "host",
                                   "-v", f"{run_dir}:/input:ro", "cachette-bench-oha",
                                   "--no-tui", "--output-format", "json", "-z", f"{args.seconds}s", "-w",
                                   "-t", "120s", "-c", str(concurrency), "-m", method, "-H", f"Authorization: {AUTH}"]
                            if method == "PUT":
                                oha += ["-D", "/input/body"]
                            try:
                                url = f"http://{LISTEN}:{PORTS[server]}/{key}"
                                client = None
                                if args.tailscale_worker:
                                    from network import cpu_ticks, cpu_busy
                                    prepared = rpc(action="prepare", url=url, method=method,
                                                   body=base64.b64encode(payload).decode() if method == "PUT" else None,
                                                   concurrency=concurrency, seconds=args.seconds)
                                    cpu_start = cpu_ticks()
                                    client = rpc(action="measure")
                                    server_cpu = cpu_busy(cpu_start, cpu_ticks())
                                    client.update(prepared)
                                    client.update(rpc(action="finish"))
                                    result = client.pop("oha")
                                else:
                                    result = json.loads(command(*oha, url))
                            finally:
                                stop.set()
                                if monitor:
                                    monitor.join()
                            if sample_errors:
                                raise RuntimeError("RSS sampling failed") from sample_errors[0]
                            rows.append(dict(server=server, durability=durability, client=client,
                                             server_cpu_busy=server_cpu if args.tailscale_worker else None,
                                             size=size, method=method, concurrency=concurrency,
                                             repeat=repeat, peak_rss_kib=peak[0], load=os.getloadavg(), oha=result))
                            print(f"  {method} {size} c={concurrency} run={repeat}: {statistics(result)}", flush=True)
            if args.tailscale_worker:
                command("docker", "rm", "-f", name)
                containers.remove(name)
                shutil.rmtree(data)
                continue
            # A fresh directory isolates eviction from throughput's repeated overwrites.
            command("docker", "rm", "-f", name)
            containers.remove(name)
            shutil.rmtree(data)
            prepare_data(server, data)
            containers.append(name)
            command(*start)
            time.sleep(1)
            payload = os.urandom(33554432)
            start_time = time.monotonic()
            def disk_sample():
                while not pressure_stop.is_set():
                    try:
                        allocated = allocated_bytes(data)
                    except Exception as exc:
                        sample_errors.append(exc)
                        return
                    pressure.append(dict(server=server, durability=durability, seconds=time.monotonic() - start_time, allocated=allocated))
                    pressure_stop.wait(0.1)
            pressure_monitor = threading.Thread(target=disk_sample)
            pressure_monitor.start()
            count = args.cap_mib // 32 * 3
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                statuses = list(pool.map(lambda n: pressure_put(server, n, payload), range(count)))
            time.sleep(1)
            pressure_stop.set()
            pressure_monitor.join()
            pressure_monitor = None
            pressure_stop.clear()
            if sample_errors:
                raise RuntimeError("Disk sampling failed") from sample_errors[0]
            pressure.append(dict(server=server, durability=durability, seconds=time.monotonic() - start_time,
                                 allocated=allocated_bytes(data)))
            probes.append(f"{server} pressure: {count} unique x 32 MiB, concurrency 16, statuses {statuses}")
            command("docker", "rm", "-f", name)
            containers.remove(name)
            shutil.rmtree(data)
    except BaseException as exc:
        failure = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        pressure_stop.set()
        if pressure_monitor:
            pressure_monitor.join()
        for name in containers + ["cachette-bench-client"]:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)
        shutil.rmtree(run_dir)
        metadata["uptime_end"] = command("uptime")
        if failure:
            report = run_dir.parent / f"failed-{stamp}.md"
        render(report, metadata, versions, probes, rows, pressure, failure)
        if args.tailscale_worker and not failure:
            rpc(action="report", name=report.name, text=report.read_text())
        print(report, flush=True)


def render(path, metadata, versions, probes, rows, pressure, failure):
    lines = [f"# Benchmark {metadata['utc']} — {metadata['host']}", "", "Generated by bench/run.py.",
             "Raw evidence is embedded below.", "", "```json", json.dumps(metadata, indent=2), "```",
             "", "## Versions", "", "```json", json.dumps(versions, indent=2), "```", "", "## HTTP probes", ""]
    lines += [f"- {probe}" for probe in probes]
    lines += ["", f"Run failure: {failure or 'none'}.", "", "## Throughput and latency", "",
              "PUT repeatedly uploads one key (CAS on bazel-remote); GET is a warmed hit on that key. HTTP/1.1 keepalive.",
              "Errors include non-2xx and transport errors. MiB/s is successful requests/s × payload size (PUT ingress, GET egress).",
              "RSS is summed process RSS sampled every 100 ms (shared pages may be counted twice).", "",
              "| Server | Durability | Method | Bytes | c | Run | req/s | MiB/s | p50 ms | p99 ms | Errors % | Peak RSS MiB |",
              "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        rps, errors = statistics(row["oha"])
        latency = row["oha"]["latencyPercentiles"]
        statuses = row["oha"]["statusCodeDistribution"]
        good = sum(n for status, n in statuses.items() if 200 <= int(status) < 300)
        good_rps = good / row["oha"]["summary"]["total"]
        rss = f"{row['peak_rss_kib'] / 1024:.2f}" if row["peak_rss_kib"] else "—"
        lines.append(f"| {row['server']} | {row.get('durability', DURABILITY.get(row['server'], 'unspecified'))} | {row['method']} | {row['size']} | {row['concurrency']} | {row['repeat']} | {rps:.2f} | {good_rps * row['size'] / 1048576:.2f} | {latency['p50'] * 1000:.3f} | {latency['p99'] * 1000:.3f} | {errors:.3f} | {rss} |")
    lines += ["", "## Repeatability", "", "Second throughput relative to first; |delta| <= 10% passes.", "",
              "| Server | Durability | Method | Bytes | c | Delta % | <=10% |", "|---|---|---|---:|---:|---:|---|"]
    for first, second in zip(rows[::2], rows[1::2]):
        r1, _ = statistics(first["oha"])
        r2, _ = statistics(second["oha"])
        delta = 100 * (r2 / r1 - 1) if r1 else float('inf')
        lines.append(f"| {first['server']} | {first.get('durability', DURABILITY.get(first['server'], 'unspecified'))} | {first['method']} | {first['size']} | {first['concurrency']} | {delta:.2f} | {'yes' if abs(delta) <= 10 else 'NO'} |")
    lines += ["", "## Eviction pressure", "", "Allocated bytes (sum of st_blocks × 512) include objects, temporary files, metadata and directory blocks. Scans are not atomic; vanished files are ignored.",
              "The configured cap accounts for blobs, not all these filesystem costs. nginx has no cap/evictor.", "",
              "| Server | Durability | Elapsed s | Allocated bytes |", "|---|---|---:|---:|"]
    lines += [f"| {p['server']} | {p.get('durability', DURABILITY.get(p['server'], 'unspecified'))} | {p['seconds']:.3f} | {p['allocated']} |" for p in pressure]
    lines += ["", "## Limits and replay", "", "See ../README.md: loopback, shared machine, hot kernel page cache, different durability policies.",
              "cachette is unmeasured unless explicitly present above; no comparative claim about it follows.", "",
              "Final replay from the repository root on dev2 (after store/http merge):", "", "```sh",
              "sudo timeout 3600 python3 bench/run.py --work-dir /home/debian/cachette-bench --seconds 10", "```", ""]
    if metadata.get("path") == "Tailscale":
        lines += ["", "## Tailscale and host CPU", "",
                  "CPU is whole-host busy percentage over each request interval, including other workloads and Tailscale; not process attribution.",
                  "RTT and direct/DERP evidence are the tailscale ping output before and after each cell in raw evidence.", "",
                  "| Server | Durability | Method | Bytes | c | Run | Client CPU % | Server CPU % | RTT min/max ms (before/after) | Route (before/after) | Client load 1/5/15 |",
                  "|---|---|---|---:|---:|---:|---:|---:|---|---|---|"]
        for row in rows:
            client = row["client"]
            samples = client["ping_before"]["rtt_ms"] + client["ping_after"]["rtt_ms"]
            states = sorted(set(client["ping_before"]["states"] + client["ping_after"]["states"]))
            lines.append(f"| {row['server']} | {row['durability']} | {row['method']} | {row['size']} | {row['concurrency']} | {row['repeat']} | {client['cpu_busy']:.2f} | {row['server_cpu_busy']:.2f} | {min(samples):.2f}/{max(samples):.2f} | {', '.join(states)} | {client['load']} |")
    evidence = dict(metadata=metadata, versions=versions, probes=probes, rows=rows, pressure=pressure, failure=failure)
    lines += ["", "<details><summary>Raw evidence</summary>", "", "```json",
              json.dumps(evidence, indent=2), "```", "", "</details>", ""]
    path.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
