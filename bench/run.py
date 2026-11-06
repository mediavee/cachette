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
import subprocess
import tempfile
import threading
import time

HERE = Path(__file__).resolve().parent
PORTS = {"cachette": 18080, "nginx": 18081, "foster": 18082}
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
    connection = http.client.HTTPConnection("127.0.0.1", PORTS[server], timeout=120)
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--servers", nargs="+", choices=PORTS, default=list(PORTS))
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--max-load", type=float, default=2.0)
    parser.add_argument("--cap-mib", type=int, default=256)
    parser.add_argument("--work-dir", type=Path, default=Path.home() / "cachette-bench")
    parser.add_argument("--results", type=Path, default=HERE / "results")
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args()
    if args.seconds < 1 or args.cap_mib < 64 or args.max_load < 0:
        parser.error("seconds >= 1, cap-mib >= 64 and max-load >= 0 required")
    preflight(args.max_load)
    if args.preflight_only:
        print("Preflight passed: load low, all three ports free")
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
    raw = args.results / f"{stamp}-{socket.gethostname()}.json"
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
    }
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
        for target in ["oha"] + args.servers:
            dockerfile = HERE / ("cachette.Dockerfile" if target == "cachette" else "Dockerfile")
            build = ["docker", "build", "-f", str(dockerfile), "-t", f"cachette-bench-{target}"]
            if target != "cachette":
                build += ["--target", target]
            command(*build, str(HERE.parent if target == "cachette" else HERE))
            versions[target] = command("docker", "image", "inspect", f"cachette-bench-{target}",
                                       "--format", "{{.Id}} {{json .RootFS.Layers}}")
        versions["nginx-packages"] = command("docker", "run", "--rm", "--name", "cachette-bench-version",
            "--entrypoint", "dpkg-query", "cachette-bench-nginx", "-W", "nginx", "libnginx-mod-http-dav-ext") if "nginx" in args.servers else "not selected"
        # Build activity must settle before any server/client measurement.
        preflight(args.max_load)
        for server in args.servers:
            print(f"Measuring {server}", flush=True)
            data = run_dir / "data"
            data.mkdir()
            (data / "objects").mkdir()
            (data / "tmp").mkdir()
            config = run_dir / "cachette.toml"
            config.write_text(f'data_dir = "/data"\nmax_size = "{args.cap_mib}MiB"\nmax_object_size = "64MiB"\nmin_free_disk = "1GiB"\nfsync = true\n[[tokens]]\nname = "bench"\nsecret = "{TOKEN}"\naccess = "write"\n')
            name = f"cachette-bench-{server}"
            containers.append(name)
            start = ["docker", "run", "-d", "--name", name, "--network", "host",
                     "--log-driver", "none", "-v", f"{data}:/data", "-v", f"{HERE}:/bench:ro"]
            if server == "foster":
                env = {"FSCACHE_ADDR": "127.0.0.1:18082", "FSCACHE_DATA_DIR": "/data",
                       "FSCACHE_MAX_BYTES": str(args.cap_mib * 1048576),
                       "FSCACHE_MAX_BODY_BYTES": str(64 * 1048576),
                       "FSCACHE_USERNAME": "bench", "FSCACHE_PASSWORD": TOKEN}
                for key, value in env.items():
                    start += ["-e", f"{key}={value}"]
            if server == "cachette":
                start += ["-v", f"{config}:/config.toml:ro"]
            start += [f"cachette-bench-{server}"]
            if server == "cachette":
                start += ["--config", "/config.toml", "--listen", "127.0.0.1:18080"]
            command(*start)
            for attempt in range(100):
                try:
                    if request(server, "HEAD", "bench-ready")[0] == 404:
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
            for size in [4096, 1048576, 33554432]:
                payload = os.urandom(size)
                body = run_dir / "body"
                body.write_bytes(payload)
                key = f"bench-{size}"
                if request(server, "PUT", key, payload)[0] not in (200, 201, 204):
                    raise RuntimeError(f"{server}: seed PUT failed")
                status, length, digest = request(server, "GET", key)
                if (status, length, digest) != (200, size, hashlib.sha256(payload).hexdigest()):
                    raise RuntimeError(f"{server}: seed readback failed")
                probes.append(f"{server} {size} bytes: PUT and full GET SHA-256 match")
                for method in ["GET", "PUT"]:
                    for concurrency in [1, 16, 64]:
                        for repeat in [1, 2]:
                            peak = [0]
                            stop = threading.Event()
                            def sample():
                                while not stop.is_set():
                                    # docker top includes the master and all nginx workers, in KiB.
                                    try:
                                        text = command("docker", "top", name, "-eo", "pid,rss")
                                        peak[0] = max(peak[0], sum(int(v.split()[1]) for v in text.splitlines()[1:]))
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
                                result = json.loads(command(*oha, f"http://127.0.0.1:{PORTS[server]}/{key}"))
                            finally:
                                stop.set()
                                if monitor:
                                    monitor.join()
                            if sample_errors:
                                raise RuntimeError("RSS sampling failed") from sample_errors[0]
                            rows.append(dict(server=server, size=size, method=method, concurrency=concurrency,
                                             repeat=repeat, peak_rss_kib=peak[0], load=os.getloadavg(), oha=result))
                            print(f"  {method} {size} c={concurrency} run={repeat}: {statistics(result)}", flush=True)
            # A fresh directory isolates eviction from throughput's repeated overwrites.
            command("docker", "rm", "-f", name)
            containers.remove(name)
            shutil.rmtree(data)
            data.mkdir()
            (data / "objects").mkdir()
            (data / "tmp").mkdir()
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
                    pressure.append(dict(server=server, seconds=time.monotonic() - start_time, allocated=allocated))
                    pressure_stop.wait(0.1)
            pressure_monitor = threading.Thread(target=disk_sample)
            pressure_monitor.start()
            count = args.cap_mib // 32 * 3
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                statuses = list(pool.map(lambda n: request(server, "PUT", f"pressure-{n}", payload)[0], range(count)))
            time.sleep(1)
            pressure_stop.set()
            pressure_monitor.join()
            pressure_monitor = None
            pressure_stop.clear()
            if sample_errors:
                raise RuntimeError("Disk sampling failed") from sample_errors[0]
            pressure.append(dict(server=server, seconds=time.monotonic() - start_time,
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
        raw.write_text(json.dumps(dict(metadata=metadata, versions=versions, probes=probes,
                                      rows=rows, pressure=pressure, failure=failure), indent=2) + "\n")
        render(report, raw, metadata, versions, probes, rows, pressure, failure)
        print(report, flush=True)


def render(path, raw, metadata, versions, probes, rows, pressure, failure):
    lines = [f"# Benchmark {metadata['utc']} — {metadata['host']}", "", "Generated by bench/run.py.",
             f"Raw evidence: [{raw.name}]({raw.name}).", "", "```json", json.dumps(metadata, indent=2), "```",
             "", "## Versions", "", "```json", json.dumps(versions, indent=2), "```", "", "## HTTP probes", ""]
    lines += [f"- {probe}" for probe in probes]
    lines += ["", f"Run failure: {failure or 'none'}.", "", "## Throughput and latency", "",
              "PUT is repeated overwrite of one key; GET is a warmed hit on that key. HTTP/1.1 keepalive.",
              "Errors include non-2xx and transport errors. MiB/s is successful requests/s × payload size (PUT ingress, GET egress).",
              "RSS is summed process RSS sampled every 100 ms (shared pages may be counted twice).", "",
              "| Server | Method | Bytes | c | Run | req/s | MiB/s | p50 ms | p99 ms | Errors % | Peak RSS MiB |",
              "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        rps, errors = statistics(row["oha"])
        latency = row["oha"]["latencyPercentiles"]
        statuses = row["oha"]["statusCodeDistribution"]
        good = sum(n for status, n in statuses.items() if 200 <= int(status) < 300)
        good_rps = good / row["oha"]["summary"]["total"]
        rss = f"{row['peak_rss_kib'] / 1024:.2f}" if row["peak_rss_kib"] else "—"
        lines.append(f"| {row['server']} | {row['method']} | {row['size']} | {row['concurrency']} | {row['repeat']} | {rps:.2f} | {good_rps * row['size'] / 1048576:.2f} | {latency['p50'] * 1000:.3f} | {latency['p99'] * 1000:.3f} | {errors:.3f} | {rss} |")
    lines += ["", "## Repeatability", "", "Second throughput relative to first; |delta| <= 10% passes.", "",
              "| Server | Method | Bytes | c | Delta % | <=10% |", "|---|---|---:|---:|---:|---|"]
    for first, second in zip(rows[::2], rows[1::2]):
        r1, _ = statistics(first["oha"])
        r2, _ = statistics(second["oha"])
        delta = 100 * (r2 / r1 - 1) if r1 else float('inf')
        lines.append(f"| {first['server']} | {first['method']} | {first['size']} | {first['concurrency']} | {delta:.2f} | {'yes' if abs(delta) <= 10 else 'NO'} |")
    lines += ["", "## Eviction pressure", "", "Allocated bytes (sum of st_blocks × 512) include objects, temporary files, metadata and directory blocks. Scans are not atomic; vanished files are ignored.",
              "The configured cap accounts for blobs, not all these filesystem costs. nginx has no cap/evictor.", "",
              "| Server | Elapsed s | Allocated bytes |", "|---|---:|---:|"]
    lines += [f"| {p['server']} | {p['seconds']:.3f} | {p['allocated']} |" for p in pressure]
    lines += ["", "## Limits and replay", "", "See ../README.md: loopback, shared machine, hot kernel page cache, different durability policies.",
              "cachette is unmeasured unless explicitly present above; no comparative claim about it follows.", "",
              "Final replay from the repository root on dev2 (after store/http merge):", "", "```sh",
              "sudo timeout 3600 python3 bench/run.py --work-dir /home/debian/cachette-bench --seconds 10", "```", ""]
    path.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
