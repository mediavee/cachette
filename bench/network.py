"""SSH control plane; all measured HTTP traffic uses the remote Tailscale IPv4."""
import base64
import ctypes
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import sys
import tempfile

from run import AUTH, command


def require_quiet(max_load):
    load = os.getloadavg()
    if load[0] > max_load:
        raise RuntimeError(f"Refusing client: 1-minute load {load[0]} exceeds {max_load} (1/5/15 min: {load})")
    return load


def cpu_ticks():
    if platform.system() == "Linux":
        values = list(map(int, Path("/proc/stat").read_text().splitlines()[0].split()[1:9]))
        return sum(values), values[3] + values[4]
    if platform.system() == "Darwin":
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
        ticks = (ctypes.c_uint32 * 4)()
        count = ctypes.c_uint32(4)
        if lib.host_statistics(lib.mach_host_self(), 3, ticks, ctypes.byref(count)) != 0:
            raise RuntimeError("host_statistics CPU sampling failed")
        return sum(ticks), ticks[2]
    raise RuntimeError("CPU sampling requires Linux or macOS")


def cpu_busy(before, after):
    total, idle = (new - old for old, new in zip(before, after))
    if total <= 0 or not 0 <= idle <= total:
        raise RuntimeError("Invalid CPU counter interval")
    return 100 * (total - idle) / total


def rpc(**message):
    print(json.dumps(message), file=sys.__stdout__, flush=True)
    response = sys.stdin.readline()
    if not response:
        raise RuntimeError("Tailscale client control channel closed")
    value = json.loads(response)
    if "error" in value:
        raise RuntimeError(value["error"])
    return value


def coordinate(args):
    require_quiet(args.max_load)
    if not args.oha or command(str(args.oha), "--version") != "oha 1.14.0":
        raise RuntimeError("Pass --oha /absolute/path/to/oha version 1.14.0 (see README)")
    if args.preflight_only:
        print("Client preflight passed: load low, native oha 1.14.0")
        return
    head = command("git", "-C", str(Path(__file__).parent), "rev-parse", "HEAD")
    root = args.remote_root
    remote_head = command("ssh", args.remote, shlex.join(["git", "-C", root, "rev-parse", "HEAD"]))
    if head != remote_head:
        raise RuntimeError(f"Client/server source mismatch: {head} != {remote_head}")
    address = command("ssh", args.remote, "tailscale ip -4").splitlines()[0]
    def ping():
        return command("tailscale", "ping", "--c", "3", "--until-direct=false", address)
    context = dict(host=platform.node(), platform=platform.platform(), nproc=os.cpu_count(),
                   uptime=command("uptime"), load=os.getloadavg(), oha=command(str(args.oha), "--version"),
                   tailscale_version=command("tailscale", "version"), ping=ping(), server_address=address)
    remote = ["sudo", "timeout", "3600", "python3", "-B", root + "/bench/run.py",
              "--tailscale-worker", "--servers", *args.servers, "--seconds", str(args.seconds),
              "--max-load", str(args.max_load), "--cap-mib", str(args.cap_mib),
              "--work-dir", str(args.work_dir)]
    process = subprocess.Popen(["ssh", args.remote, shlex.join(remote)], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, text=True)
    received = False
    try:
        for line in process.stdout:
            message = json.loads(line)
            try:
                action = message["action"]
                if action == "context":
                    response = context
                elif action == "measure":
                    load = require_quiet(args.max_load)
                    before_ping = ping()
                    url = message["url"]
                    if not url.startswith(f"http://{address}:"):
                        raise RuntimeError("Refusing measurement outside remote Tailscale address")
                    with tempfile.TemporaryDirectory(prefix="cachette-bench-client-") as directory:
                        body = Path(directory) / "body"
                        oha = [str(args.oha), "--no-tui", "--output-format", "json", "-z",
                               f"{message['seconds']}s", "-w", "-t", "120s", "-c", str(message["concurrency"]),
                               "-m", message["method"], "-H", f"Authorization: {AUTH}"]
                        if message["body"] is not None:
                            body.write_bytes(base64.b64decode(message["body"]))
                            oha += ["-D", str(body)]
                        before_cpu = cpu_ticks()
                        result = json.loads(command(*oha, url))
                        busy = cpu_busy(before_cpu, cpu_ticks())
                    response = dict(oha=result, cpu_busy=busy, load=load, load_end=os.getloadavg(),
                                    ping_before=before_ping, ping_after=ping())
                elif action == "report":
                    if Path(message["name"]).name != message["name"]:
                        raise RuntimeError("Invalid report filename")
                    args.results.mkdir(parents=True, exist_ok=True)
                    report = args.results / message["name"]
                    report.write_text(message["text"])
                    print(report, flush=True)
                    received = True
                    response = {}
                else:
                    raise RuntimeError(f"Unknown worker action {action}")
            except Exception as exc:
                response = {"error": str(exc)}
            process.stdin.write(json.dumps(response) + "\n")
            process.stdin.flush()
        if process.wait(timeout=60) != 0 or not received:
            raise RuntimeError("Remote benchmark failed; inspect remote failed-*.md, not results/")
    finally:
        process.stdin.close()
        process.stdout.close()
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=60)
