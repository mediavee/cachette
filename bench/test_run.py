from pathlib import Path
import socket
import hashlib
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import subprocess
import tempfile

import run
import sys
import unittest
from unittest.mock import patch

RUN = Path(__file__).with_name("run.py")


class GuardTests(unittest.TestCase):
    def cli(self, code):
        return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)

    def test_high_load_refused_before_docker(self):
        result = self.cli(f'import os, runpy, sys; os.getloadavg=lambda: (3, 0, 0); sys.argv=[{str(RUN)!r}, "--preflight-only"]; runpy.run_path({str(RUN)!r}, run_name="__main__")')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("1-minute load", result.stderr)

    def test_historical_load_does_not_reject_a_quiet_host(self):
        result = self.cli(f'import os, runpy, sys; os.getloadavg=lambda: (0.1, 4, 4); sys.argv=[{str(RUN)!r}, "--preflight-only"]; runpy.run_path({str(RUN)!r}, run_name="__main__")')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_occupied_port_refused_before_docker(self):
        for port in [18081, 18083]:
            with self.subTest(port=port), socket.socket() as sock:
                sock.bind(("0.0.0.0", port))
                sock.listen()
                result = self.cli(f'import os, runpy, sys; os.getloadavg=lambda: (0, 0, 0); sys.argv=[{str(RUN)!r}, "--preflight-only"]; runpy.run_path({str(RUN)!r}, run_name="__main__")')
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"port {port} is occupied", result.stderr)

    def test_bazel_rejects_fractional_gib_caps_before_docker(self):
        result = self.cli(f'import runpy, sys; sys.argv=[{str(RUN)!r}, "--servers", "bazel", "--cap-mib", "256", "--preflight-only"]; runpy.run_path({str(RUN)!r}, run_name="__main__")')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("bazel requires cap-mib to be a positive multiple of 1024", result.stderr)

    def test_low_load_and_free_ports_allowed(self):
        result = self.cli(f'import os, runpy, sys; os.getloadavg=lambda: (0, 0, 0); sys.argv=[{str(RUN)!r}, "--preflight-only"]; runpy.run_path({str(RUN)!r}, run_name="__main__")')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Preflight passed", result.stdout)


class DiskSamplingTests(unittest.TestCase):
    def test_disappearing_upload_does_not_abort_disk_sampling(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retained = root / "retained"
            vanished = root / "temporary"
            retained.write_bytes(b"x" * 4096)
            vanished.write_bytes(b"x" * 4096)
            expected = (root.stat().st_blocks + retained.stat().st_blocks) * 512
            stat = Path.stat

            def disappearing_stat(path, *args, **kwargs):
                if path == vanished:
                    path.unlink()
                return stat(path, *args, **kwargs)

            with patch.object(Path, "stat", disappearing_stat):
                self.assertEqual(run.allocated_bytes(root), expected)

    def test_disk_permission_errors_are_not_hidden(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(Path, "stat", side_effect=PermissionError("denied")):
                with self.assertRaises(PermissionError):
                    run.allocated_bytes(Path(directory))


class DataDirectoryTests(unittest.TestCase):
    def test_bazel_starts_with_an_empty_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            run.prepare_data("bazel", root)
            self.assertEqual(list(root.iterdir()), [])


class BazelCASTests(unittest.TestCase):
    def setUp(self):
        self.objects = {}
        objects = self.objects

        class CASHandler(BaseHTTPRequestHandler):
            def do_PUT(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                expected = "/cas/" + hashlib.sha256(body).hexdigest()
                if self.path == expected:
                    objects[self.path] = body
                    self.send_response(201)
                else:
                    self.send_response(400)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), CASHandler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.port = patch.dict(run.PORTS, {"bazel": self.server.server_port})
        self.port.start()

    def tearDown(self):
        self.port.stop()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_seed_url_is_the_sha256_of_the_uploaded_body(self):
        key = run.object_key("bazel", b"abc", "bench-3")
        self.assertEqual(key, "cas/ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        self.assertEqual(run.request("bazel", "PUT", key, b"abc")[0], 201)
        self.assertEqual(self.objects["/" + key], b"abc")

    def test_pressure_creates_distinct_valid_cas_objects(self):
        statuses = [run.pressure_put("bazel", n, b"x" * 32) for n in range(3)]
        self.assertEqual(statuses, [201, 201, 201])
        self.assertEqual(len(self.objects), 3)
        self.assertEqual({len(body) for body in self.objects.values()}, {32})


class MatrixTests(unittest.TestCase):
    def test_size_boundary_and_both_durability_modes(self):
        self.assertEqual(run.SIZES, [4096, 65536, 1048576, 33554432])
        self.assertEqual(run.variants(["cachette", "nginx"]),
                         [("cachette", "purge-on-crash"), ("cachette", "fsync"), ("nginx", "no fsync")])
        for mode in ["purge-on-crash", "fsync"]:
            config = run.cachette_config(4096, mode)
            self.assertIn(f'durability = "{mode}"', config)
            self.assertNotIn("fsync =", config)

    def test_rss_includes_master_and_each_worker(self):
        self.assertEqual(run.process_rss("PID RSS\n100 1024\n101 2048\n102 4096"), 7168)


class ReportIdentityTests(unittest.TestCase):
    def test_report_name_and_host_come_from_the_label_not_the_hostname(self):
        with patch("socket.gethostname", return_value="leak-host"), \
             patch("platform.node", return_value="leak-host"), \
             patch("platform.system", return_value="Linux"), \
             patch("platform.machine", return_value="x86_64"), \
             patch("os.cpu_count", return_value=16):
            host, name = run.report_identity("2026-11-06T204503Z")
        self.assertEqual(host, "linux-x86_64-16t")
        self.assertEqual(name, "2026-11-06T204503Z-linux-x86_64-16t.md")
        self.assertNotIn("leak-host", host + name)


class ReportTests(unittest.TestCase):
    def test_http_and_transport_failures_do_not_inflate_goodput(self):
        evidence = {
            "statusCodeDistribution": {"200": 8, "500": 1},
            "errorDistribution": {"connection reset": 1},
            "summary": {"requestsPerSec": 5, "total": 2, "successRate": 0.9},
            "latencyPercentiles": {"p50": 0.01, "p99": 0.02},
        }
        row = dict(server="cachette", durability="fsync", method="GET", size=1048576, concurrency=1,
                   repeat=1, peak_rss_kib=0, oha=evidence)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.md"
            run.render(path, {"utc": "fixture", "host": "fixture"},
                       {}, [], [row], [], None)
            text = path.read_text()
            self.assertIn("| 5.00 | 4.00 | 10.000 | 20.000 | 20.000 |", text)
            self.assertIn("| cachette | fsync | GET |", text)
            raw = text.split("<details><summary>Raw evidence</summary>")[1].split("```json\n")[1].split("\n```")[0]
            self.assertEqual(json.loads(raw)["rows"], [row])
            self.assertEqual(list(Path(directory).iterdir()), [path])


if __name__ == "__main__":
    unittest.main()
