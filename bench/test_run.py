from pathlib import Path
import socket
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
        with socket.socket() as sock:
            sock.bind(("0.0.0.0", 18081))
            sock.listen()
            result = self.cli(f'import os, runpy, sys; os.getloadavg=lambda: (0, 0, 0); sys.argv=[{str(RUN)!r}, "--preflight-only"]; runpy.run_path({str(RUN)!r}, run_name="__main__")')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("port 18081 is occupied", result.stderr)

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


class ReportTests(unittest.TestCase):
    def test_http_and_transport_failures_do_not_inflate_goodput(self):
        evidence = {
            "statusCodeDistribution": {"200": 8, "500": 1},
            "errorDistribution": {"connection reset": 1},
            "summary": {"requestsPerSec": 5, "total": 2, "successRate": 0.9},
            "latencyPercentiles": {"p50": 0.01, "p99": 0.02},
        }
        row = dict(server="fixture", method="GET", size=1048576, concurrency=1,
                   repeat=1, peak_rss_kib=0, oha=evidence)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.md"
            run.render(path, Path("raw.json"), {"utc": "fixture", "host": "fixture"},
                       {}, [], [row], [], None)
            self.assertIn("| 5.00 | 4.00 | 10.000 | 20.000 | 20.000 |", path.read_text())


if __name__ == "__main__":
    unittest.main()
