from pathlib import Path
import socket
import subprocess
import tempfile

import run
import sys
import unittest

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
