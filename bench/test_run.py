from pathlib import Path
import socket
import subprocess
import sys
import unittest

RUN = Path(__file__).with_name("run.py")


class GuardTests(unittest.TestCase):
    def cli(self, code):
        return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)

    def test_high_load_refused_before_docker(self):
        result = self.cli(f'import os, runpy, sys; os.getloadavg=lambda: (3, 0, 0); sys.argv=[{str(RUN)!r}, "--preflight-only"]; runpy.run_path({str(RUN)!r}, run_name="__main__")')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("load averages", result.stderr)

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


if __name__ == "__main__":
    unittest.main()
