from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import json
import tempfile
import threading
import unittest

import gradle

# Captured from a real `gradle build --console=plain --build-cache` of a small project: a hit build.
HIT_BUILD = """\
> Task :compileJava FROM-CACHE
> Task :processResources NO-SOURCE
> Task :classes UP-TO-DATE
> Task :jar FROM-CACHE
> Task :assemble UP-TO-DATE
> Task :compileTestJava FROM-CACHE
> Task :processTestResources NO-SOURCE
> Task :testClasses UP-TO-DATE
> Task :test FROM-CACHE
> Task :check UP-TO-DATE
> Task :build UP-TO-DATE
> Task :generateReport FROM-CACHE
> Task :lib:compileJava
> Task :lib:skipped SKIPPED

BUILD SUCCESSFUL in 3s
7 actionable tasks: 5 from cache, 2 up-to-date
"""

METRICS = """\
# HELP cachette_entries Objects in the store.
# TYPE cachette_entries gauge
cachette_entries {entries}
cachette_hits_total{{kind="gradle"}} 3
cachette_hits_total{{kind="other"}} 4
cachette_request_duration_seconds_bucket{{le="0.1"}} 9
"""


class FakeServer:
    """A server with a /healthz, a /metrics and a count of the connections it accepted."""

    def __init__(self, entries):
        self.connections = 0
        self.entries = entries
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                outer.connections += 1
                super().setup()

            def do_GET(self):
                body = METRICS.format(entries=outer.entries).encode() if self.path == "/metrics" else b""
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class OutcomeTests(unittest.TestCase):
    def test_every_outcome_is_read_from_captured_output(self):
        outcomes = gradle.parse_outcomes(HIT_BUILD)
        self.assertEqual(outcomes[":compileJava"], "FROM-CACHE")
        self.assertEqual(outcomes[":processResources"], "NO-SOURCE")
        self.assertEqual(outcomes[":classes"], "UP-TO-DATE")
        self.assertEqual(outcomes[":lib:compileJava"], "EXECUTED")
        self.assertEqual(outcomes[":lib:skipped"], "SKIPPED")
        self.assertEqual(len(outcomes), 14)
        self.assertEqual(gradle.tally(outcomes), {"FROM-CACHE": 5, "EXECUTED": 1, "UP-TO-DATE": 5,
                                                   "NO-SOURCE": 2, "SKIPPED": 1, "FAILED": 0})

    def test_other_lines_are_not_tasks(self):
        text = "> Configure project :\n> Transform foo.jar\nBUILD SUCCESSFUL\n> Task :a FAILED\n"
        self.assertEqual(gradle.parse_outcomes(text), {":a": "FAILED"})

    def test_hit_rate_counts_tasks_that_ran_or_came_from_the_cache(self):
        # 5 FROM-CACHE and 1 EXECUTED: up-to-date, no-source and skipped tasks are not in the ratio.
        self.assertAlmostEqual(gradle.hit_rate(gradle.parse_outcomes(HIT_BUILD)), 5 / 6)

    def test_hit_rate_is_none_when_nothing_ran(self):
        self.assertIsNone(gradle.hit_rate({":a": "UP-TO-DATE"}))


class StatisticsTests(unittest.TestCase):
    def test_p99_is_the_nearest_rank(self):
        self.assertEqual(gradle.percentile(range(1, 101), 0.99), 99)
        self.assertEqual(gradle.percentile([5], 0.99), 5)

    def test_spread(self):
        self.assertEqual(gradle.spread([3, 1, 2]), {"n": 3, "median": 2, "min": 1, "max": 3, "stdev": 1.0})
        self.assertEqual(gradle.spread([4])["stdev"], 0.0)

    def test_summary_pairs_baseline_with_hit_and_computes_the_time_saved(self):
        builds = [dict(kind=kind, repetition=rep, wall_seconds=seconds, hit_rate=rate)
                  for rep, kind, seconds, rate in [(1, "baseline", 100, None), (1, "hit", 40, 0.9),
                                                   (2, "hit", 50, 0.8), (2, "baseline", 120, None),
                                                   (3, "baseline", 999, None)]]
        builds.append(dict(kind="fill", wall_seconds=1, hit_rate=0.0))
        summary = gradle.summarize(builds)
        self.assertEqual(summary["repetitions"], 2)
        self.assertEqual(summary["baseline"]["median"], 110)
        self.assertEqual(summary["hit"]["median"], 45)
        self.assertEqual(summary["saved_seconds"], 65)
        self.assertAlmostEqual(summary["hit_rate"]["median"], 0.85)


class MetricsTests(unittest.TestCase):
    def test_labelled_series_are_summed_and_similar_names_are_not_mixed(self):
        samples = gradle.parse_metrics(METRICS.format(entries=0))
        self.assertEqual(gradle.metric_total(samples, "cachette_hits_total"), 7)
        self.assertEqual(gradle.metric_total(samples, "cachette_entries"), 0)
        self.assertIsNone(gradle.metric_total(samples, "cachette_misses_total"))

    def test_a_server_that_holds_entries_is_refused(self):
        server = FakeServer(entries=3)
        self.addCleanup(server.close)
        with self.assertRaisesRegex(gradle.Refused, "holds 3 entries"):
            gradle.require_empty(gradle.parse_metrics(gradle.read_metrics(server.url)))

    def test_an_empty_server_passes(self):
        server = FakeServer(entries=0)
        self.addCleanup(server.close)
        gradle.require_empty(gradle.parse_metrics(gradle.read_metrics(server.url)))

    def test_a_server_without_the_entries_metric_is_refused(self):
        with self.assertRaisesRegex(gradle.Refused, "cannot be shown empty"):
            gradle.require_empty({})


class RoundTripTests(unittest.TestCase):
    def test_requests_share_one_connection(self):
        server = FakeServer(entries=0)
        self.addCleanup(server.close)
        result = gradle.round_trip(server.url, count=50)
        self.assertEqual(server.connections, 1)
        self.assertEqual(result["requests"], 50)
        self.assertLessEqual(result["min_ms"], result["median_ms"])
        self.assertLessEqual(result["median_ms"], result["p99_ms"])
        self.assertLessEqual(result["p99_ms"], result["max_ms"])


class LoadTests(unittest.TestCase):
    def test_waits_until_both_loads_are_low(self):
        readings = iter([(40, 1), (20, 1), (10, 9), (10, 2)])
        slept = []
        quiet, waited = gradle.wait_for_quiet(lambda: next(readings), 16, 8, 600, sleep=slept.append, interval=30)
        self.assertTrue(quiet)
        self.assertEqual((waited, slept), (90, [30, 30, 30]))

    def test_gives_up_when_the_budget_is_spent(self):
        quiet, waited = gradle.wait_for_quiet(lambda: (40, None), 16, 8, 90, sleep=lambda s: None, interval=30)
        self.assertFalse(quiet)
        self.assertEqual(waited, 90)

    def test_a_missing_server_load_does_not_block(self):
        self.assertEqual(gradle.wait_for_quiet(lambda: (1, None), 16, 8, 0), (True, 0))

    def test_server_load_reads_the_first_field(self):
        self.assertEqual(gradle.server_load("echo '0.52 0.40 0.30 1/200 1234'"), 0.52)
        self.assertIsNone(gradle.server_load(None))

    def test_baseline_and_hit_alternate_their_order(self):
        self.assertEqual([gradle.alternating(n) for n in (1, 2, 3)],
                         [("baseline", "hit"), ("hit", "baseline"), ("baseline", "hit")])


class BuildTests(unittest.TestCase):
    def test_tokens_are_in_the_environment_never_on_the_command_line(self):
        command = gradle.build_command("./gradlew", "/w/bench.init.gradle", ["build", "-x", "test"], ["--max-workers=4"])
        env = gradle.build_env({"PATH": "/bin"}, "/w/home", "http://host:1", "s3cret", True)
        self.assertNotIn("s3cret", " ".join(command))
        self.assertEqual(env["BENCH_CACHE_TOKEN"], "s3cret")
        self.assertEqual(env["BENCH_CACHE_PUSH"], "true")
        self.assertEqual(env["BENCH_CACHE_URL"], "http://host:1/")
        self.assertEqual(env["GRADLE_USER_HOME"], "/w/home")
        self.assertNotIn("s3cret", gradle.INIT_SCRIPT)
        self.assertIn("--no-daemon", command)
        self.assertEqual(command[-3:], ["build", "-x", "test"])

    def test_a_baseline_environment_has_no_cache_even_if_the_caller_has_one(self):
        env = gradle.build_env({"BENCH_CACHE_URL": "http://old", "BENCH_CACHE_TOKEN": "old"}, "/w/home")
        self.assertFalse({"BENCH_CACHE_URL", "BENCH_CACHE_TOKEN", "BENCH_CACHE_PUSH"} & env.keys())

    def test_the_init_script_turns_the_local_cache_off(self):
        self.assertIn("local { enabled = false }", gradle.INIT_SCRIPT)

    def test_a_copy_leaves_out_the_build_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / "source", Path(directory) / "target"
            (source / "module" / "build").mkdir(parents=True)
            (source / "module" / "build" / "out.class").write_text("x")
            (source / "module" / "Main.java").write_text("x")
            gradle.fresh_copy(source, target, ["build"])
            self.assertEqual([p.name for p in (target / "module").iterdir()], ["Main.java"])


class ReportTests(unittest.TestCase):
    def build(self, kind, repetition, seconds, outcomes):
        record = dict(kind=kind, exit=0, wall_seconds=seconds, outcomes=outcomes, counts=gradle.tally(outcomes),
                      hit_rate=gradle.hit_rate(outcomes), cache_warnings=0, client_load_before=[1.0, 1.0, 1.0],
                      client_load_after=[1.0, 1.0, 1.0], server_load_before=None, server_load_after=None)
        if repetition:
            record["repetition"] = repetition
        return record

    def data(self):
        tasks = {":secretproject-core:compileJava": "FROM-CACHE", ":secretproject-core:secretprojectCheck": "EXECUTED",
                 ":assemble": "UP-TO-DATE"}
        rtt = dict(requests=3, median_ms=1.0, p99_ms=2.0, min_ms=0.5, max_ms=3.0)
        return {"meta": dict(utc="2026-01-01T000000Z", host="linux-x86_64-4t", tasks="build", gradle_args="",
                             versions=dict(gradle="Gradle 9", jvm="25", server="cachette 0.1.0"), daemon="none",
                             max_client_load=16, max_server_load=8, waited_seconds=0, max_wait_seconds=7200,
                             repetitions=1, completed=1, stopped=None, replay="python3 bench/gradle.py"),
                "builds": [self.build("baseline", 1, 100.0, {t: "EXECUTED" for t in tasks}),
                           self.build("hit", 1, 40.0, tasks)],
                "round_trip": {"before": rtt, "after": rtt}, "metrics": {"before": "", "after": ""}}

    def render(self, data, scrubber):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.md"
            gradle.render(path, data, scrubber)
            return path.read_text()

    def test_the_report_names_no_project_module_user_or_path(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "SecretProject"
            project.mkdir()
            scrubber = gradle.Scrubber(project, url="http://secret-host.example:1")
            data = self.data()
            data["meta"]["replay"] += f" {project} secret-host.example"
            text = self.render(data, scrubber)
        for leak in ("secretproject", "secret-host", directory, str(Path.home())):
            self.assertNotIn(leak.lower(), text.lower())
        self.assertIn("module-01:compileJava", text)
        self.assertIn("root:assemble", text)
        raw = json.loads(text.split("```json\n")[1].split("\n```")[0])
        self.assertEqual(raw["summary"]["saved_seconds"], 60.0)

    def test_the_readme_figures_are_recomputed_from_the_raw_data_of_a_report(self):
        data = self.data()
        counters = lambda hits, misses, entries, written, read: {
            "cachette_hits_total": hits, "cachette_misses_total": misses, "cachette_entries": entries,
            "cachette_written_bytes_total": written, "cachette_read_bytes_total": read}
        zero = counters(0, 0, 0, 0, 0)
        fill = self.build("fill", None, 9.0, {":a:compileJava": "EXECUTED"})
        fill.update(metrics_before=zero, metrics_after=counters(0, 3, 3, 300, 0))
        data["builds"].insert(0, fill)
        for build in data["builds"][1:]:
            build.update(metrics_before=counters(0, 3, 3, 300, 0), metrics_after=counters(0, 3, 3, 300, 0))
        data["builds"][2].update(metrics_after=counters(1, 4, 3, 300, 70))
        with tempfile.TemporaryDirectory() as directory:
            scrubber = gradle.Scrubber(Path(directory) / "secretproject")
            figures = gradle.figures(gradle.raw_data(self.render(data, scrubber)))
        self.assertEqual((figures["lookups_per_hit_build"], figures["server_hits_per_hit_build"]), (2, 1))
        self.assertEqual(figures["hit_rate_of_lookups"], 0.5)
        self.assertEqual((figures["entries_stored_by_fill"], figures["bytes_stored_by_fill"]), (3, 300))
        self.assertEqual(figures["bytes_read_per_hit_build"], 70)
        self.assertEqual((figures["tasks_per_build"], figures["projects_with_tasks"]), (3, 2))
        self.assertEqual(figures["saved_seconds"], 60.0)

    def test_a_name_that_survives_scrubbing_stops_the_write(self):
        with tempfile.TemporaryDirectory() as directory:
            scrubber = gradle.Scrubber(Path(directory) / "secretproject")
            scrubber.text = lambda value: value
            with self.assertRaisesRegex(gradle.Refused, "identifying name"):
                self.render(self.data(), scrubber)

    def test_name_and_host_come_from_the_machine_label(self):
        host, name = gradle.report_identity("2026-11-06T204503Z")
        self.assertEqual(name, f"2026-11-06T204503Z-gradle-{host}.md")

    def test_the_replay_line_hides_the_project_the_url_and_the_host_command(self):
        replay = gradle.replay_command(["--project", "/home/me/Secret", "--url=http://10.0.0.1:1",
                                        "--server-load-command", "ssh box cat /proc/loadavg",
                                        "--tasks", "build -x test", "--repetitions", "5"])
        for leak in ("/home/me", "Secret", "10.0.0.1", "ssh box"):
            self.assertNotIn(leak, replay)
        self.assertIn("--project '<project>'", replay)
        self.assertIn("--url='<url>'", replay)
        self.assertIn("--tasks 'build -x test'", replay)


if __name__ == "__main__":
    unittest.main()
