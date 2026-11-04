"""The live-swarm harness, exercised only against loopback peers and fake commands."""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "benchmarks"))
import run_live  # noqa: E402


def fake(script):
    """A command builder that runs a small Python script instead of cbtorrent."""
    def command(torrent, policy, output, report, extra_args):
        return [sys.executable, "-c", script, str(report)]
    return command


def report_script(complete, seconds=2.0, code=0):
    body = dict(complete=complete, completion_seconds=seconds if complete else None,
                elapsed_seconds=seconds, wire_received_bytes=1000, wasted_payload_bytes=0,
                connections=3, peer_failure_phases={"connect": 2})
    return ("import json,sys\n"
            f"open(sys.argv[1],'w').write(json.dumps({body!r}))\n"
            f"sys.exit({code})\n")


class PlanTests(unittest.TestCase):
    def test_every_round_runs_each_policy_once_per_torrent(self):
        trials = run_live.plan(["a", "b"], ("heuristic", "adaptive", "timed"), 4, seed=1)
        self.assertEqual(len(trials), 2 * 3 * 4)
        for round_index in range(4):
            for torrent in ("a", "b"):
                group = [t["policy"] for t in trials if t["round"] == round_index and t["torrent"] == torrent]
                self.assertCountEqual(group, ["heuristic", "adaptive", "timed"])

    def test_order_is_seeded_and_varies_between_rounds(self):
        first = run_live.plan(["a"], ("heuristic", "adaptive"), 12, seed=7)
        self.assertEqual(first, run_live.plan(["a"], ("heuristic", "adaptive"), 12, seed=7))
        leaders = {t["policy"] for t in first if t["position"] == 0}
        self.assertEqual(leaders, {"heuristic", "adaptive"})


class TrialTests(unittest.TestCase):
    def trial(self):
        return dict(round=0, torrent="x.torrent", policy="heuristic", position=0)

    def test_completed_run_keeps_metrics_and_drops_stderr(self):
        row = run_live.run_trial(self.trial(), 30, [], command=fake(report_script(True, 3.5)))
        self.assertEqual(row["outcome"], "ok")
        self.assertTrue(row["complete"])
        self.assertEqual(row["metrics"]["completion_seconds"], 3.5)
        self.assertEqual(row["stderr_tail"], "")

    def test_failed_download_is_kept_with_its_report(self):
        row = run_live.run_trial(self.trial(), 30, [], command=fake(report_script(False, code=1)))
        self.assertEqual(row["outcome"], "failed")
        self.assertFalse(row["complete"])
        self.assertEqual(row["metrics"]["peer_failure_phases"], {"connect": 2})

    def test_deadline_kills_the_process(self):
        started = time.monotonic()
        row = run_live.run_trial(self.trial(), 0.5, [], command=fake("import time; time.sleep(30)"))
        self.assertEqual(row["outcome"], "deadline")
        self.assertIsNone(row["metrics"])
        self.assertLess(time.monotonic() - started, 15)

    def test_success_exit_without_report_is_not_counted_complete(self):
        row = run_live.run_trial(self.trial(), 30, [], command=fake("pass"))
        self.assertEqual(row["outcome"], "no-report")
        self.assertFalse(row["complete"])

    def test_report_rows_hide_local_paths_and_peer_addresses(self):
        script = ("import json,sys\n"
                  "open(sys.argv[1],'w').write(json.dumps(dict(complete=False, dht_errors=["
                  "'get_peers 203.0.113.9:6881: no reply', 'ping [2001:db8::1]:6881: reset'])))\n"
                  "sys.stderr.write('peer 198.51.100.7:51413 refused'); sys.exit(1)\n")
        trial = dict(self.trial(), torrent=str(Path(tempfile.gettempdir()) / "secret" / "x.torrent"))
        row = run_live.run_trial(trial, 30, [], command=fake(script))
        self.assertEqual(row["torrent"], "x.torrent")
        self.assertEqual(row["metrics"]["dht_errors"],
                         ["get_peers <address>: no reply", "ping <address>: reset"])
        self.assertEqual(row["stderr_tail"], "peer <address> refused")
        self.assertEqual(run_live.label("magnet:?xt=urn:btih:ab"), "magnet:?xt=urn:btih:ab")

    def test_work_directory_is_removed(self):
        before = set(Path(tempfile.gettempdir()).glob("cbtorrent-live-*"))
        run_live.run_trial(self.trial(), 30, [], command=fake(report_script(True)))
        self.assertEqual(set(Path(tempfile.gettempdir()).glob("cbtorrent-live-*")), before)


class SummaryTests(unittest.TestCase):
    def row(self, round_index, policy, seconds, complete=True, outcome="ok"):
        metrics = dict(complete=complete, completion_seconds=seconds, wire_received_bytes=10,
                       wasted_payload_bytes=0, connections=2, peer_failure_phases={"handshake": 1})
        return dict(round=round_index, torrent="t", policy=policy, outcome=outcome,
                    complete=complete, metrics=metrics)

    def test_medians_use_completed_runs_and_failures_are_counted(self):
        rows = [self.row(0, "heuristic", 10), self.row(1, "heuristic", 30),
                self.row(2, "heuristic", None, complete=False, outcome="deadline")]
        [item] = run_live.summarize(rows, ("heuristic",))
        self.assertEqual((item["attempts"], item["completed"]), (3, 2))
        self.assertEqual(item["median_completion_seconds"], 20)
        self.assertEqual(item["outcomes"], {"ok": 2, "deadline": 1})
        self.assertEqual(item["peer_failure_phases_total"], {"handshake": 3})

    def test_paired_ratio_only_uses_rounds_where_both_completed(self):
        rows = [self.row(0, "heuristic", 10), self.row(0, "adaptive", 5),
                self.row(1, "heuristic", None, complete=False, outcome="deadline"),
                self.row(1, "adaptive", 8)]
        ratios = run_live.paired_ratios(rows, "heuristic", ("heuristic", "adaptive"))
        self.assertEqual(ratios["adaptive"]["pairs"], 1)
        self.assertEqual(ratios["adaptive"]["median_ratio"], 0.5)


class LoopbackEndToEndTest(unittest.TestCase):
    """One real download per policy from a local seeder; no trackers or DHT."""

    def test_harness_drives_real_downloads_against_a_local_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            payload = tmp / "payload.bin"
            payload.write_bytes(os.urandom(256 * 1024))
            torrent = tmp / "payload.torrent"
            subprocess.run([sys.executable, "-m", "cbtorrent", "create", str(payload),
                            "--output", str(torrent), "--piece-length", "32768"],
                           cwd=ROOT, check=True, capture_output=True, timeout=60)
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
            seeder = subprocess.Popen(
                [sys.executable, "-m", "cbtorrent", "seed", str(torrent), "--file", str(payload),
                 "--listen-host", "127.0.0.1", "--port", str(port), "--no-trackers", "--no-dht"],
                cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 20
                while True:
                    try:
                        socket.create_connection(("127.0.0.1", port), timeout=1).close()
                        break
                    except OSError:
                        if time.monotonic() > deadline or seeder.poll() is not None:
                            self.fail("local seeder did not start")
                        time.sleep(0.1)
                report = tmp / "live.json"
                with open(os.devnull, "w") as quiet:
                    stdout, sys.stdout = sys.stdout, quiet
                    try:
                        run_live.main(["--torrent", str(torrent), "--policies", "heuristic,adaptive",
                                       "--rounds", "1", "--deadline", "60", "--report", str(report),
                                       "--", "--peer", f"127.0.0.1:{port}", "--no-trackers",
                                       "--no-dht", "--no-pex", "--no-port-mapping", "--port", "0"])
                    finally:
                        sys.stdout = stdout
                result = json.loads(report.read_text(encoding="utf-8"))
                # Reports are exclusive: an existing path is refused before any trial runs.
                with self.assertRaises(SystemExit), open(os.devnull, "w") as quiet:
                    stderr, sys.stderr = sys.stderr, quiet
                    try:
                        run_live.main(["--torrent", str(torrent), "--report", str(report)])
                    finally:
                        sys.stderr = stderr
            finally:
                seeder.terminate()
                seeder.wait(timeout=10)
        self.assertEqual(len(result["trials"]), 2)
        for row in result["trials"]:
            self.assertTrue(row["complete"], row)
            self.assertEqual(row["metrics"]["verified_bytes"], 256 * 1024)
        self.assertEqual(result["paired_vs_baseline"]["adaptive"]["pairs"], 1)


if __name__ == "__main__":
    unittest.main()
