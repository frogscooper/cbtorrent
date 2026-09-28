import unittest
from types import SimpleNamespace

from cbtorrent.observe import (
    DownloadSnapshot, RateTracker, build_snapshot, format_bytes, format_eta,
    format_percent, format_rate, peer_address,
)


class FormatTests(unittest.TestCase):
    def test_format_bytes_and_rate(self):
        self.assertEqual(format_bytes(0), "0 B")
        self.assertEqual(format_bytes(512), "512 B")
        self.assertEqual(format_bytes(1536), "1.5 KiB")
        self.assertEqual(format_rate(0), "0 B/s")
        self.assertEqual(format_rate(2048), "2.0 KiB/s")

    def test_format_eta_and_percent(self):
        self.assertEqual(format_eta(None), "unknown")
        self.assertEqual(format_eta(12), "12s")
        self.assertEqual(format_eta(75), "1m 15s")
        self.assertEqual(format_eta(3725), "1h 02m")
        self.assertEqual(format_percent(0, 100), "0.0%")
        self.assertEqual(format_percent(50, 100), "50.0%")
        self.assertEqual(format_percent(100, 0), "0%")

    def test_peer_address_ipv6(self):
        self.assertEqual(peer_address(("1.2.3.4", 6881)), "1.2.3.4:6881")
        self.assertEqual(peer_address(("::1", 6881)), "[::1]:6881")


class SnapshotTests(unittest.TestCase):
    def test_build_snapshot_peer_states(self):
        metrics = SimpleNamespace(
            resumed_bytes=10, verified_bytes=40, uploaded_bytes=5,
            payload_received_bytes=50, _started=0)
        observations = {
            ("127.0.0.1", 1): SimpleNamespace(verified_bytes=40, seconds=2.0, failures=0),
            ("127.0.0.1", 2): SimpleNamespace(verified_bytes=0, seconds=0.5, failures=1),
        }
        sessions = {
            ("127.0.0.1", 1): SimpleNamespace(choked=False),
            ("127.0.0.1", 3): SimpleNamespace(choked=True),
        }
        active = {("127.0.0.1", 1): SimpleNamespace(received=100, last_progress=1.0)}
        retired = {("127.0.0.1", 2)}
        snap = build_snapshot(
            name="demo.bin", length=100, metrics=metrics,
            observations=observations, sessions=sessions, active=active,
            retired=retired)
        self.assertEqual(snap.name, "demo.bin")
        self.assertEqual(snap.done_bytes, 50)
        self.assertEqual(snap.percent, 50.0)
        by_addr = {p.address: p for p in snap.peers}
        self.assertEqual(by_addr["127.0.0.1:1"].state, "downloading")
        self.assertEqual(by_addr["127.0.0.1:2"].state, "failed")
        self.assertEqual(by_addr["127.0.0.1:3"].state, "choked")
        self.assertGreater(by_addr["127.0.0.1:1"].down_rate, 0)
        self.assertEqual(snap.peer_count, 2)

    def test_rate_tracker_eta(self):
        tracker = RateTracker()
        first = DownloadSnapshot(
            name="x", length=1000, done_bytes=100, verified_bytes=100,
            resumed_bytes=0, uploaded_bytes=0, payload_received_bytes=100,
            elapsed_seconds=1.0, peer_count=1)
        second = DownloadSnapshot(
            name="x", length=1000, done_bytes=300, verified_bytes=300,
            resumed_bytes=0, uploaded_bytes=50, payload_received_bytes=300,
            elapsed_seconds=2.0, peer_count=1)
        tracker.update(first)
        live = tracker.update(second)
        self.assertAlmostEqual(live.down_rate, 200.0)
        self.assertAlmostEqual(live.up_rate, 50.0)
        self.assertAlmostEqual(live.eta_seconds, 700 / 200.0)
        done = tracker.update(DownloadSnapshot(
            name="x", length=1000, done_bytes=1000, verified_bytes=1000,
            resumed_bytes=0, uploaded_bytes=50, payload_received_bytes=1000,
            elapsed_seconds=3.0, peer_count=0, status="complete"))
        self.assertEqual(done.eta_seconds, 0.0)


class GuiCliTests(unittest.TestCase):
    def _parse(self, argv):
        from cbtorrent.__main__ import build_parser, normalize_argv
        return build_parser().parse_args(normalize_argv(argv))

    def test_gui_help_without_display(self):
        from cbtorrent.__main__ import build_parser
        parser = build_parser()
        args = parser.parse_args([
            "gui", "sample.torrent", "--output", "out.bin",
            "--peer", "127.0.0.1:6881", "--no-trackers", "--policy", "adaptive",
        ])
        self.assertEqual(args.command, "gui")
        self.assertEqual(str(args.torrent), "sample.torrent")
        self.assertEqual(args.peer, [("127.0.0.1", 6881)])
        self.assertTrue(args.no_trackers)
        self.assertEqual(args.policy, "adaptive")
        empty = parser.parse_args(["gui"])
        self.assertEqual(empty.command, "gui")
        self.assertIsNone(empty.torrent)
        self.assertIsNone(empty.output)

    def test_empty_argv_defaults_to_gui(self):
        args = self._parse([])
        self.assertEqual(args.command, "gui")
        self.assertIsNone(args.torrent)

    def test_bare_torrent_opens_gui(self):
        args = self._parse(["foo.torrent"])
        self.assertEqual(args.command, "gui")
        self.assertEqual(str(args.torrent), "foo.torrent")

    def test_bare_torrent_honors_gui_flags(self):
        args = self._parse([
            "foo.torrent", "--output", "out", "--peer", "127.0.0.1:6881",
        ])
        self.assertEqual(args.command, "gui")
        self.assertEqual(str(args.torrent), "foo.torrent")
        self.assertEqual(str(args.output), "out")
        self.assertEqual(args.peer, [("127.0.0.1", 6881)])

    def test_explicit_download_still_download(self):
        args = self._parse([
            "download", "foo.torrent", "--output", "out.bin",
        ])
        self.assertEqual(args.command, "download")
        self.assertEqual(str(args.torrent), "foo.torrent")
        self.assertEqual(str(args.output), "out.bin")

    def test_explicit_gui_still_gui(self):
        args = self._parse(["gui"])
        self.assertEqual(args.command, "gui")
        self.assertIsNone(args.torrent)

    def test_gui_package_imports_without_tkinter(self):
        # Controller and observe must not require a display.
        from cbtorrent.gui.controller import DownloadController
        from cbtorrent import observe
        self.assertTrue(callable(DownloadController))
        self.assertTrue(hasattr(observe, "RateTracker"))


if __name__ == "__main__":
    unittest.main()
