"""Live observe hook during a real loopback download (no tkinter)."""
import asyncio
import tempfile
import unittest
from pathlib import Path

from cbtorrent.client import download
from cbtorrent.metainfo import create
from cbtorrent.observe import RateTracker
from cbtorrent.policy import ThroughputPolicy
from cbtorrent.seeder import FileSource, SeedServer


class LiveObserveTests(unittest.IsolatedAsyncioTestCase):
    async def test_observe_receives_progress_and_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "payload.bin"
            source.write_bytes(b"gui-observe-" * 4000)
            torrent = create(source, root / "payload.torrent", piece_length=16384)
            file_source = FileSource(torrent, source)
            server = SeedServer(torrent, file_source)
            try:
                port = await server.start("127.0.0.1", 0)
                seen = []
                rates = RateTracker()

                def observe(snapshot):
                    seen.append(rates.update(snapshot))

                report = await download(
                    torrent, [("127.0.0.1", port)], root / "out.bin",
                    policy=ThroughputPolicy(), use_trackers=False,
                    listen_host="127.0.0.1", timeout=5, piece_timeout=20,
                    observe=observe)
                self.assertTrue(report["complete"])
                self.assertGreaterEqual(len(seen), 2)
                self.assertEqual(seen[-1].status, "complete")
                self.assertEqual(seen[-1].done_bytes, torrent.length)
                self.assertTrue(any(s.peer_count >= 1 or s.peers for s in seen))
            finally:
                await server.close()
                file_source.close()
