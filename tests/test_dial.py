"""Dial-ahead: unreachable or silent addresses must not idle transfer slots."""
import asyncio
import tempfile
import unittest
from pathlib import Path

from cbtorrent.client import DIAL_AHEAD, DownloadError, download
from cbtorrent.metainfo import create
from cbtorrent.policy import ThroughputPolicy
from cbtorrent.seeder import FileSource, SeedServer
from cbtorrent.wire import PROTOCOL, message


class DialAheadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.data = bytes(range(256)) * 400
        (self.root / "source").write_bytes(self.data)
        self.torrent = create(self.root / "source", self.root / "a.torrent", piece_length=16384)
        self.accepted, self.closed = 0, 0
        self.all_accepted = asyncio.Event()

    async def good_peer(self):
        source = FileSource(self.torrent, self.root / "source")
        self.addCleanup(source.close)
        server = SeedServer(self.torrent, source)
        self.addAsyncCleanup(server.close)
        return "127.0.0.1", await server.start("127.0.0.1", 0)

    async def silent_peers(self, count, expected=None):
        """Accept TCP, read our handshake, and never answer (a dead-end address)."""
        expected = count if expected is None else expected
        async def serve(reader, writer):
            self.accepted += 1
            if self.accepted == expected:
                self.all_accepted.set()
            try:
                await reader.read()  # returns at EOF once we close the socket
            finally:
                self.closed += 1
                writer.close()
        addresses = []
        for _ in range(count):
            server = await asyncio.start_server(serve, "127.0.0.1", 0)
            self.addAsyncCleanup(server.wait_closed)
            self.addCleanup(server.close)
            addresses.append(("127.0.0.1", server.sockets[0].getsockname()[1]))
        return addresses

    async def test_silent_addresses_do_not_hold_the_only_transfer_slot(self):
        silent = await self.silent_peers(6)
        good = await self.good_peer()
        policy = ThroughputPolicy()  # explores peers in list order
        timeout = 3.0
        report = await asyncio.wait_for(download(
            self.torrent, silent + [good], self.root / "out", use_trackers=False,
            concurrency=1, timeout=timeout, piece_timeout=5.0, policy=policy), 30)
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        # Without dial-ahead the single slot spends one timeout on each silent
        # peer first, and each of those ends as a peer failure. Check that
        # structurally, not with a tight wall-clock bound: the test loop runs in
        # asyncio debug mode, which is several times slower on Windows.
        self.assertEqual(report["peer_failures"], 0)
        self.assertLess(report["completion_seconds"], timeout)
        self.assertEqual(report["peer_observations"][f"{good[0]}:{good[1]}"]["verified_bytes"],
                         len(self.data))
        for host, port in silent:
            self.assertEqual(report["peer_observations"].get(f"{host}:{port}", {}).get("verified_bytes", 0), 0)

    async def choking_peers(self, count):
        """Complete the handshake and bitfield, then keep us choked with keep-alives."""
        async def serve(reader, writer):
            try:
                await reader.readexactly(68)
                count_ = len(self.torrent.hashes)
                bits = bytearray([0xff] * ((count_ + 7) // 8))
                if count_ % 8:
                    bits[-1] &= 0xff << (8 - count_ % 8) & 0xff
                writer.write(PROTOCOL + bytes(8) + self.torrent.info_hash
                             + b"-XX0000-" + b"c" * 12 + message(5, bytes(bits)))
                while True:
                    writer.write(bytes(4))
                    await writer.drain()
                    await asyncio.sleep(0.05)
            except (OSError, asyncio.IncompleteReadError):
                pass
            finally:
                writer.close()
        addresses = []
        for _ in range(count):
            server = await asyncio.start_server(serve, "127.0.0.1", 0)
            self.addAsyncCleanup(server.wait_closed)
            self.addCleanup(server.close)
            addresses.append(("127.0.0.1", server.sockets[0].getsockname()[1]))
        return addresses

    async def test_choking_peers_do_not_use_up_dial_slots(self):
        """Connected peers waiting for an unchoke are not half-open attempts."""
        choking = await self.choking_peers(DIAL_AHEAD)
        good = await self.good_peer()
        piece_timeout = 5.0
        report = await asyncio.wait_for(download(
            self.torrent, choking + [good], self.root / "out", use_trackers=False,
            concurrency=1, timeout=1.0, piece_timeout=piece_timeout), 30)
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        # If waiting-for-unchoke peers held dial slots, the good peer would be
        # dialed only after a choking dial hit piece_timeout and failed.
        self.assertEqual(report["peer_failures"], 0)
        self.assertLess(report["completion_seconds"], piece_timeout)

    async def test_failed_dials_are_reported_and_end_the_download(self):
        silent = await self.silent_peers(3)
        with self.assertRaises(DownloadError) as caught:
            await asyncio.wait_for(download(
                self.torrent, silent, self.root / "out", use_trackers=False,
                timeout=0.2, piece_timeout=5.0, peer_retries=0), 5)
        report = caught.exception.report
        self.assertEqual(report["connections"], 3)
        self.assertEqual(report["peer_failure_phases"], {"handshake": 3})
        for host, port in silent:
            observation = report["peer_observations"][f"{host}:{port}"]
            self.assertEqual((observation["failures"], observation["samples"]), (1, 1))

    async def test_dials_are_bounded_and_cancellation_closes_them(self):
        silent = await self.silent_peers(DIAL_AHEAD + 4, expected=DIAL_AHEAD)
        task = asyncio.create_task(download(
            self.torrent, silent, self.root / "out", use_trackers=False,
            timeout=30, piece_timeout=60))
        await asyncio.wait_for(self.all_accepted.wait(), 2)
        await asyncio.sleep(0.1)
        self.assertEqual(self.accepted, DIAL_AHEAD)  # extras wait for a dial slot
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        for _ in range(100):
            if self.closed == DIAL_AHEAD:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(self.closed, DIAL_AHEAD)
        self.assertTrue((self.root / "out.part").exists())  # partial work is kept


if __name__ == "__main__":
    unittest.main()
