"""Independent loopback peers for recovery, endgame, cancellation and bounds."""
import asyncio
import hashlib
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cbtorrent.bencode import encode
from cbtorrent.client import DownloadError, download
from cbtorrent.metainfo import Torrent
from cbtorrent.policy import AdaptivePolicy
from cbtorrent.metrics import Metrics
from cbtorrent.storage import Storage
from cbtorrent.wire import Peer, PieceBuffer


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.data = bytes(range(251)) * 262  # Five blocks, final one short.
        info = {b"name": b"fixture", b"length": len(self.data), b"piece length": 131072,
                b"pieces": hashlib.sha1(self.data).digest()}
        self.torrent = Torrent.from_bytes(encode({b"info": info}))
        self.servers, self.handlers = [], set()
        self.connections = self.live = self.max_live = 0
        self.requests, self.cancels = [], []
        self.requested, self.helper_started = asyncio.Event(), asyncio.Event()

    async def asyncTearDown(self):
        for server in self.servers:
            server.close()
            await server.wait_closed()
        tasks = tuple(self.handlers)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.temp.cleanup()

    async def seed(self, *, fail_first=None, always_fail=None, hold_last=False,
                   corrupt_prefix=False, wait_helper=False, no_piece=False,
                   hold_all=False, duplicate_reply=False):
        attempts = 0
        async def handle(reader, writer):
            nonlocal attempts
            task = asyncio.current_task()
            self.handlers.add(task)
            attempts += 1
            attempt = attempts
            self.connections += 1
            self.live += 1
            self.max_live = max(self.max_live, self.live)
            try:
                request = await reader.readexactly(68)
                writer.write(request[:20] + bytes(8) + self.torrent.info_hash + b"s" * 20)
                writer.write(struct.pack("!IBB", 2, 5, 0 if no_piece else 128))
                writer.write(struct.pack("!IB", 1, 1))
                await writer.drain()
                fault = always_fail or (fail_first if attempt == 1 else None)
                while True:
                    size = struct.unpack("!I", await reader.readexactly(4))[0]
                    if not size:
                        continue
                    packet = await reader.readexactly(size)
                    if packet[0] == 8:
                        self.cancels.append(struct.unpack("!III", packet[1:]))
                    if packet[0] != 6:
                        continue
                    index, offset, length = struct.unpack("!III", packet[1:])
                    self.requests.append((writer.get_extra_info("sockname")[1], attempt, index, offset, length))
                    self.requested.set()
                    if fault == "disconnect":
                        return
                    if fault == "choke":
                        writer.write(struct.pack("!IB", 1, 0))
                        await writer.drain()
                        return
                    if fault == "malformed":
                        writer.write(struct.pack("!IB", 2, 0) + b"x")
                        await writer.drain()
                        # Keep the socket open until the client rejects the
                        # frame. Closing with unread pipelined requests can
                        # reset TCP before Python 3.11 exposes the bad bytes.
                        await reader.read()
                        return
                    if hold_all or (hold_last and offset + length == len(self.data)):
                        continue
                    if wait_helper:
                        self.helper_started.set()
                    block = self.data[offset:offset + length]
                    if corrupt_prefix and offset == 0:
                        block = bytes([block[0] ^ 255]) + block[1:]
                    response = struct.pack("!IBII", len(block) + 9, 7, index, offset) + block
                    writer.write(response)
                    if duplicate_reply:
                        writer.write(response)
                    await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                self.live -= 1
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
                self.handlers.discard(task)
        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        self.servers.append(server)
        return "127.0.0.1", server.sockets[0].getsockname()[1]

    async def get(self, peers, **kwargs):
        defaults = dict(use_trackers=False, timeout=0.4, piece_timeout=2,
                        retry_delay=0.01, endgame_delay=0.03, concurrency=1,
                        max_connections=2)
        defaults.update(kwargs)
        return await asyncio.wait_for(download(self.torrent, peers, self.root / "out", **defaults), 5)

    async def test_disconnect_and_choke_recover_without_manual_resume(self):
        for fault in ("disconnect", "choke"):
            with self.subTest(fault=fault):
                peer = await self.seed(fail_first=fault)
                report = await self.get([peer])
                self.assertEqual((self.root / "out").read_bytes(), self.data)
                self.assertEqual(report["peer_retries"], 1)
                self.assertEqual(report["peer_failures"], 1)
                self.assertEqual(report["connections"], 2)
                self.assertEqual(report["wasted_payload_bytes"], 0)
                (self.root / "out").unlink()

    async def test_permanent_protocol_failure_is_not_retried(self):
        peer = await self.seed(always_fail="malformed")
        with self.assertRaises(DownloadError) as caught:
            await self.get([peer])
        self.assertEqual(caught.exception.report["peer_retries"], 0)
        self.assertEqual(caught.exception.report["peers_banned"], 1)
        self.assertEqual(self.connections, 1)

    async def test_repeated_disconnects_have_finite_budget(self):
        peer = await self.seed(always_fail="disconnect")
        with self.assertRaises(DownloadError) as caught:
            await self.get([peer], peer_retries=2)
        report = caught.exception.report
        self.assertEqual(report["connections"], 3)
        self.assertEqual(report["peer_retries"], 2)
        self.assertEqual(report["peer_failures"], 3)
        self.assertTrue((self.root / "out.part").exists())
        self.assertFalse((self.root / "out").exists())

    async def test_zero_retry_budget_preserves_fail_fast_option(self):
        peer = await self.seed(always_fail="disconnect")
        with self.assertRaises(DownloadError) as caught:
            await self.get([peer], peer_retries=0)
        self.assertEqual(caught.exception.report["connections"], 1)

    async def test_endgame_fetches_only_missing_block_and_sends_cancel(self):
        slow = await self.seed(hold_last=True)
        fast = await self.seed()
        policy = AdaptivePolicy()
        report = await self.get([slow, fast], timeout=1, policy=policy)
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        fast_requests = [r for r in self.requests if r[0] == fast[1]]
        self.assertEqual(len(fast_requests), 1)
        self.assertEqual(fast_requests[0][3], 65536)
        self.assertEqual(report["endgame_requested_bytes"], len(self.data) - 65536)
        self.assertEqual(report["endgame_transfers"], 1)
        self.assertEqual(report["peer_failures"], 0)
        self.assertEqual(report["verified_bytes"], len(self.data))
        self.assertEqual(report["payload_received_bytes"], len(self.data))
        self.assertEqual(report["endgame_verified_bytes"], len(self.data))
        self.assertEqual(policy.models, {})
        self.assertTrue(all(o["samples"] == 0 for o in report["peer_observations"].values()))
        await asyncio.sleep(0.01)
        self.assertIn((0, 65536, len(self.data) - 65536), self.cancels)
        self.assertLessEqual(self.max_live, 2)

    async def test_endgame_never_exceeds_helper_byte_budget(self):
        slow = await self.seed(hold_all=True)
        fast = await self.seed()
        report = await self.get([slow, fast], endgame_budget=16384, timeout=0.1)
        self.assertEqual(report["endgame_transfers"], 0)
        self.assertEqual(report["endgame_requested_bytes"], 0)
        self.assertEqual((self.root / "out").read_bytes(), self.data)

    async def test_one_connection_cap_cannot_be_bypassed_by_endgame(self):
        slow = await self.seed(hold_last=True)
        fast = await self.seed()
        report = await self.get([slow, fast], max_connections=1, timeout=0.1)
        self.assertEqual(report["endgame_transfers"], 0)
        self.assertLessEqual(self.max_live, 1)
        self.assertEqual((self.root / "out").read_bytes(), self.data)

    async def test_mixed_corruption_retries_piece_in_isolation_without_ml_training(self):
        bad = await self.seed(hold_last=True, corrupt_prefix=True)
        good = await self.seed()
        policy = AdaptivePolicy()
        report = await self.get([bad, good], policy=policy)
        self.assertEqual((self.root / "out").read_bytes(), self.data)
        self.assertGreaterEqual(report["hash_failures"], 1)
        self.assertEqual(report["endgame_transfers"], 1)
        self.assertNotIn(bad, policy.models)
        self.assertEqual(sum(m.samples for m in policy.models.values()), 1)
        self.assertEqual(report["verified_bytes"], len(self.data))
        self.assertGreater(report["wasted_payload_bytes"], 0)

    async def test_cancel_during_backoff_closes_sessions_and_preserves_partial(self):
        peer = await self.seed(always_fail="disconnect")
        task = asyncio.create_task(self.get([peer], retry_delay=2))
        await asyncio.wait_for(self.requested.wait(), 2)
        await asyncio.sleep(0.02)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.01)
        self.assertEqual(self.live, 0)
        self.assertTrue((self.root / "out.part").exists())
        self.assertFalse((self.root / "out").exists())

    async def test_cancel_with_two_endgame_owners_drains_both(self):
        slow = await self.seed(hold_last=True)
        other = await self.seed(hold_all=True)
        task = asyncio.create_task(self.get([slow, other], timeout=1))
        for _ in range(200):
            if any(r[0] == other[1] for r in self.requests):
                break
            await asyncio.sleep(0.005)
        else:
            self.fail("endgame helper never requested its block")
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.01)
        self.assertEqual(self.live, 0)
        self.assertFalse((self.root / "out").exists())
        self.assertTrue((self.root / "out.part").exists())

    async def test_unsolicited_duplicate_block_is_a_permanent_failure(self):
        bad = await self.seed(duplicate_reply=True)
        good = await self.seed()
        report = await self.get([bad, good])
        self.assertEqual(report["peers_banned"], 1)
        self.assertEqual((self.root / "out").read_bytes(), self.data)

    async def test_raced_piece_commits_once_and_disk_failure_does_not_blame_peers(self):
        slow, fast = await self.seed(hold_last=True), await self.seed()
        original = Storage.write
        calls = []
        def write(storage, index, data):
            calls.append(index)
            return original(storage, index, data)
        with patch.object(Storage, "write", write):
            await self.get([slow, fast])
        self.assertEqual(calls, [0])
        (self.root / "out").unlink()
        with patch.object(Storage, "write", side_effect=OSError("disk full")):
            with self.assertRaises(DownloadError) as caught:
                await self.get([slow, fast])
        self.assertEqual(caught.exception.report["peer_failures"], 0)
        self.assertEqual(caught.exception.report["verified_bytes"], 0)
        await asyncio.sleep(0.01)
        self.assertEqual(self.live, 0)

    async def test_legal_late_duplicate_after_cancel_is_counted_without_replacing_data(self):
        address = await self.seed()
        reader, writer = await asyncio.open_connection(*address)
        metrics = Metrics()
        peer = Peer(reader, writer, self.torrent, metrics, 1)
        buffer = PieceBuffer(len(self.data))
        buffer.raced = True
        buffer.accept(0, self.data[:16384])
        original_send = peer.send
        injected = False
        async def send(data):
            nonlocal injected
            await original_send(data)
            if data[4] == 6 and not injected:
                injected = True
                # Another owner fills this already-requested block, then a
                # response arrives despite our cancel. Pending tuple validates it.
                buffer.accept(16384, self.data[16384:32768])
                peer.cancel_block(0, 16384)
        try:
            await peer.handshake(b"t" * 20)
            await peer.ready()
            peer.send = send
            data = await peer.download_piece(0, 4, buffer=buffer)
            self.assertEqual(data, self.data)
            self.assertEqual(metrics.endgame_duplicate_bytes, 16384)
            self.assertEqual(metrics.cancel_requests, 1)
        finally:
            await peer.close()

    async def test_new_limits_reject_invalid_values(self):
        for options in ({"peer_retries": 9}, {"retry_delay": float("nan")},
                        {"endgame_delay": 0}, {"endgame_budget": 1048577}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                await self.get([], **options)
